use super::*;

pub(super) struct Base64DecodeReorg {
    pending_encoded: Vec<u8>,
    pending_len: usize,
    decoded: Vec<u8>,
    saw_padding: bool,
    ignore_garbage: bool,
}

impl Base64DecodeReorg {
    pub(super) fn new(compacted_capacity: usize, ignore_garbage: bool) -> Self {
        let compacted_capacity = compacted_capacity.max(4) + 4;
        Self {
            pending_encoded: vec![0u8; compacted_capacity],
            pending_len: 0,
            decoded: vec![0u8; (compacted_capacity / 4) * 3],
            saw_padding: false,
            ignore_garbage,
        }
    }

    fn flush_ready<W: Write>(&mut self, out: &mut W, flush_all: bool) -> io::Result<()> {
        let flush_len = if flush_all {
            self.pending_len
        } else {
            self.pending_len & !3
        };
        if flush_len == 0 {
            return Ok(());
        }
        let written = decode_base64_block_into_with_kernel(
            &self.pending_encoded[..flush_len],
            &mut self.decoded[..(flush_len / 4) * 3],
            Base64DecodeKernel::Auto,
        )?;
        out.write_all(&self.decoded[..written])?;
        let remainder = self.pending_len - flush_len;
        if remainder != 0 {
            self.pending_encoded
                .copy_within(flush_len..self.pending_len, 0);
        }
        self.pending_len = remainder;
        Ok(())
    }

    fn compact_from_raw_scalar(
        dst: &mut [u8],
        mut write: usize,
        raw: &[u8],
        ignore_garbage: bool,
        saw_padding: &mut bool,
    ) -> io::Result<usize> {
        let mut read = 0usize;
        while read < raw.len() {
            let byte = raw[read];
            if byte == b'=' {
                *saw_padding = true;
                dst[write] = byte;
                write += 1;
                read += 1;
                continue;
            }
            if base64_decode_value(byte).is_some() {
                if *saw_padding {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "padding must terminate base64 stream",
                    ));
                }
                let run_start = read;
                read += 1;
                while read < raw.len() && base64_decode_value(raw[read]).is_some() {
                    read += 1;
                }
                if write != run_start {
                    dst[write..write + (read - run_start)].copy_from_slice(&raw[run_start..read]);
                }
                write += read - run_start;
                continue;
            }
            if base64_is_ignored_decode_byte(byte) || ignore_garbage {
                read += 1;
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid base64 quartet",
            ));
        }
        Ok(write)
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn compact_from_raw_avx2(
        dst: &mut [u8],
        mut write: usize,
        raw: &[u8],
        saw_padding: &mut bool,
    ) -> io::Result<(usize, usize)> {
        #[cfg(target_arch = "x86")]
        use std::arch::x86::{
            _mm256_and_si256, _mm256_cmpgt_epi8, _mm256_cmpeq_epi8, _mm256_loadu_si256,
            _mm256_movemask_epi8, _mm256_or_si256, _mm256_set1_epi8,
        };
        #[cfg(target_arch = "x86_64")]
        use std::arch::x86_64::{
            _mm256_and_si256, _mm256_cmpgt_epi8, _mm256_cmpeq_epi8, _mm256_loadu_si256,
            _mm256_movemask_epi8, _mm256_or_si256, _mm256_set1_epi8,
        };

        if *saw_padding {
            return Ok((write, 0));
        }

        let mut read = 0usize;
        while read + 32 <= raw.len() {
            let chunk = _mm256_loadu_si256(raw.as_ptr().add(read) as *const _);

            let ge_upper_a = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'A' as i8) - 1));
            let le_upper_z = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'Z' as i8) + 1), chunk);
            let is_upper = _mm256_and_si256(ge_upper_a, le_upper_z);

            let ge_lower_a = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'a' as i8) - 1));
            let le_lower_z = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'z' as i8) + 1), chunk);
            let is_lower = _mm256_and_si256(ge_lower_a, le_lower_z);

            let ge_digit_0 = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'0' as i8) - 1));
            let le_digit_9 = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'9' as i8) + 1), chunk);
            let is_digit = _mm256_and_si256(ge_digit_0, le_digit_9);

            let is_plus = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'+' as i8));
            let is_slash = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'/' as i8));
            let is_pad = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'=' as i8));
            let is_lf = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\n' as i8));
            let is_cr = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\r' as i8));

            let valid = _mm256_or_si256(
                _mm256_or_si256(is_upper, is_lower),
                _mm256_or_si256(is_digit, _mm256_or_si256(is_plus, is_slash)),
            );
            let ignored = _mm256_or_si256(is_lf, is_cr);
            let acceptable = _mm256_or_si256(valid, _mm256_or_si256(is_pad, ignored));
            let acceptable_mask = _mm256_movemask_epi8(acceptable) as u32;
            if acceptable_mask != u32::MAX {
                break;
            }
            if (_mm256_movemask_epi8(is_pad) as u32) != 0 {
                break;
            }

            let valid_mask = _mm256_movemask_epi8(valid) as u32;
            if valid_mask == u32::MAX {
                dst[write..write + 32].copy_from_slice(&raw[read..read + 32]);
                write += 32;
            } else if valid_mask != 0 {
                let mut bits = valid_mask;
                while bits != 0 {
                    let start = bits.trailing_zeros() as usize;
                    let shifted = bits >> start;
                    let len = shifted.trailing_ones() as usize;
                    let mask = ((1u32 << len) - 1) << start;
                    dst[write..write + len]
                        .copy_from_slice(&raw[read + start..read + start + len]);
                    write += len;
                    bits &= !mask;
                }
            }
            read += 32;
        }
        Ok((write, read))
    }

    fn compact_from_raw(&mut self, raw: &[u8]) -> io::Result<()> {
        let mut write = self.pending_len;
        let mut consumed = 0usize;
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if !self.ignore_garbage && raw.len() >= 32 && std::arch::is_x86_feature_detected!("avx2") {
            let (next_write, next_consumed) = unsafe {
                Self::compact_from_raw_avx2(
                    &mut self.pending_encoded,
                    write,
                    raw,
                    &mut self.saw_padding,
                )?
            };
            write = next_write;
            consumed = next_consumed;
        }
        write = Self::compact_from_raw_scalar(
            &mut self.pending_encoded,
            write,
            &raw[consumed..],
            self.ignore_garbage,
            &mut self.saw_padding,
        )?;
        self.pending_len = write;
        Ok(())
    }

    pub(super) fn consume_block<W: Write>(&mut self, out: &mut W, raw: &[u8]) -> io::Result<()> {
        let mut offset = 0usize;
        while offset < raw.len() {
            if self.pending_len >= self.pending_encoded.len() - 4 {
                self.flush_ready(out, false)?;
            }
            let available = self.pending_encoded.len() - self.pending_len;
            let take = available.min(raw.len() - offset);
            self.compact_from_raw(&raw[offset..offset + take])?;
            offset += take;
            if self.saw_padding && (self.pending_len & 3) == 0 {
                self.flush_ready(out, true)?;
            }
        }
        if self.pending_len >= self.pending_encoded.len() - 4 {
            self.flush_ready(out, false)?;
        }
        Ok(())
    }

    pub(super) fn finish<W: Write>(&mut self, out: &mut W) -> io::Result<()> {
        if self.pending_len % 4 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid base64 quartet",
            ));
        }
        self.flush_ready(out, true)
    }
}
