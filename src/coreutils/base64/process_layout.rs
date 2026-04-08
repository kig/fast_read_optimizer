use super::*;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RegularDecodeLayout {
    Clean,
    WrappedLf76,
    Unknown,
}

fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

pub(super) fn line_aligned_block_size(target: u64, line_bytes: usize) -> u64 {
    let line_bytes = line_bytes as u64;
    let lcm = line_bytes
        .checked_mul(4096 / gcd_u64(4096, line_bytes))
        .unwrap_or(line_bytes);
    let aligned = if target >= lcm {
        (target / lcm) * lcm
    } else {
        0
    };
    if aligned != 0 {
        return aligned;
    }
    let fallback = (target / line_bytes) * line_bytes;
    if fallback != 0 {
        fallback
    } else {
        line_bytes
    }
}

pub(super) fn wrapped_encode_block_sizes(wrap_cols: usize, target_read: u64) -> (u64, usize) {
    let line_input_bytes = (wrap_cols / 4) * 3;
    let read_block_size = line_aligned_block_size(target_read, line_input_bytes);
    let line_count = (read_block_size as usize) / line_input_bytes;
    (read_block_size, line_count * (wrap_cols + 1))
}

pub(super) fn wrapped_decode_block_sizes(target_read: u64) -> (u64, usize) {
    const WRAPPED_LINE_BYTES: usize = 77;
    const WRAPPED_DECODED_LINE_BYTES: usize = 57;
    let read_block_size = line_aligned_block_size(target_read, WRAPPED_LINE_BYTES);
    let line_count = (read_block_size as usize) / WRAPPED_LINE_BYTES;
    (read_block_size, line_count * WRAPPED_DECODED_LINE_BYTES)
}

pub(super) fn regular_input_path(input: &StreamInput) -> io::Result<Option<String>> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => Ok(Some(path.clone())),
        StreamInput::Stdin { .. } => {
            let stdin_fd = io::stdin().as_raw_fd();
            if !is_regular_fd(stdin_fd) {
                return Ok(None);
            }
            let read_path_buf = fs::read_link(format!("/proc/self/fd/{stdin_fd}"))?;
            let read_path = read_path_buf.to_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "stdin path is not valid UTF-8")
            })?;
            Ok(Some(read_path.to_string()))
        }
        _ => Ok(None),
    }
}

pub(super) fn detect_regular_decode_layout(path: &str) -> io::Result<RegularDecodeLayout> {
    const FAST_PATH_COMPAT_PROBE_BYTES: u64 = 256 * 1024;
    let mut file = File::open(path)?;
    let mut buf = [0u8; 64 * 1024];
    let mut remaining = FAST_PATH_COMPAT_PROBE_BYTES;
    let mut saw_lf = false;
    let mut saw_non_lf = false;
    let mut line_len = 0usize;
    loop {
        if remaining == 0 {
            break;
        }
        let to_read = remaining.min(buf.len() as u64) as usize;
        let read = file.read(&mut buf[..to_read])?;
        if read == 0 {
            break;
        }
        remaining -= read as u64;
        for &byte in &buf[..read] {
            match byte {
                b'\n' => {
                    saw_lf = true;
                    if line_len != 76 {
                        return Ok(RegularDecodeLayout::Unknown);
                    }
                    line_len = 0;
                }
                b'\r' => return Ok(RegularDecodeLayout::Unknown),
                b'=' => {
                    saw_non_lf = true;
                    line_len += 1;
                }
                _ if base64_decode_value(byte).is_some() => {
                    saw_non_lf = true;
                    line_len += 1;
                }
                _ => return Ok(RegularDecodeLayout::Unknown),
            }
            if saw_lf && line_len > 76 {
                return Ok(RegularDecodeLayout::Unknown);
            }
        }
    }
    if saw_lf {
        if line_len != 0 && line_len != 76 && line_len > 76 {
            return Ok(RegularDecodeLayout::Unknown);
        }
        Ok(RegularDecodeLayout::WrappedLf76)
    } else if saw_non_lf {
        Ok(RegularDecodeLayout::Clean)
    } else {
        Ok(RegularDecodeLayout::Unknown)
    }
}

pub(super) struct SliceWriter<'a> {
    pub(super) out: &'a mut [u8],
    pub(super) written: usize,
}

impl<'a> Write for SliceWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let end = self
            .written
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::other("slice writer overflow"))?;
        if end > self.out.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "wrapped base64 output buffer too small",
            ));
        }
        self.out[self.written..end].copy_from_slice(buf);
        self.written = end;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn decode_bytes_are_fast_path_compatible(bytes: &[u8]) -> bool {
    !bytes
        .iter()
        .any(|&byte| byte != b'=' && base64_decode_value(byte).is_none())
}

pub(super) fn read_decode_fast_path_probe(reader: &mut File) -> io::Result<(Vec<u8>, bool)> {
    const FAST_PATH_COMPAT_PROBE_BYTES: usize = 256 * 1024;
    let mut prefix = Vec::with_capacity(FAST_PATH_COMPAT_PROBE_BYTES);
    let mut buf = [0u8; 64 * 1024];
    while prefix.len() < FAST_PATH_COMPAT_PROBE_BYTES {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        prefix.extend_from_slice(&buf[..read]);
    }
    let compatible = decode_bytes_are_fast_path_compatible(&prefix);
    Ok((prefix, compatible))
}

pub(super) fn decode_input_can_use_fast_path(input: &StreamInput) -> io::Result<bool> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            Ok(detect_regular_decode_layout(path)? == RegularDecodeLayout::Clean)
        }
        StreamInput::Stdin { .. } => {
            let stdin_fd = io::stdin().as_raw_fd();
            if !is_regular_fd(stdin_fd) {
                return Ok(true);
            }
            let read_path_buf = fs::read_link(format!("/proc/self/fd/{stdin_fd}"))?;
            let read_path = read_path_buf.to_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "stdin path is not valid UTF-8")
            })?;
            Ok(detect_regular_decode_layout(read_path)? == RegularDecodeLayout::Clean)
        }
        _ => Ok(false),
    }
}

pub(super) fn decode_input_can_use_wrapped_fast_path(input: &StreamInput) -> io::Result<bool> {
    match regular_input_path(input)? {
        Some(path) => Ok(detect_regular_decode_layout(&path)? == RegularDecodeLayout::WrappedLf76),
        None => Ok(false),
    }
}

pub(super) fn encode_input_can_use_wrapped_fast_path(
    input: &StreamInput,
    wrap_cols: usize,
) -> io::Result<bool> {
    if wrap_cols == 0 || wrap_cols % 4 != 0 {
        return Ok(false);
    }
    Ok(regular_input_path(input)?.is_some())
}
