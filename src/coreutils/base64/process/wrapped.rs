use super::*;

#[cfg(test)]
pub(in super::super) fn append_wrapped_base64_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    wrap_cols: usize,
    current_line_len: &mut usize,
) {
    if wrap_cols == 0 {
        out.extend_from_slice(bytes);
        return;
    }

    let mut start = 0usize;
    while start < bytes.len() {
        let available = wrap_cols.saturating_sub(*current_line_len);
        let take = available.min(bytes.len() - start);
        out.extend_from_slice(&bytes[start..(start + take)]);
        *current_line_len += take;
        start += take;
        if *current_line_len == wrap_cols {
            out.push(b'\n');
            *current_line_len = 0;
        }
    }
}

pub(super) fn wrapped_base64_output_capacity(
    encoded_len: usize,
    wrap_cols: usize,
    current_line_len: usize,
) -> usize {
    if wrap_cols == 0 {
        encoded_len
    } else {
        encoded_len + ((current_line_len + encoded_len) / wrap_cols)
    }
}

pub(super) struct OrderedWriteBatcher<'a, W: Write> {
    out: &'a mut W,
    pending: Vec<u8>,
}

impl<'a, W: Write> OrderedWriteBatcher<'a, W> {
    pub(super) fn with_capacity(out: &'a mut W, capacity: usize) -> Self {
        Self {
            out,
            pending: Vec::with_capacity(capacity.max(1)),
        }
    }

    pub(super) fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if self.pending.is_empty() && bytes.len() >= self.pending.capacity() {
            return self.out.write_all(bytes);
        }
        if self.pending.len() + bytes.len() > self.pending.capacity() {
            self.flush()?;
            if bytes.len() >= self.pending.capacity() {
                return self.out.write_all(bytes);
            }
        }
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    pub(super) fn push_byte(&mut self, byte: u8) -> io::Result<()> {
        if self.pending.len() == self.pending.capacity() {
            self.flush()?;
        }
        self.pending.push(byte);
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.out.write_all(&self.pending)?;
        self.pending.clear();
        Ok(())
    }
}

pub(super) fn ordered_input_block_bound(input: &StreamInput, io_mode: IOMode) -> io::Result<usize> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let config = load_config(None);
            let page_cache = config.get_params_for_path("read", false, path);
            let direct = config.get_params_for_path("read", true, path);
            let block_size = match io_mode {
                IOMode::Direct => direct.block_size,
                IOMode::PageCache => page_cache.block_size,
                IOMode::Auto => page_cache.block_size.max(direct.block_size),
            };
            Ok(block_size as usize)
        }
        _ => Ok(1 << 20),
    }
}

pub(super) fn encode_wrapped_block_into(
    bytes: &[u8],
    wrap_cols: usize,
    out: &mut [u8],
) -> io::Result<usize> {
    let line_input_bytes = (wrap_cols / 4) * 3;
    let full_line_input_len = (bytes.len() / line_input_bytes) * line_input_bytes;
    let mut written = 0usize;
    if full_line_input_len != 0 {
        written += encode_full_wrapped_lines_in_place(
            &bytes[..full_line_input_len],
            line_input_bytes,
            wrap_cols,
            out,
        );
    }
    let leftover = &bytes[full_line_input_len..];
    if !leftover.is_empty() {
        let encoded_len = encoded_base64_len(leftover.len());
        let line_buf = &mut out[written..written + encoded_len + 1];
        let tail_written = encode_base64_block_into(leftover, &mut line_buf[..encoded_len]);
        line_buf[tail_written] = b'\n';
        written += tail_written + 1;
    }
    Ok(written)
}

pub(super) fn decode_wrapped_block_into(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    let mut writer = SliceWriter { out, written: 0 };
    let mut reorg = Base64DecodeReorg::new(bytes.len().max(32 * 1024), false);
    reorg.consume_block(&mut writer, bytes)?;
    reorg.finish(&mut writer)?;
    Ok(writer.written)
}

pub(super) fn wrap_encoded_bytes_into(
    encoded: &[u8],
    wrap_cols: usize,
    current_line_len: &mut usize,
    out: &mut [u8],
) -> usize {
    if wrap_cols == 0 {
        out[..encoded.len()].copy_from_slice(encoded);
        return encoded.len();
    }

    let mut src = 0usize;
    let mut dst = 0usize;
    let mut line_len = *current_line_len;
    while src < encoded.len() {
        let available = wrap_cols.saturating_sub(line_len);
        let take = available.min(encoded.len() - src);
        out[dst..dst + take].copy_from_slice(&encoded[src..src + take]);
        dst += take;
        src += take;
        line_len += take;
        if line_len == wrap_cols {
            out[dst] = b'\n';
            dst += 1;
            line_len = 0;
        }
    }
    *current_line_len = line_len;
    dst
}

pub(super) fn encode_full_wrapped_lines_in_place(
    bytes: &[u8],
    line_input_bytes: usize,
    wrap_cols: usize,
    out: &mut [u8],
) -> usize {
    debug_assert_eq!(bytes.len() % line_input_bytes, 0);
    let full_lines = bytes.len() / line_input_bytes;
    let encoded_len = full_lines * wrap_cols;
    let wrapped_len = full_lines * (wrap_cols + 1);
    let written = encode_base64_block_into(bytes, &mut out[..encoded_len]);
    debug_assert_eq!(written, encoded_len);
    for line_index in (0..full_lines).rev() {
        let src_start = line_index * wrap_cols;
        let dst_start = line_index * (wrap_cols + 1);
        if src_start != dst_start {
            out.copy_within(src_start..src_start + wrap_cols, dst_start);
        }
        out[dst_start + wrap_cols] = b'\n';
    }
    wrapped_len
}
