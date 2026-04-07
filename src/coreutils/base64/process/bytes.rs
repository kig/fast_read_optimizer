use super::*;

pub(in super::super) fn encode_base64_bytes_via_wrapped_path(
    bytes: &[u8],
    wrap_cols: usize,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(wrapped_base64_output_capacity(
        encoded_base64_len(bytes.len()),
        wrap_cols,
        0,
    ));
    let mut stdin_reader = io::Cursor::new(bytes);

    if wrap_cols != 0 && wrap_cols % 4 == 0 {
        let line_input_bytes = (wrap_cols / 4) * 3;
        let block_bound = 32 * 1024;
        let full_line_input_bound = (block_bound / line_input_bytes) * line_input_bytes;
        let mut carry = vec![0u8; line_input_bytes];
        let mut carry_len = 0usize;
        let mut line_buf = vec![0u8; wrap_cols + 1];
        let mut wrapped_slab =
            vec![0u8; (full_line_input_bound / line_input_bytes) * (wrap_cols + 1)];
        let mut block = vec![0u8; 32 * 1024];
        loop {
            let read = stdin_reader.read(&mut block)?;
            if read == 0 {
                break;
            }
            let slice = &block[..read];
            let mut start = 0usize;

            if carry_len != 0 {
                let needed = line_input_bytes - carry_len;
                let take = needed.min(slice.len());
                carry[carry_len..carry_len + take].copy_from_slice(&slice[..take]);
                carry_len += take;
                start = take;
                if carry_len == line_input_bytes {
                    let written = encode_base64_block_into(
                        &carry[..line_input_bytes],
                        &mut line_buf[..wrap_cols],
                    );
                    debug_assert_eq!(written, wrap_cols);
                    line_buf[wrap_cols] = b'\n';
                    out.extend_from_slice(&line_buf);
                    carry_len = 0;
                }
            }

            let remaining = &slice[start..];
            let full_line_input_len = (remaining.len() / line_input_bytes) * line_input_bytes;
            if full_line_input_len != 0 {
                let wrapped_len = encode_full_wrapped_lines_in_place(
                    &remaining[..full_line_input_len],
                    line_input_bytes,
                    wrap_cols,
                    &mut wrapped_slab,
                );
                out.extend_from_slice(&wrapped_slab[..wrapped_len]);
            }

            let leftover = &remaining[full_line_input_len..];
            if !leftover.is_empty() {
                carry[..leftover.len()].copy_from_slice(leftover);
                carry_len = leftover.len();
            }
        }

        if carry_len != 0 {
            let encoded_len = encoded_base64_len(carry_len);
            let written =
                encode_base64_block_into(&carry[..carry_len], &mut line_buf[..encoded_len]);
            line_buf[written] = b'\n';
            out.extend_from_slice(&line_buf[..written + 1]);
        }
        return Ok(out);
    }

    let mut current_line_len = 0usize;
    let max_process_len = ((32 * 1024 + 2) / 3) * 3;
    let max_encoded_len = encoded_base64_len(max_process_len);
    let mut carry = [0u8; 2];
    let mut carry_len = 0usize;
    let mut encoded_slab = vec![0u8; max_encoded_len.max(4)];
    let mut wrapped_slab = vec![
        0u8;
        wrapped_base64_output_capacity(
            max_encoded_len.max(4),
            wrap_cols,
            wrap_cols.saturating_sub(1)
        )
    ];
    let mut block = vec![0u8; 32 * 1024];
    loop {
        let read = stdin_reader.read(&mut block)?;
        if read == 0 {
            break;
        }
        let slice = &block[..read];
        let total_len = carry_len + slice.len();
        let process_len = (total_len / 3) * 3;
        if process_len == 0 {
            carry[..slice.len()].copy_from_slice(slice);
            carry_len = slice.len();
            continue;
        }
        let mut block_start = 0usize;
        if carry_len != 0 {
            let needed = 3 - carry_len;
            let mut merged = [0u8; 3];
            merged[..carry_len].copy_from_slice(&carry[..carry_len]);
            merged[carry_len..].copy_from_slice(&slice[..needed]);
            let written = encode_base64_block_into(&merged, &mut encoded_slab[..4]);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..written],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            out.extend_from_slice(&wrapped_slab[..wrapped_len]);
            block_start = needed;
        }
        let remaining_process_len = ((slice.len() - block_start) / 3) * 3;
        if remaining_process_len != 0 {
            let input_slice = &slice[block_start..block_start + remaining_process_len];
            let encoded_len = encoded_base64_len(input_slice.len());
            let written = encode_base64_block_into(input_slice, &mut encoded_slab[..encoded_len]);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..written],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            out.extend_from_slice(&wrapped_slab[..wrapped_len]);
            block_start += remaining_process_len;
        }
        let remainder = &slice[block_start..];
        if !remainder.is_empty() {
            carry[..remainder.len()].copy_from_slice(remainder);
            carry_len = remainder.len();
        } else {
            carry_len = 0;
        }
    }
    if carry_len != 0 {
        let encoded_len = encoded_base64_len(carry_len);
        let written =
            encode_base64_block_into(&carry[..carry_len], &mut encoded_slab[..encoded_len]);
        let wrapped_len = wrap_encoded_bytes_into(
            &encoded_slab[..written],
            wrap_cols,
            &mut current_line_len,
            &mut wrapped_slab,
        );
        out.extend_from_slice(&wrapped_slab[..wrapped_len]);
    }
    if wrap_cols != 0 && current_line_len != 0 {
        out.push(b'\n');
    }
    Ok(out)
}

#[cfg(test)]
pub(in super::super) fn encode_base64_bytes_via_wrapped_writer_for_test(
    bytes: &[u8],
    wrap_cols: usize,
    read_block_size: usize,
) -> io::Result<(Vec<u8>, usize)> {
    struct CountingWriter {
        bytes: Vec<u8>,
        writes: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut writer = CountingWriter {
        bytes: Vec::new(),
        writes: 0,
    };
    let mut cursor = io::Cursor::new(bytes);
    let mut block = vec![0u8; read_block_size.max(1)];

    if wrap_cols != 0 && wrap_cols % 4 == 0 {
        let line_input_bytes = (wrap_cols / 4) * 3;
        let full_line_input_bound = (block.len() / line_input_bytes) * line_input_bytes;
        let mut carry = vec![0u8; line_input_bytes];
        let mut carry_len = 0usize;
        let mut line_buf = vec![0u8; wrap_cols + 1];
        let mut wrapped_slab =
            vec![0u8; (full_line_input_bound / line_input_bytes) * (wrap_cols + 1)];
        let mut batched_out = OrderedWriteBatcher::with_capacity(
            &mut writer,
            wrapped_slab
                .len()
                .saturating_add(line_buf.len())
                .max(64 * 1024),
        );

        loop {
            let read = cursor.read(&mut block)?;
            if read == 0 {
                break;
            }
            let slice = &block[..read];
            let mut start = 0usize;

            if carry_len != 0 {
                let needed = line_input_bytes - carry_len;
                let take = needed.min(slice.len());
                carry[carry_len..carry_len + take].copy_from_slice(&slice[..take]);
                carry_len += take;
                start = take;
                if carry_len == line_input_bytes {
                    let written = encode_base64_block_into(
                        &carry[..line_input_bytes],
                        &mut line_buf[..wrap_cols],
                    );
                    debug_assert_eq!(written, wrap_cols);
                    line_buf[wrap_cols] = b'\n';
                    batched_out.write_all(&line_buf)?;
                    carry_len = 0;
                }
            }

            let remaining = &slice[start..];
            let full_line_input_len = (remaining.len() / line_input_bytes) * line_input_bytes;
            if full_line_input_len != 0 {
                let wrapped_len = encode_full_wrapped_lines_in_place(
                    &remaining[..full_line_input_len],
                    line_input_bytes,
                    wrap_cols,
                    &mut wrapped_slab,
                );
                batched_out.write_all(&wrapped_slab[..wrapped_len])?;
            }

            let leftover = &remaining[full_line_input_len..];
            if !leftover.is_empty() {
                carry[..leftover.len()].copy_from_slice(leftover);
                carry_len = leftover.len();
            }
        }

        if carry_len != 0 {
            let encoded_len = encoded_base64_len(carry_len);
            let written =
                encode_base64_block_into(&carry[..carry_len], &mut line_buf[..encoded_len]);
            line_buf[written] = b'\n';
            batched_out.write_all(&line_buf[..written + 1])?;
        }
        batched_out.flush()?;
        return Ok((writer.bytes, writer.writes));
    }

    let mut current_line_len = 0usize;
    let max_process_len = ((block.len() + 2) / 3) * 3;
    let max_encoded_len = encoded_base64_len(max_process_len);
    let mut carry = [0u8; 2];
    let mut carry_len = 0usize;
    let mut encoded_slab = vec![0u8; max_encoded_len.max(4)];
    let mut wrapped_slab = vec![
        0u8;
        wrapped_base64_output_capacity(
            max_encoded_len.max(4),
            wrap_cols,
            wrap_cols.saturating_sub(1)
        )
    ];
    let mut batched_out = OrderedWriteBatcher::with_capacity(
        &mut writer,
        wrapped_slab.len().saturating_add(8).max(64 * 1024),
    );

    loop {
        let read = cursor.read(&mut block)?;
        if read == 0 {
            break;
        }
        let slice = &block[..read];
        let total_len = carry_len + slice.len();
        let process_len = (total_len / 3) * 3;
        if process_len == 0 {
            carry[..slice.len()].copy_from_slice(slice);
            carry_len = slice.len();
            continue;
        }
        let mut block_start = 0usize;
        if carry_len != 0 {
            let needed = 3 - carry_len;
            let mut merged = [0u8; 3];
            merged[..carry_len].copy_from_slice(&carry[..carry_len]);
            merged[carry_len..].copy_from_slice(&slice[..needed]);
            let written = encode_base64_block_into(&merged, &mut encoded_slab[..4]);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..written],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            batched_out.write_all(&wrapped_slab[..wrapped_len])?;
            block_start = needed;
        }
        let remaining_process_len = ((slice.len() - block_start) / 3) * 3;
        if remaining_process_len != 0 {
            let input_slice = &slice[block_start..block_start + remaining_process_len];
            let encoded_len = encoded_base64_len(input_slice.len());
            let written = encode_base64_block_into(input_slice, &mut encoded_slab[..encoded_len]);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..written],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            batched_out.write_all(&wrapped_slab[..wrapped_len])?;
            block_start += remaining_process_len;
        }
        let remainder = &slice[block_start..];
        if !remainder.is_empty() {
            carry[..remainder.len()].copy_from_slice(remainder);
            carry_len = remainder.len();
        } else {
            carry_len = 0;
        }
    }
    if carry_len != 0 {
        let encoded_len = encoded_base64_len(carry_len);
        let written =
            encode_base64_block_into(&carry[..carry_len], &mut encoded_slab[..encoded_len]);
        let wrapped_len = wrap_encoded_bytes_into(
            &encoded_slab[..written],
            wrap_cols,
            &mut current_line_len,
            &mut wrapped_slab,
        );
        batched_out.write_all(&wrapped_slab[..wrapped_len])?;
    }
    if wrap_cols != 0 && current_line_len != 0 {
        batched_out.push_byte(b'\n')?;
    }
    batched_out.flush()?;
    Ok((writer.bytes, writer.writes))
}

pub(in super::super) fn decode_base64_bytes_via_reorg_path(
    bytes: &[u8],
    ignore_garbage: bool,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    let mut reorg = Base64DecodeReorg::new(32 * 1024, ignore_garbage);
    let mut cursor = io::Cursor::new(bytes);
    let mut block = vec![0u8; 32 * 1024];
    loop {
        let read = cursor.read(&mut block)?;
        if read == 0 {
            break;
        }
        reorg.consume_block(&mut out, &block[..read])?;
    }
    reorg.finish(&mut out)?;
    Ok(out)
}

pub(in super::super) fn decode_base64_bytes_with_detect_fallback(
    bytes: &[u8],
    ignore_garbage: bool,
    kernel: Base64DecodeKernel,
) -> io::Result<Vec<u8>> {
    if ignore_garbage {
        return decode_base64_bytes_via_reorg_path(bytes, true);
    }
    let mut out = vec![0u8; (bytes.len() / 4) * 3];
    match decode_base64_block_into_with_fused_fallback(bytes, &mut out, kernel)? {
        Base64DecodeFastOutcome::Decoded(written) => {
            out.truncate(written);
            Ok(out)
        }
        Base64DecodeFastOutcome::NeedsFallback => decode_base64_bytes_via_reorg_path(bytes, false),
    }
}

#[cfg(test)]
pub(in super::super) fn append_sanitized_base64_bytes(
    raw: &[u8],
    block_index: usize,
    pending_encoded: &mut Vec<u8>,
) -> io::Result<Option<usize>> {
    let mut first_pad = None;
    for &byte in raw {
        if base64_is_ignored_decode_byte(byte) {
            continue;
        }
        if byte == b'=' || base64_decode_value(byte).is_some() {
            if first_pad.is_none() && byte == b'=' {
                first_pad = Some(pending_encoded.len());
            }
            pending_encoded.push(byte);
            continue;
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("base64 block {block_index} contains unsupported characters"),
        ));
    }
    Ok(first_pad)
}
