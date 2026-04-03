use super::*;
use super::process_layout::{decode_input_can_use_fast_path, decode_input_can_use_wrapped_fast_path,
    detect_regular_decode_layout, encode_input_can_use_wrapped_fast_path, read_decode_fast_path_probe,
    wrapped_decode_block_sizes, wrapped_encode_block_sizes, RegularDecodeLayout, SliceWriter};
use crate::stream::allocate_pipe_output_buffer;
use crate::stream::transform::{
    increase_pipe_capacity, process_reader_to_file_fast, process_reader_to_pipe_fast,
};
mod decode_reorg;
use decode_reorg::Base64DecodeReorg;

fn process_pipe_to_pipe(
    dest: &mut File,
    input: &StreamInput,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if options.decode {
        let mut reader = open_stream_input_file(input)?;
        let (prefix, compatible) = read_decode_fast_path_probe(&mut reader)?;
        let mut replay = io::Cursor::new(prefix).chain(reader);
        if compatible {
            process_reader_to_pipe_fast(
                plan.process_chunk,
                dest,
                &mut replay,
                plan.pipe_input_read_block_size,
                plan.pipe_input_write_block_size,
                plan.input_chunk_multiple,
            )?;
            return Ok(false);
        }
        return decode_base64_from_reader(
            dest,
            &mut replay,
            plan.pipe_input_read_block_size as usize,
            options.ignore_garbage,
        );
    }
    process_pipe_input_to_pipe_fast(
        plan.process_chunk,
        dest,
        input,
        plan.pipe_input_read_block_size,
        plan.pipe_input_write_block_size,
        plan.input_chunk_multiple,
    )?;
    return Ok(false);
}

fn process_pipe_to_file(
    dest: &mut File,
    input: &StreamInput,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if options.decode {
        let mut reader = open_stream_input_file(input)?;
        let (prefix, compatible) = read_decode_fast_path_probe(&mut reader)?;
        let mut replay = io::Cursor::new(prefix).chain(reader);
        if compatible {
            process_reader_to_file_fast(
                &mut replay,
                dest,
                plan.pipe_file_read_block_size,
                plan.pipe_file_write_block_size,
                plan.input_chunk_multiple,
                plan.process_chunk,
            )?;
            return Ok(false);
        }
        return decode_base64_from_reader(
            dest,
            &mut replay,
            plan.pipe_file_read_block_size as usize,
            options.ignore_garbage,
        );
    }
    let mut reader = open_stream_input_file(input)?;
    let mut read_buf = vec![0u8; plan.pipe_file_read_block_size as usize];
    let mut write_buf = vec![0u8; plan.pipe_file_write_block_size];
    let mut carry = Vec::new();
    let mut merged =
        Vec::with_capacity(plan.pipe_file_read_block_size as usize + plan.input_chunk_multiple);
    loop {
        let read = reader.read(&mut read_buf)?;
        if read == 0 {
            break;
        }
        let ready_len = if plan.input_chunk_multiple <= 1 {
            read
        } else {
            let total = carry.len() + read;
            (total / plan.input_chunk_multiple) * plan.input_chunk_multiple
        };
        if ready_len == 0 {
            carry.extend_from_slice(&read_buf[..read]);
            continue;
        }
        let produced = if carry.is_empty() {
            let process_len = ready_len.min(read);
            let produced = (plan.process_chunk)(&read_buf[..process_len], &mut write_buf[..])?;
            if process_len < read {
                carry.extend_from_slice(&read_buf[process_len..read]);
            }
            produced
        } else {
            let take_from_read = ready_len - carry.len();
            merged.clear();
            merged.extend_from_slice(&carry);
            merged.extend_from_slice(&read_buf[..take_from_read]);
            carry.clear();
            if take_from_read < read {
                carry.extend_from_slice(&read_buf[take_from_read..read]);
            }
            (plan.process_chunk)(&merged, &mut write_buf[..])?
        };
        if produced != 0 {
            dest.write_all(&write_buf[..produced])?;
        }
    }
    if !carry.is_empty() {
        let produced = (plan.process_chunk)(&carry, &mut write_buf[..])?;
        if produced != 0 {
            dest.write_all(&write_buf[..produced])?;
        }
    }
    Ok(false)
}

fn open_stream_input_file(input: &StreamInput) -> io::Result<File> {
    match input {
        StreamInput::File(path) => File::open(path),
        StreamInput::Stdin { .. } => {
            let dupfd = unsafe { libc::dup(libc::STDIN_FILENO) };
            if dupfd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { File::from_raw_fd(dupfd) })
        }
    }
}

fn process_file_to_pipe(
    dest: &mut File,
    path: &str,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if !options.decode && options.wrap_cols != 0 && options.wrap_cols % 4 == 0 {
        let config = load_config(None);
        let wrap_cols = options.wrap_cols;
        let (read_block_size, write_block_size) =
            wrapped_encode_block_sizes(wrap_cols, BASE64_ENCODE_FILE_PIPE_READ_BLOCK_SIZE);
        increase_pipe_capacity(dest.as_raw_fd(), write_block_size);
        ParallelStream::map_file_to_pipe_with_owned_buffers(
            &config,
            path,
            dest,
            read_block_size,
            write_block_size,
            move |input: &[u8]| -> io::Result<Vec<u8>> {
                let mut out = allocate_pipe_output_buffer(write_block_size);
                let produced = encode_wrapped_block_into(input, wrap_cols, &mut out[..])?;
                out.truncate(produced);
                Ok(out)
            },
        )?;
        return Ok(false);
    }
    if options.decode && detect_regular_decode_layout(path)? == RegularDecodeLayout::WrappedLf76 {
        let config = load_config(None);
        let (read_block_size, write_block_size) =
            wrapped_decode_block_sizes(BASE64_DECODE_FILE_PIPE_READ_BLOCK_SIZE);
        increase_pipe_capacity(dest.as_raw_fd(), write_block_size);
        ParallelStream::map_file_to_pipe_with_owned_buffers(
            &config,
            path,
            dest,
            read_block_size,
            write_block_size,
            move |input: &[u8]| -> io::Result<Vec<u8>> {
                let mut out = allocate_pipe_output_buffer(write_block_size);
                let produced = decode_wrapped_block_into(input, &mut out[..])?;
                out.truncate(produced);
                Ok(out)
            },
        )?;
        return Ok(false);
    }
    let config = load_config(None);
    let decode = options.decode;
    let pipe_write_block_size = plan.file_pipe_write_block_size;
    let process_chunk = plan.process_chunk;
    increase_pipe_capacity(dest.as_raw_fd(), pipe_write_block_size);
    let _report = ParallelStream::map_file_to_pipe_with_owned_buffers(
        &config,
        path,
        dest,
        plan.file_pipe_read_block_size,
        pipe_write_block_size,
        move |input: &[u8]| -> io::Result<Vec<u8>> {
            if decode {
                let mut out = allocate_pipe_output_buffer(pipe_write_block_size);
                let produced = process_chunk(input, &mut out[..])?;
                out.truncate(produced);
                Ok(out)
            } else {
                Ok(encode_base64_block_aligned(input))
            }
        },
    )?;
    return Ok(false);
}

fn process_file_to_file(
    dest: &mut File,
    path: &str,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if !options.decode && options.wrap_cols != 0 && options.wrap_cols % 4 == 0 {
        let config = load_config(None);
        let wrap_cols = options.wrap_cols;
        let (read_block_size, write_block_size) =
            wrapped_encode_block_sizes(wrap_cols, BASE64_ENCODE_FAST_READ_BLOCK_SIZE);
        ParallelStream::map_file_fixed_size_to_file(
            &config,
            path,
            dest,
            read_block_size,
            write_block_size,
            move |input: &[u8], out: &mut [u8]| {
                encode_wrapped_block_into(input, wrap_cols, out)
            },
        )?;
        return Ok(false);
    }
    if options.decode && detect_regular_decode_layout(path)? == RegularDecodeLayout::WrappedLf76 {
        let config = load_config(None);
        let (read_block_size, write_block_size) =
            wrapped_decode_block_sizes(BASE64_DECODE_FAST_READ_BLOCK_SIZE);
        ParallelStream::map_file_fixed_size_to_file(
            &config,
            path,
            dest,
            read_block_size,
            write_block_size,
            move |input: &[u8], out: &mut [u8]| decode_wrapped_block_into(input, out),
        )?;
        return Ok(false);
    }
    let config = load_config(None);
    let _report = ParallelStream::map_file_fixed_size_to_file(
        &config,
        path,
        dest,
        plan.read_block_size,
        plan.write_block_size,
        plan.process_chunk,
    )?;
    return Ok(false);
}

#[cfg(test)]
pub(super) fn append_wrapped_base64_bytes(
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

fn wrapped_base64_output_capacity(
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

fn ordered_input_block_bound(input: &StreamInput, io_mode: IOMode) -> io::Result<usize> {
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


fn encode_wrapped_block_into(bytes: &[u8], wrap_cols: usize, out: &mut [u8]) -> io::Result<usize> {
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

fn decode_wrapped_block_into(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    let mut writer = SliceWriter { out, written: 0 };
    let mut reorg = Base64DecodeReorg::new(bytes.len().max(32 * 1024), false);
    reorg.consume_block(&mut writer, bytes)?;
    reorg.finish(&mut writer)?;
    Ok(writer.written)
}

fn wrap_encoded_bytes_into(
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

fn encode_full_wrapped_lines_in_place(
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

fn process_pipe_input_to_pipe_fast(
    processor: fn(&[u8], &mut [u8]) -> io::Result<usize>,
    dest: &mut File,
    input: &StreamInput,
    read_block_size: u64,
    write_block_size: usize,
    input_chunk_multiple: usize,
) -> io::Result<()> {
    let mut reader = open_stream_input_file(input)?;
    process_reader_to_pipe_fast(
        processor,
        dest,
        &mut reader,
        read_block_size,
        write_block_size,
        input_chunk_multiple,
    )
}

fn decode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    ignore_garbage: bool,
) -> io::Result<bool> {
    let compacted_capacity = ordered_input_block_bound(input, io_mode)?.max(32 * 1024);
    let mut reorg = Base64DecodeReorg::new(compacted_capacity, ignore_garbage);
    let mut invalid = false;
    visit_ordered_input(input, io_mode, |block| {
        if invalid {
            return Ok(());
        }
        if let Err(err) = reorg.consume_block(out, block) {
            invalid = true;
            if err.kind() != io::ErrorKind::InvalidData {
                return Err(err);
            }
            return Ok(());
        }
        Ok(())
    })?;
    if invalid {
        return Ok(true);
    }
    if let Err(err) = reorg.finish(out) {
        if err.kind() == io::ErrorKind::InvalidData {
            return Ok(true);
        }
        return Err(err);
    }
    Ok(false)
}

fn decode_base64_from_reader<W: Write, R: Read>(
    out: &mut W,
    reader: &mut R,
    compacted_capacity: usize,
    ignore_garbage: bool,
) -> io::Result<bool> {
    let mut reorg = Base64DecodeReorg::new(compacted_capacity.max(32 * 1024), ignore_garbage);
    let mut invalid = false;
    let mut block = vec![0u8; compacted_capacity.max(32 * 1024)];
    loop {
        let read = reader.read(&mut block)?;
        if read == 0 {
            break;
        }
        if let Err(err) = reorg.consume_block(out, &block[..read]) {
            invalid = true;
            if err.kind() != io::ErrorKind::InvalidData {
                return Err(err);
            }
            break;
        }
    }
    if invalid {
        return Ok(true);
    }
    if let Err(err) = reorg.finish(out) {
        if err.kind() == io::ErrorKind::InvalidData {
            return Ok(true);
        }
        return Err(err);
    }
    Ok(false)
}

fn encode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    wrap_cols: usize,
) -> io::Result<()> {
    if wrap_cols != 0 && wrap_cols % 4 == 0 {
        let line_input_bytes = (wrap_cols / 4) * 3;
        let block_bound = ordered_input_block_bound(input, io_mode)?;
        let full_line_input_bound = (block_bound / line_input_bytes) * line_input_bytes;
        let mut carry = vec![0u8; line_input_bytes];
        let mut carry_len = 0usize;
        let mut line_buf = vec![0u8; wrap_cols + 1];
        let mut wrapped_slab = vec![
            0u8;
            (full_line_input_bound / line_input_bytes) * (wrap_cols + 1)
        ];
        visit_ordered_input(input, io_mode, |block| {
            let mut start = 0usize;

            if carry_len != 0 {
                let needed = line_input_bytes - carry_len;
                let take = needed.min(block.len());
                carry[carry_len..carry_len + take].copy_from_slice(&block[..take]);
                carry_len += take;
                start = take;
                if carry_len == line_input_bytes {
                    let written =
                        encode_base64_block_into(&carry[..line_input_bytes], &mut line_buf[..wrap_cols]);
                    debug_assert_eq!(written, wrap_cols);
                    line_buf[wrap_cols] = b'\n';
                    out.write_all(&line_buf)?;
                    carry_len = 0;
                }
            }

            let remaining = &block[start..];
            let full_line_input_len = (remaining.len() / line_input_bytes) * line_input_bytes;
            if full_line_input_len != 0 {
                let wrapped_len = encode_full_wrapped_lines_in_place(
                    &remaining[..full_line_input_len],
                    line_input_bytes,
                    wrap_cols,
                    &mut wrapped_slab,
                );
                out.write_all(&wrapped_slab[..wrapped_len])?;
            }

            let leftover = &remaining[full_line_input_len..];
            if !leftover.is_empty() {
                carry[..leftover.len()].copy_from_slice(leftover);
                carry_len = leftover.len();
            }
            Ok(())
        })?;
        if carry_len != 0 {
            let encoded_len = encoded_base64_len(carry_len);
            let written = encode_base64_block_into(&carry[..carry_len], &mut line_buf[..encoded_len]);
            debug_assert_eq!(written, encoded_len);
            line_buf[encoded_len] = b'\n';
            out.write_all(&line_buf[..encoded_len + 1])?;
        }
        return Ok(());
    }

    let block_bound = ordered_input_block_bound(input, io_mode)?;
    let max_process_len = ((block_bound + 2) / 3) * 3;
    let max_encoded_len = encoded_base64_len(max_process_len);
    let mut carry = [0u8; 2];
    let mut carry_len = 0usize;
    let mut current_line_len = 0usize;
    let mut encoded_slab = vec![0u8; max_encoded_len.max(4)];
    let mut wrapped_slab = vec![
        0u8;
        wrapped_base64_output_capacity(max_encoded_len.max(4), wrap_cols, wrap_cols.saturating_sub(1))
    ];
    visit_ordered_input(input, io_mode, |block| {
        let total_len = carry_len + block.len();
        let process_len = (total_len / 3) * 3;
        if process_len == 0 {
            carry[..block.len()].copy_from_slice(block);
            carry_len = block.len();
            return Ok(());
        }
        let mut block_start = 0usize;
        if carry_len != 0 {
            let needed = 3 - carry_len;
            let mut merged = [0u8; 3];
            merged[..carry_len].copy_from_slice(&carry[..carry_len]);
            merged[carry_len..].copy_from_slice(&block[..needed]);
            let written = encode_base64_block_into(&merged, &mut encoded_slab[..4]);
            debug_assert_eq!(written, 4);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..4],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            out.write_all(&wrapped_slab[..wrapped_len])?;
            block_start = needed;
            carry_len = 0;
        }
        let remaining_process_len = process_len - block_start;
        if remaining_process_len != 0 {
            let input_slice = &block[block_start..block_start + remaining_process_len];
            let encoded_len = encoded_base64_len(input_slice.len());
            let written = encode_base64_block_into(input_slice, &mut encoded_slab[..encoded_len]);
            let wrapped_len = wrap_encoded_bytes_into(
                &encoded_slab[..written],
                wrap_cols,
                &mut current_line_len,
                &mut wrapped_slab,
            );
            out.write_all(&wrapped_slab[..wrapped_len])?;
            block_start += remaining_process_len;
        }
        let remainder = &block[block_start..];
        if !remainder.is_empty() {
            carry[..remainder.len()].copy_from_slice(remainder);
            carry_len = remainder.len();
        } else {
            carry_len = 0;
        }
        Ok(())
    })?;
    if carry_len != 0 {
        let encoded_len = encoded_base64_len(carry_len);
        let written = encode_base64_block_into(&carry[..carry_len], &mut encoded_slab[..encoded_len]);
        let wrapped_len = wrap_encoded_bytes_into(
            &encoded_slab[..written],
            wrap_cols,
            &mut current_line_len,
            &mut wrapped_slab,
        );
        out.write_all(&wrapped_slab[..wrapped_len])?;
    }
    if wrap_cols != 0 && current_line_len != 0 {
        out.write_all(b"\n")?;
    }
    Ok(())
}

pub(super) fn encode_base64_bytes_via_wrapped_path(
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
        let mut wrapped_slab = vec![
            0u8;
            (full_line_input_bound / line_input_bytes) * (wrap_cols + 1)
        ];
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
            let written = encode_base64_block_into(&carry[..carry_len], &mut line_buf[..encoded_len]);
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
        wrapped_base64_output_capacity(max_encoded_len.max(4), wrap_cols, wrap_cols.saturating_sub(1))
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
        let remaining_process_len = process_len - block_start;
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
        let written = encode_base64_block_into(&carry[..carry_len], &mut encoded_slab[..encoded_len]);
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

pub(super) fn decode_base64_bytes_via_reorg_path(
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

pub(super) fn decode_base64_bytes_with_detect_fallback(
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
pub(super) fn append_sanitized_base64_bytes(
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

fn encode_process_plan() -> Base64ProcessPlan {
    Base64ProcessPlan {
        read_block_size: base64_parallel_encode_block_size(BASE64_ENCODE_FAST_READ_BLOCK_SIZE),
        write_block_size: BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE,
        file_pipe_read_block_size: BASE64_ENCODE_FILE_PIPE_READ_BLOCK_SIZE,
        file_pipe_write_block_size: BASE64_ENCODE_FILE_PIPE_WRITE_BLOCK_SIZE,
        pipe_file_read_block_size: base64_parallel_encode_block_size(BASE64_ENCODE_FAST_READ_BLOCK_SIZE),
        pipe_file_write_block_size: BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE,
        pipe_input_read_block_size: base64_parallel_encode_block_size(BASE64_ENCODE_FAST_READ_BLOCK_SIZE),
        pipe_input_write_block_size: BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE,
        input_chunk_multiple: 3,
        process_chunk: encode_base64_block_processor,
    }
}

fn decode_process_plan() -> Base64ProcessPlan {
    let process_chunk = {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::arch::is_x86_feature_detected!("avx2") {
                decode_base64_block_processor_avx2
            } else {
                decode_base64_block_processor_scalar
            }
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            decode_base64_block_processor_scalar
        }
    };
    Base64ProcessPlan {
        read_block_size: BASE64_DECODE_FAST_READ_BLOCK_SIZE,
        write_block_size: BASE64_DECODE_FAST_WRITE_BLOCK_SIZE,
        file_pipe_read_block_size: BASE64_DECODE_FILE_PIPE_READ_BLOCK_SIZE,
        file_pipe_write_block_size: BASE64_DECODE_FILE_PIPE_WRITE_BLOCK_SIZE,
        pipe_file_read_block_size: BASE64_DECODE_FILE_PIPE_READ_BLOCK_SIZE,
        pipe_file_write_block_size: BASE64_DECODE_FILE_PIPE_WRITE_BLOCK_SIZE,
        pipe_input_read_block_size: BASE64_DECODE_FAST_READ_BLOCK_SIZE,
        pipe_input_write_block_size: BASE64_DECODE_FAST_WRITE_BLOCK_SIZE,
        input_chunk_multiple: 4,
        process_chunk,
    }
}

pub(super) fn run_base64(args: &[String]) -> io::Result<i32> {
    let options = match parse_base64_options(args)? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };
    run_base64_io(options)
}

fn run_base64_io(options: Base64Options) -> io::Result<i32> {
    // This branch is still required because the fast runners are stateless block
    // transforms. Wrapped encode needs cross-block line-length state, and wrapped
    // / whitespace-tolerant / ignore-garbage decode needs cross-block sanitized
    // quartet state that the current process_chunk contract cannot carry.
    let requires_stateful_path = if options.decode {
        options.ignore_garbage
            || (!decode_input_can_use_fast_path(&options.input)?
                && !decode_input_can_use_wrapped_fast_path(&options.input)?)
    } else {
        options.wrap_cols != 0
            && !encode_input_can_use_wrapped_fast_path(&options.input, options.wrap_cols)?
    };

    if requires_stateful_path {
        let mut out = stdout_buf_writer()?;
        let invalid = if options.decode {
            decode_base64_input(
                &mut out,
                &options.input,
                options.io_mode,
                options.ignore_garbage,
            )?
        } else {
            encode_base64_input(
                &mut out,
                &options.input,
                options.io_mode,
                options.wrap_cols,
            )?;
            false
        };
        out.into_inner()?;
        if invalid {
            eprintln!("base64: invalid input");
            return Ok(1);
        }
        return Ok(0);
    }

    let plan = if options.decode {
        decode_process_plan()
    } else {
        encode_process_plan()
    };

    let mut stdout = {
        let dupfd = unsafe { libc::dup(io::stdout().as_raw_fd()) };
        if dupfd < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { File::from_raw_fd(dupfd) }
    };

    let invalid = match &options.input {
        StreamInput::File(path) => {
            if is_regular_input_path(path)? {
                if is_stdout_dev_null() || is_stdout_file() {
                    process_file_to_file(&mut stdout, path, &options, &plan)?
                } else {
                    process_file_to_pipe(&mut stdout, path, &options, &plan)?
                }
            } else if is_stdout_dev_null() || is_stdout_file() {
                process_pipe_to_file(&mut stdout, &options.input, &options, &plan)?
            } else {
                process_pipe_to_pipe(&mut stdout, &options.input, &options, &plan)?
            }
        }
        StreamInput::Stdin { .. } => {
            let stdin_fd = io::stdin().as_raw_fd();
            if is_regular_fd(stdin_fd) {
                let read_path_buf = fs::read_link(format!("/proc/self/fd/{stdin_fd}"))?;
                let read_path = read_path_buf.to_str().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "stdin path is not valid UTF-8")
                })?;
                if is_stdout_dev_null() || is_stdout_file() {
                    process_file_to_file(&mut stdout, read_path, &options, &plan)?
                } else {
                    process_file_to_pipe(&mut stdout, read_path, &options, &plan)?
                }
            } else if is_stdout_dev_null() || is_stdout_file() {
                process_pipe_to_file(&mut stdout, &options.input, &options, &plan)?
            } else {
                process_pipe_to_pipe(&mut stdout, &options.input, &options, &plan)?
            }
        }
    };

    if invalid {
        eprintln!("base64: invalid input");
        return Ok(1);
    }
    Ok(0)
}
