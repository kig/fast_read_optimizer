use super::process_layout::{
    decode_input_can_use_fast_path, decode_input_can_use_wrapped_fast_path,
    detect_regular_decode_layout, encode_input_can_use_wrapped_fast_path,
    read_decode_fast_path_probe, wrapped_decode_block_sizes, wrapped_encode_block_sizes,
    RegularDecodeLayout, SliceWriter,
};
use super::*;
use crate::stream::allocate_pipe_output_buffer;
use crate::stream::transform::{
    run_file_transform_to_pipe_with_owned_output, run_reader_transform_to_file,
    run_reader_transform_to_pipe, run_staged_file_transform_to_file, run_transform_with_specs,
    PipeOutputPolicy, ReaderTransformGeometry, TransformInputSpec, TransformOutputSpec,
};
mod decode_reorg;
use decode_reorg::Base64DecodeReorg;
pub(super) mod bytes;
pub(super) mod wrapped;
use wrapped::{
    decode_wrapped_block_into, encode_full_wrapped_lines_in_place, encode_wrapped_block_into,
    ordered_input_block_bound, wrap_encoded_bytes_into, wrapped_base64_output_capacity,
    OrderedWriteBatcher,
};

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
            run_reader_transform_to_pipe(
                dest,
                &mut replay,
                ReaderTransformGeometry {
                    read_block_size: plan.pipe_input_read_block_size,
                    write_block_size: plan.pipe_input_write_block_size,
                    input_chunk_multiple: plan.input_chunk_multiple,
                },
                PipeOutputPolicy::GiftedAlignedPages,
                plan.process_chunk,
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
            run_reader_transform_to_file(
                &mut replay,
                dest,
                ReaderTransformGeometry {
                    read_block_size: plan.pipe_file_read_block_size,
                    write_block_size: plan.pipe_file_write_block_size,
                    input_chunk_multiple: plan.input_chunk_multiple,
                },
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
    run_reader_transform_to_file(
        &mut reader,
        dest,
        ReaderTransformGeometry {
            read_block_size: plan.pipe_file_read_block_size,
            write_block_size: plan.pipe_file_write_block_size,
            input_chunk_multiple: plan.input_chunk_multiple,
        },
        plan.process_chunk,
    )?;
    Ok(false)
}

fn open_stream_input_file(input: &StreamInput) -> io::Result<File> {
    match input {
        StreamInput::File(path) => File::open(path),
        StreamInput::Stdin { .. } => {
            let dupfd = unsafe { libc::dup(fro::command_io::stdin_fd()) };
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
        let wrap_cols = options.wrap_cols;
        let (read_block_size, write_block_size) =
            wrapped_encode_block_sizes(wrap_cols, BASE64_ENCODE_FILE_PIPE_READ_BLOCK_SIZE);
        run_file_transform_to_pipe_with_owned_output(
            path,
            dest,
            ReaderTransformGeometry {
                read_block_size,
                write_block_size,
                input_chunk_multiple: 1,
            },
            PipeOutputPolicy::GiftedAlignedPages,
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
        let (read_block_size, write_block_size) =
            wrapped_decode_block_sizes(BASE64_DECODE_FILE_PIPE_READ_BLOCK_SIZE);
        run_file_transform_to_pipe_with_owned_output(
            path,
            dest,
            ReaderTransformGeometry {
                read_block_size,
                write_block_size,
                input_chunk_multiple: 1,
            },
            PipeOutputPolicy::GiftedAlignedPages,
            move |input: &[u8]| -> io::Result<Vec<u8>> {
                let mut out = allocate_pipe_output_buffer(write_block_size);
                let produced = decode_wrapped_block_into(input, &mut out[..])?;
                out.truncate(produced);
                Ok(out)
            },
        )?;
        return Ok(false);
    }
    let decode = options.decode;
    let pipe_write_block_size = plan.file_pipe_write_block_size;
    let process_chunk = plan.process_chunk;
    run_file_transform_to_pipe_with_owned_output(
        path,
        dest,
        ReaderTransformGeometry {
            read_block_size: plan.file_pipe_read_block_size,
            write_block_size: pipe_write_block_size,
            input_chunk_multiple: plan.input_chunk_multiple,
        },
        PipeOutputPolicy::GiftedAlignedPages,
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
        let wrap_cols = options.wrap_cols;
        let (read_block_size, write_block_size) =
            wrapped_encode_block_sizes(wrap_cols, BASE64_ENCODE_FAST_READ_BLOCK_SIZE);
        run_staged_file_transform_to_file(
            path,
            dest,
            ReaderTransformGeometry {
                read_block_size,
                write_block_size,
                input_chunk_multiple: 1,
            },
            BASE64_STAGED_FILE_TO_FILE_LIMIT,
            move |input: &[u8], out: &mut [u8]| encode_wrapped_block_into(input, wrap_cols, out),
        )?;
        return Ok(false);
    }
    if options.decode && detect_regular_decode_layout(path)? == RegularDecodeLayout::WrappedLf76 {
        let (read_block_size, write_block_size) =
            wrapped_decode_block_sizes(BASE64_DECODE_FAST_READ_BLOCK_SIZE);
        run_staged_file_transform_to_file(
            path,
            dest,
            ReaderTransformGeometry {
                read_block_size,
                write_block_size,
                input_chunk_multiple: 1,
            },
            BASE64_STAGED_FILE_TO_FILE_LIMIT,
            move |input: &[u8], out: &mut [u8]| decode_wrapped_block_into(input, out),
        )?;
        return Ok(false);
    }
    run_staged_file_transform_to_file(
        path,
        dest,
        ReaderTransformGeometry {
            read_block_size: plan.read_block_size,
            write_block_size: plan.write_block_size,
            input_chunk_multiple: plan.input_chunk_multiple,
        },
        BASE64_STAGED_FILE_TO_FILE_LIMIT,
        plan.process_chunk,
    )?;
    return Ok(false);
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
    run_reader_transform_to_pipe(
        dest,
        &mut reader,
        ReaderTransformGeometry {
            read_block_size,
            write_block_size,
            input_chunk_multiple,
        },
        PipeOutputPolicy::GiftedAlignedPages,
        processor,
    )
}

fn decode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    ignore_garbage: bool,
) -> io::Result<(bool, u64)> {
    let compacted_capacity = ordered_input_block_bound(input, io_mode)?.max(32 * 1024);
    let mut reorg = Base64DecodeReorg::new(compacted_capacity, ignore_garbage);
    let mut invalid = false;
    let bytes = visit_ordered_input_counted(input, io_mode, |block| {
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
        return Ok((true, bytes));
    }
    if let Err(err) = reorg.finish(out) {
        if err.kind() == io::ErrorKind::InvalidData {
            return Ok((true, bytes));
        }
        return Err(err);
    }
    Ok((false, bytes))
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
) -> io::Result<u64> {
    if wrap_cols != 0 && wrap_cols % 4 == 0 {
        let line_input_bytes = (wrap_cols / 4) * 3;
        let block_bound = ordered_input_block_bound(input, io_mode)?;
        let full_line_input_bound = (block_bound / line_input_bytes) * line_input_bytes;
        let mut carry = vec![0u8; line_input_bytes];
        let mut carry_len = 0usize;
        let mut line_buf = vec![0u8; wrap_cols + 1];
        let mut wrapped_slab =
            vec![0u8; (full_line_input_bound / line_input_bytes) * (wrap_cols + 1)];
        let mut batched_out = OrderedWriteBatcher::with_capacity(
            out,
            wrapped_slab
                .len()
                .saturating_add(line_buf.len())
                .max(64 * 1024),
        );
        let bytes = visit_ordered_input_counted(input, io_mode, |block| {
            let mut start = 0usize;

            if carry_len != 0 {
                let needed = line_input_bytes - carry_len;
                let take = needed.min(block.len());
                carry[carry_len..carry_len + take].copy_from_slice(&block[..take]);
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

            let remaining = &block[start..];
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
            Ok(())
        })?;
        if carry_len != 0 {
            let encoded_len = encoded_base64_len(carry_len);
            let written =
                encode_base64_block_into(&carry[..carry_len], &mut line_buf[..encoded_len]);
            debug_assert_eq!(written, encoded_len);
            line_buf[encoded_len] = b'\n';
            batched_out.write_all(&line_buf[..encoded_len + 1])?;
        }
        batched_out.flush()?;
        return Ok(bytes);
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
        wrapped_base64_output_capacity(
            max_encoded_len.max(4),
            wrap_cols,
            wrap_cols.saturating_sub(1)
        )
    ];
    let mut batched_out = OrderedWriteBatcher::with_capacity(
        out,
        wrapped_slab.len().saturating_add(8).max(64 * 1024),
    );
    let bytes = visit_ordered_input_counted(input, io_mode, |block| {
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
            batched_out.write_all(&wrapped_slab[..wrapped_len])?;
            block_start = needed;
            carry_len = 0;
        }
        let remaining_process_len = ((block.len() - block_start) / 3) * 3;
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
            batched_out.write_all(&wrapped_slab[..wrapped_len])?;
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
    Ok(bytes)
}

fn encode_process_plan() -> Base64ProcessPlan {
    Base64ProcessPlan {
        read_block_size: base64_parallel_encode_block_size(BASE64_ENCODE_FAST_READ_BLOCK_SIZE),
        write_block_size: BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE,
        file_pipe_read_block_size: BASE64_ENCODE_FILE_PIPE_READ_BLOCK_SIZE,
        file_pipe_write_block_size: BASE64_ENCODE_FILE_PIPE_WRITE_BLOCK_SIZE,
        pipe_file_read_block_size: base64_parallel_encode_block_size(
            BASE64_ENCODE_FAST_READ_BLOCK_SIZE,
        ),
        pipe_file_write_block_size: BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE,
        pipe_input_read_block_size: base64_parallel_encode_block_size(
            BASE64_ENCODE_FAST_READ_BLOCK_SIZE,
        ),
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
    let started_at = std::time::Instant::now();

    if requires_stateful_path {
        let mut out = stdout_buf_writer()?;
        let (invalid, processed_bytes) = if options.decode {
            decode_base64_input(
                &mut out,
                &options.input,
                options.io_mode,
                options.ignore_garbage,
            )?
        } else {
            (
                false,
                encode_base64_input(&mut out, &options.input, options.io_mode, options.wrap_cols)?,
            )
        };
        out.into_inner()?;
        if options.report_gbps {
            report_gbps("base64", processed_bytes, started_at);
        }
        if invalid {
            fro::cio_eprintln!("base64: invalid input");
            return Ok(1);
        }
        return Ok(0);
    }

    let plan = if options.decode {
        decode_process_plan()
    } else {
        encode_process_plan()
    };

    let invalid = run_transform_with_specs(
        transform_input_spec(&options.input),
        TransformOutputSpec::Stdout,
        |input_path, mut output| process_file_to_file(&mut output, &input_path, &options, &plan),
        |input_path, mut output| process_file_to_pipe(&mut output, &input_path, &options, &plan),
        |_, mut output| process_pipe_to_file(&mut output, &options.input, &options, &plan),
        |_, mut output| process_pipe_to_pipe(&mut output, &options.input, &options, &plan),
    )?;
    if options.report_gbps {
        if let StreamInput::File(path) = &options.input {
            report_gbps("base64", fs::metadata(path)?.len(), started_at);
        }
    }

    if invalid {
        fro::cio_eprintln!("base64: invalid input");
        return Ok(1);
    }
    Ok(0)
}

fn transform_input_spec(input: &StreamInput) -> TransformInputSpec<'_> {
    match input {
        StreamInput::File(path) => TransformInputSpec::Path(path),
        StreamInput::Stdin { .. } => TransformInputSpec::Stdin,
    }
}
