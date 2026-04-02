use super::*;

fn process_regular_file_blocks_in_order<T, X, D>(
    path: &str,
    io_mode: IOMode,
    read_block_size: u64,
    transform: X,
    mut drain: D,
) -> io::Result<()>
where
    T: Send + 'static,
    X: Fn(&[u8]) -> io::Result<T> + Send + Sync + 'static,
    D: FnMut(usize, T) -> io::Result<()>,
{
    let config = load_config(None);
    let page_cache = config.get_params_for_path("compute", false, path);
    let direct = config.get_params_for_path("compute", true, path);
    let transform = Arc::new(transform);
    let (tx, rx) = mpsc::channel::<(usize, io::Result<T>)>();
    let sender = tx.clone();
    let visit_result = crate::reader::visit_file_blocks(
        path,
        page_cache.num_threads,
        read_block_size,
        page_cache.qd,
        direct.num_threads,
        read_block_size,
        direct.qd,
        internal_io_mode(io_mode),
        move |block| {
            let result = transform(block.data);
            sender
                .send((block.block_index, result))
                .map_err(|_| io::Error::other("failed to queue transformed block"))
        },
    )?;
    drop(tx);

    let mut next_block = 0usize;
    let mut pending = BTreeMap::<usize, io::Result<T>>::new();
    while let Ok((block_index, item)) = rx.recv() {
        pending.insert(block_index, item);
        while let Some(item) = pending.remove(&next_block) {
            drain(next_block, item?)?;
            next_block += 1;
        }
    }
    if !pending.is_empty() {
        return Err(io::Error::other(
            "missing transformed block data while finalizing ordered output",
        ));
    }
    let _ = visit_result;
    Ok(())
}

fn process_pipe_to_pipe(
    dest: &mut File,
    input: &StreamInput,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if !options.decode
        && options.wrap_cols == 0
        && plan.read_block_size == 1572864
        && plan.write_block_size == 2097152
    {
        encode_pipe_input_to_pipe_fast(
            dest,
            input,
            plan.read_block_size,
            plan.write_block_size,
        )?;
        return Ok(false);
    }

    let mut inbuf = vec![0u8; plan.read_block_size as usize];
    let mut outbuf = Vec::with_capacity(plan.write_block_size);
    let mut state = Base64ProcessState::new();

    let mut reader = open_stream_input_file(input)?;
    loop {
        let read = reader.read(&mut inbuf)?;
        if read == 0 {
            break;
        }
        outbuf.clear();
        (plan.process_chunk)(options, &inbuf[..read], &mut outbuf, &mut state)?;
        if !outbuf.is_empty() {
            dest.write_all(&outbuf)?;
        }
    }

    outbuf.clear();
    (plan.finish)(options, &mut outbuf, &mut state)?;
    if !outbuf.is_empty() {
        dest.write_all(&outbuf)?;
    }
    Ok(state.invalid)
}

fn process_pipe_to_file(
    dest: &mut File,
    input: &StreamInput,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    process_pipe_to_pipe(dest, input, options, plan)
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
    if !options.decode
        && options.wrap_cols == 0
        && plan.read_block_size == 1572864
        && plan.write_block_size == 2097152
    {
        let config = load_config(None);
        increase_pipe_capacity(dest.as_raw_fd(), plan.write_block_size);
        let processor =
            |input: &[u8]| -> io::Result<Vec<u8>> { Ok(encode_base64_block_aligned(input)) };
        let _report = ParallelStream::map_file_fixed_size_to_pipe(
            &config,
            path,
            dest,
            plan.read_block_size,
            plan.write_block_size,
            processor,
        )?;
        return Ok(false);
    }

    increase_pipe_capacity(dest.as_raw_fd(), plan.write_block_size);
    let out = BufWriter::with_capacity(4, dest.try_clone()?, 1, plan.write_block_size as u64)?;
    let mut state = Base64ProcessState::new();
    process_regular_file_blocks_in_order(
        path,
        options.io_mode,
        plan.read_block_size,
        |input| Ok(input.to_vec()),
        |_, block| {
            let mut produced = Vec::with_capacity(plan.write_block_size);
            (plan.process_chunk)(options, &block, &mut produced, &mut state)?;
            if !produced.is_empty() {
                out.write_vec(produced)?;
            }
            Ok(())
        },
    )?;
    let mut tail = Vec::new();
    (plan.finish)(options, &mut tail, &mut state)?;
    if !tail.is_empty() {
        out.write_vec(tail)?;
    }
    out.into_inner()?;
    Ok(state.invalid)
}

fn process_file_to_file(
    dest: &mut File,
    path: &str,
    options: &Base64Options,
    plan: &Base64ProcessPlan,
) -> io::Result<bool> {
    if !options.decode
        && options.wrap_cols == 0
        && plan.read_block_size == 1572864
        && plan.write_block_size == 2097152
    {
        let config = load_config(None);
        let processor =
            |input: &[u8], out: &mut [u8]| -> io::Result<usize> { Ok(encode_base64_block_into(input, out)) };
        let _report = ParallelStream::map_file_fixed_size_to_file(
            &config,
            path,
            dest,
            plan.read_block_size,
            plan.write_block_size,
            processor,
        )?;
        return Ok(false);
    }

    let out = BufWriter::with_capacity(4, dest.try_clone()?, 1, plan.write_block_size as u64)?;
    let mut state = Base64ProcessState::new();
    process_regular_file_blocks_in_order(
        path,
        options.io_mode,
        plan.read_block_size,
        |input| Ok(input.to_vec()),
        |_, block| {
            let mut produced = Vec::with_capacity(plan.write_block_size);
            (plan.process_chunk)(options, &block, &mut produced, &mut state)?;
            if !produced.is_empty() {
                out.write_vec(produced)?;
            }
            Ok(())
        },
    )?;
    let mut tail = Vec::new();
    (plan.finish)(options, &mut tail, &mut state)?;
    if !tail.is_empty() {
        out.write_vec(tail)?;
    }
    out.into_inner()?;
    Ok(state.invalid)
}

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

fn encode_pipe_chunk(
    options: &Base64Options,
    input: &[u8],
    out: &mut Vec<u8>,
    state: &mut Base64ProcessState,
) -> io::Result<()> {
    let produced = encoded_base64_len(input.len());
    let start = out.len();
    out.resize(start + produced, 0);
    let written = encode_base64_block_into(input, &mut out[start..]);
    out.truncate(start + written);
    if options.wrap_cols != 0 {
        let encoded = out.split_off(start);
        append_wrapped_base64_bytes(out, &encoded, options.wrap_cols, &mut state.wrap_line_len);
    }
    Ok(())
}

fn encode_finish(
    options: &Base64Options,
    out: &mut Vec<u8>,
    state: &mut Base64ProcessState,
) -> io::Result<()> {
    if options.wrap_cols != 0 && state.wrap_line_len != 0 {
        out.extend_from_slice(b"\n");
        state.wrap_line_len = 0;
    }
    Ok(())
}

use libc::{fcntl, F_SETPIPE_SZ};

fn increase_pipe_capacity(pipe_fd: i32, new_size: usize) {
    unsafe {
        let _res = fcntl(pipe_fd, F_SETPIPE_SZ, new_size);
    }
}

fn encode_pipe_input_to_pipe_fast(
    dest: &mut File,
    input: &StreamInput,
    read_block_size: u64,
    write_block_size: usize,
) -> io::Result<()> {
    use std::os::unix::io::RawFd;

    unsafe fn vmsplice_all(pipe_fd: RawFd, buf: &[u8]) -> io::Result<usize> {
        let mut written_total = 0usize;
        let mut ptr = buf.as_ptr();
        let mut remaining = buf.len();
        while remaining > 0 {
            let rc = if remaining % 4096 == 0 && ptr.align_offset(4096) == 0 {
                let iov = libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: remaining,
                };
                libc::vmsplice(pipe_fd, &iov as *const libc::iovec, 1, libc::SPLICE_F_GIFT)
            } else {
                libc::write(pipe_fd, ptr as *mut libc::c_void, remaining)
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EINTR | libc::EAGAIN) => continue,
                    _ => return Err(err),
                }
            }
            let n = rc as usize;
            written_total = written_total
                .checked_add(n)
                .ok_or_else(|| io::Error::other("vmsplice overflow"))?;
            ptr = ptr.add(n);
            remaining -= n;
        }
        Ok(written_total)
    }

    fn aligned_output_buffer(capacity: usize) -> Vec<u8> {
        let layout = Layout::from_size_align(capacity, 4096).expect("invalid aligned vec layout");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        unsafe { Vec::from_raw_parts(ptr, capacity, capacity) }
    }

    increase_pipe_capacity(dest.as_raw_fd(), write_block_size);
    let mut reader = open_stream_input_file(input)?;
    let mut read_bufs = (0..3)
        .map(|_| vec![0u8; read_block_size as usize])
        .collect::<Vec<_>>();
    let (free_tx, free_rx) = mpsc::sync_channel::<Vec<u8>>(3);
    for _ in 0..3 {
        free_tx
            .send(aligned_output_buffer(write_block_size))
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    let writer_pool_tx = free_tx.clone();
    drop(free_tx);

    let (tx, rx) = mpsc::sync_channel::<io::Result<(Vec<u8>, usize)>>(3);
    let writer = dest.try_clone()?;
    let writer_thread = thread::spawn(move || -> io::Result<()> {
        let pipe_fd = writer.as_raw_fd();
        for item in rx {
            let (buf, len) = item?;
            if len != 0 {
                let mut written = 0usize;
                while written < len {
                    written += unsafe { vmsplice_all(pipe_fd, &buf[written..len])? };
                }
            }
            writer_pool_tx
                .send(buf)
                .map_err(|err| io::Error::other(err.to_string()))?;
        }
        Ok(())
    });

    let mut read_slot = 0usize;
    loop {
        let read = reader.read(&mut read_bufs[read_slot])?;
        if read == 0 {
            break;
        }
        let out = free_rx
            .recv()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let mut out = out;
        let produced = encode_base64_block_into(&read_bufs[read_slot][..read], &mut out[..]);
        tx.send(Ok((out, produced)))
            .map_err(|err| io::Error::other(err.to_string()))?;
        read_slot = (read_slot + 1) % read_bufs.len();
    }
    drop(tx);
    writer_thread
        .join()
        .map_err(|_| io::Error::other("base64 pipe writer thread panicked"))??;
    Ok(())
}

fn decode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    ignore_garbage: bool,
) -> io::Result<bool> {
    let mut quartet = [0_u8; 4];
    let mut quartet_len = 0usize;
    let mut invalid = false;
    visit_ordered_input(input, io_mode, |block| {
        for &byte in block {
            if invalid {
                break;
            }
            if base64_is_ignored_decode_byte(byte) {
                continue;
            }
            if byte == b'=' || base64_decode_value(byte).is_some() {
                quartet[quartet_len] = byte;
                quartet_len += 1;
                if quartet_len == 4 {
                    let Some((decoded, decoded_len)) = decode_base64_quartet(quartet) else {
                        invalid = true;
                        break;
                    };
                    out.write_all(&decoded[..decoded_len])?;
                    quartet_len = 0;
                }
                continue;
            }
            if ignore_garbage {
                continue;
            }
            invalid = true;
            break;
        }
        Ok(())
    })?;
    Ok(invalid || quartet_len != 0)
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

fn decode_pipe_chunk(
    _options: &Base64Options,
    input: &[u8],
    out: &mut Vec<u8>,
    state: &mut Base64ProcessState,
) -> io::Result<()> {
    if state.invalid {
        return Ok(());
    }
    for &byte in input {
        if base64_is_ignored_decode_byte(byte) {
            continue;
        }
        if state.saw_padding {
            state.invalid = true;
            return Ok(());
        }
        if byte == b'=' || base64_decode_value(byte).is_some() {
            if byte == b'=' {
                state.saw_padding = true;
            }
            state.carry.push(byte);
            if state.carry.len() == 4 {
                let decoded_cap = decoded_base64_capacity(4);
                state.scratch.resize(decoded_cap, 0);
                let written = decode_base64_clean_block(&state.carry, &mut state.scratch)?;
                out.extend_from_slice(&state.scratch[..written]);
                state.carry.clear();
            }
            continue;
        }
        state.invalid = true;
        return Ok(());
    }
    Ok(())
}

fn decode_finish(
    _options: &Base64Options,
    out: &mut Vec<u8>,
    state: &mut Base64ProcessState,
) -> io::Result<()> {
    if state.invalid {
        return Ok(());
    }
    if state.carry.is_empty() {
        return Ok(());
    }
    if state.carry.len() != 4 {
        state.invalid = true;
        return Ok(());
    }
    let decoded_cap = decoded_base64_capacity(4);
    state.scratch.resize(decoded_cap, 0);
    let written = decode_base64_clean_block(&state.carry, &mut state.scratch)?;
    out.extend_from_slice(&state.scratch[..written]);
    state.carry.clear();
    Ok(())
}

fn encode_process_plan() -> Base64ProcessPlan {
    Base64ProcessPlan {
        read_block_size: base64_parallel_encode_block_size(1572864),
        write_block_size: 2097152,
        process_chunk: encode_pipe_chunk,
        finish: encode_finish,
    }
}

fn decode_process_plan() -> Base64ProcessPlan {
    Base64ProcessPlan {
        read_block_size: BASE64_DECODE_FAST_READ_BLOCK_SIZE,
        write_block_size: BASE64_DECODE_FAST_WRITE_BLOCK_SIZE,
        process_chunk: decode_pipe_chunk,
        finish: decode_finish,
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
            if options.decode && options.ignore_garbage {
                let mut out = stdout_buf_writer()?;
                let invalid = decode_base64_input(
                    &mut out,
                    &options.input,
                    options.io_mode,
                    options.ignore_garbage,
                )?;
                out.into_inner()?;
                invalid
            } else if is_regular_input_path(path)? {
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
            if options.decode && options.ignore_garbage {
                let mut out = stdout_buf_writer()?;
                let invalid = decode_base64_input(
                    &mut out,
                    &options.input,
                    options.io_mode,
                    options.ignore_garbage,
                )?;
                out.into_inner()?;
                invalid
            } else {
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
        }
    };

    if invalid {
        eprintln!("base64: invalid input");
        return Ok(1);
    }
    Ok(0)
}
