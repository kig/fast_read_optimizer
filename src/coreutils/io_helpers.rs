use super::*;

pub(crate) fn print_coreutils_version(invoked: &str) {
    println!("{invoked} (fro coreutils) {FRO_VERSION}");
}
pub(crate) fn permission_denied_components(kind: io::ErrorKind, raw_os_error: Option<i32>) -> bool {
    matches!(kind, io::ErrorKind::PermissionDenied)
        || matches!(raw_os_error, Some(libc::EACCES | libc::EPERM))
}

pub(crate) fn is_permission_denied(err: &io::Error) -> bool {
    permission_denied_components(err.kind(), err.raw_os_error())
}

pub(crate) fn write_warning_line(tool: &str, path: &Path, err: &io::Error, message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{tool}: {message} '{}': {err}", path.display());
}

pub(crate) fn invoked_name(program: &str) -> Option<String> {
    Path::new(program)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

pub(crate) fn parse_io_mode(args: &[String]) -> io::Result<(IOMode, Vec<String>)> {
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    for arg in args {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
    }
    Ok((io_mode, files))
}

pub(crate) fn ensure_files(
    program: &str,
    files: Vec<String>,
    usage: &str,
) -> io::Result<Vec<String>> {
    if files.is_empty() {
        eprintln!("Usage: {} {}", program, usage);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing file operand",
        ));
    }
    Ok(files)
}

pub(crate) fn internal_io_mode(io_mode: IOMode) -> crate::common::IOMode {
    match io_mode {
        IOMode::Auto => crate::common::IOMode::Auto,
        IOMode::Direct => crate::common::IOMode::Direct,
        IOMode::PageCache => crate::common::IOMode::PageCache,
    }
}

pub(crate) fn load_file_bytes(path: &str, io_mode: IOMode, mode: &str) -> io::Result<LoadedFile> {
    let config = load_config(None);
    load_file_to_memory_for_mode(&config, mode, path, internal_io_mode(io_mode))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamInput {
    File(String),
    Stdin { label: Option<String> },
}

pub(crate) fn parse_stream_inputs(files: Vec<String>) -> Vec<StreamInput> {
    if files.is_empty() {
        return vec![StreamInput::Stdin { label: None }];
    }
    files
        .into_iter()
        .map(|file| {
            if file == "-" {
                StreamInput::Stdin {
                    label: Some("-".to_string()),
                }
            } else {
                StreamInput::File(file)
            }
        })
        .collect()
}

pub(crate) fn stdout_buf_writer() -> io::Result<BufWriter> {
    let config = load_config(None);
    let params = config.get_params("write", false);
    BufWriter::stdout(params.qd, params.block_size, 4)
}

pub(crate) fn stdin_buf_reader() -> io::Result<BufReader<std::fs::File>> {
    BufReader::stdin()
}

pub(crate) fn is_regular_input_path(path: &str) -> io::Result<bool> {
    if path.starts_with("/dev/fd/") || path.starts_with("/proc/self/fd/") {
        return Ok(false);
    }
    Ok(fs::metadata(path)?.file_type().is_file())
}

pub(crate) fn is_regular_fd(fd: std::os::unix::io::RawFd) -> bool {
    unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut stat) != 0 {
            return false;
        }
        (stat.st_mode & libc::S_IFMT) == libc::S_IFREG
    }
}

#[allow(dead_code)]
pub(crate) fn is_stdout_file() -> bool {
    use std::io::stdout;
    let stdout_fd = stdout().as_raw_fd();
    is_regular_fd(stdout_fd)
}

#[allow(dead_code)]
pub(crate) fn is_stdout_dev_null() -> bool {
    use std::io::stdout;
    let stdout_fd = stdout().as_raw_fd();
    unsafe {
        let mut stdout_stat: libc::stat = std::mem::zeroed();
        let mut dev_null_stat: libc::stat = std::mem::zeroed();
        if libc::fstat(stdout_fd, &mut stdout_stat) != 0 {
            return false;
        }
        // stat("/dev/null") - ensure C string is NUL terminated
        let path = b"/dev/null\0".as_ptr() as *const libc::c_char;
        if libc::stat(path, &mut dev_null_stat) != 0 {
            return false;
        }
        stdout_stat.st_dev == dev_null_stat.st_dev && stdout_stat.st_ino == dev_null_stat.st_ino
    }
}

pub(crate) fn visit_reader_blocks<R, F>(reader: &mut R, mut on_block: F) -> io::Result<()>
where
    R: Read,
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let mut buffer = vec![0_u8; 1 << 20];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        on_block(&buffer[..read])?;
    }
}

pub(crate) fn visit_ordered_input<F>(
    input: &StreamInput,
    io_mode: IOMode,
    on_block: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            visit_ordered_blocks(path, io_mode, on_block)
        }
        StreamInput::File(path) => {
            let mut reader = BufReader::new(std::fs::File::open(path)?);
            visit_reader_blocks(&mut reader, on_block)
        }
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            visit_reader_blocks(&mut reader, on_block)
        }
    }
}

pub(crate) fn loaded_or_stream_bytes(input: &StreamInput, io_mode: IOMode) -> io::Result<Vec<u8>> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            Ok(read_file_with_mode(path, io_mode)?)
        }
        StreamInput::File(path) => {
            let mut reader = BufReader::new(std::fs::File::open(path)?);
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            Ok(buffer)
        }
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            Ok(buffer)
        }
    }
}

pub(crate) fn copy_file_like_to_output<W: Write>(
    out: &mut W,
    input: &StreamInput,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) => {
            let mut reader = BufReader::new(std::fs::File::open(path)?);
            let mut buffer = vec![0_u8; 1 << 20];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    return Ok(());
                }
                out.write_all(&buffer[..read])?;
            }
        }
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            let mut buffer = vec![0_u8; 1 << 20];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    return Ok(());
                }
                out.write_all(&buffer[..read])?;
            }
        }
    }
}

pub(crate) fn fd_is_fifo(fd: libc::c_int) -> io::Result<bool> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_mode & libc::S_IFMT) == libc::S_IFIFO)
}

pub(crate) fn fd_is_regular(fd: libc::c_int) -> io::Result<bool> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_mode & libc::S_IFMT) == libc::S_IFREG)
}

pub(crate) fn grow_pipe_best_effort(fd: libc::c_int) -> io::Result<()> {
    if !fd_is_fifo(fd)? {
        return Ok(());
    }
    let target_size = 1 << 20;
    let rc = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, target_size) };
    if rc >= 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EPERM | libc::EINVAL | libc::EBUSY) => Ok(()),
        _ => Err(err),
    }
}

pub(crate) fn pipe_size_best_effort(fd: libc::c_int, target_size: usize) -> io::Result<usize> {
    if !fd_is_fifo(fd)? {
        return Ok(0);
    }
    let mut actual = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, target_size as libc::c_int) };
    if actual < 0 {
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EPERM | libc::EINVAL | libc::EBUSY) => {
                actual = unsafe { libc::fcntl(fd, libc::F_GETPIPE_SZ) };
                if actual < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            _ => return Err(err),
        }
    }
    Ok(actual as usize)
}

pub(crate) fn write_raw_fd_all(fd: libc::c_int, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let written = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
        if written > 0 {
            buf = &buf[written as usize..];
            continue;
        }
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write to raw fd",
            ));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN) => continue,
            _ => return Err(err),
        }
    }
    Ok(())
}

pub(crate) fn splice_all(
    src_fd: libc::c_int,
    dst_fd: libc::c_int,
    mut len: u64,
) -> io::Result<u64> {
    let mut total = 0u64;
    while len != 0 {
        let chunk = len.min(FAST_COPY_SPLICE_CHUNK_SIZE as u64) as usize;
        let moved = unsafe {
            libc::splice(
                src_fd,
                std::ptr::null_mut(),
                dst_fd,
                std::ptr::null_mut(),
                chunk,
                0,
            )
        };
        if moved > 0 {
            let moved = moved as u64;
            total = total
                .checked_add(moved)
                .ok_or_else(|| io::Error::other("splice byte count overflow"))?;
            len -= moved;
            continue;
        }
        if moved == 0 {
            break;
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN) => continue,
            _ => return Err(err),
        }
    }
    Ok(total)
}

pub(crate) fn copy_pipe_tail_to_stdout_small(
    src_fd: libc::c_int,
    count: u64,
) -> io::Result<Option<u64>> {
    if count == 0 {
        return Ok(Some(0));
    }
    if !fd_is_fifo(src_fd)? {
        return Ok(None);
    }

    let mut pipe_fds = [0; 2];
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let pipe_read = pipe_fds[0];
    let pipe_write = pipe_fds[1];
    let result = (|| -> io::Result<Option<u64>> {
        let desired = STREAM_WINDOW_BLOCK_SIZE
            .max(count as usize)
            .saturating_add(4096);
        let actual_size = pipe_size_best_effort(pipe_write, desired)?;
        if actual_size as u64 <= count {
            return Ok(None);
        }
        grow_pipe_best_effort(src_fd)?;
        grow_pipe_best_effort(libc::STDOUT_FILENO)?;
        let dev_null = OpenOptions::new().write(true).open("/dev/null")?;
        let dev_null_fd = dev_null.as_raw_fd();
        let mut buffered = 0u64;
        loop {
            let free_space = (actual_size as u64).saturating_sub(buffered);
            if free_space == 0 {
                return Ok(None);
            }
            let read_len = free_space.min(FAST_COPY_SPLICE_CHUNK_SIZE as u64) as usize;
            let moved = unsafe {
                libc::splice(
                    src_fd,
                    std::ptr::null_mut(),
                    pipe_write,
                    std::ptr::null_mut(),
                    read_len,
                    0,
                )
            };
            if moved > 0 {
                buffered = buffered
                    .checked_add(moved as u64)
                    .ok_or_else(|| io::Error::other("pipe tail byte count overflow"))?;
                if buffered > count {
                    let drop_len = buffered - count;
                    let dropped = splice_all(pipe_read, dev_null_fd, drop_len)?;
                    if dropped != drop_len {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "pipe tail short drop while trimming prefix",
                        ));
                    }
                    buffered -= dropped;
                }
                continue;
            }
            if moved == 0 {
                let emitted = splice_all(pipe_read, libc::STDOUT_FILENO, buffered)?;
                return Ok(Some(emitted));
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR | libc::EAGAIN) => continue,
                Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
                    return Ok(None)
                }
                _ => return Err(err),
            }
        }
    })();
    unsafe {
        libc::close(pipe_read);
        libc::close(pipe_write);
    }
    result
}

pub(crate) fn copy_pipe_tail_to_stdout_large(
    src_fd: libc::c_int,
    count: u64,
) -> io::Result<Option<u64>> {
    if count == 0 {
        return Ok(Some(0));
    }
    if !fd_is_fifo(src_fd)? {
        return Ok(None);
    }
    let window_size = count.saturating_add((STREAM_WINDOW_BLOCK_SIZE - 1) as u64)
        / STREAM_WINDOW_BLOCK_SIZE as u64
        * STREAM_WINDOW_BLOCK_SIZE as u64;
    let window_len = usize::try_from(window_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "tail byte window does not fit in usize",
        )
    })?;
    let mut window = AlignedBuffer::new_uninit(window_len)?;
    let mut total = 0u64;
    let mut filled = 0usize;
    let mut write_pos = 0usize;
    loop {
        let remaining = window_len - write_pos;
        let read = unsafe {
            libc::read(
                src_fd,
                window.as_mut_slice()[write_pos..].as_mut_ptr().cast(),
                remaining,
            )
        };
        if read > 0 {
            let read = read as usize;
            total = total
                .checked_add(read as u64)
                .ok_or_else(|| io::Error::other("pipe tail total overflow"))?;
            filled = filled.saturating_add(read).min(window_len);
            write_pos += read;
            if write_pos == window_len {
                write_pos = 0;
            }
            continue;
        }
        if read == 0 {
            break;
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN) => continue,
            _ => return Err(err),
        }
    }
    let emit_len = (total.min(count)) as usize;
    if emit_len == 0 {
        return Ok(Some(0));
    }
    let start = (write_pos + window_len - emit_len) % window_len;
    if start + emit_len <= window_len {
        write_raw_fd_all(
            libc::STDOUT_FILENO,
            &window.as_slice()[start..start + emit_len],
        )?;
    } else {
        write_raw_fd_all(libc::STDOUT_FILENO, &window.as_slice()[start..])?;
        let split = emit_len - (window_len - start);
        write_raw_fd_all(libc::STDOUT_FILENO, &window.as_slice()[..split])?;
    }
    Ok(Some(emit_len as u64))
}

const FAST_COPY_SPLICE_CHUNK_SIZE: usize = 1 << 20;
const FAST_COPY_SENDFILE_CHUNK_SIZE: usize = 0x7fff_f000usize;

pub(crate) fn copy_regular_fd_to_fd_sendfile_counted<F>(
    src_fd: libc::c_int,
    dst_fd: libc::c_int,
    progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    if !fd_is_regular(src_fd)? {
        return Ok(None);
    }
    grow_pipe_best_effort(dst_fd)?;
    let mut total = 0u64;
    loop {
        let copied = unsafe {
            libc::sendfile(
                dst_fd,
                src_fd,
                std::ptr::null_mut(),
                FAST_COPY_SENDFILE_CHUNK_SIZE,
            )
        };
        if copied > 0 {
            total = total
                .checked_add(copied as u64)
                .ok_or_else(|| io::Error::other("sendfile byte count overflow"))?;
            progress(total)?;
            continue;
        }
        if copied == 0 {
            return Ok(Some(total));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => return Ok(None),
            _ => return Err(err),
        }
    }
}

pub(crate) fn copy_fd_to_fd_splice_maybe_limited_counted<F>(
    src_fd: libc::c_int,
    dst_fd: libc::c_int,
    limit: Option<u64>,
    progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    if !fd_is_fifo(src_fd)? && !fd_is_fifo(dst_fd)? {
        return Ok(None);
    }
    grow_pipe_best_effort(src_fd)?;
    grow_pipe_best_effort(dst_fd)?;
    let mut total = 0u64;
    if let Ok(mut ring) = IoUring::new(8) {
        loop {
            let chunk_size = match limit {
                Some(limit) => {
                    let remaining = limit.saturating_sub(total);
                    if remaining == 0 {
                        return Ok(Some(total));
                    }
                    remaining.min(FAST_COPY_SPLICE_CHUNK_SIZE as u64) as usize
                }
                None => FAST_COPY_SPLICE_CHUNK_SIZE,
            };
            let mut sqe = ring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            unsafe {
                sqe.prep_splice(
                    src_fd,
                    -1,
                    dst_fd,
                    -1,
                    chunk_size.try_into().unwrap(),
                    SpliceFlags::empty(),
                );
                sqe.set_user_data(0x5350_4c49_4345);
            }
            ring.submit_sqes().map_err(io::Error::other)?;
            let cqe = ring.wait_for_cqe().map_err(io::Error::other)?;
            match cqe.result() {
                Ok(copied) if copied > 0 => {
                    total = total
                        .checked_add(copied as u64)
                        .ok_or_else(|| io::Error::other("splice byte count overflow"))?;
                    progress(total)?;
                    continue;
                }
                Ok(0) => return Ok(Some(total)),
                Ok(_) => {}
                Err(err) => match err.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(
                        libc::EBADF | libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV,
                    ) => break,
                    _ => return Err(err),
                },
            }
        }
    }
    loop {
        let chunk_size = match limit {
            Some(limit) => {
                let remaining = limit.saturating_sub(total);
                if remaining == 0 {
                    return Ok(Some(total));
                }
                remaining.min(FAST_COPY_SPLICE_CHUNK_SIZE as u64) as usize
            }
            None => FAST_COPY_SPLICE_CHUNK_SIZE,
        };
        let copied = unsafe {
            libc::splice(
                src_fd,
                std::ptr::null_mut(),
                dst_fd,
                std::ptr::null_mut(),
                chunk_size,
                0,
            )
        };
        if copied > 0 {
            total = total
                .checked_add(copied as u64)
                .ok_or_else(|| io::Error::other("splice byte count overflow"))?;
            progress(total)?;
            continue;
        }
        if copied == 0 {
            return Ok(Some(total));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => return Ok(None),
            _ => return Err(err),
        }
    }
}

pub(crate) fn copy_fd_to_fd_splice_counted<F>(
    src_fd: libc::c_int,
    dst_fd: libc::c_int,
    progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    copy_fd_to_fd_splice_maybe_limited_counted(src_fd, dst_fd, None, progress)
}

pub(crate) fn copy_fd_to_fd_splice_limited_counted<F>(
    src_fd: libc::c_int,
    dst_fd: libc::c_int,
    limit: u64,
    progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    copy_fd_to_fd_splice_maybe_limited_counted(src_fd, dst_fd, Some(limit), progress)
}

pub(crate) fn try_fast_copy_to_stdout_counted<F>(
    input: &StreamInput,
    io_mode: IOMode,
    progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    if io_mode == IOMode::Direct {
        return Ok(None);
    }
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let file = std::fs::File::open(path)?;
            copy_regular_fd_to_fd_sendfile_counted(file.as_raw_fd(), libc::STDOUT_FILENO, progress)
        }
        StreamInput::Stdin { .. } => {
            if let Some(bytes) = copy_regular_fd_to_fd_sendfile_counted(
                libc::STDIN_FILENO,
                libc::STDOUT_FILENO,
                progress,
            )? {
                return Ok(Some(bytes));
            }
            copy_fd_to_fd_splice_counted(libc::STDIN_FILENO, libc::STDOUT_FILENO, progress)
        }
        StreamInput::File(path) => {
            let file_type = fs::metadata(path)?.file_type();
            if file_type.is_fifo() {
                let file = std::fs::File::open(path)?;
                return copy_fd_to_fd_splice_counted(
                    file.as_raw_fd(),
                    libc::STDOUT_FILENO,
                    progress,
                );
            }
            Ok(None)
        }
    }
}

pub(crate) fn try_fast_cat_copy(input: &StreamInput, io_mode: IOMode) -> io::Result<bool> {
    let mut noop = |_bytes: u64| Ok(());
    Ok(try_fast_copy_to_stdout_counted(input, io_mode, &mut noop)?.is_some())
}

pub(crate) fn visit_ordered_blocks<F>(
    path: &str,
    io_mode: IOMode,
    mut on_block: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>)>();
    let sender = tx.clone();
    let visit_result = visit_blocks_with_mode(path, io_mode, move |block_index, data| {
        sender
            .send((block_index, data.to_vec()))
            .map_err(|_| io::Error::other("failed to queue ordered block"))
    });
    drop(tx);

    let mut next_block = 0usize;
    let mut pending = BTreeMap::<usize, Vec<u8>>::new();
    while let Ok((block_index, data)) = rx.recv() {
        pending.insert(block_index, data);
        while let Some(block) = pending.remove(&next_block) {
            on_block(&block)?;
            next_block += 1;
        }
    }
    if !pending.is_empty() {
        return Err(io::Error::other(
            "missing block data while finalizing ordered visitor",
        ));
    }
    visit_result?;
    Ok(())
}
