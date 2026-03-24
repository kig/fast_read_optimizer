use crate::config::load_config;
use crate::differ::diff_files_window;
use crate::reader::{
    grep_match_offsets_for_mode, load_file_to_memory_for_mode, map_file_blocks_for_mode,
    visit_file_blocks, BufReader, LoadedFile,
};
use crate::writer::{write_generated_file, BufWriter, GeneratedWritePattern};
use fro::{hash_file, read_file_with_mode, visit_blocks_with_mode, HashAlgorithm, IOMode};
use iou::sqe::SpliceFlags;
use iou::IoUring;
use memchr::{memchr_iter, memmem::Finder};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::{CStr, CString};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};

mod base64;
mod cat;
mod cmp;
mod du;
mod fgrep;
mod find;
mod hash;
mod shred;
mod tac;
mod wc;

pub(crate) use base64::{parse_base64_encode_kernel, Base64EncodeKernel};
const FRO_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn is_coreutils_command(name: &str) -> bool {
    matches!(
        name,
        "cat"
            | "base64"
            | "cmp"
            | "dd"
            | "fgrep"
            | "find"
            | "du"
            | "tac"
            | "wc"
            | "cksum"
            | "b3sum"
            | "b2sum"
            | "md5sum"
            | "sha224sum"
            | "sha256sum"
            | "sha384sum"
            | "sha512sum"
            | "shred"
    )
}

pub fn rewrite_alias_args(args: Vec<String>) -> Vec<String> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return args;
    };
    let Some(mode) = (match invoked.as_str() {
        "cp" => Some("copy"),
        _ => None,
    }) else {
        return args;
    };

    let mut rewritten = Vec::with_capacity(args.len() + 1);
    rewritten.push(args[0].clone());
    rewritten.push(mode.to_string());
    rewritten.extend(args.into_iter().skip(1));
    rewritten
}

pub fn rewrite_subcommand_alias(args: Vec<String>) -> Vec<String> {
    if args.get(1).map(String::as_str) == Some("cp") {
        let mut rewritten = args;
        rewritten[1] = "copy".to_string();
        return rewritten;
    }
    args
}

pub fn try_run_multicall(args: &[String]) -> io::Result<Option<i32>> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return Ok(None);
    };
    run_named_command(&invoked, args)
}

pub fn try_run_subcommand(
    program: &str,
    command: &str,
    command_args: &[String],
) -> io::Result<Option<i32>> {
    if !is_coreutils_command(command) {
        return Ok(None);
    }
    let mut args = Vec::with_capacity(command_args.len() + 1);
    args.push(format!("{program} {command}"));
    args.extend(command_args.iter().cloned());
    run_named_command(command, &args)
}

pub(crate) fn bench_base64_encode(iterations: u64, kernel: Base64EncodeKernel) -> io::Result<()> {
    base64::bench_base64_encode(iterations, kernel)
}

fn run_named_command(invoked: &str, args: &[String]) -> io::Result<Option<i32>> {
    if !is_coreutils_command(invoked) {
        return Ok(None);
    }
    if args.get(1).map(String::as_str) == Some("--version") {
        print_coreutils_version(invoked);
        return Ok(Some(0));
    }
    let code = match invoked {
        "cat" => {
            cat::run_cat(args)?;
            0
        }
        "base64" => base64::run_base64(args)?,
        "cmp" => cmp::run_cmp(args)?,
        "dd" => {
            fro::dd_tool::run_dd(args)?;
            0
        }
        "fgrep" => fgrep::run_fgrep(args)?,
        "find" => find::run_find(args)?,
        "du" => du::run_du(args)?,
        "tac" => {
            tac::run_tac(args)?;
            0
        }
        "wc" => {
            wc::run_wc(args)?;
            0
        }
        "cksum" => {
            hash::run_cksum(args)?;
            0
        }
        "b3sum" => hash::run_hash_sum(args, HashAlgorithm::Blake3)?,
        "b2sum" => hash::run_hash_sum(args, HashAlgorithm::Blake2b512)?,
        "md5sum" => hash::run_hash_sum(args, HashAlgorithm::Md5)?,
        "sha224sum" => hash::run_hash_sum(args, HashAlgorithm::Sha224)?,
        "sha256sum" => hash::run_hash_sum(args, HashAlgorithm::Sha256)?,
        "sha384sum" => hash::run_hash_sum(args, HashAlgorithm::Sha384)?,
        "sha512sum" => hash::run_hash_sum(args, HashAlgorithm::Sha512)?,
        "shred" => {
            shred::run_shred(args)?;
            0
        }
        _ => return Ok(None),
    };
    Ok(Some(code))
}

fn print_coreutils_version(invoked: &str) {
    println!("{invoked} (fro coreutils) {FRO_VERSION}");
}
fn permission_denied_components(kind: io::ErrorKind, raw_os_error: Option<i32>) -> bool {
    matches!(kind, io::ErrorKind::PermissionDenied)
        || matches!(raw_os_error, Some(libc::EACCES | libc::EPERM))
}

fn is_permission_denied(err: &io::Error) -> bool {
    permission_denied_components(err.kind(), err.raw_os_error())
}

fn write_warning_line(tool: &str, path: &Path, err: &io::Error, message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{tool}: {message} '{}': {err}", path.display());
}

fn invoked_name(program: &str) -> Option<String> {
    Path::new(program)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn parse_io_mode(args: &[String]) -> io::Result<(IOMode, Vec<String>)> {
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

fn ensure_files(program: &str, files: Vec<String>, usage: &str) -> io::Result<Vec<String>> {
    if files.is_empty() {
        eprintln!("Usage: {} {}", program, usage);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing file operand",
        ));
    }
    Ok(files)
}

fn internal_io_mode(io_mode: IOMode) -> crate::common::IOMode {
    match io_mode {
        IOMode::Auto => crate::common::IOMode::Auto,
        IOMode::Direct => crate::common::IOMode::Direct,
        IOMode::PageCache => crate::common::IOMode::PageCache,
    }
}

fn load_file_bytes(path: &str, io_mode: IOMode, mode: &str) -> io::Result<LoadedFile> {
    let config = load_config(None);
    load_file_to_memory_for_mode(&config, mode, path, internal_io_mode(io_mode))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamInput {
    File(String),
    Stdin { label: Option<String> },
}

fn parse_stream_inputs(files: Vec<String>) -> Vec<StreamInput> {
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

fn stdout_buf_writer() -> io::Result<BufWriter> {
    let config = load_config(None);
    let params = config.get_params("write", false);
    BufWriter::stdout(params.qd, params.block_size, 4)
}

fn stdin_buf_reader() -> io::Result<BufReader<std::fs::File>> {
    BufReader::stdin()
}

fn is_regular_input_path(path: &str) -> io::Result<bool> {
    if path.starts_with("/dev/fd/") || path.starts_with("/proc/self/fd/") {
        return Ok(false);
    }
    Ok(fs::metadata(path)?.file_type().is_file())
}

fn visit_reader_blocks<R, F>(reader: &mut R, mut on_block: F) -> io::Result<()>
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

fn visit_ordered_input<F>(input: &StreamInput, io_mode: IOMode, on_block: F) -> io::Result<()>
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

fn loaded_or_stream_bytes(input: &StreamInput, io_mode: IOMode) -> io::Result<Vec<u8>> {
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

fn copy_file_like_to_output<W: Write>(out: &mut W, input: &StreamInput) -> io::Result<()> {
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

fn fd_is_fifo(fd: libc::c_int) -> io::Result<bool> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_mode & libc::S_IFMT) == libc::S_IFIFO)
}

fn fd_is_regular(fd: libc::c_int) -> io::Result<bool> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_mode & libc::S_IFMT) == libc::S_IFREG)
}

fn grow_pipe_best_effort(fd: libc::c_int) -> io::Result<()> {
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

fn copy_regular_file_to_stdout_sendfile(path: &str) -> io::Result<bool> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    grow_pipe_best_effort(libc::STDOUT_FILENO)?;
    let mut offset = 0 as libc::off_t;
    let max_chunk = 0x7fff_f000usize;
    while (offset as u64) < file_len {
        let remaining = (file_len - offset as u64).min(max_chunk as u64) as usize;
        let copied = unsafe {
            libc::sendfile(
                libc::STDOUT_FILENO,
                file.as_raw_fd(),
                &mut offset,
                remaining,
            )
        };
        if copied > 0 {
            continue;
        }
        if copied == 0 {
            break;
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => return Ok(false),
            _ => return Err(err),
        }
    }
    Ok(offset as u64 == file_len)
}

fn copy_stdin_to_stdout_splice() -> io::Result<bool> {
    if !fd_is_fifo(libc::STDIN_FILENO)? && !fd_is_fifo(libc::STDOUT_FILENO)? {
        return Ok(false);
    }
    grow_pipe_best_effort(libc::STDIN_FILENO)?;
    grow_pipe_best_effort(libc::STDOUT_FILENO)?;
    if let Ok(mut ring) = IoUring::new(8) {
        loop {
            let mut sqe = ring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            unsafe {
                sqe.prep_splice(
                    libc::STDIN_FILENO,
                    -1,
                    libc::STDOUT_FILENO,
                    -1,
                    1 << 20,
                    SpliceFlags::empty(),
                );
                sqe.set_user_data(0x5350_4c49_4345);
            }
            ring.submit_sqes().map_err(io::Error::other)?;
            let cqe = ring.wait_for_cqe().map_err(io::Error::other)?;
            match cqe.result() {
                Ok(copied) if copied > 0 => continue,
                Ok(0) => return Ok(true),
                Ok(_) => {}
                Err(err) => match err.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(
                        libc::EBADF | libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV,
                    ) => {
                        break;
                    }
                    _ => return Err(err),
                },
            }
        }
    }
    loop {
        let copied = unsafe {
            libc::splice(
                libc::STDIN_FILENO,
                std::ptr::null_mut(),
                libc::STDOUT_FILENO,
                std::ptr::null_mut(),
                1 << 20,
                0,
            )
        };
        if copied > 0 {
            continue;
        }
        if copied == 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => return Ok(false),
            _ => return Err(err),
        }
    }
}

fn copy_stdin_to_stdout_sendfile() -> io::Result<bool> {
    if !fd_is_regular(libc::STDIN_FILENO)? {
        return Ok(false);
    }
    grow_pipe_best_effort(libc::STDOUT_FILENO)?;
    let max_chunk = 0x7fff_f000usize;
    loop {
        let copied = unsafe {
            libc::sendfile(
                libc::STDOUT_FILENO,
                libc::STDIN_FILENO,
                std::ptr::null_mut(),
                max_chunk,
            )
        };
        if copied > 0 {
            continue;
        }
        if copied == 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => return Ok(false),
            _ => return Err(err),
        }
    }
}

fn copy_stdin_to_stdout_fast() -> io::Result<bool> {
    if copy_stdin_to_stdout_sendfile()? {
        return Ok(true);
    }
    copy_stdin_to_stdout_splice()
}

fn try_fast_cat_copy(input: &StreamInput, io_mode: IOMode) -> io::Result<bool> {
    if io_mode == IOMode::Direct {
        return Ok(false);
    }
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            copy_regular_file_to_stdout_sendfile(path)
        }
        StreamInput::Stdin { .. } => copy_stdin_to_stdout_fast(),
        StreamInput::File(path) => {
            let file_type = fs::metadata(path)?.file_type();
            if file_type.is_fifo() {
                let file = std::fs::File::open(path)?;
                grow_pipe_best_effort(file.as_raw_fd())?;
                grow_pipe_best_effort(libc::STDOUT_FILENO)?;
                if let Ok(mut ring) = IoUring::new(8) {
                    loop {
                        let mut sqe = ring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        unsafe {
                            sqe.prep_splice(
                                file.as_raw_fd(),
                                -1,
                                libc::STDOUT_FILENO,
                                -1,
                                1 << 20,
                                SpliceFlags::empty(),
                            );
                            sqe.set_user_data(0x5350_4c49_4345);
                        }
                        ring.submit_sqes().map_err(io::Error::other)?;
                        let cqe = ring.wait_for_cqe().map_err(io::Error::other)?;
                        match cqe.result() {
                            Ok(copied) if copied > 0 => continue,
                            Ok(0) => return Ok(true),
                            Ok(_) => {}
                            Err(err) => match err.raw_os_error() {
                                Some(libc::EINTR) => continue,
                                Some(
                                    libc::EBADF
                                    | libc::EINVAL
                                    | libc::ENOSYS
                                    | libc::EOPNOTSUPP
                                    | libc::EXDEV,
                                ) => break,
                                _ => return Err(err),
                            },
                        }
                    }
                }
                loop {
                    let copied = unsafe {
                        libc::splice(
                            file.as_raw_fd(),
                            std::ptr::null_mut(),
                            libc::STDOUT_FILENO,
                            std::ptr::null_mut(),
                            1 << 20,
                            0,
                        )
                    };
                    if copied > 0 {
                        continue;
                    }
                    if copied == 0 {
                        return Ok(true);
                    }
                    let err = io::Error::last_os_error();
                    match err.raw_os_error() {
                        Some(libc::EINTR) => continue,
                        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
                            return Ok(false);
                        }
                        _ => return Err(err),
                    }
                }
            }
            Ok(false)
        }
    }
}

fn visit_ordered_blocks<F>(path: &str, io_mode: IOMode, mut on_block: F) -> io::Result<()>
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
struct WorkQueue<T> {
    state: Mutex<WorkState<T>>,
    ready: Condvar,
}

struct WorkState<T> {
    queue: VecDeque<T>,
    active_workers: usize,
}

impl<T> Default for WorkQueue<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(WorkState {
                queue: VecDeque::new(),
                active_workers: 0,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<T> WorkQueue<T> {
    fn enqueue(&self, items: impl IntoIterator<Item = T>) {
        let mut state = self.state.lock().unwrap();
        let mut added = false;
        for item in items {
            state.queue.push_back(item);
            added = true;
        }
        if added {
            self.ready.notify_all();
        }
    }

    fn enqueue_one(&self, item: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(item);
        self.ready.notify_one();
    }

    fn claim(&self, stop: &AtomicBool) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(item) = state.queue.pop_front() {
                state.active_workers += 1;
                return Some(item);
            }
            if state.active_workers == 0 {
                return None;
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    fn complete_claim(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_workers = state.active_workers.saturating_sub(1);
        self.ready.notify_all();
    }

    fn wake_all(&self) {
        self.ready.notify_all();
    }
}
fn run_parallel_work_queue<T, F>(
    queue: Arc<WorkQueue<T>>,
    stop: Arc<AtomicBool>,
    worker_count: usize,
    run_task: F,
) -> io::Result<()>
where
    T: Send + 'static,
    F: Fn(T, &WorkQueue<T>, &AtomicBool) -> io::Result<()> + Send + Sync + 'static,
{
    let run_task = Arc::new(run_task);
    let mut threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let queue = queue.clone();
        let stop = stop.clone();
        let run_task = run_task.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(item) = queue.claim(&stop) {
                let result = run_task(item, &queue, &stop);
                queue.complete_claim();
                if let Err(err) = result {
                    stop.store(true, Ordering::SeqCst);
                    queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("directory walk worker thread panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}
