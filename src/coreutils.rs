use crate::config::load_config;
use crate::differ::diff_files;
use crate::reader::{
    grep_match_offsets_for_mode, load_file_to_memory_for_mode, map_file_blocks_for_mode, BufReader,
    LoadedFile,
};
use crate::writer::{write_generated_file, BufWriter, GeneratedWritePattern};
use fro::{hash_file, read_file_with_mode, visit_blocks_with_mode, HashAlgorithm, IOMode};
use iou::sqe::SpliceFlags;
use iou::IoUring;
use memchr::{memchr_iter, memmem::Finder};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};

const DEFAULT_SHRED_PASSES: usize = 1;
const FIND_OUTPUT_CHUNK_BYTES: usize = 1 << 20;
pub fn is_coreutils_command(name: &str) -> bool {
    matches!(
        name,
        "cat"
            | "cmp"
            | "fgrep"
            | "find"
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

fn run_named_command(invoked: &str, args: &[String]) -> io::Result<Option<i32>> {
    let code = match invoked {
        "cat" => {
            run_cat(args)?;
            0
        }
        "cmp" => run_cmp(args)?,
        "fgrep" => run_fgrep(args)?,
        "find" => {
            run_find(args)?;
            0
        }
        "tac" => {
            run_tac(args)?;
            0
        }
        "wc" => {
            run_wc(args)?;
            0
        }
        "cksum" => {
            run_cksum(args)?;
            0
        }
        "b3sum" => {
            run_hash_sum(args, HashAlgorithm::Blake3)?;
            0
        }
        "b2sum" => {
            run_hash_sum(args, HashAlgorithm::Blake2b512)?;
            0
        }
        "md5sum" => {
            run_hash_sum(args, HashAlgorithm::Md5)?;
            0
        }
        "sha224sum" => {
            run_hash_sum(args, HashAlgorithm::Sha224)?;
            0
        }
        "sha256sum" => {
            run_hash_sum(args, HashAlgorithm::Sha256)?;
            0
        }
        "sha384sum" => {
            run_hash_sum(args, HashAlgorithm::Sha384)?;
            0
        }
        "sha512sum" => {
            run_hash_sum(args, HashAlgorithm::Sha512)?;
            0
        }
        "shred" => {
            run_shred(args)?;
            0
        }
        _ => return Ok(None),
    };
    Ok(Some(code))
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
                    Some(libc::EBADF | libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
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

fn try_fast_cat_copy(input: &StreamInput, io_mode: IOMode) -> io::Result<bool> {
    if io_mode == IOMode::Direct {
        return Ok(false);
    }
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => copy_regular_file_to_stdout_sendfile(path),
        StreamInput::Stdin { .. } => copy_stdin_to_stdout_splice(),
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

#[derive(Debug, Clone, Copy)]
struct WcCountOptions {
    lines: bool,
    words: bool,
    bytes: bool,
}

fn count_wc_block(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    let lines = if options.lines {
        memchr_iter(b'\n', block).count() as u64
    } else {
        0
    };
    let bytes = if options.bytes { block.len() as u64 } else { 0 };

    if !options.words {
        return WcBlockCounts {
            lines,
            words: 0,
            bytes,
            starts_in_word: false,
            ends_in_word: false,
        };
    }

    let mut words = 0_u64;
    let mut prev_is_whitespace = true;
    for &byte in block {
        let is_whitespace = WC_WHITESPACE_TABLE[byte as usize] != 0;
        words += u64::from(!is_whitespace && prev_is_whitespace);
        prev_is_whitespace = is_whitespace;
    }

    WcBlockCounts {
        lines,
        words,
        bytes,
        starts_in_word: block
            .first()
            .is_some_and(|byte| !is_wc_whitespace(*byte)),
        ends_in_word: block
            .last()
            .is_some_and(|byte| !is_wc_whitespace(*byte)),
    }
}

fn wc_totals_from_reader<R: Read>(reader: &mut R, options: WcCountOptions) -> io::Result<WcTotals> {
    if options.bytes && !options.lines && !options.words {
        let mut totals = WcTotals {
            lines: 0,
            words: 0,
            bytes: 0,
        };
        let mut buffer = vec![0_u8; 8 << 20];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                return Ok(totals);
            }
            totals.bytes += read as u64;
        }
    }

    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    let mut buffer = vec![0_u8; 8 << 20];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(totals);
        }
        let block = &buffer[..read];
        let counts = count_wc_block(block, options);
        totals.lines += counts.lines;
        totals.words += counts.words;
        totals.bytes += counts.bytes;
        if options.words && previous_ended_in_word && counts.starts_in_word {
            totals.words = totals.words.saturating_sub(1);
        }
        previous_ended_in_word = options.words && counts.ends_in_word;
    }
}

fn wc_totals_from_reader_parallel<R: Read>(
    reader: &mut R,
    options: WcCountOptions,
) -> io::Result<WcTotals> {
    wc_totals_from_reader(reader, options)
}

fn write_wc_result<W: Write>(
    out: &mut W,
    totals: WcTotals,
    label: Option<&str>,
    print_lines: bool,
    print_words: bool,
    print_bytes: bool,
) -> io::Result<()> {
    let mut first = true;
    for (enabled, value) in [
        (print_lines, totals.lines),
        (print_words, totals.words),
        (print_bytes, totals.bytes),
    ] {
        if enabled {
            if !first {
                write!(out, " ")?;
            }
            write!(out, "{value}")?;
            first = false;
        }
    }
    if let Some(label) = label {
        writeln!(out, " {label}")
    } else {
        writeln!(out)
    }
}

fn write_matching_stream_lines<R: BufRead, W: Write>(
    out: &mut W,
    label: Option<&str>,
    reader: &mut R,
    pattern: &[u8],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<bool> {
    let finder = Finder::new(pattern);
    let mut matched_any = false;
    let mut line = Vec::new();
    let mut line_no = 1_u64;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(matched_any);
        }
        if finder.find(&line).is_some() {
            matched_any = true;
            if multi_file {
                if let Some(label) = label {
                    write!(out, "{label}:")?;
                }
            }
            if print_line_numbers {
                write!(out, "{line_no}:")?;
            }
            out.write_all(&line)?;
        }
        line_no += 1;
    }
}

fn count_newlines_up_to(
    path: &str,
    io_mode: IOMode,
    mode: &str,
    end_offset: u64,
) -> io::Result<u64> {
    let data = load_file_bytes(path, io_mode, mode)?;
    let bytes = data.data.as_slice();
    let end = usize::try_from(end_offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?
        .min(bytes.len());
    Ok(memchr_iter(b'\n', &bytes[..end]).count() as u64)
}

fn write_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    matches: &[u64],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<()> {
    let bytes = data;
    let mut next_match = 0usize;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < bytes.len() {
        let rel_end = bytes[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(bytes.len() - line_start);
        let line_end = line_start + rel_end;
        let mut matched = false;
        while next_match < matches.len() && matches[next_match] < line_end as u64 {
            if matches[next_match] >= line_start as u64 {
                matched = true;
            }
            next_match += 1;
        }
        if matched {
            if multi_file {
                write!(out, "{}:", file)?;
            }
            if print_line_numbers {
                write!(out, "{}:", line_no)?;
            }
            out.write_all(&bytes[line_start..line_end])?;
        }
        line_start = line_end;
        line_no += 1;
    }
    Ok(())
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

fn run_cat(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    for input in inputs {
        if try_fast_cat_copy(&input, io_mode)? {
            continue;
        }
        copy_file_like_to_output(&mut out, &input)?;
    }
    out.into_inner()
}

fn run_cmp(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file1> <file2>",
    )?;
    if files.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cmp requires exactly two file operands",
        ));
    }

    let config = load_config(None);
    let diff_page_cache = config.get_params_for_path("diff", false, &files[0]);
    let diff_direct = config.get_params_for_path("diff", true, &files[0]);
    let mismatch = diff_files(
        &files[0],
        &files[1],
        diff_page_cache.num_threads,
        diff_page_cache.block_size,
        diff_page_cache.qd,
        diff_direct.num_threads,
        diff_direct.block_size,
        diff_direct.qd,
        internal_io_mode(io_mode),
        false,
        false,
    )?;
    if mismatch != 0 {
        let index = mismatch as usize - 1;
        let line = 1 + count_newlines_up_to(&files[0], io_mode, "read", mismatch - 1)?;
        println!(
            "{} {} differ: byte {}, line {}",
            files[0],
            files[1],
            index + 1,
            line
        );
        return Ok(1);
    }

    let first_len = fs::metadata(&files[0])?.len();
    let second_len = fs::metadata(&files[1])?.len();
    let shared_len = first_len.min(second_len);
    if first_len != second_len {
        let eof_file = if first_len < second_len {
            &files[0]
        } else {
            &files[1]
        };
        let line = count_newlines_up_to(eof_file, io_mode, "read", shared_len)?;
        eprintln!(
            "cmp: EOF on {} after byte {}, line {}",
            eof_file, shared_len, line
        );
        return Ok(1);
    }

    Ok(0)
}

fn run_fgrep(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut print_line_numbers = false;
    let mut pattern = None::<String>;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-n" => print_line_numbers = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other if pattern.is_none() => pattern = Some(other.to_string()),
            other => files.push(other.to_string()),
        }
    }
    let pattern = pattern.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep requires a search pattern",
        )
    })?;
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    let mut matched_any = false;
    let multi_file = inputs.len() > 1;
    let config = load_config(None);
    for input in inputs {
        match input {
            StreamInput::File(file) if is_regular_input_path(&file)? => {
                let (matches, _) = grep_match_offsets_for_mode(
                    &config,
                    "grep",
                    &file,
                    internal_io_mode(io_mode),
                    pattern.as_bytes(),
                )?;
                if matches.is_empty() {
                    continue;
                }
                matched_any = true;
                let data = load_file_bytes(&file, io_mode, "read_to_memory")?;
                write_matching_lines(
                    &mut out,
                    &file,
                    data.data.as_slice(),
                    &matches,
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::File(file) => {
                let mut reader = BufReader::new(std::fs::File::open(&file)?);
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    Some(&file),
                    &mut reader,
                    pattern.as_bytes(),
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::Stdin { label } => {
                let mut reader = stdin_buf_reader()?;
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    label.as_deref(),
                    &mut reader,
                    pattern.as_bytes(),
                    multi_file,
                    print_line_numbers,
                )?;
            }
        }
    }
    out.into_inner()?;
    Ok(if matched_any { 0 } else { 1 })
}

fn run_find(args: &[String]) -> io::Result<()> {
    let roots = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        vec![".".to_string()]
    };
    let worker_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1);
    let config = load_config(None);
    let write_params = config.get_params("write", false);
    let output = Arc::new(BufWriter::stdout(
        write_params.qd,
        write_params.block_size,
        worker_count.saturating_mul(2),
    )?);
    let queue = Arc::new(FindWorkQueue::default());
    let stop = Arc::new(AtomicBool::new(false));

    for root in roots {
        let path = PathBuf::from(root);
        write_find_path(&output, &path)?;
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_dir() {
            queue.enqueue_one(path);
        }
    }

    let mut threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let queue = queue.clone();
        let output = output.clone();
        let stop = stop.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(start_dir) = queue.claim(&stop) {
                let result = walk_find_subtree(&start_dir, &queue, &output, &stop);
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
            .map_err(|_| io::Error::other("find worker thread panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }

    let output = Arc::into_inner(output)
        .ok_or_else(|| io::Error::other("find output writer still has active references"))?;
    let finish_result = output.into_inner();
    if let Some(err) = first_error {
        return Err(err);
    }
    finish_result
}

#[derive(Default)]
struct FindWorkQueue {
    state: Mutex<FindWorkState>,
    ready: Condvar,
}

#[derive(Default)]
struct FindWorkState {
    queue: VecDeque<PathBuf>,
    active_workers: usize,
}

impl FindWorkQueue {
    fn enqueue(&self, dirs: impl IntoIterator<Item = PathBuf>) {
        let mut state = self.state.lock().unwrap();
        let mut added = false;
        for dir in dirs {
            state.queue.push_back(dir);
            added = true;
        }
        if added {
            self.ready.notify_all();
        }
    }

    fn enqueue_one(&self, dir: PathBuf) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(dir);
        self.ready.notify_one();
    }

    fn claim(&self, stop: &AtomicBool) -> Option<PathBuf> {
        let mut state = self.state.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(dir) = state.queue.pop_front() {
                state.active_workers += 1;
                return Some(dir);
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

fn walk_find_subtree(
    start_dir: &Path,
    queue: &FindWorkQueue,
    output: &BufWriter,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start_dir.to_path_buf()];
    let mut chunk = Vec::with_capacity(FIND_OUTPUT_CHUNK_BYTES);
    while let Some(dir) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            append_find_path(&mut chunk, &path);
            if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                output.write_all(&std::mem::take(&mut chunk))?;
            }
            if entry.file_type()?.is_dir() {
                child_dirs.push(path);
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    output.write_all(&chunk)
}

fn write_find_path(output: &BufWriter, path: &Path) -> io::Result<()> {
    let mut chunk = Vec::with_capacity(path.as_os_str().as_bytes().len() + 1);
    append_find_path(&mut chunk, path);
    output.write_all(&chunk)
}

fn append_find_path(chunk: &mut Vec<u8>, path: &Path) {
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(b'\n');
}

fn run_tac(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    let out = stdout_buf_writer()?;
    for input in &inputs {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let mut parts = data
            .split_inclusive(|&byte| byte == b'\n')
            .collect::<Vec<_>>();
        if parts.is_empty() && !data.is_empty() {
            parts.push(data.as_slice());
        }
        for part in parts.into_iter().rev() {
            out.write_all(part)?;
        }
    }
    out.into_inner()
}

#[derive(Debug)]
struct WcBlockCounts {
    lines: u64,
    words: u64,
    bytes: u64,
    starts_in_word: bool,
    ends_in_word: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WcTotals {
    lines: u64,
    words: u64,
    bytes: u64,
}

const fn wc_whitespace_table() -> [u8; 256] {
    let mut table = [0u8; 256];
    table[b' ' as usize] = 1;
    table[b'\t' as usize] = 1;
    table[b'\n' as usize] = 1;
    table[0x0b] = 1;
    table[0x0c] = 1;
    table[b'\r' as usize] = 1;
    table
}

static WC_WHITESPACE_TABLE: [u8; 256] = wc_whitespace_table();

fn is_wc_whitespace(byte: u8) -> bool {
    WC_WHITESPACE_TABLE[byte as usize] != 0
}

fn reduce_wc_counts(blocks: &[WcBlockCounts]) -> WcTotals {
    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    for block in blocks {
        totals.lines += block.lines;
        totals.words += block.words;
        totals.bytes += block.bytes;
        if previous_ended_in_word && block.starts_in_word {
            totals.words -= 1;
        }
        previous_ended_in_word = block.ends_in_word;
    }
    totals
}

fn run_wc(args: &[String]) -> io::Result<()> {
    let mut print_lines = false;
    let mut print_words = false;
    let mut print_bytes = false;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-l" => print_lines = true,
            "-w" => print_words = true,
            "-c" => print_bytes = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
    }
    if !print_lines && !print_words && !print_bytes {
        print_lines = true;
        print_words = true;
        print_bytes = true;
    }
    let options = WcCountOptions {
        lines: print_lines,
        words: print_words,
        bytes: print_bytes,
    };

    let inputs = parse_stream_inputs(files);
    let config = load_config(None);
    let mut out = stdout_buf_writer()?;
    for input in inputs {
        let (totals, label) = match input {
            StreamInput::File(file) if is_regular_input_path(&file)? => {
                let blocks = map_file_blocks_for_mode(
                    &config,
                    "read",
                    &file,
                    internal_io_mode(io_mode),
                    move |block| {
                        Ok::<_, io::Error>(count_wc_block(block.data, options))
                    },
                )?;
                (reduce_wc_counts(&blocks.blocks), Some(file))
            }
            StreamInput::File(file) => {
                let mut reader = BufReader::new(std::fs::File::open(&file)?);
                (wc_totals_from_reader_parallel(&mut reader, options)?, Some(file))
            }
            StreamInput::Stdin { label } => {
                let mut reader = stdin_buf_reader()?;
                (wc_totals_from_reader_parallel(&mut reader, options)?, label)
            }
        };
        write_wc_result(
            &mut out,
            totals,
            label.as_deref(),
            print_lines,
            print_words,
            print_bytes,
        )?;
    }
    out.into_inner()
}

fn run_hash_sum(args: &[String], algorithm: HashAlgorithm) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    for input in inputs {
        let label = match &input {
            StreamInput::File(file) => Some(file.as_str()),
            StreamInput::Stdin { label } => Some(label.as_deref().unwrap_or("-")),
        };
        let digest = match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => {
                hash_file(file, algorithm, io_mode)?
            }
            _ => {
                let mut data = Vec::new();
                visit_ordered_input(&input, io_mode, |block| {
                    data.extend_from_slice(block);
                    Ok(())
                })?;
                match algorithm {
                    HashAlgorithm::Md5
                    | HashAlgorithm::Blake2b512
                    | HashAlgorithm::Sha224
                    | HashAlgorithm::Sha256
                    | HashAlgorithm::Sha384
                    | HashAlgorithm::Sha512 => {
                        let digest = openssl::hash::hash(
                            ordered_digest(algorithm).ok_or_else(|| {
                                io::Error::new(io::ErrorKind::InvalidInput, "unsupported digest")
                            })?,
                            &data,
                        )
                        .map_err(io::Error::other)?;
                        digest.to_vec()
                    }
                    HashAlgorithm::Blake3 => {
                        let mut hasher = blake3::Hasher::new();
                        hasher.update(&data);
                        hasher.finalize().as_bytes().to_vec()
                    }
                    HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "block hash sums do not support stream input",
                        ));
                    }
                }
            }
        };
        if let Some(label) = label {
            println!("{}  {}", hex_digest(&digest), label);
        } else {
            println!("{}", hex_digest(&digest));
        }
    }
    Ok(())
}

fn crc32_cksum_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn run_cksum(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    for input in inputs {
        let mut crc = 0_u32;
        let mut bytes = 0_u64;
        visit_ordered_input(&input, io_mode, |block| {
            crc = crc32_cksum_update(crc, block);
            bytes += block.len() as u64;
            Ok(())
        })?;
        let mut length = bytes;
        while length != 0 {
            crc = crc32_cksum_update(crc, &[(length & 0xff) as u8]);
            length >>= 8;
        }
        match input {
            StreamInput::File(file) => println!("{} {} {}", !crc, bytes, file),
            StreamInput::Stdin { label: Some(label) } => println!("{} {} {}", !crc, bytes, label),
            StreamInput::Stdin { label: None } => println!("{} {}", !crc, bytes),
        }
    }
    Ok(())
}

fn ordered_digest(algorithm: HashAlgorithm) -> Option<openssl::hash::MessageDigest> {
    match algorithm {
        HashAlgorithm::Md5 => Some(openssl::hash::MessageDigest::md5()),
        HashAlgorithm::Blake2b512 => openssl::hash::MessageDigest::from_name("BLAKE2b512"),
        HashAlgorithm::Sha224 => Some(openssl::hash::MessageDigest::sha224()),
        HashAlgorithm::Sha256 => Some(openssl::hash::MessageDigest::sha256()),
        HashAlgorithm::Sha384 => Some(openssl::hash::MessageDigest::sha384()),
        HashAlgorithm::Sha512 => Some(openssl::hash::MessageDigest::sha512()),
        HashAlgorithm::Blake3 | HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => None,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn run_shred(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let mut passes = DEFAULT_SHRED_PASSES;
    let mut zero_last = false;
    let mut remove_after = false;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-n" => {
                i += 1;
                let count = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for -n")
                })?;
                passes = count.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid pass count")
                })?;
            }
            "-z" => zero_last = true,
            "-u" => remove_after = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    let files = ensure_files(
        program,
        files,
        "[-n passes] [-z] [-u] [--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    for file in files {
        let size = fs::metadata(&file)?.len();
        for _ in 0..passes {
            overwrite_with_pattern(&file, size, io_mode, true)?;
        }
        if zero_last {
            overwrite_with_pattern(&file, size, io_mode, false)?;
        }
        if remove_after {
            fs::remove_file(&file)?;
        }
    }
    Ok(())
}

fn overwrite_with_pattern(path: &str, size: u64, io_mode: IOMode, random: bool) -> io::Result<()> {
    if size == 0 {
        return Ok(());
    }
    let config = load_config(None);
    let page_cache = config.get_params_for_path("write", false, path);
    let direct = config.get_params_for_path("write", true, path);
    write_generated_file(
        path,
        size,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        internal_io_mode(io_mode),
        if random {
            GeneratedWritePattern::Random
        } else {
            GeneratedWritePattern::Zero
        },
    )?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()
}

#[cfg(test)]
mod tests {
    use super::{is_wc_whitespace, reduce_wc_counts, WcBlockCounts, WcTotals};

    #[test]
    fn reduce_wc_counts_merges_cross_block_words() {
        let blocks = [
            WcBlockCounts {
                lines: 0,
                words: 1,
                bytes: 3,
                starts_in_word: true,
                ends_in_word: true,
            },
            WcBlockCounts {
                lines: 1,
                words: 1,
                bytes: 4,
                starts_in_word: true,
                ends_in_word: false,
            },
        ];

        assert_eq!(
            reduce_wc_counts(&blocks),
            WcTotals {
                lines: 1,
                words: 1,
                bytes: 7,
            }
        );
    }

    #[test]
    fn wc_whitespace_matches_posix_ascii_set() {
        for byte in [b' ', b'\t', b'\n', 0x0b, 0x0c, b'\r'] {
            assert!(is_wc_whitespace(byte), "byte {byte:#x} should split words");
        }
        for byte in [0_u8, b'a', 0x1c, 0x7f, 0x80, 0xff] {
            assert!(
                !is_wc_whitespace(byte),
                "byte {byte:#x} should not split words"
            );
        }
    }
}
