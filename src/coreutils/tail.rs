use super::*;
use crate::common::AlignedBuffer;
use fro::{
    copy_path_range_to_fd_with_progress, visit_path_range_ordered, ByteRange, OrderedVisitDecision,
};
use memchr::memrchr_iter;
use std::collections::VecDeque;
use std::ffi::CStr;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

mod windowed;
use windowed::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TailCount {
    FromEnd(u64),
    FromStart(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TailMode {
    Lines(TailCount),
    Bytes(TailCount),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeaderMode {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RecordTerminator {
    #[default]
    Newline,
    Nul,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FollowMode {
    Descriptor,
    Name,
}

#[derive(Clone, Debug)]
struct TailFollowOptions {
    mode: Option<FollowMode>,
    retry: bool,
    sleep_interval: Duration,
    pid: Option<libc::pid_t>,
    max_unchanged_stats: u64,
}

impl Default for TailFollowOptions {
    fn default() -> Self {
        Self {
            mode: None,
            retry: false,
            sleep_interval: Duration::from_secs(1),
            pid: None,
            max_unchanged_stats: 5,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

#[derive(Debug)]
struct TailFollowState {
    path: String,
    header_label: String,
    input_index: usize,
    mode: FollowMode,
    offset: u64,
    file: Option<File>,
    identity: Option<FileIdentity>,
    ever_opened: bool,
    missing_reported: bool,
    unchanged_iterations: u64,
}

impl RecordTerminator {
    fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
}

const TAIL_SCAN_BLOCK_SIZE: usize = 1 << 20;
const TAIL_PIPE_WINDOW_SIZE: usize = 1 << 20;
const TAIL_PIPE_WINDOW_MIN_CAPACITY: usize = 64 << 10;
const TAIL_STDIN_PREBUFFER_LIMIT: usize = 64 << 10;

fn parse_follow_mode(value: &str, flag: &str) -> io::Result<FollowMode> {
    match value {
        "descriptor" => Ok(FollowMode::Descriptor),
        "name" => Ok(FollowMode::Name),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        )),
    }
}

fn parse_sleep_interval(value: &str, flag: &str) -> io::Result<Duration> {
    let seconds = value.parse::<f64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        )
    })?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        ));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn parse_pid(value: &str, flag: &str) -> io::Result<libc::pid_t> {
    let pid = value.parse::<i32>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        )
    })?;
    if pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        ));
    }
    Ok(pid)
}

fn parse_follow_u64(value: &str, flag: &str) -> io::Result<u64> {
    value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid argument for {flag}: {value}"),
        )
    })
}

fn regular_stdin_path() -> io::Result<Option<&'static str>> {
    if fd_is_regular(fro::command_io::stdin_fd())? {
        Ok(Some("/proc/self/fd/0"))
    } else {
        Ok(None)
    }
}

fn parse_tail_count(value: &str, flag: &str) -> io::Result<TailCount> {
    let raw_value = value.trim();
    if raw_value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {raw_value}"),
        ));
    }
    let (from_start, value) = match raw_value.as_bytes()[0] {
        b'+' => (true, &raw_value[1..]),
        b'-' => (false, &raw_value[1..]),
        _ => (false, raw_value),
    };
    if value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {raw_value}"),
        ));
    }
    let value_lc = value.to_ascii_lowercase();
    let split = value_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value_lc.len());
    let (num_str, suffix) = value_lc.split_at(split);
    let num = num_str.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {value}"),
        )
    })?;
    let mult = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid count for {flag}: {raw_value}"),
            ))
        }
    };
    let count = num.checked_mul(mult).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {raw_value}"),
        )
    })?;
    Ok(if from_start {
        TailCount::FromStart(count)
    } else {
        TailCount::FromEnd(count)
    })
}

fn parse_tail_options(
    args: &[String],
) -> io::Result<(
    IOMode,
    TailMode,
    RecordTerminator,
    HeaderMode,
    TailFollowOptions,
    bool,
    Vec<String>,
)> {
    let mut io_mode = IOMode::Auto;
    let mut mode = TailMode::Lines(TailCount::FromEnd(10));
    let mut terminator = RecordTerminator::Newline;
    let mut header_mode = HeaderMode::Auto;
    let mut follow = TailFollowOptions::default();
    let mut report_gbps = false;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--report-gbps" => report_gbps = true,
            "--lines" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for --lines")
                })?;
                mode = TailMode::Lines(parse_tail_count(value, "--lines")?);
            }
            "--bytes" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for --bytes")
                })?;
                mode = TailMode::Bytes(parse_tail_count(value, "--bytes")?);
            }
            "--quiet" | "--silent" | "-q" => header_mode = HeaderMode::Never,
            "--verbose" | "-v" => header_mode = HeaderMode::Always,
            "-z" | "--zero-terminated" => terminator = RecordTerminator::Nul,
            "-f" | "--follow" => follow.mode = Some(FollowMode::Descriptor),
            "-F" => {
                follow.mode = Some(FollowMode::Name);
                follow.retry = true;
            }
            "--retry" => follow.retry = true,
            "--sleep-interval" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument for --sleep-interval",
                    )
                })?;
                follow.sleep_interval = parse_sleep_interval(value, "--sleep-interval")?;
            }
            "-s" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -s")
                })?;
                follow.sleep_interval = parse_sleep_interval(value, "-s")?;
            }
            "--pid" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for --pid")
                })?;
                follow.pid = Some(parse_pid(value, "--pid")?);
            }
            "--max-unchanged-stats" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument for --max-unchanged-stats",
                    )
                })?;
                follow.max_unchanged_stats =
                    parse_follow_u64(value, "--max-unchanged-stats")?.max(1);
            }
            "-n" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -n")
                })?;
                mode = TailMode::Lines(parse_tail_count(value, "-n")?);
            }
            "-c" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -c")
                })?;
                mode = TailMode::Bytes(parse_tail_count(value, "-c")?);
            }
            other if other.starts_with("-n") && other.len() > 2 => {
                mode = TailMode::Lines(parse_tail_count(&other[2..], "-n")?);
            }
            other if other.starts_with("-c") && other.len() > 2 => {
                mode = TailMode::Bytes(parse_tail_count(&other[2..], "-c")?);
            }
            other if other.starts_with("--lines=") => {
                mode = TailMode::Lines(parse_tail_count(&other["--lines=".len()..], "--lines")?);
            }
            other if other.starts_with("--bytes=") => {
                mode = TailMode::Bytes(parse_tail_count(&other["--bytes=".len()..], "--bytes")?);
            }
            other if other.starts_with("--follow=") => {
                follow.mode = Some(parse_follow_mode(&other["--follow=".len()..], "--follow")?);
            }
            other if other.starts_with("--sleep-interval=") => {
                follow.sleep_interval =
                    parse_sleep_interval(&other["--sleep-interval=".len()..], "--sleep-interval")?;
            }
            other if other.starts_with("--pid=") => {
                follow.pid = Some(parse_pid(&other["--pid=".len()..], "--pid")?);
            }
            other if other.starts_with("--max-unchanged-stats=") => {
                follow.max_unchanged_stats = parse_follow_u64(
                    &other["--max-unchanged-stats=".len()..],
                    "--max-unchanged-stats",
                )?
                .max(1);
            }
            other if other.starts_with('-') && other != "-" => {
                for flag in other[1..].chars() {
                    match flag {
                        'q' => header_mode = HeaderMode::Never,
                        'v' => header_mode = HeaderMode::Always,
                        'z' => terminator = RecordTerminator::Nul,
                        'f' => follow.mode = Some(FollowMode::Descriptor),
                        'F' => {
                            follow.mode = Some(FollowMode::Name);
                            follow.retry = true;
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("unsupported tail flag: {other}"),
                            ))
                        }
                    }
                }
            }
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    Ok((
        io_mode,
        mode,
        terminator,
        header_mode,
        follow,
        report_gbps,
        files,
    ))
}

fn write_regular_range<W: Write>(out: &mut W, path: &str, range: ByteRange) -> io::Result<()> {
    visit_path_range_ordered(path, range, |_, block| {
        out.write_all(block)?;
        Ok(OrderedVisitDecision::Continue)
    })?;
    Ok(())
}

fn regular_tail_line_start(
    path: &str,
    lines: u64,
    terminator: RecordTerminator,
) -> io::Result<u64> {
    let total_len = std::fs::metadata(path)?.len();
    if lines == 0 {
        return Ok(total_len);
    }
    if total_len == 0 {
        return Ok(0);
    }

    let mut file = File::open(path)?;
    let mut remaining = lines;
    let mut buffer = vec![0_u8; TAIL_SCAN_BLOCK_SIZE];
    let mut position = total_len;
    while position > 0 {
        let chunk_start = position.saturating_sub(buffer.len() as u64);
        let chunk_len = (position - chunk_start) as usize;
        file.seek(SeekFrom::Start(chunk_start))?;
        file.read_exact(&mut buffer[..chunk_len])?;
        for rel_offset in memrchr_iter(terminator.byte(), &buffer[..chunk_len]) {
            let newline_offset = chunk_start + rel_offset as u64;
            if newline_offset.saturating_add(1) >= total_len {
                continue;
            }
            remaining = remaining.saturating_sub(1);
            if remaining == 0 {
                return Ok(newline_offset.saturating_add(1));
            }
        }
        position = chunk_start;
    }
    Ok(0)
}

fn regular_line_start(
    path: &str,
    start_line: u64,
    terminator: RecordTerminator,
) -> io::Result<u64> {
    let total_len = std::fs::metadata(path)?.len();
    if total_len == 0 || start_line <= 1 {
        return Ok(0);
    }
    let mut remaining = start_line.saturating_sub(1);
    let mut start_offset = total_len;
    visit_path_range_ordered(path, ByteRange::default(), |block_offset, block| {
        for newline_offset in memchr_iter(terminator.byte(), block) {
            remaining = remaining.saturating_sub(1);
            if remaining == 0 {
                start_offset = (block_offset + newline_offset as u64 + 1).min(total_len);
                return Ok(OrderedVisitDecision::Stop);
            }
        }
        Ok(OrderedVisitDecision::Continue)
    })?;
    Ok(start_offset)
}

fn regular_tail_start_offset(
    path: &str,
    mode: TailMode,
    terminator: RecordTerminator,
) -> io::Result<u64> {
    let total_len = std::fs::metadata(path)?.len();
    match mode {
        TailMode::Bytes(TailCount::FromEnd(bytes)) => Ok(total_len.saturating_sub(bytes)),
        TailMode::Bytes(TailCount::FromStart(bytes)) => Ok(total_len.min(bytes.saturating_sub(1))),
        TailMode::Lines(TailCount::FromEnd(lines)) => {
            regular_tail_line_start(path, lines, terminator)
        }
        TailMode::Lines(TailCount::FromStart(lines)) => regular_line_start(path, lines, terminator),
    }
}

fn try_write_tail_regular_path_fast(path: &str, start_offset: u64) -> io::Result<Option<u64>> {
    let mut noop = |_bytes: u64| Ok(());
    let copied = copy_path_range_to_fd_with_progress(
        path,
        fro::command_io::stdout_fd(),
        ByteRange::starting_at(start_offset),
        &mut noop,
    )?;
    let emitted_len = std::fs::metadata(path)?.len().saturating_sub(start_offset);
    Ok(copied.map(|written| written.min(emitted_len)))
}

fn write_follow_stderr(
    stderr: &mut Option<std::io::BufWriter<File>>,
    message: impl AsRef<str>,
) -> io::Result<()> {
    let stderr = stderr.get_or_insert(fro::command_io::stderr_buf_writer(4096)?);
    stderr.write_all(message.as_ref().as_bytes())?;
    stderr.flush()
}

fn follow_error_text(err: &io::Error) -> String {
    err.raw_os_error()
        .map(|code| {
            unsafe { CStr::from_ptr(libc::strerror(code)) }
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| err.to_string())
}

fn write_follow_open_error(
    stderr: &mut Option<std::io::BufWriter<File>>,
    path: &str,
    err: &io::Error,
) -> io::Result<()> {
    write_follow_stderr(
        stderr,
        format!(
            "tail: cannot open '{path}' for reading: {}\n",
            follow_error_text(err)
        ),
    )
}

fn write_follow_missing_notice(
    stderr: &mut Option<std::io::BufWriter<File>>,
    path: &str,
    err: &io::Error,
) -> io::Result<()> {
    write_follow_stderr(
        stderr,
        format!("tail: {path}: {}\n", follow_error_text(err)),
    )
}

fn write_follow_appeared_notice(
    stderr: &mut Option<std::io::BufWriter<File>>,
    path: &str,
) -> io::Result<()> {
    write_follow_stderr(
        stderr,
        format!("tail: '{path}' has appeared;  following new file\n"),
    )
}

fn write_follow_retry_warning(stderr: &mut Option<std::io::BufWriter<File>>) -> io::Result<()> {
    write_follow_stderr(
        stderr,
        "tail: warning: --retry only effective for the initial open\n",
    )
}

fn write_follow_truncated_notice(
    stderr: &mut Option<std::io::BufWriter<File>>,
    path: &str,
) -> io::Result<()> {
    write_follow_stderr(stderr, format!("tail: {path}: file truncated\n"))
}

fn is_pid_alive(pid: libc::pid_t) -> bool {
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn write_file_from_offset<W: Write>(
    out: &mut W,
    file: &mut File,
    start_offset: u64,
) -> io::Result<u64> {
    file.seek(SeekFrom::Start(start_offset))?;
    let mut buffer = vec![0_u8; TAIL_SCAN_BLOCK_SIZE];
    let mut written = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(written);
        }
        out.write_all(&buffer[..read])?;
        written = written
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("tail follow byte count overflow"))?;
    }
}

fn emit_follow_header(
    out: &mut Option<BufWriter>,
    show_headers: bool,
    last_output_index: &mut Option<usize>,
    state: &TailFollowState,
) -> io::Result<()> {
    if !show_headers || *last_output_index == Some(state.input_index) {
        return Ok(());
    }
    let out = out.get_or_insert(stdout_buf_writer()?);
    if last_output_index.is_some() {
        out.write_all(b"\n")?;
    }
    writeln!(out, "==> {} <==", state.header_label)?;
    *last_output_index = Some(state.input_index);
    Ok(())
}

fn write_follow_state_output(
    state: &mut TailFollowState,
    out: &mut Option<BufWriter>,
    show_headers: bool,
    last_output_index: &mut Option<usize>,
) -> io::Result<u64> {
    emit_follow_header(out, show_headers, last_output_index, state)?;
    let Some(file) = state.file.as_mut() else {
        return Ok(0);
    };
    let out = out.get_or_insert(stdout_buf_writer()?);
    let written = write_file_from_offset(out, file, state.offset)?;
    state.offset = state
        .offset
        .checked_add(written)
        .ok_or_else(|| io::Error::other("tail follow offset overflow"))?;
    Ok(written)
}

fn make_follow_state(
    path: &str,
    input_index: usize,
    mode: FollowMode,
    offset: u64,
    ever_opened: bool,
    file: Option<File>,
    identity: Option<FileIdentity>,
) -> TailFollowState {
    TailFollowState {
        path: path.to_string(),
        header_label: path.to_string(),
        input_index,
        mode,
        offset,
        file,
        identity,
        ever_opened,
        missing_reported: !ever_opened,
        unchanged_iterations: 0,
    }
}

fn poll_descriptor_follow(
    state: &mut TailFollowState,
    out: &mut Option<BufWriter>,
    stderr: &mut Option<std::io::BufWriter<File>>,
    show_headers: bool,
    last_output_index: &mut Option<usize>,
) -> io::Result<u64> {
    if state.file.is_none() {
        match File::open(&state.path) {
            Ok(file) => {
                let metadata = file.metadata()?;
                if !metadata.file_type().is_file() {
                    return Ok(0);
                }
                if state.missing_reported {
                    write_follow_appeared_notice(stderr, &state.path)?;
                }
                state.offset = 0;
                state.identity = Some(FileIdentity::from_metadata(&metadata));
                state.file = Some(file);
                state.ever_opened = true;
                state.missing_reported = false;
            }
            Err(err) => {
                if !state.missing_reported {
                    write_follow_open_error(stderr, &state.path, &err)?;
                    state.missing_reported = true;
                }
                return Ok(0);
            }
        }
    }

    let metadata = state
        .file
        .as_ref()
        .ok_or_else(|| io::Error::other("descriptor follow handle missing"))?
        .metadata()?;
    if metadata.len() < state.offset {
        write_follow_truncated_notice(stderr, &state.path)?;
        state.offset = 0;
    }
    if metadata.len() == state.offset {
        return Ok(0);
    }
    write_follow_state_output(state, out, show_headers, last_output_index)
}

fn poll_name_follow(
    state: &mut TailFollowState,
    out: &mut Option<BufWriter>,
    stderr: &mut Option<std::io::BufWriter<File>>,
    show_headers: bool,
    last_output_index: &mut Option<usize>,
    max_unchanged_stats: u64,
) -> io::Result<u64> {
    let metadata = match std::fs::metadata(&state.path) {
        Ok(metadata) => metadata,
        Err(err) => {
            if state.ever_opened && !state.missing_reported {
                write_follow_missing_notice(stderr, &state.path, &err)?;
                state.missing_reported = true;
            }
            state.file = None;
            state.identity = None;
            state.unchanged_iterations = 0;
            return Ok(0);
        }
    };
    if !metadata.file_type().is_file() {
        state.file = None;
        state.identity = None;
        state.unchanged_iterations = 0;
        return Ok(0);
    }

    let identity = FileIdentity::from_metadata(&metadata);
    let same_length = metadata.len() == state.offset;
    if same_length {
        state.unchanged_iterations = state.unchanged_iterations.saturating_add(1);
    } else {
        state.unchanged_iterations = 0;
    }

    let changed_identity = state.identity != Some(identity);
    let should_reopen = state.file.is_none()
        || (changed_identity
            && (!same_length || state.unchanged_iterations >= max_unchanged_stats));

    if should_reopen {
        let file = File::open(&state.path)?;
        if state.ever_opened || state.missing_reported {
            write_follow_appeared_notice(stderr, &state.path)?;
        }
        state.offset = 0;
        state.file = Some(file);
        state.identity = Some(identity);
        state.ever_opened = true;
        state.missing_reported = false;
        state.unchanged_iterations = 0;
    } else if metadata.len() < state.offset {
        write_follow_truncated_notice(stderr, &state.path)?;
        state.offset = 0;
    }

    if metadata.len() == state.offset {
        return Ok(0);
    }
    write_follow_state_output(state, out, show_headers, last_output_index)
}

fn follow_tail_inputs(
    out: &mut Option<BufWriter>,
    stderr: &mut Option<std::io::BufWriter<File>>,
    states: &mut [TailFollowState],
    show_headers: bool,
    last_output_index: &mut Option<usize>,
    follow: &TailFollowOptions,
) -> io::Result<u64> {
    let mut total_output_bytes = 0_u64;
    loop {
        for state in states.iter_mut() {
            let emitted = match state.mode {
                FollowMode::Descriptor => {
                    poll_descriptor_follow(state, out, stderr, show_headers, last_output_index)?
                }
                FollowMode::Name => poll_name_follow(
                    state,
                    out,
                    stderr,
                    show_headers,
                    last_output_index,
                    follow.max_unchanged_stats,
                )?,
            };
            total_output_bytes = total_output_bytes
                .checked_add(emitted)
                .ok_or_else(|| io::Error::other("tail follow output byte count overflow"))?;
        }
        if let Some(out) = out.as_mut() {
            out.flush()?;
        }
        if let Some(pid) = follow.pid {
            if !is_pid_alive(pid) {
                return Ok(total_output_bytes);
            }
        }
        std::thread::sleep(follow.sleep_interval);
    }
}

fn write_tail_input(
    out: &mut Option<BufWriter>,
    input: &StreamInput,
    io_mode: IOMode,
    mode: TailMode,
    terminator: RecordTerminator,
    report_throughput: bool,
) -> io::Result<u64> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let start_offset = regular_tail_start_offset(path, mode, terminator)?;
            if let Some(out) = out.as_mut() {
                out.flush()?;
            }
            if let Some(written) = try_write_tail_regular_path_fast(path, start_offset)? {
                Ok(written)
            } else {
                let out = out.get_or_insert(stdout_buf_writer()?);
                let mut counted = CountingWrite::new(out);
                write_regular_range(&mut counted, path, ByteRange::starting_at(start_offset))?;
                Ok(counted.bytes_written())
            }
        }
        StreamInput::Stdin { .. } => {
            if let Some(path) = regular_stdin_path()? {
                let start_offset = regular_tail_start_offset(path, mode, terminator)?;
                if let Some(out) = out.as_mut() {
                    out.flush()?;
                }
                if let Some(written) = try_write_tail_regular_path_fast(path, start_offset)? {
                    Ok(written)
                } else {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_regular_range(&mut counted, path, ByteRange::starting_at(start_offset))?;
                    Ok(counted.bytes_written())
                }
            } else if let TailMode::Bytes(TailCount::FromEnd(count)) = mode {
                if !report_throughput {
                    if let Some(out) = out.as_mut() {
                        out.flush()?;
                    }
                }
                if !report_throughput && try_write_tail_pipe_bytes_fast(input, count)? {
                    Ok(count)
                } else {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                    Ok(counted.bytes_written())
                }
            } else if matches!(mode, TailMode::Lines(TailCount::FromEnd(_))) {
                let out = out.get_or_insert(stdout_buf_writer()?);
                let mut counted = CountingWrite::new(out);
                write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                Ok(counted.bytes_written())
            } else {
                let out = out.get_or_insert(stdout_buf_writer()?);
                let mut counted = CountingWrite::new(out);
                write_tail_from_start(&mut counted, input, io_mode, mode, terminator)?;
                Ok(counted.bytes_written())
            }
        }
        StreamInput::File(path) => {
            let file_type = std::fs::metadata(path)?.file_type();
            match mode {
                TailMode::Bytes(TailCount::FromEnd(count))
                    if file_type.is_fifo() && !report_throughput =>
                {
                    if let Some(out) = out.as_mut() {
                        out.flush()?;
                    }
                    if !try_write_tail_pipe_bytes_fast(input, count)? {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        write_tail_windowed(out, input, io_mode, mode, terminator)?;
                    }
                    Ok(count)
                }
                TailMode::Bytes(TailCount::FromEnd(_)) | TailMode::Lines(TailCount::FromEnd(_)) => {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                    Ok(counted.bytes_written())
                }
                TailMode::Bytes(TailCount::FromStart(_))
                | TailMode::Lines(TailCount::FromStart(_)) => {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_from_start(&mut counted, input, io_mode, mode, terminator)?;
                    Ok(counted.bytes_written())
                }
            }
        }
    }
}

pub(super) fn run_tail(args: &[String]) -> io::Result<()> {
    let (io_mode, mode, terminator, header_mode, follow, report_throughput, files) =
        parse_tail_options(args)?;
    let inputs = parse_stream_inputs(files);
    let show_headers = match header_mode {
        HeaderMode::Auto => inputs.len() > 1,
        HeaderMode::Always => true,
        HeaderMode::Never => false,
    };
    let started_at = report_throughput.then(std::time::Instant::now);
    let mut total_output_bytes = 0_u64;
    let mut out = None;
    let mut stderr = None;
    let mut follow_states = Vec::new();
    let mut last_output_index = None;
    if follow.mode == Some(FollowMode::Descriptor) && follow.retry {
        write_follow_retry_warning(&mut stderr)?;
    }
    for (index, input) in inputs.iter().enumerate() {
        if show_headers {
            let out = out.get_or_insert(stdout_buf_writer()?);
            if index != 0 {
                out.write_all(b"\n")?;
            }
            let label = match input {
                StreamInput::File(file) => file.as_str(),
                StreamInput::Stdin { .. } => "standard input",
            };
            writeln!(out, "==> {label} <==")?;
            last_output_index = Some(index);
        }
        let emitted_bytes = match input {
            StreamInput::File(path) if follow.mode.is_some() && follow.retry => {
                match write_tail_input(
                    &mut out,
                    input,
                    io_mode,
                    mode,
                    terminator,
                    report_throughput,
                ) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        write_follow_open_error(&mut stderr, path, &err)?;
                        follow_states.push(make_follow_state(
                            path,
                            index,
                            follow.mode.expect("follow mode"),
                            0,
                            false,
                            None,
                            None,
                        ));
                        0
                    }
                }
            }
            _ => write_tail_input(
                &mut out,
                input,
                io_mode,
                mode,
                terminator,
                report_throughput,
            )?,
        };
        if let (Some(follow_mode), StreamInput::File(path)) = (follow.mode, input) {
            if let Ok(metadata) = std::fs::metadata(path) {
                if metadata.file_type().is_file() {
                    let file = File::open(path)?;
                    let file_metadata = file.metadata()?;
                    follow_states.push(make_follow_state(
                        path,
                        index,
                        follow_mode,
                        file_metadata.len(),
                        true,
                        Some(file),
                        Some(FileIdentity::from_metadata(&file_metadata)),
                    ));
                }
            }
        }
        total_output_bytes = total_output_bytes
            .checked_add(emitted_bytes)
            .ok_or_else(|| io::Error::other("tail output byte count overflow"))?;
    }
    if follow.mode.is_some() && !follow_states.is_empty() {
        total_output_bytes = total_output_bytes
            .checked_add(follow_tail_inputs(
                &mut out,
                &mut stderr,
                &mut follow_states,
                show_headers,
                &mut last_output_index,
                &follow,
            )?)
            .ok_or_else(|| io::Error::other("tail output byte count overflow"))?;
    }
    match out {
        Some(out) => out.into_inner(),
        None => Ok(()),
    }?;
    if let Some(started_at) = started_at {
        report_gbps("tail", total_output_bytes, started_at);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
