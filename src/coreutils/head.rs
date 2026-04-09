use super::*;
use fro::{
    copy_fd_range_to_fd_with_progress, copy_path_range_to_fd_with_progress,
    visit_path_range_ordered, ByteRange, OrderedVisitDecision,
};
use std::fs;
use std::io::Read;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadCount {
    FromStart(u64),
    AllButLast(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadMode {
    Lines(HeadCount),
    Bytes(HeadCount),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeaderMode {
    Auto,
    Always,
    Never,
}

const HEAD_LINE_PREFIX_SCAN_BLOCK_SIZE: usize = 256 << 10;
const HEAD_SMALL_STREAM_RAW_READ_BLOCK_SIZE: usize = 64 << 10;
const HEAD_SMALL_STREAM_LINE_CUTOFF: u64 = 64;
const OBSOLETE_HEAD_BLOCK_MULTIPLIER: u64 = 512;

fn regular_stdin_path() -> io::Result<Option<&'static str>> {
    if fd_is_regular(libc::STDIN_FILENO)? {
        Ok(Some("/proc/self/fd/0"))
    } else {
        Ok(None)
    }
}

fn parse_head_count(value: &str, flag: &str) -> io::Result<HeadCount> {
    let raw_value = value.trim();
    if raw_value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {raw_value}"),
        ));
    }
    let (count_mode, value) = match raw_value.as_bytes()[0] {
        b'+' => (HeadCount::FromStart(0), &raw_value[1..]),
        b'-' => (HeadCount::AllButLast(0), &raw_value[1..]),
        _ => (HeadCount::FromStart(0), raw_value),
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
                format!("invalid count for {flag}: {value}"),
            ))
        }
    };
    let count = num.checked_mul(mult).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {raw_value}"),
        )
    })?;
    Ok(match count_mode {
        HeadCount::FromStart(_) => HeadCount::FromStart(count),
        HeadCount::AllButLast(_) => HeadCount::AllButLast(count),
    })
}

fn parse_obsolete_head_arg(arg: &str) -> io::Result<Option<(HeadMode, Option<HeaderMode>)>> {
    let Some(rest) = arg.strip_prefix('-') else {
        return Ok(None);
    };
    if rest.is_empty() || !rest.as_bytes()[0].is_ascii_digit() {
        return Ok(None);
    }

    let digit_end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    let count = rest[..digit_end].parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid obsolete head count: {arg}"),
        )
    })?;

    let mut mode = HeadMode::Lines(HeadCount::FromStart(count));
    let mut header_mode = None;
    for flag in rest[digit_end..].chars() {
        match flag.to_ascii_lowercase() {
            'c' => mode = HeadMode::Bytes(HeadCount::FromStart(count)),
            'b' => {
                mode = HeadMode::Bytes(HeadCount::FromStart(
                    count
                        .checked_mul(OBSOLETE_HEAD_BLOCK_MULTIPLIER)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid obsolete head count: {arg}"),
                            )
                        })?,
                ))
            }
            'k' => {
                mode = HeadMode::Bytes(HeadCount::FromStart(count.checked_mul(1024).ok_or_else(
                    || {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid obsolete head count: {arg}"),
                        )
                    },
                )?))
            }
            'm' => {
                mode = HeadMode::Bytes(HeadCount::FromStart(
                    count.checked_mul(1024_u64.pow(2)).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid obsolete head count: {arg}"),
                        )
                    })?,
                ))
            }
            'q' => header_mode = Some(HeaderMode::Never),
            'v' => header_mode = Some(HeaderMode::Always),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported head flag: {arg}"),
                ))
            }
        }
    }

    Ok(Some((mode, header_mode)))
}

fn parse_head_options(args: &[String]) -> io::Result<(IOMode, HeadMode, HeaderMode, Vec<String>)> {
    let mut io_mode = IOMode::Auto;
    let mut mode = HeadMode::Lines(HeadCount::FromStart(10));
    let mut header_mode = HeaderMode::Auto;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--quiet" | "--silent" => header_mode = HeaderMode::Never,
            "--verbose" => header_mode = HeaderMode::Always,
            "--lines" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for --lines")
                })?;
                mode = HeadMode::Lines(parse_head_count(value, "--lines")?);
            }
            "--bytes" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for --bytes")
                })?;
                mode = HeadMode::Bytes(parse_head_count(value, "--bytes")?);
            }
            "-q" => header_mode = HeaderMode::Never,
            "-v" => header_mode = HeaderMode::Always,
            "-n" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -n")
                })?;
                mode = HeadMode::Lines(parse_head_count(value, "-n")?);
            }
            "-c" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -c")
                })?;
                mode = HeadMode::Bytes(parse_head_count(value, "-c")?);
            }
            other if other.starts_with("-n") && other.len() > 2 => {
                mode = HeadMode::Lines(parse_head_count(&other[2..], "-n")?);
            }
            other if other.starts_with("-c") && other.len() > 2 => {
                mode = HeadMode::Bytes(parse_head_count(&other[2..], "-c")?);
            }
            other if other.starts_with("--lines=") => {
                mode = HeadMode::Lines(parse_head_count(&other["--lines=".len()..], "--lines")?);
            }
            other if other.starts_with("--bytes=") => {
                mode = HeadMode::Bytes(parse_head_count(&other["--bytes=".len()..], "--bytes")?);
            }
            other => {
                if let Some((obsolete_mode, obsolete_header_mode)) = parse_obsolete_head_arg(other)?
                {
                    mode = obsolete_mode;
                    if let Some(header) = obsolete_header_mode {
                        header_mode = header;
                    }
                } else if other.starts_with('-') && other != "-" {
                    for flag in other[1..].chars() {
                        match flag {
                            'q' => header_mode = HeaderMode::Never,
                            'v' => header_mode = HeaderMode::Always,
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unsupported head flag: {other}"),
                                ))
                            }
                        }
                    }
                } else {
                    files.push(other.to_string());
                }
            }
        }
        i += 1;
    }
    Ok((io_mode, mode, header_mode, files))
}

fn write_head_bytes_from_start<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    mut remaining: u64,
) -> io::Result<()> {
    if remaining == 0 {
        return Ok(());
    }
    visit_ordered_input(input, io_mode, |block| {
        if remaining == 0 {
            return Ok(());
        }
        let take = remaining.min(block.len() as u64) as usize;
        out.write_all(&block[..take])?;
        remaining -= take as u64;
        Ok(())
    })
}

fn write_head_bytes_fast(input: &StreamInput, bytes: u64) -> io::Result<bool> {
    if cfg!(test) {
        return Ok(false);
    }
    if bytes == 0 {
        return Ok(true);
    }
    let mut noop = |_bytes: u64| Ok(());
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            Ok(copy_path_range_to_fd_with_progress(
                path,
                libc::STDOUT_FILENO,
                ByteRange::up_to(bytes),
                &mut noop,
            )?
            .is_some())
        }
        StreamInput::Stdin { .. } => {
            if let Some(copied) = copy_fd_range_to_fd_with_progress(
                libc::STDIN_FILENO,
                libc::STDOUT_FILENO,
                ByteRange::up_to(bytes),
                &mut noop,
            )? {
                return Ok(copied == bytes);
            }
            Ok(copy_fd_to_fd_splice_limited_counted(
                libc::STDIN_FILENO,
                libc::STDOUT_FILENO,
                bytes,
                &mut noop,
            )?
            .is_some())
        }
        StreamInput::File(path) => {
            let file_type = fs::metadata(path)?.file_type();
            if file_type.is_fifo() {
                let file = std::fs::File::open(path)?;
                return Ok(copy_fd_to_fd_splice_limited_counted(
                    file.as_raw_fd(),
                    libc::STDOUT_FILENO,
                    bytes,
                    &mut noop,
                )?
                .is_some());
            }
            Ok(false)
        }
    }
}

fn write_regular_range<W: Write>(out: &mut W, path: &str, range: ByteRange) -> io::Result<()> {
    visit_path_range_ordered(path, range, |_, block| {
        out.write_all(block)?;
        Ok(OrderedVisitDecision::Continue)
    })?;
    Ok(())
}

fn write_head_bytes_all_but_last<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    trim: u64,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let len = std::fs::metadata(path)?.len();
            let end = len.saturating_sub(trim);
            return write_regular_range(
                out,
                path,
                ByteRange {
                    start_offset: 0,
                    end_offset: Some(end),
                },
            );
        }
        StreamInput::Stdin { .. } => {
            if let Some(path) = regular_stdin_path()? {
                let len = std::fs::metadata(path)?.len();
                let end = len.saturating_sub(trim);
                return write_regular_range(
                    out,
                    path,
                    ByteRange {
                        start_offset: 0,
                        end_offset: Some(end),
                    },
                );
            }
        }
        _ => {}
    }
    let bytes = loaded_or_stream_bytes(input, io_mode)?;
    let keep = bytes.len().saturating_sub(trim as usize);
    out.write_all(&bytes[..keep])
}

fn write_head_lines_regular_input<W: Write>(
    out: &mut W,
    path: &str,
    remaining_lines: u64,
) -> io::Result<()> {
    let mut file = std::fs::File::open(path)?;
    write_head_lines_reader(out, &mut file, remaining_lines)
}

fn write_head_lines_reader<W: Write, R: Read>(
    out: &mut W,
    reader: &mut R,
    mut remaining_lines: u64,
) -> io::Result<()> {
    if remaining_lines == 0 {
        return Ok(());
    }
    let mut buffer = vec![0_u8; HEAD_LINE_PREFIX_SCAN_BLOCK_SIZE];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        let block = &buffer[..read];
        if let Some(prefix_len) = head_line_prefix_len(block, &mut remaining_lines) {
            out.write_all(&block[..prefix_len])?;
            out.flush()?;
            return Ok(());
        }
        out.write_all(block)?;
    }
}

fn read_raw_fd(fd: libc::c_int, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        let read = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if read >= 0 {
            return Ok(read as usize);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN) => continue,
            _ => return Err(err),
        }
    }
}

fn write_head_lines_small_stdin_fast(remaining_lines: u64) -> io::Result<()> {
    if remaining_lines == 0 {
        return Ok(());
    }
    let mut buffer = [0_u8; HEAD_SMALL_STREAM_RAW_READ_BLOCK_SIZE];
    let mut remaining_lines = remaining_lines;
    let mut grew_pipes = false;
    loop {
        let read = read_raw_fd(libc::STDIN_FILENO, &mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        let block = &buffer[..read];
        if let Some(prefix_len) = head_line_prefix_len(block, &mut remaining_lines) {
            return write_raw_fd_all(libc::STDOUT_FILENO, &block[..prefix_len]);
        }
        write_raw_fd_all(libc::STDOUT_FILENO, block)?;
        if !grew_pipes {
            grow_pipe_best_effort(libc::STDIN_FILENO)?;
            grow_pipe_best_effort(libc::STDOUT_FILENO)?;
            grew_pipes = true;
        }
    }
}

fn head_line_prefix_len(block: &[u8], remaining_lines: &mut u64) -> Option<usize> {
    for newline_offset in memchr_iter(b'\n', block) {
        *remaining_lines -= 1;
        if *remaining_lines == 0 {
            return Some(newline_offset + 1);
        }
    }
    None
}

fn tail_line_start_offset(
    total_len: u64,
    lines: u64,
    newline_offsets: impl IntoIterator<Item = u64>,
) -> u64 {
    if lines == 0 {
        return total_len;
    }
    if total_len == 0 {
        return 0;
    }
    let mut starts = std::collections::VecDeque::from([0_u64]);
    for newline_offset in newline_offsets {
        let next_start = newline_offset.saturating_add(1);
        if next_start < total_len {
            starts.push_back(next_start);
            if starts.len() as u64 > lines {
                starts.pop_front();
            }
        }
    }
    starts.front().copied().unwrap_or(0)
}

fn write_head_lines_all_but_last_regular_input<W: Write>(
    out: &mut W,
    path: &str,
    trim_lines: u64,
) -> io::Result<()> {
    let total_len = std::fs::metadata(path)?.len();
    let mut newline_offsets = Vec::new();
    visit_path_range_ordered(path, ByteRange::default(), |block_offset, block| {
        newline_offsets
            .extend(memchr_iter(b'\n', block).map(|offset| block_offset + offset as u64));
        Ok(OrderedVisitDecision::Continue)
    })?;
    let end = tail_line_start_offset(total_len, trim_lines, newline_offsets);
    write_regular_range(
        out,
        path,
        ByteRange {
            start_offset: 0,
            end_offset: Some(end),
        },
    )
}

fn write_head_lines_from_start<W: Write>(
    out: &mut W,
    input: &StreamInput,
    remaining_lines: u64,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) => {
            let mut reader = std::io::BufReader::new(std::fs::File::open(path)?);
            write_head_lines_reader(out, &mut reader, remaining_lines)
        }
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            write_head_lines_reader(out, &mut reader, remaining_lines)
        }
    }
}

fn write_head_lines_all_but_last<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    trim_lines: u64,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            return write_head_lines_all_but_last_regular_input(out, path, trim_lines);
        }
        StreamInput::Stdin { .. } => {
            if let Some(path) = regular_stdin_path()? {
                return write_head_lines_all_but_last_regular_input(out, path, trim_lines);
            }
        }
        _ => {}
    }
    let bytes = loaded_or_stream_bytes(input, io_mode)?;
    let newline_offsets = bytes
        .iter()
        .enumerate()
        .filter_map(|(offset, byte)| (*byte == b'\n').then_some(offset as u64));
    let end = tail_line_start_offset(bytes.len() as u64, trim_lines, newline_offsets) as usize;
    out.write_all(&bytes[..end])
}

fn should_use_small_stream_stdout_fast_path(
    inputs: &[StreamInput],
    mode: HeadMode,
    show_headers: bool,
) -> io::Result<bool> {
    if cfg!(test) {
        return Ok(false);
    }
    if show_headers {
        return Ok(false);
    }
    let [input] = inputs else {
        return Ok(false);
    };
    let HeadMode::Lines(HeadCount::FromStart(lines)) = mode else {
        return Ok(false);
    };
    if lines > HEAD_SMALL_STREAM_LINE_CUTOFF {
        return Ok(false);
    }
    match input {
        StreamInput::Stdin { .. } => Ok(regular_stdin_path()?.is_none()),
        _ => Ok(false),
    }
}

pub(super) fn run_head(args: &[String]) -> io::Result<()> {
    let (io_mode, mode, header_mode, files) = parse_head_options(args)?;
    let inputs = parse_stream_inputs(files);
    let show_headers = match header_mode {
        HeaderMode::Auto => inputs.len() > 1,
        HeaderMode::Always => true,
        HeaderMode::Never => false,
    };
    if should_use_small_stream_stdout_fast_path(&inputs, mode, show_headers)? {
        let [StreamInput::Stdin { .. }] = inputs.as_slice() else {
            unreachable!("small stream fast path requires a single stdin input");
        };
        let HeadMode::Lines(HeadCount::FromStart(lines)) = mode else {
            unreachable!("small stream fast path only supports line prefixes");
        };
        return write_head_lines_small_stdin_fast(lines);
    }
    let mut out = stdout_buf_writer()?;
    for (index, input) in inputs.iter().enumerate() {
        if show_headers {
            if index != 0 {
                out.write_all(b"\n")?;
            }
            let label = match input {
                StreamInput::File(file) => file.as_str(),
                StreamInput::Stdin { .. } => "standard input",
            };
            writeln!(&mut out, "==> {label} <==")?;
        }
        match mode {
            HeadMode::Lines(count) => match count {
                HeadCount::FromStart(lines) => {
                    match input {
                        StreamInput::File(path) if is_regular_input_path(path)? => {
                            write_head_lines_regular_input(&mut out, path, lines)?;
                            continue;
                        }
                        StreamInput::Stdin { .. } => {
                            if let Some(path) = regular_stdin_path()? {
                                write_head_lines_regular_input(&mut out, path, lines)?;
                                continue;
                            }
                        }
                        _ => {}
                    }
                    write_head_lines_from_start(&mut out, input, lines)?
                }
                HeadCount::AllButLast(lines) => {
                    write_head_lines_all_but_last(&mut out, input, io_mode, lines)?
                }
            },
            HeadMode::Bytes(count) => match count {
                HeadCount::FromStart(bytes) => {
                    if write_head_bytes_fast(input, bytes)? {
                        continue;
                    }
                    write_head_bytes_from_start(&mut out, input, io_mode, bytes)?
                }
                HeadCount::AllButLast(bytes) => {
                    write_head_bytes_all_but_last(&mut out, input, io_mode, bytes)?
                }
            },
        }
    }
    out.into_inner()
}
