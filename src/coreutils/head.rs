use super::*;
use fro::{
    copy_fd_range_to_fd_with_progress, copy_path_range_to_fd_with_progress,
    visit_path_range_ordered, ByteRange, OrderedVisitDecision,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadMode {
    Lines(u64),
    Bytes(u64),
}

fn parse_head_count(value: &str, flag: &str) -> io::Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {value}"),
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
    num.checked_mul(mult).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count for {flag}: {value}"),
        )
    })
}

fn parse_head_options(args: &[String]) -> io::Result<(IOMode, HeadMode, Vec<String>)> {
    let mut io_mode = IOMode::Auto;
    let mut mode = HeadMode::Lines(10);
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
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
            other if other.starts_with('-') && other != "-" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported head flag: {other}"),
                ));
            }
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    Ok((io_mode, mode, files))
}

fn write_head_bytes<W: Write>(
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
    if bytes == 0 {
        return Ok(true);
    }
    let mut noop = |_bytes: u64| Ok(());
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => Ok(copy_path_range_to_fd_with_progress(
            path,
            libc::STDOUT_FILENO,
            ByteRange::up_to(bytes),
            &mut noop,
        )?
        .is_some()),
        StreamInput::Stdin { .. } => Ok(copy_fd_range_to_fd_with_progress(
            libc::STDIN_FILENO,
            libc::STDOUT_FILENO,
            ByteRange::up_to(bytes),
            &mut noop,
        )?
        .is_some()),
        _ => Ok(false),
    }
}

fn write_head_lines_regular_file<W: Write>(
    out: &mut W,
    path: &str,
    mut remaining_lines: u64,
) -> io::Result<()> {
    if remaining_lines == 0 {
        return Ok(());
    }
    let mut pending = Vec::new();
    visit_path_range_ordered(path, ByteRange::default(), |_, block| {
        if remaining_lines == 0 {
            return Ok(OrderedVisitDecision::Stop);
        }
        let mut line_start = 0usize;
        for newline_offset in memchr_iter(b'\n', block) {
            pending.extend_from_slice(&block[line_start..=newline_offset]);
            out.write_all(&pending)?;
            pending.clear();
            remaining_lines -= 1;
            if remaining_lines == 0 {
                return Ok(OrderedVisitDecision::Stop);
            }
            line_start = newline_offset + 1;
        }
        if line_start < block.len() {
            pending.extend_from_slice(&block[line_start..]);
            out.write_all(&pending)?;
            pending.clear();
        }
        Ok(OrderedVisitDecision::Continue)
    })?;
    Ok(())
}

fn write_head_lines<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    mut remaining_lines: u64,
) -> io::Result<()> {
    if remaining_lines == 0 {
        return Ok(());
    }
    let mut pending = Vec::new();
    visit_ordered_input(input, io_mode, |block| {
        if remaining_lines == 0 {
            return Ok(());
        }
        let mut line_start = 0usize;
        for newline_offset in memchr_iter(b'\n', block) {
            pending.extend_from_slice(&block[line_start..=newline_offset]);
            out.write_all(&pending)?;
            pending.clear();
            remaining_lines -= 1;
            if remaining_lines == 0 {
                return Ok(());
            }
            line_start = newline_offset + 1;
        }
        if line_start < block.len() {
            pending.extend_from_slice(&block[line_start..]);
        }
        Ok(())
    })?;
    if remaining_lines != 0 && !pending.is_empty() {
        out.write_all(&pending)?;
    }
    Ok(())
}

pub(super) fn run_head(args: &[String]) -> io::Result<()> {
    let (io_mode, mode, files) = parse_head_options(args)?;
    let inputs = parse_stream_inputs(files);
    let multi_file = inputs.len() > 1;
    let mut out = stdout_buf_writer()?;
    for (index, input) in inputs.iter().enumerate() {
        if multi_file {
            if index != 0 {
                out.write_all(b"\n")?;
            }
            let label = match input {
                StreamInput::File(file) => file.as_str(),
                StreamInput::Stdin { label } => label.as_deref().unwrap_or("standard input"),
            };
            writeln!(&mut out, "==> {label} <==")?;
        }
        match mode {
            HeadMode::Lines(lines) => {
                if let StreamInput::File(path) = input {
                    if is_regular_input_path(path)? {
                        write_head_lines_regular_file(&mut out, path, lines)?;
                        continue;
                    }
                }
                write_head_lines(&mut out, input, io_mode, lines)?
            }
            HeadMode::Bytes(bytes) => {
                if write_head_bytes_fast(input, bytes)? {
                    continue;
                }
                write_head_bytes(&mut out, input, io_mode, bytes)?
            }
        }
    }
    out.into_inner()
}
