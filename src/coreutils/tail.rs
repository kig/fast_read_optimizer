use super::*;
use crate::common::AlignedBuffer;
use fro::{
    copy_path_range_to_fd_with_progress, visit_path_range_ordered, ByteRange, OrderedVisitDecision,
};
use memchr::memrchr_iter;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;

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

fn regular_stdin_path() -> io::Result<Option<&'static str>> {
    if fd_is_regular(libc::STDIN_FILENO)? {
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
    bool,
    Vec<String>,
)> {
    let mut io_mode = IOMode::Auto;
    let mut mode = TailMode::Lines(TailCount::FromEnd(10));
    let mut terminator = RecordTerminator::Newline;
    let mut header_mode = HeaderMode::Auto;
    let mut report_gbps = false;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--report-gbps" => report_gbps = true,
            "-q" => header_mode = HeaderMode::Never,
            "-v" => header_mode = HeaderMode::Always,
            "-z" | "--zero-terminated" => terminator = RecordTerminator::Nul,
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
            other if other.starts_with('-') && other != "-" => {
                for flag in other[1..].chars() {
                    match flag {
                        'q' => header_mode = HeaderMode::Never,
                        'v' => header_mode = HeaderMode::Always,
                        'z' => terminator = RecordTerminator::Nul,
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
    Ok((io_mode, mode, terminator, header_mode, report_gbps, files))
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
        libc::STDOUT_FILENO,
        ByteRange::starting_at(start_offset),
        &mut noop,
    )?;
    let emitted_len = std::fs::metadata(path)?.len().saturating_sub(start_offset);
    Ok(copied.map(|written| written.min(emitted_len)))
}

#[derive(Debug)]
struct TailSegment {
    start_offset: u64,
    data: Vec<u8>,
}

#[derive(Debug)]
struct ByteTailPipeWindow {
    slots: Vec<AlignedBuffer>,
    slot_len: usize,
    capacity: usize,
    write_pos: usize,
    total_seen: u64,
}

impl ByteTailPipeWindow {
    fn new(count: u64) -> io::Result<Self> {
        let requested = usize::try_from(count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tail byte count {count} does not fit in usize"),
            )
        })?;
        let capacity = requested
            .max(TAIL_PIPE_WINDOW_MIN_CAPACITY)
            .next_multiple_of(4096);
        let slot_len = capacity.min(TAIL_PIPE_WINDOW_SIZE);
        let slot_count = capacity.div_ceil(slot_len);
        let mut slots = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            slots.push(AlignedBuffer::new_uninit(slot_len)?);
        }
        Ok(Self {
            slots,
            slot_len,
            capacity,
            write_pos: 0,
            total_seen: 0,
        })
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    fn push_bytes(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let slot_index = self.write_pos / self.slot_len;
            let slot_offset = self.write_pos % self.slot_len;
            let copy_len = (self.slot_len - slot_offset).min(data.len());
            self.slots[slot_index][slot_offset..slot_offset + copy_len]
                .copy_from_slice(&data[..copy_len]);
            self.advance(copy_len);
            data = &data[copy_len..];
        }
    }

    fn advance(&mut self, len: usize) {
        self.write_pos = (self.write_pos + len) % self.capacity;
        self.total_seen = self.total_seen.saturating_add(len as u64);
    }

    fn read_from<R: Read>(&mut self, reader: &mut R) -> io::Result<()> {
        loop {
            let slot_index = self.write_pos / self.slot_len;
            let slot_offset = self.write_pos % self.slot_len;
            let read = reader.read(&mut self.slots[slot_index][slot_offset..self.slot_len])?;
            if read == 0 {
                return Ok(());
            }
            self.advance(read);
        }
    }

    fn read_from_raw_fd_until(&mut self, fd: libc::c_int, limit: usize) -> io::Result<bool> {
        let mut remaining = limit;
        while remaining != 0 {
            let slot_index = self.write_pos / self.slot_len;
            let slot_offset = self.write_pos % self.slot_len;
            let read_len = (self.slot_len - slot_offset).min(remaining);
            let read = read_raw_fd(
                fd,
                &mut self.slots[slot_index][slot_offset..slot_offset + read_len],
            )?;
            if read == 0 {
                return Ok(true);
            }
            self.advance(read);
            remaining -= read;
        }
        Ok(false)
    }

    fn for_each_tail_chunk<F>(&self, count: u64, mut visit: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]) -> io::Result<()>,
    {
        let keep = usize::try_from(count.min(self.total_seen)).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tail byte count {count} does not fit in usize"),
            )
        })?;
        if keep == 0 {
            return Ok(0);
        }
        let start = if keep == self.capacity {
            self.write_pos
        } else {
            (self.write_pos + self.capacity - keep) % self.capacity
        };
        let mut remaining = keep;
        let mut pos = start;
        while remaining > 0 {
            let slot_index = pos / self.slot_len;
            let slot_offset = pos % self.slot_len;
            let chunk_len = remaining.min(self.slot_len - slot_offset);
            visit(&self.slots[slot_index][slot_offset..slot_offset + chunk_len])?;
            remaining -= chunk_len;
            pos = (pos + chunk_len) % self.capacity;
        }
        Ok(keep)
    }

    fn write_last<W: Write>(&self, out: &mut W, count: u64) -> io::Result<()> {
        self.for_each_tail_chunk(count, |chunk| out.write_all(chunk))?;
        Ok(())
    }

    fn write_last_to_raw_fd(&self, fd: libc::c_int, count: u64) -> io::Result<u64> {
        let keep = self.for_each_tail_chunk(count, |chunk| write_raw_fd_all(fd, chunk))?;
        Ok(keep as u64)
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

#[derive(Debug, Default)]
struct TailWindow {
    segments: VecDeque<TailSegment>,
    total_len: u64,
}

impl TailWindow {
    fn push_block(&mut self, block: &[u8]) {
        if block.is_empty() {
            return;
        }
        let start_offset = self.total_len;
        self.total_len = self.total_len.saturating_add(block.len() as u64);
        self.segments.push_back(TailSegment {
            start_offset,
            data: block.to_vec(),
        });
    }

    fn trim_before(&mut self, keep_from: u64) {
        while let Some(front) = self.segments.front() {
            let front_end = front.start_offset.saturating_add(front.data.len() as u64);
            if front_end <= keep_from {
                self.segments.pop_front();
            } else {
                break;
            }
        }
        if let Some(front) = self.segments.front_mut() {
            if keep_from > front.start_offset {
                let trim = (keep_from - front.start_offset) as usize;
                front.data.drain(..trim);
                front.start_offset = keep_from;
            }
        }
    }

    fn write_all<W: Write>(&self, out: &mut W) -> io::Result<()> {
        for segment in &self.segments {
            out.write_all(&segment.data)?;
        }
        Ok(())
    }
}

fn write_tail_bytes_windowed_from_reader<W: Write, R: Read>(
    out: &mut W,
    reader: &mut R,
    count: u64,
) -> io::Result<()> {
    if count == 0 {
        return Ok(());
    }
    let mut window = ByteTailPipeWindow::new(count)?;
    window.read_from(reader)?;
    window.write_last(out, count)
}

fn try_write_tail_stdin_small_prefetched(count: u64) -> io::Result<bool> {
    let mut window = ByteTailPipeWindow::new(count)?;
    let prebuffer_limit = TAIL_STDIN_PREBUFFER_LIMIT
        .max(count as usize)
        .min(TAIL_PIPE_WINDOW_SIZE);
    if window.read_from_raw_fd_until(libc::STDIN_FILENO, prebuffer_limit)? {
        window.write_last_to_raw_fd(libc::STDOUT_FILENO, count)?;
        return Ok(true);
    }

    let mut pipe_fds = [0; 2];
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        window.read_from_raw_fd_until(libc::STDIN_FILENO, usize::MAX)?;
        window.write_last_to_raw_fd(libc::STDOUT_FILENO, count)?;
        return Ok(true);
    }
    let pipe_read = pipe_fds[0];
    let pipe_write = pipe_fds[1];
    let result = (|| -> io::Result<bool> {
        let desired = STREAM_WINDOW_BLOCK_SIZE
            .max(count as usize)
            .saturating_add(4096);
        let actual_size = pipe_size_best_effort(pipe_write, desired)?;
        if actual_size as u64 <= count {
            window.read_from_raw_fd_until(libc::STDIN_FILENO, usize::MAX)?;
            window.write_last_to_raw_fd(libc::STDOUT_FILENO, count)?;
            return Ok(true);
        }
        grow_pipe_best_effort(libc::STDIN_FILENO)?;
        grow_pipe_best_effort(libc::STDOUT_FILENO)?;
        let mut buffered = window.write_last_to_raw_fd(pipe_write, count)?;
        let dev_null = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
        let dev_null_fd = dev_null.as_raw_fd();
        loop {
            let free_space = (actual_size as u64).saturating_sub(buffered);
            if free_space == 0 {
                let drop_len = buffered.saturating_sub(count);
                if drop_len == 0 {
                    window.read_from_raw_fd_until(libc::STDIN_FILENO, usize::MAX)?;
                    window.write_last_to_raw_fd(libc::STDOUT_FILENO, count)?;
                    return Ok(true);
                }
                let dropped = splice_all(pipe_read, dev_null_fd, drop_len)?;
                if dropped != drop_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "pipe tail short drop while trimming prefetched prefix",
                    ));
                }
                buffered -= dropped;
                continue;
            }
            let read_len = free_space.min(TAIL_PIPE_WINDOW_SIZE as u64) as usize;
            let moved = unsafe {
                libc::splice(
                    libc::STDIN_FILENO,
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
                            "pipe tail short drop while trimming prefetched prefix",
                        ));
                    }
                    buffered -= dropped;
                }
                continue;
            }
            if moved == 0 {
                let emitted = splice_all(pipe_read, libc::STDOUT_FILENO, buffered)?;
                return Ok(emitted == buffered);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR | libc::EAGAIN) => continue,
                Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
                    window.read_from_raw_fd_until(libc::STDIN_FILENO, usize::MAX)?;
                    window.write_last_to_raw_fd(libc::STDOUT_FILENO, count)?;
                    return Ok(true);
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

fn try_write_tail_pipe_bytes_fast(input: &StreamInput, count: u64) -> io::Result<bool> {
    if count == 0 {
        return Ok(true);
    }
    match input {
        StreamInput::Stdin { .. } => {
            let copied = if count <= TAIL_PIPE_WINDOW_SIZE as u64 {
                return try_write_tail_stdin_small_prefetched(count);
            } else {
                copy_pipe_tail_to_stdout_large(libc::STDIN_FILENO, count)?
            };
            Ok(copied.is_some())
        }
        StreamInput::File(path) => {
            let file = File::open(path)?;
            let copied = if count <= TAIL_PIPE_WINDOW_SIZE as u64 {
                copy_pipe_tail_to_stdout_small(file.as_raw_fd(), count)?
            } else {
                copy_pipe_tail_to_stdout_large(file.as_raw_fd(), count)?
            };
            Ok(copied.is_some())
        }
    }
}

fn write_tail_from_start<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    mode: TailMode,
    terminator: RecordTerminator,
) -> io::Result<()> {
    match mode {
        TailMode::Bytes(TailCount::FromStart(count)) => {
            let mut skip = count.saturating_sub(1);
            visit_ordered_input(input, io_mode, |block| {
                if skip >= block.len() as u64 {
                    skip -= block.len() as u64;
                    return Ok(());
                }
                let start = skip as usize;
                skip = 0;
                out.write_all(&block[start..])
            })
        }
        TailMode::Lines(TailCount::FromStart(start_line)) => {
            let mut remaining = start_line.saturating_sub(1);
            visit_ordered_input(input, io_mode, |block| {
                if remaining == 0 {
                    return out.write_all(block);
                }
                for newline_offset in memchr_iter(terminator.byte(), block) {
                    remaining -= 1;
                    if remaining == 0 {
                        return out.write_all(&block[newline_offset + 1..]);
                    }
                }
                Ok(())
            })
        }
        _ => unreachable!("from-start helper only accepts +N tail modes"),
    }
}

fn write_tail_windowed<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    mode: TailMode,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut window = TailWindow::default();
    match mode {
        TailMode::Bytes(TailCount::FromEnd(count)) => match input {
            StreamInput::Stdin { .. } => {
                grow_pipe_best_effort(libc::STDIN_FILENO)?;
                let mut reader = stdin_buf_reader()?;
                return write_tail_bytes_windowed_from_reader(out, &mut reader, count);
            }
            StreamInput::File(path) => {
                let file = File::open(path)?;
                grow_pipe_best_effort(file.as_raw_fd())?;
                let mut reader = BufReader::new(file);
                return write_tail_bytes_windowed_from_reader(out, &mut reader, count);
            }
        },
        TailMode::Lines(TailCount::FromEnd(lines)) => {
            if lines == 0 {
                return Ok(());
            }
            let keep_starts = lines.saturating_add(1) as usize;
            let mut line_starts = VecDeque::from([0_u64]);
            visit_ordered_input(input, io_mode, |block| {
                let block_start = window.total_len;
                window.push_block(block);
                for newline_offset in memchr_iter(terminator.byte(), block) {
                    line_starts.push_back(block_start + newline_offset as u64 + 1);
                    if line_starts.len() > keep_starts {
                        line_starts.pop_front();
                    }
                }
                if let Some(&keep_from) = line_starts.front() {
                    window.trim_before(keep_from);
                }
                Ok(())
            })?;
            while matches!(line_starts.back(), Some(&offset) if offset >= window.total_len) {
                line_starts.pop_back();
            }
            while line_starts.len() > lines as usize {
                line_starts.pop_front();
            }
            window.trim_before(line_starts.front().copied().unwrap_or(window.total_len));
        }
        _ => unreachable!("windowed helper only accepts trailing tail modes"),
    }
    window.write_all(out)
}

pub(super) fn run_tail(args: &[String]) -> io::Result<()> {
    let (io_mode, mode, terminator, header_mode, report_throughput, files) =
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
        }
        let emitted_bytes = match input {
            StreamInput::File(path) if is_regular_input_path(path)? => {
                let start_offset = regular_tail_start_offset(path, mode, terminator)?;
                if let Some(out) = out.as_mut() {
                    out.flush()?;
                }
                if let Some(written) = try_write_tail_regular_path_fast(path, start_offset)? {
                    written
                } else {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_regular_range(&mut counted, path, ByteRange::starting_at(start_offset))?;
                    counted.bytes_written()
                }
            }
            StreamInput::Stdin { .. } => {
                if let Some(path) = regular_stdin_path()? {
                    let start_offset = regular_tail_start_offset(path, mode, terminator)?;
                    if let Some(out) = out.as_mut() {
                        out.flush()?;
                    }
                    if let Some(written) = try_write_tail_regular_path_fast(path, start_offset)? {
                        written
                    } else {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        let mut counted = CountingWrite::new(out);
                        write_regular_range(
                            &mut counted,
                            path,
                            ByteRange::starting_at(start_offset),
                        )?;
                        counted.bytes_written()
                    }
                } else if let TailMode::Bytes(TailCount::FromEnd(count)) = mode {
                    if !report_throughput {
                        if let Some(out) = out.as_mut() {
                            out.flush()?;
                        }
                    }
                    if !report_throughput && try_write_tail_pipe_bytes_fast(input, count)? {
                        count
                    } else {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        let mut counted = CountingWrite::new(out);
                        write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                        counted.bytes_written()
                    }
                } else if matches!(mode, TailMode::Lines(TailCount::FromEnd(_))) {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                    counted.bytes_written()
                } else {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_from_start(&mut counted, input, io_mode, mode, terminator)?;
                    counted.bytes_written()
                }
            }
            StreamInput::File(path) => {
                if let TailMode::Bytes(TailCount::FromEnd(count)) = mode {
                    let file_type = std::fs::metadata(path)?.file_type();
                    if file_type.is_fifo() && !report_throughput {
                        if let Some(out) = out.as_mut() {
                            out.flush()?;
                        }
                        if !try_write_tail_pipe_bytes_fast(input, count)? {
                            let out = out.get_or_insert(stdout_buf_writer()?);
                            write_tail_windowed(out, input, io_mode, mode, terminator)?;
                        }
                        count
                    } else if file_type.is_fifo() {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        let mut counted = CountingWrite::new(out);
                        write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                        counted.bytes_written()
                    } else if matches!(
                        mode,
                        TailMode::Bytes(TailCount::FromEnd(_))
                            | TailMode::Lines(TailCount::FromEnd(_))
                    ) {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        let mut counted = CountingWrite::new(out);
                        write_tail_windowed(&mut counted, input, io_mode, mode, terminator)?;
                        counted.bytes_written()
                    } else {
                        let out = out.get_or_insert(stdout_buf_writer()?);
                        let mut counted = CountingWrite::new(out);
                        write_tail_from_start(&mut counted, input, io_mode, mode, terminator)?;
                        counted.bytes_written()
                    }
                } else {
                    let out = out.get_or_insert(stdout_buf_writer()?);
                    let mut counted = CountingWrite::new(out);
                    write_tail_from_start(&mut counted, input, io_mode, mode, terminator)?;
                    counted.bytes_written()
                }
            }
        };
        total_output_bytes = total_output_bytes
            .checked_add(emitted_bytes)
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
mod tests {
    use super::*;

    #[test]
    fn reverse_tail_line_scan_handles_trailing_newline() {
        let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fro-tail-unit-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("tail-lines.txt");
        std::fs::write(&path, b"alpha\nbeta\ngamma\n").unwrap();

        assert_eq!(
            regular_tail_line_start(path.to_str().unwrap(), 1, RecordTerminator::Newline).unwrap(),
            11
        );
        assert_eq!(
            regular_tail_line_start(path.to_str().unwrap(), 2, RecordTerminator::Newline).unwrap(),
            6
        );
    }

    #[test]
    fn reverse_tail_line_scan_handles_missing_trailing_newline() {
        let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fro-tail-unit-no-nl-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("tail-lines.txt");
        std::fs::write(&path, b"alpha\nbeta\ngamma").unwrap();

        assert_eq!(
            regular_tail_line_start(path.to_str().unwrap(), 1, RecordTerminator::Newline).unwrap(),
            11
        );
        assert_eq!(
            regular_tail_line_start(path.to_str().unwrap(), 2, RecordTerminator::Newline).unwrap(),
            6
        );
    }

    #[test]
    fn byte_tail_pipe_window_uses_64k_floor_for_small_counts() {
        let window = ByteTailPipeWindow::new(65_536).unwrap();
        assert_eq!(window.capacity(), TAIL_PIPE_WINDOW_MIN_CAPACITY);
    }

    #[test]
    fn byte_tail_pipe_window_rounds_large_counts_to_4k_multiple() {
        let window = ByteTailPipeWindow::new(65_537).unwrap();
        assert_eq!(window.capacity(), 69_632);
    }

    #[test]
    fn byte_tail_pipe_window_wraps_and_keeps_requested_suffix() {
        let mut window = ByteTailPipeWindow::new(65_536).unwrap();
        let input = (0..(TAIL_PIPE_WINDOW_SIZE + 32_768))
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();
        window.push_bytes(&input);

        let mut out = Vec::new();
        window.write_last(&mut out, 65_536).unwrap();
        assert_eq!(out, input[input.len() - 65_536..]);
    }

    #[test]
    fn tail_windowed_lines_drop_older_complete_lines() {
        let input = StreamInput::File(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("test-tmp")
                .join(format!("fro-tail-window-lines-{}", std::process::id()))
                .join("lines.txt")
                .to_string_lossy()
                .into_owned(),
        );
        let path = match &input {
            StreamInput::File(path) => path,
            _ => unreachable!(),
        };
        let path_buf = std::path::PathBuf::from(path);
        std::fs::create_dir_all(path_buf.parent().unwrap()).unwrap();
        std::fs::write(path, b"alpha\nbeta\ngamma").unwrap();

        let mut out = Vec::new();
        write_tail_windowed(
            &mut out,
            &input,
            IOMode::PageCache,
            TailMode::Lines(TailCount::FromEnd(2)),
            RecordTerminator::Newline,
        )
        .unwrap();
        assert_eq!(out, b"beta\ngamma");
    }
}
