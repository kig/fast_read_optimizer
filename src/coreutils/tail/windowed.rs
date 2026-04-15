use super::*;

#[derive(Debug)]
struct TailSegment {
    start_offset: u64,
    data: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct ByteTailPipeWindow {
    slots: Vec<AlignedBuffer>,
    slot_len: usize,
    capacity: usize,
    write_pos: usize,
    total_seen: u64,
}

impl ByteTailPipeWindow {
    pub(super) fn new(count: u64) -> io::Result<Self> {
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
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(super) fn push_bytes(&mut self, mut data: &[u8]) {
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

    pub(super) fn write_last<W: Write>(&self, out: &mut W, count: u64) -> io::Result<()> {
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
    if window.read_from_raw_fd_until(fro::command_io::stdin_fd(), prebuffer_limit)? {
        window.write_last_to_raw_fd(fro::command_io::stdout_fd(), count)?;
        return Ok(true);
    }

    let mut pipe_fds = [0; 2];
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        window.read_from_raw_fd_until(fro::command_io::stdin_fd(), usize::MAX)?;
        window.write_last_to_raw_fd(fro::command_io::stdout_fd(), count)?;
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
            window.read_from_raw_fd_until(fro::command_io::stdin_fd(), usize::MAX)?;
            window.write_last_to_raw_fd(fro::command_io::stdout_fd(), count)?;
            return Ok(true);
        }
        grow_pipe_best_effort(fro::command_io::stdin_fd())?;
        grow_pipe_best_effort(fro::command_io::stdout_fd())?;
        let mut buffered = window.write_last_to_raw_fd(pipe_write, count)?;
        let dev_null = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
        let dev_null_fd = dev_null.as_raw_fd();
        loop {
            let free_space = (actual_size as u64).saturating_sub(buffered);
            if free_space == 0 {
                let drop_len = buffered.saturating_sub(count);
                if drop_len == 0 {
                    window.read_from_raw_fd_until(fro::command_io::stdin_fd(), usize::MAX)?;
                    window.write_last_to_raw_fd(fro::command_io::stdout_fd(), count)?;
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
                    fro::command_io::stdin_fd(),
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
                let emitted = splice_all(pipe_read, fro::command_io::stdout_fd(), buffered)?;
                return Ok(emitted == buffered);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR | libc::EAGAIN) => continue,
                Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
                    window.read_from_raw_fd_until(fro::command_io::stdin_fd(), usize::MAX)?;
                    window.write_last_to_raw_fd(fro::command_io::stdout_fd(), count)?;
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

pub(super) fn try_write_tail_pipe_bytes_fast(input: &StreamInput, count: u64) -> io::Result<bool> {
    if count == 0 {
        return Ok(true);
    }
    match input {
        StreamInput::Stdin { .. } => {
            let copied = if count <= TAIL_PIPE_WINDOW_SIZE as u64 {
                return try_write_tail_stdin_small_prefetched(count);
            } else {
                copy_pipe_tail_to_stdout_large(fro::command_io::stdin_fd(), count)?
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

pub(super) fn write_tail_from_start<W: Write>(
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

pub(super) fn write_tail_windowed<W: Write>(
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
                grow_pipe_best_effort(fro::command_io::stdin_fd())?;
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
