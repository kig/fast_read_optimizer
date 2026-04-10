use crate::common::{AlignedBuffer, CopyStrategy, IOMode};
use crate::config::LoadedConfig;
use crate::io_util::{
    checked_posix_fallocate, note_direct_unaligned_fallback, open_direct_reader_or_fallback,
    open_direct_writer_or_fallback,
};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use rand::RngExt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedWriteParams {
    pub use_direct: bool,
    pub qd: usize,
    pub block_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedWritePattern {
    Random,
    Zero,
}

#[allow(dead_code)]
struct PendingAppend {
    buffer: AlignedBuffer,
    len: usize,
}

#[allow(dead_code)]
struct PendingOffsetWrite {
    buffer: AlignedBuffer,
    len: usize,
    end_offset: u64,
}

#[allow(dead_code)]
pub struct SequentialWriter {
    file_page_cache: File,
    file_direct: File,
    io_uring: IoUring,
    pending: Vec<Option<PendingAppend>>,
    bytes_written: u64,
    bytes_submitted: u64,
    use_direct: bool,
    block_size: usize,
    staging: Vec<u8>,
}

pub struct BufWriter {
    inner: BufWriterInner,
}

enum BufWriterInner {
    Threaded {
        tx: SyncSender<BufWriteRequest>,
        finish_handle: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    },
    Direct(std::io::BufWriter<File>),
}

enum BufWriteRequest {
    Data(Vec<u8>),
    Flush(mpsc::Sender<std::io::Result<()>>),
    Shutdown(mpsc::Sender<std::io::Result<()>>),
}

#[allow(dead_code)]
pub struct OffsetWriter {
    file_page_cache: File,
    file_direct: File,
    io_uring: IoUring,
    pending: Vec<Option<PendingOffsetWrite>>,
    bytes_written: u64,
    max_written_extent: u64,
    use_direct: bool,
}

#[allow(dead_code)]
impl SequentialWriter {
    pub fn create(
        path: &str,
        qd: usize,
        block_size: u64,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Self::open(path, qd, block_size, io_mode, true)
    }

    #[allow(dead_code)]
    pub fn open_append(
        path: &str,
        qd: usize,
        block_size: u64,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Self::open(path, qd, block_size, io_mode, false)
    }

    fn open(
        path: &str,
        qd: usize,
        block_size: u64,
        io_mode: IOMode,
        truncate: bool,
    ) -> std::io::Result<Self> {
        let (file_page_cache, file_direct, bytes_written) =
            open_writer_files(path, truncate, None)?;
        Self::from_open_files(
            file_page_cache,
            file_direct,
            bytes_written,
            qd,
            block_size,
            io_mode != IOMode::PageCache,
        )
    }

    pub fn from_file(file: File, qd: usize, block_size: u64) -> std::io::Result<Self> {
        let file_direct = file.try_clone()?;
        Self::from_open_files(file, file_direct, 0, qd, block_size, false)
    }

    fn from_open_files(
        file_page_cache: File,
        file_direct: File,
        bytes_written: u64,
        qd: usize,
        block_size: u64,
        use_direct: bool,
    ) -> std::io::Result<Self> {
        if qd == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "qd must be greater than zero",
            ));
        }
        if block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block_size must be greater than zero",
            ));
        }
        Ok(Self {
            file_page_cache,
            file_direct,
            io_uring: IoUring::new(1024).map_err(io::Error::other)?,
            pending: std::iter::repeat_with(|| None).take(qd).collect(),
            bytes_written,
            bytes_submitted: bytes_written,
            use_direct,
            block_size: aligned_block_size(block_size as usize),
            staging: Vec::new(),
        })
    }

    pub fn append(&mut self, data: &[u8]) -> std::io::Result<u64> {
        let start = self.bytes_written;
        self.bytes_written += data.len() as u64;
        self.staging.extend_from_slice(data);
        self.submit_ready_chunks(false)?;
        Ok(start)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.submit_ready_chunks(true)?;
        while self.pending.iter().any(Option::is_some) {
            self.wait_for_one()?;
        }
        if !self.staging.is_empty() {
            let tail = std::mem::take(&mut self.staging);
            self.submit_chunk(&tail, false)?;
            while self.pending.iter().any(Option::is_some) {
                self.wait_for_one()?;
            }
        }
        self.file_page_cache.flush()
    }

    fn submit_ready_chunks(&mut self, flush_all: bool) -> std::io::Result<()> {
        while self.pending.iter().all(Option::is_some) {
            self.wait_for_one()?;
        }

        loop {
            let chunk_len = if self.use_direct {
                if self.staging.len() < self.block_size {
                    0
                } else {
                    self.block_size
                }
            } else if flush_all {
                self.staging.len().min(self.block_size)
            } else if self.staging.len() >= self.block_size {
                self.block_size
            } else {
                0
            };

            if chunk_len == 0 {
                return Ok(());
            }

            let chunk = self.staging[..chunk_len].to_vec();
            self.submit_chunk(&chunk, self.use_direct)?;
            self.staging.drain(..chunk_len);

            if self.pending.iter().all(Option::is_some) {
                self.wait_for_one()?;
            }
        }
    }

    fn submit_chunk(&mut self, data: &[u8], direct_requested: bool) -> std::io::Result<()> {
        if self.pending.iter().all(Option::is_some) {
            self.wait_for_one()?;
        }

        let slot = self
            .pending
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| io::Error::other("pending slot should exist"))?;
        let start = self.bytes_submitted;
        self.bytes_submitted += data.len() as u64;

        let mut buffer = AlignedBuffer::new(data.len());
        buffer.as_mut_slice().copy_from_slice(data);
        let use_direct = direct_requested && start % 4096 == 0 && data.len() % 4096 == 0;
        if direct_requested && !use_direct {
            note_direct_unaligned_fallback("write", start, data.len());
        }
        let fd = if use_direct {
            self.file_direct.as_raw_fd()
        } else {
            self.file_page_cache.as_raw_fd()
        };
        // SAFETY: `buffer` stays owned in `self.pending[slot]` until the CQE arrives, `start`
        // tracks a valid file offset for this append, and `slot` is reserved uniquely above.
        unsafe {
            let mut sqe = self
                .io_uring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            sqe.prep_write(fd, &buffer.as_slice()[..data.len()], start);
            sqe.set_user_data(slot as u64);
        }
        self.pending[slot] = Some(PendingAppend {
            buffer,
            len: data.len(),
        });
        self.io_uring.submit_sqes().map_err(io::Error::other)?;
        Ok(())
    }

    fn wait_for_one(&mut self) -> std::io::Result<()> {
        let cq = self.io_uring.wait_for_cqe().map_err(io::Error::other)?;
        let slot = cq.user_data() as usize;
        let written = cq.result()?;
        let pending = self.pending[slot]
            .take()
            .ok_or_else(|| std::io::Error::other("missing pending append"))?;
        let _keep_buffer_alive = pending.buffer;
        if written as usize != pending.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!(
                    "short write: expected {} bytes, wrote {}",
                    pending.len, written
                ),
            ));
        }
        Ok(())
    }
}

impl Write for SequentialWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.append(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        SequentialWriter::flush(self)
    }
}

impl BufWriter {
    pub fn stdout(qd: usize, block_size: u64, channel_depth: usize) -> io::Result<Self> {
        let file = fro::command_io::stdout_file()?;
        let _ = qd;
        let capacity = channel_depth.max(1) * aligned_block_size(block_size as usize);
        Ok(Self {
            inner: BufWriterInner::Direct(std::io::BufWriter::with_capacity(capacity, file)),
        })
    }

    #[allow(dead_code)]
    pub fn new(file: File, qd: usize, block_size: u64) -> io::Result<Self> {
        Self::with_capacity(4, file, qd, block_size)
    }

    pub fn with_capacity(
        channel_capacity: usize,
        file: File,
        qd: usize,
        block_size: u64,
    ) -> io::Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<BufWriteRequest>(channel_capacity.max(1));
        let (init_tx, init_rx) = mpsc::sync_channel::<io::Result<()>>(1);
        let finish_handle = std::thread::spawn(move || -> io::Result<()> {
            let mut writer = match SequentialWriter::from_file(file, qd, block_size) {
                Ok(writer) => {
                    let _ = init_tx.send(Ok(()));
                    writer
                }
                Err(err) => {
                    let init_err = io::Error::new(err.kind(), err.to_string());
                    let _ = init_tx.send(Err(init_err));
                    return Err(err);
                }
            };
            run_buf_writer_loop(&mut writer, rx)
        });
        init_rx
            .recv()
            .map_err(|err| io::Error::other(err.to_string()))??;
        Ok(Self {
            inner: BufWriterInner::Threaded {
                tx,
                finish_handle: Some(finish_handle),
            },
        })
    }

    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.write_vec(buf.to_vec())
    }

    pub fn write_vec(&mut self, buf: Vec<u8>) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        match &mut self.inner {
            BufWriterInner::Threaded { tx, .. } => tx
                .send(BufWriteRequest::Data(buf))
                .map_err(|err| io::Error::other(err.to_string())),
            BufWriterInner::Direct(writer) => writer.write_all(&buf),
        }
    }

    pub fn flush_shared(&mut self) -> io::Result<()> {
        match &mut self.inner {
            BufWriterInner::Threaded { tx, .. } => {
                let (reply_tx, reply_rx) = mpsc::channel();
                tx.send(BufWriteRequest::Flush(reply_tx))
                    .map_err(|err| io::Error::other(err.to_string()))?;
                reply_rx
                    .recv()
                    .map_err(|err| io::Error::other(err.to_string()))?
            }
            BufWriterInner::Direct(writer) => writer.flush(),
        }
    }

    pub fn into_inner(mut self) -> io::Result<()> {
        match &mut self.inner {
            BufWriterInner::Threaded { tx, finish_handle } => {
                let (reply_tx, reply_rx) = mpsc::channel();
                tx.send(BufWriteRequest::Shutdown(reply_tx))
                    .map_err(|err| io::Error::other(err.to_string()))?;
                let shutdown_result = reply_rx
                    .recv()
                    .map_err(|err| io::Error::other(err.to_string()))?;
                let handle = finish_handle
                    .take()
                    .ok_or_else(|| io::Error::other("buf writer already finished"))?;
                shutdown_result?;
                handle
                    .join()
                    .map_err(|_| io::Error::other("buf writer thread panicked"))?
            }
            BufWriterInner::Direct(writer) => {
                writer.flush()?;
                Ok(())
            }
        }
    }
}

impl Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.write_vec(buf.to_vec())?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_shared()
    }
}

fn run_buf_writer_loop(
    writer: &mut SequentialWriter,
    rx: Receiver<BufWriteRequest>,
) -> io::Result<()> {
    while let Ok(request) = rx.recv() {
        match request {
            BufWriteRequest::Data(chunk) => writer.write_all(&chunk)?,
            BufWriteRequest::Flush(reply_tx) => {
                let result = writer.flush();
                let is_err = result.is_err();
                let _ = reply_tx.send(result);
                if is_err {
                    return Err(io::Error::other("buf writer flush failed"));
                }
            }
            BufWriteRequest::Shutdown(reply_tx) => {
                let result = writer.flush();
                let flush_ok = result.is_ok();
                let _ = reply_tx.send(result);
                if flush_ok {
                    return Ok(());
                }
                return Err(io::Error::other("buf writer shutdown flush failed"));
            }
        }
    }
    writer.flush()
}

#[allow(dead_code)]
impl OffsetWriter {
    pub fn create(
        path: &str,
        total_size: u64,
        qd: usize,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Self::with_truncate(path, total_size, qd, io_mode, true)
    }

    pub fn with_truncate(
        path: &str,
        total_size: u64,
        qd: usize,
        io_mode: IOMode,
        truncate: bool,
    ) -> std::io::Result<Self> {
        if qd == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "qd must be greater than zero",
            ));
        }
        let (file_page_cache, file_direct, _) =
            open_writer_files(path, truncate, Some(total_size))?;
        Ok(Self {
            file_page_cache,
            file_direct,
            io_uring: IoUring::new(1024).map_err(io::Error::other)?,
            pending: std::iter::repeat_with(|| None).take(qd).collect(),
            bytes_written: 0,
            max_written_extent: 0,
            use_direct: io_mode != IOMode::PageCache,
        })
    }

    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        if self.pending.iter().all(Option::is_some) {
            self.wait_for_one()?;
        }
        let slot = self
            .pending
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| io::Error::other("pending slot should exist"))?;
        let mut buffer = AlignedBuffer::new(data.len());
        buffer.as_mut_slice().copy_from_slice(data);
        let use_direct = self.use_direct && offset % 4096 == 0 && data.len() % 4096 == 0;
        if self.use_direct && !use_direct {
            note_direct_unaligned_fallback("write", offset, data.len());
        }
        let fd = if use_direct {
            self.file_direct.as_raw_fd()
        } else {
            self.file_page_cache.as_raw_fd()
        };
        // SAFETY: `buffer` is kept alive in `self.pending[slot]` until completion, `offset` is
        // the caller-provided destination offset, and `slot` is uniquely reserved above.
        unsafe {
            let mut sqe = self
                .io_uring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            sqe.prep_write(fd, &buffer.as_slice()[..data.len()], offset);
            sqe.set_user_data(slot as u64);
        }
        self.pending[slot] = Some(PendingOffsetWrite {
            buffer,
            len: data.len(),
            end_offset: offset.checked_add(data.len() as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "write end offset overflowed")
            })?,
        });
        self.io_uring.submit_sqes().map_err(io::Error::other)?;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    #[allow(dead_code)]
    pub fn written_extent(&self) -> u64 {
        self.max_written_extent
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        while self.pending.iter().any(Option::is_some) {
            self.wait_for_one()?;
        }
        self.file_page_cache.flush()
    }

    fn wait_for_one(&mut self) -> std::io::Result<()> {
        let cq = self.io_uring.wait_for_cqe().map_err(io::Error::other)?;
        let slot = cq.user_data() as usize;
        let written = cq.result()?;
        let pending = self.pending[slot]
            .take()
            .ok_or_else(|| std::io::Error::other("missing pending offset write"))?;
        let _keep_buffer_alive = pending.buffer;
        if written as usize != pending.len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!(
                    "short write: expected {} bytes, wrote {}",
                    pending.len, written
                ),
            ));
        }
        self.bytes_written += pending.len as u64;
        self.max_written_extent = self.max_written_extent.max(pending.end_offset);
        Ok(())
    }
}

#[allow(dead_code)]
fn aligned_block_size(block_size: usize) -> usize {
    block_size.max(4096) / 4096 * 4096
}

#[allow(dead_code)]
fn open_writer_files(
    path: &str,
    truncate: bool,
    set_len: Option<u64>,
) -> std::io::Result<(File, File, u64)> {
    let mut page_cache_options = OpenOptions::new();
    page_cache_options.write(true).create(true);
    if truncate {
        page_cache_options.truncate(true);
    }
    let file_page_cache = page_cache_options.open(path)?;
    let is_regular_file = file_page_cache.metadata()?.file_type().is_file();
    if let Some(size) = set_len {
        if is_regular_file {
            let current_len = file_page_cache.metadata()?.len();
            if truncate || current_len < size {
                file_page_cache.set_len(size)?;
                checked_posix_fallocate(
                    &file_page_cache,
                    0,
                    size,
                    "failed to preallocate writer output",
                )?;
            }
        }
    }
    let bytes_written = if is_regular_file {
        file_page_cache.metadata()?.len()
    } else {
        0
    };
    let file_direct = open_direct_writer_or_fallback(path, &file_page_cache)?;
    Ok((file_page_cache, file_direct, bytes_written))
}

#[allow(dead_code)]
pub fn resolve_writer_params_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> ResolvedWriteParams {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    let use_direct = io_mode != IOMode::PageCache;
    let qd = if use_direct { direct.qd } else { page_cache.qd };
    let block_size = if use_direct {
        direct.block_size
    } else {
        page_cache.block_size
    };
    ResolvedWriteParams {
        use_direct,
        qd,
        block_size,
    }
}

fn thread_writer(
    thread_id: u64,
    source_file: Option<(&File, &File)>,
    source_buffer: Option<&[u8]>,
    dest_file: (&File, &File),
    source_base_offset: u64,
    dest_base_offset: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    io_uring: &mut IoUring,
    write_count: Arc<AtomicU64>,
    progress_count: Option<Arc<AtomicU64>>,
    random_block: Option<&[u8]>,
    total_size: u64,
    use_direct_read: bool,
    use_direct_write: bool,
) -> io::Result<()> {
    let mut buffers = Vec::new();
    for _ in 0..qd {
        let mut buffer = AlignedBuffer::new(block_size as usize);
        if let Some(rb) = random_block {
            buffer.as_mut_slice().copy_from_slice(rb);
        }
        buffers.push(buffer);
    }

    let mut inflight = 0;
    let mut next_offset = thread_id * block_size;
    let mut buffer_offsets = vec![0u64; qd];

    for i in 0..qd {
        if next_offset >= total_size {
            break;
        }
        buffer_offsets[i] = next_offset;
        let len = (total_size - next_offset).min(block_size);

        let src_offset = source_base_offset + next_offset;
        let dst_offset = dest_base_offset + next_offset;
        let is_aligned_read = (src_offset % 4096 == 0) && (len == block_size);
        if use_direct_read && !is_aligned_read {
            note_direct_unaligned_fallback("copy-read", src_offset, len as usize);
        }
        if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
            let fd = if use_direct_read && is_aligned_read {
                src_direct.as_raw_fd()
            } else {
                src_pagecache.as_raw_fd()
            };
            // SAFETY: `buffers[i]` remains allocated until the matching CQE is processed, and the
            // chosen fd matches the alignment constraints checked just above.
            unsafe {
                let mut sqe = io_uring
                    .prepare_sqe()
                    .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                sqe.prep_read(
                    fd,
                    &mut buffers[i].as_mut_slice()[..len as usize],
                    src_offset,
                );
                sqe.set_user_data((i as u64) | (1u64 << 40));
            }
        } else {
            fill_write_buffer(
                &mut buffers[i].as_mut_slice()[..len as usize],
                source_buffer,
                random_block,
                source_base_offset,
                next_offset,
            )?;
            let is_aligned_write = (dst_offset % 4096 == 0) && (len == block_size);
            if use_direct_write && !is_aligned_write {
                note_direct_unaligned_fallback("write", dst_offset, len as usize);
            }
            let fd = if use_direct_write && is_aligned_write {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
            // SAFETY: `buffers[i]` stays alive for the in-flight write, and direct writes are only
            // selected for block-aligned offsets/full-block lengths.
            unsafe {
                let mut sqe = io_uring
                    .prepare_sqe()
                    .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                sqe.prep_write(fd, &buffers[i].as_slice()[..len as usize], dst_offset);
                sqe.set_user_data((i as u64) | (2u64 << 40));
            }
        }
        next_offset += num_threads * block_size;
        inflight += 1;
    }
    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(io::Error::other)?;

    while inflight > 0 {
        let cq = io_uring.wait_for_cqe().map_err(io::Error::other)?;
        let user_data = cq.user_data();
        let idx = (user_data & 0xFFFFFFFF) as usize;
        let state = (user_data >> 40) as u8;
        let result = cq.result()?;

        if state == 1 {
            // Read finished
            let len = result as u64;
            let dst_offset = dest_base_offset + buffer_offsets[idx];
            let is_aligned_write = (dst_offset % 4096 == 0) && (len % 4096 == 0);
            if use_direct_write && !is_aligned_write {
                note_direct_unaligned_fallback("write", dst_offset, len as usize);
            }
            let fd = if use_direct_write && is_aligned_write {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
            // SAFETY: The buffer remains owned by `buffers[idx]` across submission/completion, and
            // the write range corresponds to the completed read for this slot.
            unsafe {
                let mut sqe = io_uring
                    .prepare_sqe()
                    .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                sqe.prep_write(fd, &buffers[idx].as_slice()[..result as usize], dst_offset);
                sqe.set_user_data((idx as u64) | (2u64 << 40));
            }
            io_uring.submit_sqes().map_err(io::Error::other)?;
        } else {
            // Write finished
            write_count.fetch_add(result as u64, Ordering::SeqCst);
            if let Some(progress_count) = progress_count.as_ref() {
                progress_count.fetch_add(result as u64, Ordering::SeqCst);
            }
            inflight -= 1;
            if next_offset < total_size {
                buffer_offsets[idx] = next_offset;
                let len = (total_size - next_offset).min(block_size);

                let src_offset = source_base_offset + next_offset;
                let dst_offset = dest_base_offset + next_offset;
                let is_aligned_read = (src_offset % 4096 == 0) && (len == block_size);
                if use_direct_read && !is_aligned_read {
                    note_direct_unaligned_fallback("copy-read", src_offset, len as usize);
                }
                if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
                    let fd = if use_direct_read && is_aligned_read {
                        src_direct.as_raw_fd()
                    } else {
                        src_pagecache.as_raw_fd()
                    };
                    // SAFETY: `buffers[idx]` remains allocated for this slot, and the direct-read
                    // path is only used when the offset/length satisfy the required alignment.
                    unsafe {
                        let mut sqe = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe.prep_read(
                            fd,
                            &mut buffers[idx].as_mut_slice()[..len as usize],
                            src_offset,
                        );
                        sqe.set_user_data((idx as u64) | (1u64 << 40));
                    }
                } else {
                    fill_write_buffer(
                        &mut buffers[idx].as_mut_slice()[..len as usize],
                        source_buffer,
                        random_block,
                        source_base_offset,
                        next_offset,
                    )?;
                    let is_aligned_write = (dst_offset % 4096 == 0) && (len == block_size);
                    if use_direct_write && !is_aligned_write {
                        note_direct_unaligned_fallback("write", dst_offset, len as usize);
                    }
                    let fd = if use_direct_write && is_aligned_write {
                        dest_file.0.as_raw_fd()
                    } else {
                        dest_file.1.as_raw_fd()
                    };
                    // SAFETY: `buffers[idx]` stays alive until the corresponding CQE, and the
                    // chosen fd matches the alignment checks above.
                    unsafe {
                        let mut sqe = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe.prep_write(fd, &buffers[idx].as_slice()[..len as usize], dst_offset);
                        sqe.set_user_data((idx as u64) | (2u64 << 40));
                    }
                }
                io_uring.submit_sqes().map_err(io::Error::other)?;
                next_offset += num_threads * block_size;
                inflight += 1;
            }
        }
    }
    Ok(())
}

fn fill_write_buffer(
    destination: &mut [u8],
    source_buffer: Option<&[u8]>,
    random_block: Option<&[u8]>,
    source_base_offset: u64,
    chunk_offset: u64,
) -> io::Result<()> {
    if let Some(source_buffer) = source_buffer {
        let start =
            usize::try_from(source_base_offset.saturating_add(chunk_offset)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer offset does not fit in usize",
                )
            })?;
        let end = start
            .checked_add(destination.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer range overflows"))?;
        let source = source_buffer.get(start..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("buffer range {}..{} is out of bounds", start, end),
            )
        })?;
        destination.copy_from_slice(source);
        return Ok(());
    }

    if let Some(random_block) = random_block {
        destination.copy_from_slice(&random_block[..destination.len()]);
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "writer needs either a source file, source buffer, or random block",
    ))
}

mod api;
mod copy_ops;
mod copy_strategy;
#[cfg(kani)]
mod kani_proofs;
#[cfg(test)]
mod tests;

pub use api::*;
pub use copy_ops::*;
pub use copy_strategy::*;
