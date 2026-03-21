use crate::common::{AlignedBuffer, CopyStrategy, IOMode};
use crate::config::LoadedConfig;
use crate::io_util::{open_direct_reader_or_fallback, open_direct_writer_or_fallback};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use rand::RngExt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
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
        let (file_page_cache, file_direct, bytes_written) =
            open_writer_files(path, truncate, None)?;
        Ok(Self {
            file_page_cache,
            file_direct,
            io_uring: IoUring::new(1024).map_err(io::Error::other)?,
            pending: std::iter::repeat_with(|| None).take(qd).collect(),
            bytes_written,
            bytes_submitted: bytes_written,
            use_direct: io_mode != IOMode::PageCache,
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
        let fd = if use_direct {
            self.file_direct.as_raw_fd()
        } else {
            self.file_page_cache.as_raw_fd()
        };
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
        let fd = if use_direct {
            self.file_direct.as_raw_fd()
        } else {
            self.file_page_cache.as_raw_fd()
        };
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
                unsafe {
                    libc::posix_fallocate(file_page_cache.as_raw_fd(), 0, size as i64);
                }
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
        if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
            let fd = if use_direct_read && is_aligned_read {
                src_direct.as_raw_fd()
            } else {
                src_pagecache.as_raw_fd()
            };
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
            let fd = if use_direct_write && is_aligned_write {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
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
            let fd = if use_direct_write && is_aligned_write {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
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
            inflight -= 1;
            if next_offset < total_size {
                buffer_offsets[idx] = next_offset;
                let len = (total_size - next_offset).min(block_size);

                let src_offset = source_base_offset + next_offset;
                let dst_offset = dest_base_offset + next_offset;
                let is_aligned_read = (src_offset % 4096 == 0) && (len == block_size);
                if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
                    let fd = if use_direct_read && is_aligned_read {
                        src_direct.as_raw_fd()
                    } else {
                        src_pagecache.as_raw_fd()
                    };
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
                    let fd = if use_direct_write && is_aligned_write {
                        dest_file.0.as_raw_fd()
                    } else {
                        dest_file.1.as_raw_fd()
                    };
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

fn prepare_copy_destination(
    filename: &str,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
) -> io::Result<()> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let metadata = f.metadata()?;
    if metadata.file_type().is_file() {
        let required_size = dest_offset.saturating_add(copy_size);
        let current_len = metadata.len();
        let needs_resize = if truncate_target {
            current_len != required_size
        } else {
            current_len < required_size
        };
        if needs_resize {
            f.set_len(required_size)?;
            if current_len < required_size {
                let fadvise_offset = to_off_t(dest_offset, "destination offset")?;
                let fadvise_len = to_off_t(copy_size, "copy size")?;
                let allocate_len = to_off_t(required_size, "required size")?;
                unsafe {
                    libc::posix_fadvise(
                        f.as_raw_fd(),
                        fadvise_offset,
                        fadvise_len,
                        libc::POSIX_FADV_NOREUSE,
                    );
                    libc::posix_fallocate(f.as_raw_fd(), 0, allocate_len);
                }
            }
        }
    }
    Ok(())
}

fn to_off_t(value: u64, field_name: &'static str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field_name} does not fit in off_t"),
        )
    })
}

fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let read = file.read_at(&mut buf[filled..], offset + filled as u64)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "short read at offset {}: expected {} bytes, got {}",
                    offset,
                    buf.len(),
                    filled
                ),
            ));
        }
        filled += read;
    }
    Ok(())
}

pub fn copy_file_range_syscall(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy does not support direct read/write modes",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file source",
        ));
    }
    let copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;
    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file destination",
        ));
    }

    let mut copied_total = 0_u64;
    let mut source_position: libc::loff_t = source_offset.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "source offset does not fit in loff_t",
        )
    })?;
    let mut target_position: libc::loff_t = dest_offset.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination offset does not fit in loff_t",
        )
    })?;

    while copied_total < copy_size {
        let remaining = copy_size - copied_total;
        let chunk = remaining.min(usize::MAX as u64) as usize;
        let copied = unsafe {
            libc::copy_file_range(
                source_file.as_raw_fd(),
                &mut source_position,
                target_file.as_raw_fd(),
                &mut target_position,
                chunk,
                0,
            )
        };
        if copied < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(io::Error::new(
                err.kind(),
                format!("copy_file_range syscall failed: {}", err),
            ));
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "copy_file_range stopped early after {} of {} bytes",
                    copied_total, copy_size
                ),
            ));
        }
        copied_total = copied_total.saturating_add(copied as u64);
    }

    Ok(copied_total)
}

pub fn copy_file_range_chunked(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy does not support direct read/write modes",
        ));
    }
    if copy_range_threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires at least one worker thread",
        ));
    }
    if copy_range_block_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a non-zero block size",
        ));
    }
    if copy_range_qd == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires qd greater than zero",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file source",
        ));
    }
    let copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;
    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file destination",
        ));
    }
    if copy_size == 0 {
        return Ok(0);
    }

    let chunk_size = copy_range_block_size
        .checked_mul(copy_range_qd as u64)
        .filter(|&size| size > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "copy_file_range chunk size overflowed",
            )
        })?;
    let next_offset = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();

    for _ in 0..copy_range_threads {
        let next_offset = next_offset.clone();
        let source = source.to_string();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<u64> {
            let source_file = File::open(&source)?;
            let target_file = OpenOptions::new().read(true).write(true).open(&filename)?;
            let mut copied_local = 0_u64;

            loop {
                let local_offset = next_offset.fetch_add(chunk_size, Ordering::SeqCst);
                if local_offset >= copy_size {
                    return Ok(copied_local);
                }
                let remaining = copy_size - local_offset;
                let extent_len = remaining.min(chunk_size);
                let mut src_pos: libc::loff_t = source_offset
                    .checked_add(local_offset)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "source offset overflowed")
                    })?
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "source offset does not fit in loff_t",
                        )
                    })?;
                let mut dst_pos: libc::loff_t = dest_offset
                    .checked_add(local_offset)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "destination offset overflowed",
                        )
                    })?
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "destination offset does not fit in loff_t",
                        )
                    })?;
                let mut copied_extent = 0_u64;
                while copied_extent < extent_len {
                    let step = (extent_len - copied_extent).min(usize::MAX as u64) as usize;
                    let copied = unsafe {
                        libc::copy_file_range(
                            source_file.as_raw_fd(),
                            &mut src_pos,
                            target_file.as_raw_fd(),
                            &mut dst_pos,
                            step,
                            0,
                        )
                    };
                    if copied < 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(io::Error::new(
                            err.kind(),
                            format!("copy_file_range syscall failed: {}", err),
                        ));
                    }
                    if copied == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "copy_file_range stopped early after {} of {} bytes for chunk at {}",
                                copied_extent, extent_len, local_offset
                            ),
                        ));
                    }
                    copied_extent += copied as u64;
                }
                copied_local += copied_extent;
            }
        }));
    }

    let mut copied_total = 0_u64;
    for thread in threads {
        copied_total += thread
            .join()
            .map_err(|_| io::Error::other("copy_file_range worker thread panicked"))??;
    }
    if copied_total != copy_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "copy_file_range copied {} bytes but expected {}",
                copied_total, copy_size
            ),
        ));
    }
    Ok(copied_total)
}

pub fn copy_file_reflink(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy does not support direct read/write modes",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy requires a regular-file source",
        ));
    }
    let effective_copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    if source_offset != 0
        || dest_offset != 0
        || !truncate_target
        || effective_copy_size != source_meta.len()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy currently supports only whole-file copies into a truncated destination",
        ));
    }

    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy requires a regular-file destination",
        ));
    }

    let rc = unsafe {
        libc::ioctl(
            target_file.as_raw_fd(),
            libc::FICLONE,
            source_file.as_raw_fd(),
        )
    };
    if rc != 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            format!("reflink clone failed: {}", err),
        ));
    }

    Ok(source_meta.len())
}

fn copy_file_range_threaded_impl(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    sync_after_write: bool,
) -> io::Result<u64> {
    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let source_meta = File::open(source)?.metadata()?;
    let source_size = source_meta.len();
    let copy_size = copy_size.min(source_size.saturating_sub(source_offset));

    let direct_write = io_mode_write != IOMode::PageCache;
    let direct_read = io_mode_read == IOMode::Direct || io_mode_read == IOMode::Auto;

    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };

    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source = source.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let src_nodir = File::open(&source)?;
            let src_dir = open_direct_reader_or_fallback(&source, &src_nodir)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                Some((&src_dir, &src_nodir)),
                None,
                (&dest_file_dir, &dest_file_nodir),
                source_offset,
                dest_offset,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                None,
                copy_size,
                direct_read,
                direct_write,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }

    if sync_after_write && io_mode_write != IOMode::PageCache {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)?
            .sync_all()?;
    }

    Ok(write_count.load(Ordering::SeqCst))
}

pub fn copy_file_range_threaded(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    copy_file_range_threaded_impl(
        source,
        filename,
        source_offset,
        dest_offset,
        copy_size,
        truncate_target,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode_read,
        io_mode_write,
        true,
    )
}

pub fn overwrite_changed_chunks_direct(
    source: &str,
    filename: &str,
    scan_threads: u64,
    scan_block_size: u64,
    scan_qd: usize,
    _num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
) -> io::Result<u64> {
    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk overwrite requires a regular-file source",
        ));
    }

    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk overwrite requires a regular-file destination",
        ));
    }

    let source_len = source_meta.len();
    let target_len = target_meta.len();
    prepare_copy_destination(filename, 0, source_len, false)?;

    let num_threads = usize::try_from(scan_threads.max(1)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk thread count does not fit in usize",
        )
    })?;
    let scan_block_size = usize::try_from(scan_block_size.max(4096)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk block size does not fit in usize",
        )
    })?;
    let write_block_size = usize::try_from(block_size_d.max(4096)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "direct write block size does not fit in usize",
        )
    })?;
    let stride = (num_threads as u64)
        .checked_mul(scan_block_size as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "changed-chunk stride overflowed",
            )
        })?;
    let common_len = source_len.min(target_len);
    let mut threads = Vec::with_capacity(num_threads);

    for thread_id in 0..num_threads {
        let source = source.to_string();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let source_file = File::open(&source)?;
            let target_file = File::open(&filename)?;
            let mut writer =
                OffsetWriter::with_truncate(&filename, source_len, qd_d, IOMode::Direct, false)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            let read_qd = scan_qd.max(1);
            let mut source_buffers = Vec::with_capacity(read_qd);
            let mut target_buffers = Vec::with_capacity(read_qd);
            for _ in 0..read_qd {
                source_buffers.push(AlignedBuffer::new(scan_block_size));
                target_buffers.push(AlignedBuffer::new(scan_block_size));
            }
            let mut ready_source = vec![false; read_qd];
            let mut ready_target = vec![false; read_qd];
            let mut buffer_offsets = vec![0u64; read_qd];
            let mut inflight = 0usize;
            let mut free_slots: Vec<usize> = (0..read_qd).collect();
            let mut next_offset = (thread_id as u64)
                .checked_mul(scan_block_size as u64)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk offset overflowed",
                    )
                })?;

            while next_offset < common_len && free_slots.last().is_some() {
                let slot = free_slots.pop().unwrap();
                let len = (common_len - next_offset).min(scan_block_size as u64) as usize;
                buffer_offsets[slot] = next_offset;
                unsafe {
                    let mut sqe_source = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe_source.prep_read(
                        source_file.as_raw_fd(),
                        &mut source_buffers[slot].as_mut_slice()[..len],
                        next_offset,
                    );
                    sqe_source.set_user_data(slot as u64);

                    let mut sqe_target = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe_target.prep_read(
                        target_file.as_raw_fd(),
                        &mut target_buffers[slot].as_mut_slice()[..len],
                        next_offset,
                    );
                    sqe_target.set_user_data((slot as u64) | (1u64 << 40));
                }
                next_offset = next_offset.checked_add(stride).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk offset overflowed",
                    )
                })?;
                inflight += 2;
            }
            if inflight > 0 {
                io_uring.submit_sqes().map_err(io::Error::other)?;
            }

            while inflight > 0 {
                let cq = io_uring.wait_for_cqe().map_err(io::Error::other)?;
                let ud = cq.user_data();
                let slot = (ud & 0xFFFF_FFFF) as usize;
                let target_side = (ud >> 40) != 0;
                cq.result()?;
                inflight -= 1;
                if target_side {
                    ready_target[slot] = true;
                } else {
                    ready_source[slot] = true;
                }

                let mut ready_slots = vec![slot];
                while io_uring.cq_ready() > 0 {
                    let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                        io::Error::other("completion queue reported ready but no CQE was available")
                    })?;
                    let ud = cq.user_data();
                    let slot = (ud & 0xFFFF_FFFF) as usize;
                    let target_side = (ud >> 40) != 0;
                    cq.result()?;
                    inflight -= 1;
                    if target_side {
                        ready_target[slot] = true;
                    } else {
                        ready_source[slot] = true;
                    }
                    if !ready_slots.contains(&slot) {
                        ready_slots.push(slot);
                    }
                }

                let mut submitted = false;
                for slot in ready_slots {
                    if !(ready_source[slot] && ready_target[slot]) {
                        continue;
                    }
                    ready_source[slot] = false;
                    ready_target[slot] = false;
                    let offset = buffer_offsets[slot];
                    let len = usize::try_from((common_len - offset).min(scan_block_size as u64))
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "changed-chunk length does not fit in usize",
                            )
                        })?;
                    if source_buffers[slot].as_slice()[..len]
                        != target_buffers[slot].as_slice()[..len]
                    {
                        let mut write_offset = 0usize;
                        while write_offset < len {
                            let write_end = (write_offset + write_block_size).min(len);
                            writer.write_at(
                                offset + write_offset as u64,
                                &source_buffers[slot].as_slice()[write_offset..write_end],
                            )?;
                            write_offset = write_end;
                        }
                    }
                    free_slots.push(slot);
                }

                while next_offset < common_len && !free_slots.is_empty() {
                    let slot = free_slots.pop().unwrap();
                    let len = (common_len - next_offset).min(scan_block_size as u64) as usize;
                    buffer_offsets[slot] = next_offset;
                    unsafe {
                        let mut sqe_source = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe_source.prep_read(
                            source_file.as_raw_fd(),
                            &mut source_buffers[slot].as_mut_slice()[..len],
                            next_offset,
                        );
                        sqe_source.set_user_data(slot as u64);

                        let mut sqe_target = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe_target.prep_read(
                            target_file.as_raw_fd(),
                            &mut target_buffers[slot].as_mut_slice()[..len],
                            next_offset,
                        );
                        sqe_target.set_user_data((slot as u64) | (1u64 << 40));
                    }
                    next_offset = next_offset.checked_add(stride).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "changed-chunk offset overflowed",
                        )
                    })?;
                    inflight += 2;
                    submitted = true;
                }
                if submitted {
                    io_uring.submit_sqes().map_err(io::Error::other)?;
                }
            }

            let mut tail_offset = common_len
                .checked_add((thread_id as u64) * (scan_block_size as u64))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "changed-chunk tail overflowed")
                })?;
            while tail_offset < source_len {
                let len = usize::try_from((source_len - tail_offset).min(scan_block_size as u64))
                    .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk tail length does not fit in usize",
                    )
                })?;
                let mut tail_buf = vec![0u8; len];
                read_full_at(&source_file, &mut tail_buf, tail_offset)?;
                let mut write_offset = 0usize;
                while write_offset < len {
                    let write_end = (write_offset + write_block_size).min(len);
                    writer.write_at(
                        tail_offset + write_offset as u64,
                        &tail_buf[write_offset..write_end],
                    )?;
                    write_offset = write_end;
                }
                tail_offset = tail_offset.checked_add(stride).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "changed-chunk tail overflowed")
                })?;
            }

            writer.flush()?;
            Ok(())
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("changed-chunk worker thread panicked"))??;
    }

    target_file.set_len(source_len)?;
    target_file.sync_all()?;
    Ok(source_len)
}

pub fn copy_file_range_with_strategy(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
) -> io::Result<u64> {
    match copy_strategy {
        CopyStrategy::Auto | CopyStrategy::Threaded => copy_file_range_threaded(
            source,
            filename,
            source_offset,
            dest_offset,
            copy_size,
            truncate_target,
            num_threads_p,
            block_size_p,
            qd_p,
            num_threads_d,
            block_size_d,
            qd_d,
            io_mode_read,
            io_mode_write,
        ),
        CopyStrategy::CopyFileRange => copy_file_range_chunked(
            source,
            filename,
            source_offset,
            dest_offset,
            copy_size,
            truncate_target,
            copy_range_threads,
            copy_range_block_size,
            copy_range_qd,
            io_mode_read,
            io_mode_write,
        ),
        CopyStrategy::CopyFileRangeSingle => copy_file_range_syscall(
            source,
            filename,
            source_offset,
            dest_offset,
            copy_size,
            truncate_target,
            io_mode_read,
            io_mode_write,
        ),
        CopyStrategy::Reflink => copy_file_reflink(
            source,
            filename,
            source_offset,
            dest_offset,
            copy_size,
            truncate_target,
            io_mode_read,
            io_mode_write,
        ),
    }
}

/*
write (direct)                      | 10.40        | 10.00        | PASS
write (auto, hot)                   | 10.50        | 10.00        | PASS

write (page cache, cold)            | 2.50         | 10.00        | REGRESSION
write (page cache, hot)             | 3.20         | 10.00        | REGRESSION
write (auto, cold)                  | 1.70         | 10.00        | REGRESSION
*/
pub fn write_file(
    filename: &str,
    create_size: Option<u64>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    return _write_file_internal(
        None,
        filename,
        create_size,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        IOMode::Direct,
        io_mode_write,
        GeneratedWritePattern::Random,
    );
}

pub fn write_generated_file(
    filename: &str,
    total_size: u64,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
    pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    _write_file_internal(
        None,
        filename,
        Some(total_size),
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        IOMode::Direct,
        io_mode_write,
        pattern,
    )
}

/*
copy (direct)                       | 6.50         | 6.00         | PASS
copy (auto, cold)                   | 6.40         | 6.00         | PASS

copy (page cache, cold)             | 1.00         | 0.50         | PASS

copy (hot cache R, direct W)        | 2.60         | 10.00        | REGRESSION
copy (auto, hot)                    | 1.40         | 10.00        | REGRESSION
*/
#[allow(dead_code)]
pub fn copy_file(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    return copy_file_with_strategy(
        source_filename,
        target_filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        CopyStrategy::Threaded,
    );
}

pub fn copy_file_with_strategy(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
) -> io::Result<u64> {
    copy_file_with_strategy_and_truncate(
        source_filename,
        target_filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
        true,
    )
}

pub fn copy_file_with_strategy_and_truncate(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
    truncate_target: bool,
) -> io::Result<u64> {
    return copy_file_range_with_strategy(
        source_filename,
        target_filename,
        0,
        0,
        u64::MAX,
        truncate_target,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
    );
}

pub fn write_buffer(
    filename: &str,
    data: &[u8],
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    write_buffer_range(
        filename,
        data,
        0,
        data.len(),
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode_write,
    )
}

pub fn write_buffer_range(
    filename: &str,
    data: &[u8],
    buffer_offset: usize,
    buffer_len: usize,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    let end = buffer_offset
        .checked_add(buffer_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer range overflows"))?;
    let slice = data.get(buffer_offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "buffer range {}..{} is out of bounds for {} bytes",
                buffer_offset,
                end,
                data.len()
            ),
        )
    })?;

    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let direct_write = io_mode_write != IOMode::PageCache;
    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };
    let total_size = slice.len() as u64;
    let source_buffer: Arc<[u8]> = Arc::from(slice);

    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            f.set_len(total_size)?;
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source_buffer = Arc::clone(&source_buffer);
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                None,
                Some(source_buffer.as_ref()),
                (&dest_file_dir, &dest_file_nodir),
                0,
                0,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                None,
                total_size,
                false,
                direct_write,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }

    Ok(write_count.load(Ordering::SeqCst))
}

fn _write_file_internal(
    source: Option<&str>,
    filename: &str,
    create_size: Option<u64>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    generated_pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let (total_size, _file_cached) = if let Some(s) = source {
        let file_cached = Ok(true) == is_first_page_resident(s);
        let f = File::open(s)?;
        (f.metadata()?.len(), file_cached)
    } else if let Some(size) = create_size {
        (size, false)
    } else {
        let f = File::open(filename)?;
        (f.metadata()?.len(), false)
    };

    let direct_write = io_mode_write != IOMode::PageCache;
    let direct_read = io_mode_read == IOMode::Direct || io_mode_read == IOMode::Auto;

    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };

    // println!("direct-read: {} | direct-write: {} | t={} bs={} qd={}", direct_read, direct_write, num_threads, block_size / 1024, qd);

    let random_block = if source.is_none() {
        let mut block = vec![0u8; block_size as usize];
        if generated_pattern == GeneratedWritePattern::Random {
            rand::rng().fill(&mut block[..]);
        }
        Some(Arc::new(block))
    } else {
        None
    };

    // Ensure target file exists and has the correct size.
    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            f.set_len(total_size)?;
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source = source.map(|s| s.to_string());
        let random_block = random_block.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let source_files = source
                .map(|s| -> io::Result<(File, File)> {
                    let s_nodir = File::open(&s)?;
                    let s_dir = open_direct_reader_or_fallback(&s, &s_nodir)?;
                    Ok((s_dir, s_nodir))
                })
                .transpose()?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                source_files.as_ref().map(|(d, n)| (d, n)),
                None,
                (&dest_file_dir, &dest_file_nodir),
                0,
                0,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                random_block.as_ref().map(|b| &b[..]),
                total_size,
                direct_read,
                direct_write,
            )
        }));
    }
    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }
    Ok(write_count.load(Ordering::SeqCst))
}

pub fn bench_mmap_write(filename: &str) {
    let size = 1024 * 1024 * 1024; // 1 GB
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();
    f.set_len(size as u64).unwrap();
    unsafe {
        libc::posix_fallocate(f.as_raw_fd(), 0, size as i64);
    }
    let fd = f.as_raw_fd();

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        panic!("mmap failed");
    }

    let num_threads = 16;
    let start = std::time::Instant::now();

    let mut threads = vec![];
    let chunk_size = size / num_threads;

    for t in 0..num_threads {
        let thread_ptr_addr = unsafe { ptr.add(t * chunk_size) as usize };
        threads.push(std::thread::spawn(move || {
            let slice =
                unsafe { std::slice::from_raw_parts_mut(thread_ptr_addr as *mut u8, chunk_size) };
            let mut rng = rand::rng();
            let mut block = vec![0u8; 1024 * 1024];
            rng.fill(&mut block[..]);

            for i in 0..(chunk_size / block.len()) {
                slice[i * block.len()..(i + 1) * block.len()].copy_from_slice(&block);
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    // Ensure data is written to disk
    unsafe {
        libc::msync(ptr, size, libc::MS_SYNC);
    }

    let dur = start.elapsed().as_secs_f64();
    println!(
        "Parallel Mmap write 1 GB in {:.4} s, {:.1} GB/s",
        dur,
        1.0 / dur
    );

    unsafe {
        libc::munmap(ptr, size);
    }
}

pub fn bench_write(filename: &str) {
    let size = 1024 * 1024 * 1024; // 1 GB
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();
    unsafe {
        libc::posix_fallocate(f.as_raw_fd(), 0, size as i64);
    }

    let mut block = vec![0u8; 1024 * 1024];
    rand::rng().fill(&mut block[..]);

    let start = std::time::Instant::now();

    for _ in 0..(size / block.len()) {
        f.write_all(&block).unwrap();
    }

    f.sync_all().unwrap();

    let dur = start.elapsed().as_secs_f64();
    println!("Standard write 1 GB in {:.4} s, {:.1} GB/s", dur, 1.0 / dur);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("{}-{}-{}", prefix, std::process::id(), nanos));
        path
    }

    #[test]
    fn sequential_writer_appends_in_order() {
        let path = unique_temp_file("fro-sequential-writer");
        let mut writer =
            SequentialWriter::create(path.to_str().unwrap(), 3, 4096, IOMode::Auto).unwrap();

        let a = vec![0x11; 4096];
        let b = vec![0x22; 123];
        let c = vec![0x33; 8192];

        assert_eq!(writer.append(&a).unwrap(), 0);
        assert_eq!(writer.append(&b).unwrap(), a.len() as u64);
        assert_eq!(writer.append(&c).unwrap(), (a.len() + b.len()) as u64);
        writer.flush().unwrap();

        let data = fs::read(&path).unwrap();
        assert_eq!(writer.bytes_written(), (a.len() + b.len() + c.len()) as u64);
        assert_eq!(data.len(), a.len() + b.len() + c.len());
        assert_eq!(&data[..a.len()], &a);
        assert_eq!(&data[a.len()..a.len() + b.len()], &b);
        assert_eq!(&data[a.len() + b.len()..], &c);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_tracks_written_extent_for_sparse_offsets() {
        let path = unique_temp_file("fro-offset-writer-extent");
        let mut writer =
            OffsetWriter::create(path.to_str().unwrap(), 16, 2, IOMode::PageCache).unwrap();

        writer.write_at(12, b"xy").unwrap();
        writer.write_at(0, b"abcd").unwrap();
        writer.flush().unwrap();

        assert_eq!(writer.bytes_written(), 6);
        assert_eq!(writer.written_extent(), 14);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_rejects_end_offset_overflow() {
        let path = unique_temp_file("fro-offset-writer-overflow");
        let mut writer =
            OffsetWriter::create(path.to_str().unwrap(), 16, 2, IOMode::PageCache).unwrap();

        let err = writer.write_at(u64::MAX - 1, b"abcd").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn prepare_copy_destination_keeps_exact_sized_target_intact() {
        let path = unique_temp_file("fro-prepare-copy-destination");
        fs::write(&path, b"abcdefgh").unwrap();

        prepare_copy_destination(path.to_str().unwrap(), 0, 8, true).unwrap();

        assert_eq!(fs::metadata(&path).unwrap().len(), 8);
        assert_eq!(fs::read(&path).unwrap(), b"abcdefgh");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn prepare_copy_destination_rejects_offsets_that_do_not_fit_off_t() {
        let path = unique_temp_file("fro-prepare-copy-destination-large-offset");
        fs::write(&path, b"").unwrap();

        let result = std::panic::catch_unwind(|| {
            prepare_copy_destination(path.to_str().unwrap(), (i64::MAX as u64) + 1, 4096, false)
        });
        assert!(result.is_ok(), "prepare_copy_destination should not panic");
        let err = result.unwrap().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn overwrite_changed_chunks_direct_updates_only_different_chunks() {
        let source = unique_temp_file("fro-overwrite-changed-source");
        let target = unique_temp_file("fro-overwrite-changed-target");
        let mut source_bytes = vec![0x11; 8192];
        source_bytes[4096..8192].fill(0x22);
        let mut target_bytes = source_bytes.clone();
        target_bytes[4096..8192].fill(0x33);
        fs::write(&source, &source_bytes).unwrap();
        fs::write(&target, &target_bytes).unwrap();

        overwrite_changed_chunks_direct(
            source.to_str().unwrap(),
            target.to_str().unwrap(),
            1,
            4096,
            2,
            2,
            4096,
            2,
        )
        .unwrap();

        assert_eq!(fs::read(&target).unwrap(), source_bytes);

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(target);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::to_off_t;
    use std::io;

    #[kani::proof]
    fn to_off_t_accepts_i64_range_values() {
        let value: u64 = kani::any();
        kani::assume(value <= i64::MAX as u64);
        assert_eq!(to_off_t(value, "value").unwrap(), value as i64);
    }

    #[kani::proof]
    fn to_off_t_rejects_values_past_i64_max() {
        let value: u64 = kani::any();
        kani::assume(value > i64::MAX as u64);
        let err = to_off_t(value, "value").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
