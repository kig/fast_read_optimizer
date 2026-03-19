use crate::common::{AlignedBuffer, IOMode};
use crate::config::LoadedConfig;
use crate::io_util::{open_direct_reader_or_fallback, open_direct_writer_or_fallback};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use rand::RngExt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
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
            end_offset: offset + data.len() as u64,
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

pub fn copy_file_range(
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

    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            let required_size = dest_offset.saturating_add(copy_size);
            let current_len = f.metadata()?.len();
            if truncate_target || current_len < required_size {
                f.set_len(required_size)?;
                unsafe {
                    libc::posix_fadvise(
                        f.as_raw_fd(),
                        dest_offset.try_into().unwrap(),
                        copy_size.try_into().unwrap(),
                        libc::POSIX_FADV_NOREUSE,
                    );
                    libc::posix_fallocate(f.as_raw_fd(), 0, required_size as i64);
                }
            }
        }
    }

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

    if io_mode_write != IOMode::PageCache {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)?
            .sync_all()?;
    }

    Ok(write_count.load(Ordering::SeqCst))
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
    );
}

/*
copy (direct)                       | 6.50         | 6.00         | PASS
copy (auto, cold)                   | 6.40         | 6.00         | PASS

copy (page cache, cold)             | 1.00         | 0.50         | PASS

copy (hot cache R, direct W)        | 2.60         | 10.00        | REGRESSION
copy (auto, hot)                    | 1.40         | 10.00        | REGRESSION
*/
pub fn copy_file(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    return copy_file_range(
        source_filename,
        target_filename,
        0,
        0,
        u64::MAX,
        true,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode_read,
        io_mode_write,
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
        rand::rng().fill(&mut block[..]);
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
}
