use crate::common::{AlignedBuffer, IOMode};
use crate::config::LoadedConfig;
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use rand::RngExt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedWriteParams {
    pub use_direct: bool,
    pub qd: usize,
    pub block_size: u64,
}

struct PendingAppend {
    buffer: AlignedBuffer,
    len: usize,
}

struct PendingOffsetWrite {
    buffer: AlignedBuffer,
    len: usize,
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

pub struct OffsetWriter {
    file_page_cache: File,
    file_direct: File,
    io_uring: IoUring,
    pending: Vec<Option<PendingOffsetWrite>>,
    bytes_written: u64,
    use_direct: bool,
}

impl SequentialWriter {
    pub fn create(
        path: &str,
        qd: usize,
        block_size: u64,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Self::open(path, qd, block_size, io_mode, true)
    }

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

impl OffsetWriter {
    pub fn create(
        path: &str,
        total_size: u64,
        qd: usize,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        if qd == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "qd must be greater than zero",
            ));
        }
        let (file_page_cache, file_direct, _) = open_writer_files(path, true, Some(total_size))?;
        Ok(Self {
            file_page_cache,
            file_direct,
            io_uring: IoUring::new(1024).map_err(io::Error::other)?,
            pending: std::iter::repeat_with(|| None).take(qd).collect(),
            bytes_written: 0,
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
        });
        self.io_uring.submit_sqes().map_err(io::Error::other)?;
        self.bytes_written = self.bytes_written.max(offset + data.len() as u64);
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
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
        Ok(())
    }
}

fn aligned_block_size(block_size: usize) -> usize {
    block_size.max(4096) / 4096 * 4096
}

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
    if let Some(size) = set_len {
        file_page_cache.set_len(size)?;
        unsafe {
            libc::posix_fallocate(file_page_cache.as_raw_fd(), 0, size as i64);
        }
    }
    let bytes_written = file_page_cache.metadata()?.len();
    let file_direct = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
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
    dest_file: (&File, &File),
    num_threads: u64,
    block_size: u64,
    qd: usize,
    io_uring: &mut IoUring,
    write_count: Arc<AtomicU64>,
    random_block: Option<&[u8]>,
    total_size: u64,
    use_direct: bool,
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

        let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
        if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
            let fd = if use_direct && is_aligned {
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
                    next_offset,
                );
                sqe.set_user_data((i as u64) | (1u64 << 40));
            }
        } else {
            let fd = if use_direct && is_aligned {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
            unsafe {
                let mut sqe = io_uring
                    .prepare_sqe()
                    .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                sqe.prep_write(fd, &buffers[i].as_slice()[..len as usize], next_offset);
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
            let is_aligned = (buffer_offsets[idx] % 4096 == 0) && (len % 4096 == 0);
            let fd = if use_direct && is_aligned {
                dest_file.0.as_raw_fd()
            } else {
                dest_file.1.as_raw_fd()
            };
            unsafe {
                let mut sqe = io_uring
                    .prepare_sqe()
                    .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                sqe.prep_write(
                    fd,
                    &buffers[idx].as_slice()[..result as usize],
                    buffer_offsets[idx],
                );
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

                let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
                if let Some((src_direct, src_pagecache)) = source_file.as_ref() {
                    let fd = if use_direct && is_aligned {
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
                            next_offset,
                        );
                        sqe.set_user_data((idx as u64) | (1u64 << 40));
                    }
                } else {
                    let fd = if use_direct && is_aligned {
                        dest_file.0.as_raw_fd()
                    } else {
                        dest_file.1.as_raw_fd()
                    };
                    unsafe {
                        let mut sqe = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe.prep_write(fd, &buffers[idx].as_slice()[..len as usize], next_offset);
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
/*
write (direct)                      | 10.40        | 10.00        | PASS
write (auto, hot)                   | 10.50        | 10.00        | PASS

copy (direct)                       | 6.50         | 6.00         | PASS
copy (auto, cold)                   | 6.40         | 6.00         | PASS

write (page cache, cold)            | 2.50         | 10.00        | REGRESSION
write (page cache, hot)             | 3.20         | 10.00        | REGRESSION
write (auto, cold)                  | 1.70         | 10.00        | REGRESSION

copy (page cache, cold)             | 1.00         | 0.50         | PASS

copy (hot cache R, direct W)        | 2.60         | 10.00        | REGRESSION
copy (auto, hot)                    | 1.40         | 10.00        | REGRESSION

*/

pub fn write_file(
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

    if direct_read && total_size % block_size != 0 {
        eprintln!(
            "Warning: write requested direct source reads but the final partial block will use page-cache reads"
        );
    }
    if direct_write && total_size % block_size != 0 {
        eprintln!(
            "Warning: write requested direct destination writes but the final partial block will use page-cache writes"
        );
    }

    // println!("direct-read: {} | direct-write: {} | t={} bs={} qd={}", direct_read, direct_write, num_threads, block_size / 1024, qd);

    let random_block = if source.is_none() {
        let mut block = vec![0u8; block_size as usize];
        rand::rng().fill(&mut block[..]);
        Some(Arc::new(block))
    } else {
        None
    };

    // Ensure target file exists and has the correct size. Pre-allocate blocks.
    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        f.set_len(total_size)?;
        unsafe {
            libc::posix_fallocate(f.as_raw_fd(), 0, total_size as i64);
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source = source.map(|s| s.to_string());
        let random_block = random_block.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&filename)?;
            let source_files = source
                .map(|s| -> io::Result<(File, File)> {
                    let s_nodir = File::open(&s)?;
                    let s_dir = OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_DIRECT)
                        .open(&s)?;
                    Ok((s_dir, s_nodir))
                })
                .transpose()?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                source_files.as_ref().map(|(d, n)| (d, n)),
                (&dest_file_dir, &dest_file_nodir),
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                random_block.as_ref().map(|b| &b[..]),
                total_size,
                direct_read,
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
}
