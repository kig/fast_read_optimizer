use crate::common::{AlignedBuffer, IOMode};
use crate::config::LoadedConfig;
use crate::io_util::{
    expected_read_len, open_reader_files, validate_read_result, PendingReadSlots,
};
use crate::reader::{
    resolve_reader_params, resolve_reader_params_for_mode, visit_file_blocks,
    visit_file_blocks_with_resolved_params, ResolvedReadParams,
};
use crate::writer::{
    resolve_writer_params_for_mode, OffsetWriter, ResolvedWriteParams, SequentialWriter,
};
use iou::IoUring;
use libc::{fcntl, F_GETFL, F_SETFL, O_DIRECT};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::{mpsc, Arc, Mutex};

fn get_file_flags(file: &std::fs::File) -> std::io::Result<i32> {
    let fd = file.as_raw_fd();
    unsafe {
        // Get current flags
        let flags = fcntl(fd, F_GETFL);
        if flags == -1 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(flags);
    }
}

fn set_file_flags(file: &std::fs::File, flags: i32) -> std::io::Result<i32> {
    let fd = file.as_raw_fd();
    unsafe {
        let flags = fcntl(fd, F_SETFL, flags);
        if flags == -1 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(flags);
    }
}

fn default_logical_block_size(config: &LoadedConfig, mode: &str, path: &str) -> u64 {
    let page_cache = config.get_params_for_path(mode, false, path);
    let direct = config.get_params_for_path(mode, true, path);
    page_cache.block_size.max(direct.block_size)
}

fn effective_io_mode_for_block_size(io_mode: IOMode, block_size: u64) -> IOMode {
    if block_size % 4096 == 0 {
        io_mode
    } else {
        IOMode::PageCache
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelReadReport {
    pub bytes_read: u64,
    pub file_size: u64,
    pub params: crate::reader::ResolvedReadParams,
}

#[derive(Debug)]
pub struct ParallelWriteReport {
    pub bytes_written: u64,
    pub block_ranges: Vec<BlockRange>,
    pub write_params: ResolvedWriteParams,
}

#[derive(Clone)]
pub struct ParallelFile {
    config: LoadedConfig,
    mode: String,
    path: String,
    io_mode: IOMode,
    file: Arc<File>,
}

#[derive(Clone)]
pub struct ParallelWriter {
    tx: mpsc::Sender<WriteRequest>,
    finish_handle:
        Arc<Mutex<Option<std::thread::JoinHandle<std::io::Result<ParallelWriteReport>>>>>,
}

enum WriteRequest {
    ByIndex { index: usize, data: Vec<u8> },
    ByOffset { offset: u64, data: Vec<u8> },
}

enum WriterMode {
    ByIndex { block_count: usize },
    ByOffset { total_size: u64, truncate: bool },
}

impl ParallelFile {
    pub fn open(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Ok(Self {
            config: config.clone(),
            mode: mode.to_string(),
            path: path.to_string(),
            io_mode,
            file: Arc::new(File::open(path)?),
        })
    }

    pub fn len(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn block_count(&self, block_size: u64) -> std::io::Result<usize> {
        if block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block_size must be greater than zero",
            ));
        }
        let file_size = self.len()?;
        Ok(if file_size == 0 {
            0
        } else {
            file_size.div_ceil(block_size) as usize
        })
    }

    pub fn foreach_block_parallel<F>(
        &self,
        block_size: u64,
        visit: F,
    ) -> std::io::Result<ParallelReadReport>
    where
        F: Fn(usize, &[u8]) -> std::io::Result<()> + Send + Sync + 'static,
    {
        let path = self.path.clone();
        let config = self.config.clone();
        let io_mode = effective_io_mode_for_block_size(self.io_mode, block_size);
        let visit = Arc::new(visit);
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        let (bytes_read, file_size, params) = visit_file_blocks(
            &path,
            page_cache.num_threads,
            block_size,
            page_cache.qd,
            direct.num_threads,
            block_size,
            direct.qd,
            io_mode,
            move |block| visit(block.block_index, block.data),
        )?;
        Ok(ParallelReadReport {
            bytes_read,
            file_size,
            params,
        })
    }

    pub fn block_size(&self) -> std::io::Result<u64> {
        Ok(default_logical_block_size(
            &self.config,
            &self.mode,
            &self.path,
        ))
    }

    pub fn foreach_block<F>(&self, visit: F) -> std::io::Result<ParallelReadReport>
    where
        F: Fn(usize, &[u8]) -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.foreach_block_parallel(self.block_size()?, visit)
    }

    pub fn map_reduce_blocks<T, M, R, U>(
        &self,
        block_size: u64,
        map: M,
        reduce: R,
    ) -> std::io::Result<U>
    where
        T: Send + 'static,
        M: Fn(usize, &[u8]) -> std::io::Result<T> + Send + Sync + 'static,
        R: FnOnce(Vec<T>, ParallelReadReport) -> std::io::Result<U>,
    {
        let path = self.path.clone();
        let config = self.config.clone();
        let io_mode = effective_io_mode_for_block_size(self.io_mode, block_size);
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        let params = resolve_reader_params(
            &path,
            &crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size,
                qd: page_cache.qd,
            },
            &crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size,
                qd: direct.qd,
            },
            io_mode,
        )?;
        self.map_reduce_blocks_with_params(params, map, reduce)
    }

    pub fn map_reduce_blocks_with_params<T, M, R, U>(
        &self,
        params: ResolvedReadParams,
        map: M,
        reduce: R,
    ) -> std::io::Result<U>
    where
        T: Send + 'static,
        M: Fn(usize, &[u8]) -> std::io::Result<T> + Send + Sync + 'static,
        R: FnOnce(Vec<T>, ParallelReadReport) -> std::io::Result<U>,
    {
        if params.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block_size must be greater than zero",
            ));
        }

        let block_count = self.block_count(params.block_size)?;
        let mapped = Arc::new(Mutex::new(
            std::iter::repeat_with(|| None)
                .take(block_count)
                .collect::<Vec<Option<T>>>(),
        ));
        let mapped_slots = Arc::clone(&mapped);
        let path = self.path.clone();

        let (bytes_read, file_size) =
            visit_file_blocks_with_resolved_params(&path, params, move |block| {
                let value = map(block.block_index, block.data)?;
                mapped_slots.lock().unwrap()[block.block_index] = Some(value);
                Ok(())
            })?;

        let mapped = Arc::into_inner(mapped)
            .ok_or_else(|| std::io::Error::other("mapped block storage still shared"))?
            .into_inner()
            .map_err(|_| std::io::Error::other("mapped block storage poisoned"))?;
        let values = mapped
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| std::io::Error::other(format!("missing mapped block {index}")))
            })
            .collect::<std::io::Result<Vec<_>>>()?;

        reduce(
            values,
            ParallelReadReport {
                bytes_read,
                file_size,
                params,
            },
        )
    }

    pub fn foreach_index_parallel<F>(&self, job_count: usize, visit: F) -> std::io::Result<()>
    where
        F: Fn(usize) -> std::io::Result<()> + Send + Sync + 'static,
    {
        let params =
            resolve_reader_params_for_mode(&self.config, &self.mode, &self.path, self.io_mode)?;
        let visit = Arc::new(visit);
        let mut threads = Vec::new();
        for thread_id in 0..params.num_threads {
            let visit = visit.clone();
            threads.push(std::thread::spawn(move || -> std::io::Result<()> {
                let mut job_index = thread_id as usize;
                while job_index < job_count {
                    visit(job_index)?;
                    job_index += params.num_threads as usize;
                }
                Ok(())
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|_| std::io::Error::other("indexed worker thread panicked"))??;
        }
        Ok(())
    }

    pub fn read_range(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        let mut filled = 0usize;
        while filled < len {
            let read = self
                .file
                .read_at(&mut bytes[filled..], offset + filled as u64)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "short read at offset {}: expected {} bytes, got {}",
                        offset, len, filled
                    ),
                ));
            }
            filled += read;
        }
        Ok(bytes)
    }
}

impl ParallelWriter {
    pub fn indexed(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        block_count: usize,
    ) -> std::io::Result<Self> {
        Self::spawn(
            path,
            resolve_writer_params_for_mode(config, mode, path, io_mode),
            io_mode,
            WriterMode::ByIndex { block_count },
        )
    }

    pub fn fixed_size(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        total_size: u64,
    ) -> std::io::Result<Self> {
        Self::fixed_size_with_truncate(config, mode, path, io_mode, total_size, true)
    }

    pub fn fixed_size_with_truncate(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        total_size: u64,
        truncate: bool,
    ) -> std::io::Result<Self> {
        Self::spawn(
            path,
            resolve_writer_params_for_mode(config, mode, path, io_mode),
            io_mode,
            WriterMode::ByOffset {
                total_size,
                truncate,
            },
        )
    }

    pub fn write_at_index(&self, index: usize, data: Vec<u8>) -> std::io::Result<()> {
        self.tx
            .send(WriteRequest::ByIndex { index, data })
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    pub fn write_at_offset(&self, offset: u64, data: Vec<u8>) -> std::io::Result<()> {
        self.tx
            .send(WriteRequest::ByOffset { offset, data })
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    pub fn finish(self) -> std::io::Result<ParallelWriteReport> {
        let ParallelWriter { tx, finish_handle } = self;
        drop(tx);
        let handle = finish_handle
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| std::io::Error::other("parallel writer already finished"))?;
        handle
            .join()
            .map_err(|_| std::io::Error::other("parallel writer thread panicked"))?
    }

    fn spawn(
        path: &str,
        write_params: ResolvedWriteParams,
        io_mode: IOMode,
        mode: WriterMode,
    ) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel::<WriteRequest>();
        let output_path = path.to_string();
        let join_handle = std::thread::spawn(move || -> std::io::Result<ParallelWriteReport> {
            match mode {
                WriterMode::ByIndex { block_count } => {
                    let mut out = SequentialWriter::create(
                        &output_path,
                        write_params.qd,
                        write_params.block_size,
                        io_mode,
                    )?;
                    let mut block_ranges = vec![BlockRange { offset: 0, len: 0 }; block_count];
                    let mut pending = BTreeMap::<usize, Vec<u8>>::new();
                    let mut seen = vec![false; block_count];
                    let mut next_index = 0usize;

                    for request in rx {
                        let (index, data) = match request {
                            WriteRequest::ByIndex { index, data } => (index, data),
                            WriteRequest::ByOffset { .. } => {
                                return Err(std::io::Error::other(
                                    "parallel writer is configured for indexed writes",
                                ))
                            }
                        };
                        if index >= block_count {
                            return Err(std::io::Error::other(format!(
                                "block index {} is out of range for {} blocks",
                                index, block_count
                            )));
                        }
                        if seen[index] || pending.contains_key(&index) {
                            return Err(std::io::Error::other(format!(
                                "duplicate write for block index {}",
                                index
                            )));
                        }
                        seen[index] = true;
                        pending.insert(index, data);
                        while let Some(data) = pending.remove(&next_index) {
                            let offset = out.append(&data)?;
                            block_ranges[next_index] = BlockRange {
                                offset,
                                len: data.len() as u64,
                            };
                            next_index += 1;
                        }
                    }

                    if next_index != block_count {
                        return Err(std::io::Error::other(format!(
                            "missing output blocks: wrote {}, expected {}",
                            next_index, block_count
                        )));
                    }
                    out.flush()?;
                    Ok(ParallelWriteReport {
                        bytes_written: out.bytes_written(),
                        block_ranges,
                        write_params,
                    })
                }
                WriterMode::ByOffset {
                    total_size,
                    truncate,
                } => {
                    let mut out = OffsetWriter::with_truncate(
                        &output_path,
                        total_size,
                        write_params.qd,
                        io_mode,
                        truncate,
                    )?;
                    for request in rx {
                        let (offset, data) = match request {
                            WriteRequest::ByOffset { offset, data } => (offset, data),
                            WriteRequest::ByIndex { .. } => {
                                return Err(std::io::Error::other(
                                    "parallel writer is configured for offset writes",
                                ))
                            }
                        };
                        let end_offset =
                            offset.checked_add(data.len() as u64).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "write end offset overflowed",
                                )
                            })?;
                        if end_offset > total_size {
                            return Err(std::io::Error::other(format!(
                                "write past end of file: offset {} len {} total {}",
                                offset,
                                data.len(),
                                total_size
                            )));
                        }
                        out.write_at(offset, &data)?;
                    }
                    out.flush()?;
                    Ok(ParallelWriteReport {
                        bytes_written: out.bytes_written(),
                        block_ranges: Vec::new(),
                        write_params,
                    })
                }
            }
        });

        Ok(Self {
            tx,
            finish_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
    }
}

#[derive(Clone)]
pub struct ParallelStream {}

impl ParallelStream {
    /// Map an input file to an output file using fixed-size output blocks.
    /// The processor closure receives an input block slice and a mutable Vec<u8>
    /// to write output bytes into. The produced Vec is sent to the indexed
    /// ParallelWriter at the same block index. The writer is configured for
    /// `block_count` indexed writes where block_count is derived from the
    /// input file size and the provided read_block_size.
    pub fn map_file_fixed_size<F>(
        config: &LoadedConfig,
        read_path: &str,
        write_path: &str,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8], &mut [u8]) -> std::io::Result<usize> + Send + Sync + 'static,
    {
        if read_block_size == 0 || write_block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block sizes must be greater than zero",
            ));
        }

        // Open reader to query block count and to ensure the file exists.
        let reader = ParallelFile::open(config, "read", read_path, IOMode::Auto)?;
        let block_count = reader.block_count(read_block_size)?;

        // Prepare destination file for fixed-size output blocks.
        let writer_io_mode =
            effective_io_mode_for_block_size(IOMode::Auto, write_block_size as u64);
        let write_params =
            resolve_writer_params_for_mode(config, "write", write_path, writer_io_mode);

        let total_size = (block_count as u64)
            .checked_mul(write_block_size as u64)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "output size overflowed")
            })?;

        // Ensure destination file exists and is sized appropriately.
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(write_path)?;
        if total_size > 0 {
            let metadata = dest_file.metadata()?;
            if metadata.file_type().is_file() {
                let current_len = metadata.len();
                if current_len != total_size {
                    dest_file.set_len(total_size)?;
                    unsafe {
                        libc::posix_fallocate(dest_file.as_raw_fd(), 0, total_size as i64);
                    }
                }
            }
        }

        // Resolve reader params for the requested read_block_size and perform parallel reads.
        // Build IO params using the configured per-path thread and qd values but the
        // caller-provided block_size so reads happen at the requested granularity.
        let page_cache_cfg = config.get_params_for_path("read", false, read_path);
        let direct_cfg = config.get_params_for_path("read", true, read_path);
        let io_mode = effective_io_mode_for_block_size(IOMode::Auto, read_block_size);
        let params = resolve_reader_params(
            read_path,
            &crate::config::IOParams {
                num_threads: page_cache_cfg.num_threads,
                block_size: read_block_size,
                qd: page_cache_cfg.qd,
            },
            &crate::config::IOParams {
                num_threads: direct_cfg.num_threads,
                block_size: read_block_size,
                qd: direct_cfg.qd,
            },
            io_mode,
        )?;
        let processor = Arc::new(processor);
        let mut threads = Vec::new();
        // shared vector to record produced byte ranges per block
        let shared_block_ranges = Arc::new(Mutex::new(vec![None::<BlockRange>; block_count]));

        for thread_id in 0..params.num_threads {
            let read_path = read_path.to_string();
            let write_path = write_path.to_string();
            let processor = processor.clone();
            let params = params; // copy
            let write_block_size = write_block_size;
            let write_params = write_params; // copy
            let shared_block_ranges = shared_block_ranges.clone();

            threads.push(std::thread::spawn(move || -> std::io::Result<u64> {
                // Open per-thread reader files and a reader io_uring
                let (mut file, file_direct) = open_reader_files(&read_path, params.use_direct)?;
                let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
                let mut buffers = Vec::new();
                let mut write_buffers = Vec::new();
                for _ in 0..params.qd {
                    buffers.push(AlignedBuffer::new(read_block_size.try_into().unwrap()));
                    write_buffers.push(AlignedBuffer::new(write_block_size.try_into().unwrap()));
                }

                // Open per-thread destination file descriptors (page-cache and direct fallback)
                let dest_pagecache = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&write_path)?;
                let dest_direct =
                    crate::io_util::open_direct_writer_or_fallback(&write_path, &dest_pagecache)?;

                let file_size = file.seek(SeekFrom::End(0))?;
                let thread_base = thread_id * read_block_size;
                let mut block_num: u64 = 0;
                // inflight counts outstanding io (reads + writes). Start by submitting reads.
                let mut inflight: usize = 0;
                let mut pending = PendingReadSlots::new(params.qd);

                // pending write buffers to keep memory alive until write completion
                struct PendingWrite {
                    len: usize,
                }
                let mut pending_writes: Vec<Option<PendingWrite>> =
                    (0..params.qd).map(|_| None).collect();

                // initial submit of up to qd reads
                for slot in 0..params.qd {
                    let stride = block_num
                        .checked_mul(params.num_threads)
                        .and_then(|v| v.checked_mul(read_block_size))
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "read offset calculation overflowed",
                            )
                        })?;
                    let current_offset = thread_base.checked_add(stride).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "read offset calculation overflowed",
                        )
                    })?;
                    if current_offset >= file_size {
                        break;
                    }
                    pending.reserve(slot, block_num)?;
                    let is_aligned =
                        (current_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                    let fd = if params.use_direct && is_aligned {
                        file_direct.as_raw_fd()
                    } else {
                        file.as_raw_fd()
                    };
                    unsafe {
                        let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                            std::io::Error::other("io_uring submission queue is full")
                        })?;
                        // mark as read state (1)
                        let read_len =
                            expected_read_len(file_size, current_offset, read_block_size)?;
                        sqe.prep_read(
                            fd,
                            &mut buffers[slot].as_mut_slice()[..read_len],
                            current_offset,
                        );
                        sqe.set_user_data((slot as u64) | (1u64 << 40));
                    }
                    block_num += 1;
                    inflight += 1;
                }

                if inflight == 0 {
                    // nothing to do
                    return Ok(0);
                }
                io_uring.submit_sqes().map_err(std::io::Error::other)?;

                // total bytes this thread wrote
                let mut thread_bytes_written: u64 = 0;

                while inflight > 0 {
                    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
                    let user_data = cq.user_data();
                    let mut ready = vec![(user_data, cq.result()? as u32)];
                    while io_uring.cq_ready() > 0 {
                        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                            std::io::Error::other(
                                "completion queue reported ready but no CQE was available",
                            )
                        })?;
                        ready.push((cq.user_data(), cq.result()? as u32));
                    }

                    for (user_data, result) in ready {
                        let slot = (user_data & 0xFFFFFFFF) as usize;
                        let state = (user_data >> 40) as u8;

                        if state == 1 {
                            // Read finished for slot
                            // decrement inflight for the completed read
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on read completion",
                                )
                            })?;

                            // peek block id (do NOT free the slot yet; keep it reserved until write completes)
                            let block_id = pending.peek(slot)?;

                            let stride = block_id
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let current_offset =
                                thread_base.checked_add(stride).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;

                            let expected_len =
                                expected_read_len(file_size, current_offset, read_block_size)?;
                            let actual_len = validate_read_result(
                                "map-file-fixed",
                                current_offset,
                                expected_len,
                                result,
                            )?;

                            if actual_len > 0 {
                                let block_index = (current_offset / params.block_size) as usize;
                                // Reuse the slot buffer for the write: copy processed data into it.
                                let read_buf = &buffers[slot].as_slice()[..actual_len];
                                let out = &mut write_buffers[slot].as_mut_slice();
                                let produced = processor(read_buf, out)?;

                                // copy output into the slot's aligned buffer so the buffer can be
                                // reused for future reads only after the write completes
                                let produced_len = produced;
                                let dst_offset = (block_index as u64)
                                    .checked_mul(write_block_size as u64)
                                    .ok_or_else(|| {
                                        std::io::Error::new(
                                            std::io::ErrorKind::InvalidInput,
                                            "destination offset overflowed",
                                        )
                                    })?;

                                let is_aligned_write =
                                    (dst_offset % 4096 == 0) && (produced_len % 4096 == 0);
                                let fd = if write_params.use_direct && is_aligned_write {
                                    dest_direct.as_raw_fd()
                                } else {
                                    dest_pagecache.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    // mark as write state (2)
                                    sqe.prep_write(
                                        fd,
                                        &write_buffers[slot].as_slice()[..produced_len],
                                        dst_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (2u64 << 40));
                                }
                                // record pending write length; buffer itself is already in buffers[slot]
                                pending_writes[slot] = Some(PendingWrite { len: produced_len });
                                // a write was submitted; increment inflight to account for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                            }

                            // submit any sqes prepared above
                            io_uring.submit_sqes().map_err(std::io::Error::other)?;
                        } else if state == 2 {
                            // Write finished for slot
                            let written = result as usize;
                            let pending_write = pending_writes[slot].take().ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "missing pending write buffer",
                                )
                            })?;
                            if written != pending_write.len {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    format!(
                                        "short write: expected {} bytes, wrote {}",
                                        pending_write.len, written
                                    ),
                                ));
                            }
                            thread_bytes_written = thread_bytes_written
                                .checked_add(written as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "thread bytes_written overflowed",
                                    )
                                })?;

                            // a write completed, so one inflight op finished
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on write completion",
                                )
                            })?;

                            // now free the slot's pending read record so the slot can be reused
                            let completed_block_id = pending.complete(slot)?;

                            // record the produced length for this block in the shared block ranges
                            let block_index = thread_id as usize
                                + (completed_block_id as usize) * (params.num_threads as usize);
                            if block_index >= block_count {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!(
                                        "computed block index {} out of range for {} blocks",
                                        block_index, block_count
                                    ),
                                ));
                            }
                            let dst_offset = (block_index as u64)
                                .checked_mul(write_block_size as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "destination offset overflowed",
                                    )
                                })?;
                            {
                                let mut ranges = shared_block_ranges.lock().unwrap();
                                ranges[block_index] = Some(BlockRange {
                                    offset: dst_offset,
                                    len: written as u64,
                                });
                            }

                            // attempt to submit next read into this slot
                            let stride = block_num
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let next_offset = thread_base.checked_add(stride).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "read offset calculation overflowed",
                                )
                            })?;
                            if next_offset < file_size {
                                pending.reserve(slot, block_num)?;
                                let is_aligned =
                                    (next_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                                let fd = if params.use_direct && is_aligned {
                                    file_direct.as_raw_fd()
                                } else {
                                    file.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    let read_len =
                                        expected_read_len(file_size, next_offset, read_block_size)?;
                                    sqe.prep_read(
                                        fd,
                                        &mut buffers[slot].as_mut_slice()[..read_len],
                                        next_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (1u64 << 40));
                                }
                                block_num += 1;
                                // a read was submitted; increment inflight for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                                io_uring.submit_sqes().map_err(std::io::Error::other)?;
                            }
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "unknown io_uring state",
                            ));
                        }
                    }
                }

                Ok(thread_bytes_written)
            }));
        }

        // collect thread results and sum bytes
        let mut total_written: u64 = 0;
        for thread in threads {
            let written = thread
                .join()
                .map_err(|_| std::io::Error::other("worker thread panicked"))??;
            total_written = total_written.checked_add(written).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "total bytes_written overflowed",
                )
            })?;
        }

        // assemble block ranges from per-thread records
        let mut final_block_ranges = Vec::with_capacity(block_count);
        {
            let ranges = shared_block_ranges.lock().unwrap();
            for (i, entry) in ranges.iter().enumerate() {
                match entry {
                    Some(br) => final_block_ranges.push(*br),
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("missing block range for index {}", i),
                        ));
                    }
                }
            }
        }

        // truncate the destination file to the actual written size to remove unused padding
        if total_written != 0 {
            dest_file.set_len(total_written)?;
        } else if total_size == 0 {
            // nothing to do for empty files
        } else {
            // if no bytes were written but total_size was non-zero, truncate to zero
            dest_file.set_len(0)?;
        }

        Ok(ParallelWriteReport {
            bytes_written: total_written,
            block_ranges: final_block_ranges,
            write_params,
        })
    }

    /// Variant that writes directly into an open destination File (no path-based open).
    /// Caller may open the destination with O_DIRECT set if desired.
    pub fn map_file_fixed_size_to_fd<F>(
        config: &LoadedConfig,
        read_path: &str,
        dest_file: &File,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8], &mut [u8]) -> std::io::Result<usize> + Send + Sync + 'static,
    {
        if read_block_size == 0 || write_block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block sizes must be greater than zero",
            ));
        }

        // Open reader to query block count and to ensure the file exists.
        let reader = ParallelFile::open(config, "read", read_path, IOMode::Auto)?;
        let block_count = reader.block_count(read_block_size)?;

        // Prepare writer params for the provided destination file.
        let writer_io_mode =
            effective_io_mode_for_block_size(IOMode::Auto, write_block_size as u64);
        let write_params =
            resolve_writer_params_for_mode(config, "write", read_path, writer_io_mode);

        let total_size = (block_count as u64)
            .checked_mul(write_block_size as u64)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "output size overflowed")
            })?;

        // If destination file length differs from expected, try to resize it.
        if total_size > 0 {
            let metadata = dest_file.metadata()?;
            if metadata.file_type().is_file() {
                let current_len = metadata.len();
                if current_len != total_size {
                    dest_file.set_len(total_size)?;
                    unsafe {
                        libc::posix_fallocate(dest_file.as_raw_fd(), 0, total_size as i64);
                    }
                }
            }
        }

        // Resolve reader params for the requested read_block_size and perform parallel reads.
        let page_cache_cfg = config.get_params_for_path("read", false, read_path);
        let direct_cfg = config.get_params_for_path("read", true, read_path);
        let io_mode = effective_io_mode_for_block_size(IOMode::Auto, read_block_size);
        let params = resolve_reader_params(
            read_path,
            &crate::config::IOParams {
                num_threads: page_cache_cfg.num_threads,
                block_size: read_block_size,
                qd: page_cache_cfg.qd,
            },
            &crate::config::IOParams {
                num_threads: direct_cfg.num_threads,
                block_size: read_block_size,
                qd: direct_cfg.qd,
            },
            io_mode,
        )?;
        let processor = Arc::new(processor);
        let mut threads = Vec::new();
        // shared vector to record produced byte ranges per block
        let shared_block_ranges = Arc::new(Mutex::new(vec![None::<BlockRange>; block_count]));

        for thread_id in 0..params.num_threads {
            let read_path = read_path.to_string();
            let processor = processor.clone();
            let params = params; // copy
            let write_block_size = write_block_size;
            let write_params = write_params; // copy
            let shared_block_ranges = shared_block_ranges.clone();
            let dest_file = dest_file.try_clone()?;

            threads.push(std::thread::spawn(move || -> std::io::Result<u64> {
                // Open per-thread reader files and a reader io_uring
                let (mut file, file_direct) = open_reader_files(&read_path, params.use_direct)?;
                let write_flags = get_file_flags(&dest_file)?;
                let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
                let mut buffers = Vec::new();
                let mut write_buffers = Vec::new();
                for _ in 0..params.qd {
                    buffers.push(AlignedBuffer::new(read_block_size.try_into().unwrap()));
                    write_buffers.push(AlignedBuffer::new(write_block_size.try_into().unwrap()));
                }

                // Use clones of the provided dest_file for per-thread writes
                let mut write_mode = O_DIRECT;
                let direct_flags = write_flags | O_DIRECT;
                let pagecache_flags = write_flags ^ O_DIRECT;
                set_file_flags(&dest_file, direct_flags)?;

                let file_size = file.seek(SeekFrom::End(0))?;
                let thread_base = thread_id * read_block_size;
                let mut block_num: u64 = 0;
                // inflight counts outstanding io (reads + writes). Start by submitting reads.
                let mut inflight: usize = 0;
                let mut pending = PendingReadSlots::new(params.qd);

                // pending write buffers to keep memory alive until write completion
                struct PendingWrite {
                    len: usize,
                }
                let mut pending_writes: Vec<Option<PendingWrite>> =
                    (0..params.qd).map(|_| None).collect();

                // initial submit of up to qd reads
                for slot in 0..params.qd {
                    let stride = block_num
                        .checked_mul(params.num_threads)
                        .and_then(|v| v.checked_mul(read_block_size))
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "read offset calculation overflowed",
                            )
                        })?;
                    let current_offset = thread_base.checked_add(stride).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "read offset calculation overflowed",
                        )
                    })?;
                    if current_offset >= file_size {
                        break;
                    }
                    pending.reserve(slot, block_num)?;
                    let is_aligned =
                        (current_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                    let fd = if params.use_direct && is_aligned {
                        file_direct.as_raw_fd()
                    } else {
                        file.as_raw_fd()
                    };
                    unsafe {
                        let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                            std::io::Error::other("io_uring submission queue is full")
                        })?;
                        // mark as read state (1)
                        let read_len =
                            expected_read_len(file_size, current_offset, read_block_size)?;
                        sqe.prep_read(
                            fd,
                            &mut buffers[slot].as_mut_slice()[..read_len],
                            current_offset,
                        );
                        sqe.set_user_data((slot as u64) | (1u64 << 40));
                    }
                    block_num += 1;
                    inflight += 1;
                }

                if inflight == 0 {
                    // nothing to do
                    return Ok(0);
                }
                io_uring.submit_sqes().map_err(std::io::Error::other)?;

                // total bytes this thread wrote
                let mut thread_bytes_written: u64 = 0;

                while inflight > 0 {
                    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
                    let user_data = cq.user_data();
                    let mut ready = vec![(user_data, cq.result()? as u32)];
                    while io_uring.cq_ready() > 0 {
                        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                            std::io::Error::other(
                                "completion queue reported ready but no CQE was available",
                            )
                        })?;
                        ready.push((cq.user_data(), cq.result()? as u32));
                    }

                    for (user_data, result) in ready {
                        let slot = (user_data & 0xFFFFFFFF) as usize;
                        let state = (user_data >> 40) as u8;

                        if state == 1 {
                            // Read finished for slot
                            // decrement inflight for the completed read
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on read completion",
                                )
                            })?;

                            // peek block id (do NOT free the slot yet; keep it reserved until write completes)
                            let block_id = pending.peek(slot)?;

                            let stride = block_id
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let current_offset =
                                thread_base.checked_add(stride).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;

                            let expected_len =
                                expected_read_len(file_size, current_offset, read_block_size)?;
                            let actual_len = validate_read_result(
                                "map-file-fixed",
                                current_offset,
                                expected_len,
                                result,
                            )?;

                            if actual_len > 0 {
                                let block_index = (current_offset / read_block_size) as usize;
                                // Reuse the slot buffer for the write: copy processed data into it.
                                let read_buf = &buffers[slot].as_slice()[..actual_len];
                                let out = &mut write_buffers[slot].as_mut_slice();
                                let produced = processor(read_buf, out)?;

                                // copy output into the slot's aligned buffer so the buffer can be
                                // reused for future reads only after the write completes
                                let produced_len = produced;
                                let dst_offset = (block_index as u64)
                                    .checked_mul(write_block_size as u64)
                                    .ok_or_else(|| {
                                        std::io::Error::new(
                                            std::io::ErrorKind::InvalidInput,
                                            "destination offset overflowed",
                                        )
                                    })?;

                                let is_aligned_write =
                                    (dst_offset % 4096 == 0) && (produced_len % 4096 == 0);
                                let fd = if write_params.use_direct && is_aligned_write {
                                    if write_mode != O_DIRECT {
                                        set_file_flags(&dest_file, direct_flags)?;
                                        write_mode = O_DIRECT;
                                    }
                                    dest_file.as_raw_fd()
                                } else {
                                    if write_mode == O_DIRECT {
                                        set_file_flags(&dest_file, pagecache_flags)?;
                                        write_mode = 0;
                                    }
                                    dest_file.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    // mark as write state (2)
                                    sqe.prep_write(
                                        fd,
                                        &write_buffers[slot].as_slice()[..produced_len],
                                        dst_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (2u64 << 40));
                                }
                                // record pending write length; buffer itself is already in buffers[slot]
                                pending_writes[slot] = Some(PendingWrite { len: produced_len });
                                // a write was submitted; increment inflight to account for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                            }

                            // submit any sqes prepared above
                            io_uring.submit_sqes().map_err(std::io::Error::other)?;
                        } else if state == 2 {
                            // Write finished for slot
                            let written = result as usize;
                            let pending_write = pending_writes[slot].take().ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "missing pending write buffer",
                                )
                            })?;
                            if written != pending_write.len {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    format!(
                                        "short write: expected {} bytes, wrote {}",
                                        pending_write.len, written
                                    ),
                                ));
                            }
                            thread_bytes_written = thread_bytes_written
                                .checked_add(written as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "thread bytes_written overflowed",
                                    )
                                })?;

                            // a write completed, so one inflight op finished
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on write completion",
                                )
                            })?;

                            // now free the slot's pending read record so the slot can be reused
                            let completed_block_id = pending.complete(slot)?;

                            // record the produced length for this block in the shared block ranges
                            let block_index = thread_id as usize
                                + (completed_block_id as usize) * (params.num_threads as usize);
                            if block_index >= block_count {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!(
                                        "computed block index {} out of range for {} blocks",
                                        block_index, block_count
                                    ),
                                ));
                            }
                            let dst_offset = (block_index as u64)
                                .checked_mul(write_block_size as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "destination offset overflowed",
                                    )
                                })?;
                            {
                                let mut ranges = shared_block_ranges.lock().unwrap();
                                ranges[block_index] = Some(BlockRange {
                                    offset: dst_offset,
                                    len: written as u64,
                                });
                            }

                            // attempt to submit next read into this slot
                            let stride = block_num
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let next_offset = thread_base.checked_add(stride).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "read offset calculation overflowed",
                                )
                            })?;
                            if next_offset < file_size {
                                pending.reserve(slot, block_num)?;
                                let is_aligned =
                                    (next_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                                let fd = if params.use_direct && is_aligned {
                                    file_direct.as_raw_fd()
                                } else {
                                    file.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    let read_len =
                                        expected_read_len(file_size, next_offset, read_block_size)?;
                                    sqe.prep_read(
                                        fd,
                                        &mut buffers[slot].as_mut_slice()[..read_len],
                                        next_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (1u64 << 40));
                                }
                                block_num += 1;
                                // a read was submitted; increment inflight for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                                io_uring.submit_sqes().map_err(std::io::Error::other)?;
                            }
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "unknown io_uring state",
                            ));
                        }
                    }
                }

                Ok(thread_bytes_written)
            }));
        }

        // collect thread results and sum bytes
        let mut total_written: u64 = 0;
        for thread in threads {
            let written = thread
                .join()
                .map_err(|_| std::io::Error::other("worker thread panicked"))??;
            total_written = total_written.checked_add(written).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "total bytes_written overflowed",
                )
            })?;
        }

        // assemble block ranges from per-thread records
        let mut final_block_ranges = Vec::with_capacity(block_count);
        {
            let ranges = shared_block_ranges.lock().unwrap();
            for (i, entry) in ranges.iter().enumerate() {
                match entry {
                    Some(br) => final_block_ranges.push(*br),
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("missing block range for index {}", i),
                        ));
                    }
                }
            }
        }

        // truncate the destination file to the actual written size to remove unused padding
        if total_written != 0 {
            dest_file.set_len(total_written)?;
        } else if total_size == 0 {
            // nothing to do for empty files
        } else {
            // if no bytes were written but total_size was non-zero, truncate to zero
            dest_file.set_len(0)?;
        }

        Ok(ParallelWriteReport {
            bytes_written: total_written,
            block_ranges: final_block_ranges,
            write_params,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, LoadedConfig};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("{}-{}-{}", prefix, std::process::id(), nanos))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn indexed_writer_reorders_output_by_index() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-index");
        let writer = ParallelWriter::indexed(&cfg, "write", &path, IOMode::PageCache, 3).unwrap();
        writer.write_at_index(2, b"ccc".to_vec()).unwrap();
        writer.write_at_index(0, b"a".to_vec()).unwrap();
        writer.write_at_index(1, b"bb".to_vec()).unwrap();
        let report = writer.finish().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"abbccc".to_vec());
        assert_eq!(
            report.block_ranges,
            vec![
                BlockRange { offset: 0, len: 1 },
                BlockRange { offset: 1, len: 2 },
                BlockRange { offset: 3, len: 3 },
            ]
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_writes_exact_offsets() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 8).unwrap();
        writer.write_at_offset(4, b"efgh".to_vec()).unwrap();
        writer.write_at_offset(0, b"abcd".to_vec()).unwrap();
        let report = writer.finish().unwrap();
        assert_eq!(report.bytes_written, 8);
        assert_eq!(fs::read(&path).unwrap(), b"abcdefgh".to_vec());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_rejects_end_offset_overflow() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset-overflow");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 8).unwrap();
        writer
            .write_at_offset(u64::MAX - 1, b"abcd".to_vec())
            .unwrap();

        let err = writer.finish().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn fixed_size_offset_writer_zero_fills_unwritten_gaps() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset-gaps");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 16).unwrap();
        writer.write_at_offset(0, b"abcd".to_vec()).unwrap();
        writer.write_at_offset(12, b"xy".to_vec()).unwrap();

        let report = writer.finish().unwrap();
        assert_eq!(report.bytes_written, 6);
        assert_eq!(
            fs::read(&path).unwrap(),
            b"abcd\0\0\0\0\0\0\0\0xy\0\0".to_vec()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn indexed_writer_rejects_duplicate_indexes() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-dup");
        let writer = ParallelWriter::indexed(&cfg, "write", &path, IOMode::PageCache, 1).unwrap();
        writer.write_at_index(0, b"a".to_vec()).unwrap();
        writer.write_at_index(0, b"b".to_vec()).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(err.to_string().contains("duplicate write"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn foreach_block_uses_stable_logical_block_size_across_io_modes() {
        let path = unique_temp_file("fro-parallel-file-block-size");
        fs::write(&path, (0..200).map(|i| i as u8).collect::<Vec<_>>()).unwrap();

        let mut app = AppConfig::default();
        app.read.page_cache.block_size = 64 * 1024;
        app.read.direct.block_size = 1024 * 1024;
        let cfg = LoadedConfig::Legacy {
            path: PathBuf::from("unused.json"),
            config: app,
        };

        let page_cache = ParallelFile::open(&cfg, "read", &path, IOMode::PageCache).unwrap();
        let direct = ParallelFile::open(&cfg, "read", &path, IOMode::Direct).unwrap();

        assert_eq!(page_cache.block_size().unwrap(), 1024 * 1024);
        assert_eq!(direct.block_size().unwrap(), 1024 * 1024);

        let page_cache_chunks = Arc::new(Mutex::new(Vec::new()));
        let direct_chunks = Arc::new(Mutex::new(Vec::new()));

        let page_cache_chunks_for_visit = page_cache_chunks.clone();
        page_cache
            .foreach_block(move |index, data| {
                page_cache_chunks_for_visit
                    .lock()
                    .unwrap()
                    .push((index, data.len()));
                Ok(())
            })
            .unwrap();

        let direct_chunks_for_visit = direct_chunks.clone();
        direct
            .foreach_block(move |index, data| {
                direct_chunks_for_visit
                    .lock()
                    .unwrap()
                    .push((index, data.len()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            *page_cache_chunks.lock().unwrap(),
            *direct_chunks.lock().unwrap()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn map_reduce_blocks_preserves_block_order_for_reducer() {
        let path = unique_temp_file("fro-parallel-file-map-reduce");
        fs::write(&path, b"abcdefghijkl").unwrap();

        let cfg = crate::config::load_config(None);
        let file = ParallelFile::open(&cfg, "read", &path, IOMode::PageCache).unwrap();
        let parts = file
            .map_reduce_blocks_with_params(
                ResolvedReadParams {
                    use_direct: false,
                    num_threads: 2,
                    block_size: 4,
                    qd: 1,
                },
                |block_index, data| Ok((block_index, data.to_vec())),
                |parts, report| {
                    assert_eq!(report.file_size, 12);
                    Ok(parts)
                },
            )
            .unwrap();

        assert_eq!(
            parts,
            vec![
                (0, b"abcd".to_vec()),
                (1, b"efgh".to_vec()),
                (2, b"ijkl".to_vec()),
            ]
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn parallel_stream_identity_smoke() {
        let cfg = crate::config::load_config(None);
        let input_path = unique_temp_file("fro-parallel-stream-input");
        let output_path = unique_temp_file("fro-parallel-stream-output");
        // create input with 15 bytes
        let data: Vec<u8> = (0..15).collect();
        fs::write(&input_path, &data).unwrap();

        // processor: identity copy
        let processor = |input: &[u8], out: &mut [u8]| -> std::io::Result<usize> {
            out[..input.len()].clone_from_slice(input);
            Ok(input.len())
        };

        // read block size 4, write block size 4
        let report =
            ParallelStream::map_file_fixed_size(&cfg, &input_path, &output_path, 4, 4, processor)
                .unwrap();
        // read produced bytes prefix (should equal input length)
        let produced = fs::read(&output_path).unwrap();
        assert_eq!(&produced[..15], &data[..]);

        let _ = fs::remove_file(input_path);
        let _ = fs::remove_file(output_path);
    }

    // simple base64 encoder used for tests (scalar)
    fn encode_base64_simple(input: &[u8]) -> Vec<u8> {
        const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut i = 0;
        while i + 3 <= input.len() {
            let a = input[i] as u32;
            let b = input[i + 1] as u32;
            let c = input[i + 2] as u32;
            let n = (a << 16) | (b << 8) | c;
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(T[((n >> 6) & 0x3F) as usize]);
            out.push(T[(n & 0x3F) as usize]);
            i += 3;
        }
        let rem = input.len() - i;
        if rem == 1 {
            let a = input[i] as u32;
            let n = a << 16;
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(b'=');
            out.push(b'=');
        } else if rem == 2 {
            let a = input[i] as u32;
            let b = input[i + 1] as u32;
            let n = (a << 16) | (b << 8);
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(T[((n >> 6) & 0x3F) as usize]);
            out.push(b'=');
        }
        out
    }

    #[test]
    fn parallel_stream_base64_partial_last_block() {
        let cfg = crate::config::load_config(None);
        let input_path = unique_temp_file("fro-parallel-stream-input-b64");
        let output_path = unique_temp_file("fro-parallel-stream-output-b64");
        // create input with 7 bytes so last block is 3 bytes when block_size=4
        let data: Vec<u8> = (0..7).collect();
        fs::write(&input_path, &data).unwrap();

        // read block 4, write block size (max encoded length for a full block == 8)
        let read_block = 4u64;
        let write_block = 8usize;

        let processor = |input: &[u8], out: &mut [u8]| -> std::io::Result<usize> {
            let enc = encode_base64_simple(input);
            out[..enc.len()].clone_from_slice(&enc);
            Ok(enc.len())
        };

        let _report = ParallelStream::map_file_fixed_size(
            &cfg,
            &input_path,
            &output_path,
            read_block,
            write_block,
            processor,
        )
        .unwrap();

        // compute expected concatenated encoding
        let mut expected = Vec::new();
        let mut pos = 0usize;
        while pos < data.len() {
            let end = (pos + read_block as usize).min(data.len());
            expected.extend_from_slice(&encode_base64_simple(&data[pos..end]));
            pos = end;
        }

        let produced = fs::read(&output_path).unwrap();
        // compare only the prefix equal to expected length
        assert_eq!(&produced[..expected.len()], &expected[..]);

        let _ = fs::remove_file(input_path);
        let _ = fs::remove_file(output_path);
    }
}
