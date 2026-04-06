use crate::common::{AlignedBuffer, IOMode};
use crate::config::LoadedConfig;
use crate::io_util::{
    expected_read_len, open_reader_files, validate_read_result, PendingReadSlots,
};
use crate::reader::{
    resolve_reader_params, resolve_reader_params_for_mode, visit_file_blocks_with_resolved_params,
    ResolvedReadParams,
};
use crate::writer::{
    resolve_writer_params_for_mode, OffsetWriter, ResolvedWriteParams, SequentialWriter,
};
use iou::IoUring;
use libc::{fcntl, F_GETFL, F_SETFL};
use std::alloc::{alloc, handle_alloc_error, Layout};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::sync::{mpsc, Arc, Mutex};

pub fn get_file_flags(file: &std::fs::File) -> std::io::Result<i32> {
    let fd = file.as_raw_fd();
    unsafe {
        let flags = fcntl(fd, F_GETFL);
        return Ok(flags);
    }
}

pub fn set_file_flags(file: &std::fs::File, flags: i32) -> std::io::Result<i32> {
    let fd = file.as_raw_fd();
    unsafe {
        let flags = fcntl(fd, F_SETFL, flags);
        return Ok(flags);
    }
}

pub fn default_logical_block_size(config: &LoadedConfig, mode: &str, path: &str) -> u64 {
    let page_cache = config.get_params_for_path(mode, false, path);
    let direct = config.get_params_for_path(mode, true, path);
    page_cache.block_size.max(direct.block_size)
}

pub fn effective_io_mode_for_block_size(io_mode: IOMode, block_size: u64) -> IOMode {
    if block_size % 4096 == 0 {
        io_mode
    } else {
        IOMode::PageCache
    }
}

pub fn allocate_pipe_output_buffer(min_capacity: usize) -> Vec<u8> {
    let huge_page_size = 2 * 1024 * 1024;
    let capacity = min_capacity.max(1).next_multiple_of(huge_page_size);
    let layout =
        Layout::from_size_align(capacity, huge_page_size).expect("invalid pipe buffer layout");
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        handle_alloc_error(layout);
    }
    unsafe {
        let _ret = libc::madvise(ptr as *mut libc::c_void, capacity, libc::MADV_HUGEPAGE);
    }
    unsafe { Vec::from_raw_parts(ptr, capacity, capacity) }
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
        let effective_block_size = if self.io_mode == IOMode::Direct && block_size % 4096 != 0 {
            block_size.next_multiple_of(4096)
        } else {
            block_size
        };
        let io_mode = effective_io_mode_for_block_size(self.io_mode, effective_block_size);
        let visit = Arc::new(visit);
        let mut local_config = config.clone();
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        local_config.update_params_for_path(
            &self.mode,
            false,
            &path,
            crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size: effective_block_size,
                qd: page_cache.qd,
            },
        );
        local_config.update_params_for_path(
            &self.mode,
            true,
            &path,
            crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size: effective_block_size,
                qd: direct.qd,
            },
        );
        let params = crate::reader::resolve_reader_params(
            &path,
            &crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size: effective_block_size,
                qd: page_cache.qd,
            },
            &crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size: effective_block_size,
                qd: direct.qd,
            },
            io_mode,
        )?;
        let metrics =
            crate::reader::visit_file_blocks_with_resolved_params(&path, params, move |block| {
                visit(block.block_index, block.data)
            })?;
        Ok(ParallelReadReport {
            bytes_read: metrics.bytes_read,
            file_size: metrics.file_size,
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
        let effective_block_size = if self.io_mode == IOMode::Direct && block_size % 4096 != 0 {
            block_size.next_multiple_of(4096)
        } else {
            block_size
        };
        let io_mode = effective_io_mode_for_block_size(self.io_mode, effective_block_size);
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        let mut local_config = config.clone();
        local_config.update_params_for_path(
            &self.mode,
            false,
            &path,
            crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size: effective_block_size,
                qd: page_cache.qd,
            },
        );
        local_config.update_params_for_path(
            &self.mode,
            true,
            &path,
            crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size: effective_block_size,
                qd: direct.qd,
            },
        );
        let params = crate::reader::resolve_reader_params(
            &path,
            &crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size: effective_block_size,
                qd: page_cache.qd,
            },
            &crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size: effective_block_size,
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

        let metrics = visit_file_blocks_with_resolved_params(&path, params, move |block| {
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
                bytes_read: metrics.bytes_read,
                file_size: metrics.file_size,
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

    pub fn indexed_file(
        config: &LoadedConfig,
        mode: &str,
        file: &std::fs::File,
        io_mode: IOMode,
        block_count: usize,
    ) -> std::io::Result<Self> {
        // For file-based writer params we don't have a path string; use "/" as a placeholder
        Self::spawn_file(
            file,
            resolve_writer_params_for_mode(config, mode, "/", io_mode),
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

    fn spawn_file(
        file: &std::fs::File,
        write_params: ResolvedWriteParams,
        _io_mode: IOMode,
        mode: WriterMode,
    ) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel::<WriteRequest>();
        // Clone the provided File so the spawned thread owns its own File instance
        let file_owned = file.try_clone()?;
        let join_handle = std::thread::spawn(move || -> std::io::Result<ParallelWriteReport> {
            match mode {
                WriterMode::ByIndex { block_count } => {
                    let mut out = SequentialWriter::from_file(
                        file_owned,
                        write_params.qd,
                        write_params.block_size,
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
                    total_size: _,
                    truncate: _,
                } => {
                    todo!()
                }
            }
        });

        Ok(Self {
            tx,
            finish_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
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

    /// Create an indexed writer that sends ordered blocks into a destination pipe fd
    /// using vmsplice into a local pipe and splice to the dest fd. This keeps the
    /// ordered-block semantics of the indexed writer but uses pipe zero-copy for
    /// low-overhead file->pipe output.
    pub fn indexed_pipe(
        dest_fd: std::os::unix::io::RawFd,
        block_count: usize,
        write_block_size: u64,
    ) -> std::io::Result<Self> {
        Self::indexed_pipe_with_pool(dest_fd, block_count, None, write_block_size)
    }

    /// Indexed pipe writer variant that can return used Vec<u8> buffers back to
    /// a provided pool sender so producer threads can reuse allocations.
    pub fn indexed_pipe_with_pool(
        dest_fd: std::os::unix::io::RawFd,
        block_count: usize,
        pool_sender: Option<mpsc::Sender<Vec<u8>>>,
        write_block_size: u64,
    ) -> std::io::Result<Self> {
        use std::os::unix::io::RawFd;
        let (tx, rx) = mpsc::channel::<WriteRequest>();
        let join_handle = std::thread::spawn(move || -> std::io::Result<ParallelWriteReport> {
            //let mut vmsplices = 0;
            //let mut writes = 0;

            //let mut vmsplice_all = unsafe { |pipe_write: RawFd, buf: &[u8]| -> std::io::Result<usize> {
            unsafe fn vmsplice_all(pipe_write: RawFd, buf: &[u8]) -> std::io::Result<usize> {
                let mut written_total = 0usize;
                let mut ptr = buf.as_ptr();
                let mut remaining = buf.len();
                while remaining > 0 {
                    let rc = if remaining % 4096 == 0 && ptr.align_offset(4096) == 0 {
                        //vmsplices += 1;
                        let iov = libc::iovec {
                            iov_base: ptr as *mut libc::c_void,
                            iov_len: remaining,
                        };
                        libc::vmsplice(
                            pipe_write,
                            &iov as *const libc::iovec,
                            1,
                            libc::SPLICE_F_GIFT,
                        )
                    } else {
                        //writes += 1;
                        libc::write(pipe_write, ptr as *mut libc::c_void, remaining)
                    };
                    if rc < 0 {
                        let err = std::io::Error::last_os_error();
                        match err.raw_os_error() {
                            Some(libc::EINTR) => continue,
                            Some(libc::EAGAIN) => continue,
                            _ => return Err(err),
                        }
                    }
                    let n = rc as usize;
                    written_total = written_total
                        .checked_add(n)
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::Other, "vmsplice overflow")
                        })
                        .expect("boom");
                    ptr = unsafe { ptr.add(n) };
                    remaining -= n;
                }
                Ok(written_total)
            }

            let mut pending = BTreeMap::<usize, Vec<u8>>::new();
            let mut seen = vec![false; block_count];
            let mut next_index = 0usize;
            let mut bytes_written: u64 = 0;
            let mut block_ranges = vec![BlockRange { offset: 0, len: 0 }; block_count];

            for request in rx {
                let (index, data) = match request {
                    WriteRequest::ByIndex { index, data } => (index, data),
                    _ => {
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
                    // vmsplice into pipe_write and splice to dest_fd
                    let data_len = data.len();
                    let mut remaining = data_len;
                    let mut offset = 0usize;
                    while remaining > 0 {
                        let chunk = &data[offset..offset + remaining.min(1 << 20)];
                        let pushed = unsafe { vmsplice_all(dest_fd, chunk).expect("Boom") };
                        offset += pushed;
                        remaining -= pushed;
                        bytes_written =
                            bytes_written.checked_add(pushed as u64).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "bytes_written overflow",
                                )
                            })?;
                    }
                    block_ranges[next_index] = BlockRange {
                        offset: bytes_written - data_len as u64,
                        len: data_len as u64,
                    };
                    // return buffer to producer pool if available
                    if let Some(ref sender) = pool_sender {
                        let _ = sender.send(data);
                    }
                    next_index += 1;
                }
            }

            // eprintln!("vmsplices {} writes {}", vmsplices, writes);

            Ok(ParallelWriteReport {
                bytes_written,
                block_ranges,
                write_params: ResolvedWriteParams {
                    use_direct: false,
                    qd: 1,
                    block_size: write_block_size,
                },
            })
        });

        Ok(Self {
            tx,
            finish_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
    }
}

#[derive(Clone)]
pub struct ParallelStream {}

mod parallel_stream;
mod parallel_stream_to_file;
#[cfg(test)]
mod tests;
pub(crate) mod transform;
