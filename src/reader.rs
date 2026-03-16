use crate::common::{AlignedBuffer, IOMode};
use crate::config::{IOParams, LoadedConfig};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use memchr::memmem::Finder;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedReadParams {
    pub use_direct: bool,
    pub num_threads: u64,
    pub block_size: u64,
    pub qd: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedFile {
    pub data: Vec<u8>,
    pub bytes_read: u64,
    pub params: ResolvedReadParams,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderBlock<'a> {
    pub block_index: usize,
    pub offset: u64,
    pub file_size: u64,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct MappedBlocks<T> {
    pub blocks: Vec<T>,
    pub bytes_read: u64,
    pub file_size: u64,
    pub params: ResolvedReadParams,
}

#[allow(dead_code)]
struct SharedOutput {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for SharedOutput {}
unsafe impl Sync for SharedOutput {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrepScanBlock {
    offset: u64,
    len: usize,
    prefix: Vec<u8>,
    suffix: Vec<u8>,
    matches: Vec<u64>,
}

fn block_offset(thread_base: u64, block_id: u64, num_threads: u64, block_size: u64) -> u64 {
    thread_base + block_id * num_threads * block_size
}

fn collect_grep_scan_block(block: ReaderBlock<'_>, pattern: &[u8]) -> GrepScanBlock {
    let overlap = pattern.len().saturating_sub(1);
    let finder = Finder::new(pattern);
    GrepScanBlock {
        offset: block.offset,
        len: block.data.len(),
        prefix: block.data[..block.data.len().min(overlap)].to_vec(),
        suffix: block.data[block.data.len().saturating_sub(overlap)..].to_vec(),
        matches: finder
            .find_iter(block.data)
            .map(|idx| block.offset + idx as u64)
            .collect(),
    }
}

fn find_boundary_matches(prev: &GrepScanBlock, next: &GrepScanBlock, pattern: &[u8]) -> Vec<u64> {
    if pattern.len() <= 1 || prev.suffix.is_empty() || next.prefix.is_empty() {
        return Vec::new();
    }

    let mut joined = Vec::with_capacity(prev.suffix.len() + next.prefix.len());
    joined.extend_from_slice(&prev.suffix);
    joined.extend_from_slice(&next.prefix);
    let boundary = prev.suffix.len();
    let start_offset = prev.offset + prev.len as u64 - prev.suffix.len() as u64;
    let finder = Finder::new(pattern);

    finder
        .find_iter(&joined)
        .filter(|idx| *idx < boundary && idx + pattern.len() > boundary)
        .map(|idx| start_offset + idx as u64)
        .collect()
}

#[allow(dead_code)]
fn validate_read_params(params: ResolvedReadParams) -> std::io::Result<()> {
    if params.num_threads == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "num_threads must be greater than zero",
        ));
    }
    if params.block_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "block_size must be greater than zero",
        ));
    }
    if params.qd == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "qd must be greater than zero",
        ));
    }
    if params.use_direct && params.block_size % 4096 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "direct IO requires a 4096-byte-aligned block size, got {}",
                params.block_size
            ),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
pub fn resolve_reader_params(
    filename: &str,
    page_cache: &IOParams,
    direct: &IOParams,
    io_mode: IOMode,
) -> std::io::Result<ResolvedReadParams> {
    let file_cached = match is_first_page_resident(filename) {
        Ok(true) => io_mode != IOMode::Direct,
        _ => io_mode == IOMode::PageCache,
    };
    let use_direct = (!file_cached) || io_mode == IOMode::Direct;

    let params = if use_direct {
        ResolvedReadParams {
            use_direct,
            num_threads: direct.num_threads,
            block_size: direct.block_size,
            qd: direct.qd,
        }
    } else {
        ResolvedReadParams {
            use_direct,
            num_threads: page_cache.num_threads,
            block_size: page_cache.block_size,
            qd: page_cache.qd,
        }
    };
    validate_read_params(params)?;
    Ok(params)
}

#[allow(dead_code)]
pub fn resolve_reader_params_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> std::io::Result<ResolvedReadParams> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    resolve_reader_params(filename, &page_cache, &direct, io_mode)
}

fn should_use_direct_io(use_direct: bool, offset: u64, len: usize, file_size: u64) -> bool {
    debug_assert_eq!(
        len % 4096,
        0,
        "Direct I/O requires a 4096-byte aligned read length"
    );
    use_direct && (offset % 4096 == 0) && (len % 4096 == 0) && (offset + len as u64 <= file_size)
}

fn submit_read(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
) -> std::io::Result<()> {
    let direct = should_use_direct_io(use_direct, offset, buffer.as_slice().len(), file_size);
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
        if direct {
            sqe.prep_read(file_direct.as_raw_fd(), buffer.as_mut_slice(), offset);
        } else {
            sqe.prep_read(file.as_raw_fd(), buffer.as_mut_slice(), offset);
        }
        sqe.set_user_data(block_id);
    }
    Ok(())
}

fn open_reader_files(filename: &str, use_direct: bool) -> std::io::Result<(File, File)> {
    let file = File::open(filename)?;
    let file_direct = if use_direct {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(filename)?
    } else {
        File::open(filename)?
    };
    Ok((file, file_direct))
}

fn wait_for_ready(io_uring: &mut IoUring) -> std::io::Result<Vec<(u64, u32)>> {
    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
    let mut ready = vec![(cq.user_data(), cq.result()?)];

    while io_uring.cq_ready() > 0 {
        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
            std::io::Error::other("completion queue reported ready but no CQE was available")
        })?;
        ready.push((cq.user_data(), cq.result()?));
    }

    Ok(ready)
}

#[allow(dead_code)]
fn thread_reader(
    thread_id: u64,
    pattern: String,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    use_direct: bool,
) -> std::io::Result<Vec<u64>> {
    let mut matches = Vec::new();
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(matches);
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    let finder = Finder::new(pattern.as_bytes());

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            if result > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
                if !pattern.is_empty() {
                    let buf = &buffers[block_id as usize % qd].as_slice()
                        [..std::cmp::min(result as usize, (block_size as usize) + pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(
                            block_offset(offset, block_id, num_threads, block_size) + idx as u64,
                        );
                    }
                }
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(matches);
        }
    }
}

#[allow(dead_code)]
fn thread_loader(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    output: Arc<SharedOutput>,
    use_direct: bool,
) -> std::io::Result<()> {
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            if result > 0 {
                let current_offset = block_offset(offset, block_id, num_threads, block_size);
                let result = result as usize;
                let end = current_offset as usize + result;
                if end > output.len {
                    return Err(std::io::Error::other(
                        "read wrote beyond destination buffer",
                    ));
                }
                let src = &buffers[block_id as usize % qd].as_slice()[..result];
                unsafe {
                    let dst = std::slice::from_raw_parts_mut(
                        output.ptr.add(current_offset as usize),
                        result,
                    );
                    dst.copy_from_slice(src);
                }
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

fn thread_map_blocks<T, F>(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    results: Arc<Vec<Mutex<Option<T>>>>,
    mapper: Arc<F>,
    use_direct: bool,
) -> std::io::Result<()>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            if result > 0 {
                let current_offset = block_offset(offset, block_id, num_threads, block_size);
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[block_id as usize % qd].as_slice()[..result as usize];
                let mapped = mapper(ReaderBlock {
                    block_index,
                    offset: current_offset,
                    file_size,
                    data: buf,
                })?;
                let mut slot = results[block_index].lock().unwrap();
                *slot = Some(mapped);
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

fn thread_visit_blocks<F>(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    visitor: Arc<F>,
    use_direct: bool,
) -> std::io::Result<()>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            if result > 0 {
                let current_offset = block_offset(offset, block_id, num_threads, block_size);
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[block_id as usize % qd].as_slice()[..result as usize];
                visitor(ReaderBlock {
                    block_index,
                    offset: current_offset,
                    file_size,
                    data: buf,
                })?;
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

#[allow(dead_code)]
pub fn load_file_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<LoadedFile> {
    let params = resolve_reader_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;

    let file_size = std::fs::metadata(filename)?.len();
    let file_len = usize::try_from(file_size).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "file is too large to fit in memory on this platform: {}",
                file_size
            ),
        )
    })?;

    let mut data = vec![0u8; file_len];
    if file_len == 0 {
        return Ok(LoadedFile {
            data,
            bytes_read: 0,
            params,
        });
    }
    let output = Arc::new(SharedOutput {
        ptr: data.as_mut_ptr(),
        len: data.len(),
    });
    let read_count = Arc::new(AtomicU64::new(0));

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let output = output.clone();
        let read_count = read_count.clone();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_loader(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                output,
                params.use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }

    Ok(LoadedFile {
        data,
        bytes_read: read_count.load(Ordering::SeqCst),
        params,
    })
}

#[allow(dead_code)]
pub fn load_file_to_memory_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> std::io::Result<LoadedFile> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    load_file_to_memory(
        filename,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
    )
}

pub fn map_file_blocks<T, F>(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mapper: F,
) -> std::io::Result<MappedBlocks<T>>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    let params = resolve_reader_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;

    let file_size = std::fs::metadata(filename)?.len();
    let block_count = if file_size == 0 {
        0
    } else {
        file_size.div_ceil(params.block_size) as usize
    };
    if block_count == 0 {
        return Ok(MappedBlocks {
            blocks: Vec::new(),
            bytes_read: 0,
            file_size,
            params,
        });
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let results = Arc::new(
        (0..block_count)
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>(),
    );
    let mapper = Arc::new(mapper);

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let filename = filename.to_string();
        let read_count = read_count.clone();
        let results = results.clone();
        let mapper = mapper.clone();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_map_blocks(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                results,
                mapper,
                params.use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }

    let mut blocks = Vec::with_capacity(block_count);
    for (block_index, slot) in results.iter().enumerate() {
        let value = slot.lock().unwrap().take().ok_or_else(|| {
            std::io::Error::other(format!("missing mapped result for block {}", block_index))
        })?;
        blocks.push(value);
    }

    Ok(MappedBlocks {
        blocks,
        bytes_read: read_count.load(Ordering::SeqCst),
        file_size,
        params,
    })
}

pub fn map_file_blocks_for_mode<T, F>(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
    mapper: F,
) -> std::io::Result<MappedBlocks<T>>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    map_file_blocks(
        filename,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
        mapper,
    )
}

pub fn visit_file_blocks<F>(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    visitor: F,
) -> std::io::Result<(u64, u64, ResolvedReadParams)>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    let params = resolve_reader_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;

    let file_size = std::fs::metadata(filename)?.len();
    if file_size == 0 {
        return Ok((0, 0, params));
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let visitor = Arc::new(visitor);

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let filename = filename.to_string();
        let read_count = read_count.clone();
        let visitor = visitor.clone();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_visit_blocks(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                visitor,
                params.use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }

    Ok((read_count.load(Ordering::SeqCst), file_size, params))
}

pub fn visit_file_blocks_for_mode<F>(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
    visitor: F,
) -> std::io::Result<(u64, u64, ResolvedReadParams)>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    visit_file_blocks(
        filename,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
        visitor,
    )
}

pub fn read_file(
    pattern: &str,
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<u64> {
    if !pattern.is_empty() {
        let blocks = map_file_blocks(
            filename,
            num_threads_p,
            block_size_p,
            qd_p,
            num_threads_d,
            block_size_d,
            qd_d,
            io_mode,
            {
                let pattern = pattern.as_bytes().to_vec();
                move |block| Ok::<_, std::io::Error>(collect_grep_scan_block(block, &pattern))
            },
        )?;

        let mut all_matches = Vec::new();
        for block in &blocks.blocks {
            all_matches.extend(block.matches.iter().copied());
        }
        for pair in blocks.blocks.windows(2) {
            all_matches.extend(find_boundary_matches(
                &pair[0],
                &pair[1],
                pattern.as_bytes(),
            ));
        }
        all_matches.sort_unstable();
        all_matches.dedup();
        for m in all_matches {
            println!("{}:{}", m, pattern);
        }
        return Ok(blocks.bytes_read);
    }
    let (bytes_read, _, _) = visit_file_blocks(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
        |_| Ok::<_, std::io::Error>(()),
    )?;
    Ok(bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use std::fs;
    use std::path::PathBuf;

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
    }

    #[test]
    fn load_file_to_memory_round_trips_bytes() {
        let path = unique_temp_file("fro-load");
        let data = (0..(512 * 1024 + 1234))
            .map(|i| ((i * 17) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let loaded = load_file_to_memory(
            path.to_str().unwrap(),
            4,
            128 * 1024,
            2,
            2,
            512 * 1024,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(loaded.bytes_read, data.len() as u64);
        assert_eq!(loaded.data, data);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn resolve_reader_params_for_mode_uses_config_mode() {
        let path = unique_temp_file("fro-load-config");
        fs::write(&path, b"hello world").unwrap();

        let mut cfg = AppConfig::default();
        cfg.hash.page_cache = IOParams {
            num_threads: 9,
            block_size: 2 * 1024 * 1024,
            qd: 3,
        };

        let loaded = LoadedConfig::Legacy {
            path: PathBuf::from("fro.json"),
            config: cfg,
        };

        let params = resolve_reader_params_for_mode(
            &loaded,
            "hash",
            path.to_str().unwrap(),
            IOMode::PageCache,
        )
        .unwrap();
        assert_eq!(params.num_threads, 9);
        assert_eq!(params.block_size, 2 * 1024 * 1024);
        assert_eq!(params.qd, 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn map_file_blocks_runs_callback_in_file_order() {
        let path = unique_temp_file("fro-map-blocks");
        let block_size = 128 * 1024;
        let data = (0..(block_size * 3 + 77))
            .map(|i| ((i * 19) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let mapped = map_file_blocks(
            path.to_str().unwrap(),
            3,
            block_size as u64,
            2,
            2,
            block_size as u64,
            2,
            IOMode::PageCache,
            |block| Ok::<_, std::io::Error>((block.block_index, block.data.len(), block.offset)),
        )
        .unwrap();

        assert_eq!(mapped.bytes_read, data.len() as u64);
        assert_eq!(mapped.blocks.len(), 4);
        assert_eq!(mapped.blocks[0], (0, block_size, 0));
        assert_eq!(mapped.blocks[1], (1, block_size, block_size as u64));
        assert_eq!(mapped.blocks[2], (2, block_size, (block_size * 2) as u64));
        assert_eq!(mapped.blocks[3], (3, 77, (block_size * 3) as u64));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn visit_file_blocks_visits_every_block() {
        let path = unique_temp_file("fro-visit-blocks");
        let block_size = 64 * 1024;
        let data = (0..(block_size * 2 + 55))
            .map(|i| ((i * 23) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<(usize, usize)>::new()));
        let seen_for_visit = seen.clone();
        let (bytes_read, file_size, params) = visit_file_blocks(
            path.to_str().unwrap(),
            2,
            block_size as u64,
            2,
            2,
            block_size as u64,
            2,
            IOMode::PageCache,
            move |block| {
                seen_for_visit
                    .lock()
                    .unwrap()
                    .push((block.block_index, block.data.len()));
                Ok::<_, std::io::Error>(())
            },
        )
        .unwrap();

        let mut seen = seen.lock().unwrap().clone();
        seen.sort_unstable();
        assert_eq!(bytes_read, data.len() as u64);
        assert_eq!(file_size, data.len() as u64);
        assert_eq!(params.block_size, block_size as u64);
        assert_eq!(seen, vec![(0, block_size), (1, block_size), (2, 55)]);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn boundary_match_finds_pattern_across_blocks() {
        let prev = GrepScanBlock {
            offset: 0,
            len: 4,
            prefix: b"ab".to_vec(),
            suffix: b"cd".to_vec(),
            matches: Vec::new(),
        };
        let next = GrepScanBlock {
            offset: 4,
            len: 4,
            prefix: b"ef".to_vec(),
            suffix: b"gh".to_vec(),
            matches: Vec::new(),
        };

        assert_eq!(find_boundary_matches(&prev, &next, b"cdef"), vec![2]);
    }
}
