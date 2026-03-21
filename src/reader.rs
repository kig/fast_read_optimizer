use crate::common::{AlignedBuffer, IOMode};
use crate::config::{IOParams, LoadedConfig};
use crate::io_util::{
    expected_read_len, open_reader_files, validate_read_result, PendingReadSlots,
};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use memchr::memmem::Finder;
use std::fs::File;
use std::hint::black_box;
use std::io::{Seek, SeekFrom};
use std::ops::Deref;
use std::os::unix::io::AsRawFd;
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
#[derive(Debug)]
pub struct LoadedFile {
    pub data: LoadedData,
    pub bytes_read: u64,
    pub params: ResolvedReadParams,
}

#[derive(Debug)]
pub enum LoadedData {
    Aligned(AlignedBuffer),
    Mapped(MappedReadBuffer),
}

impl LoadedData {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            LoadedData::Aligned(buffer) => buffer.as_slice(),
            LoadedData::Mapped(buffer) => buffer.as_slice(),
        }
    }
}

impl Deref for LoadedData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[derive(Debug)]
pub struct MappedReadBuffer {
    ptr: *const u8,
    len: usize,
    map_len: usize,
}

impl MappedReadBuffer {
    fn map(file: &File, len: usize) -> std::io::Result<Self> {
        let map_len = len.max(1);
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                map_len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            ptr: ptr.cast(),
            len,
            map_len,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for MappedReadBuffer {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::munmap(self.ptr.cast_mut().cast(), self.map_len);
        }
    }
}

unsafe impl Send for MappedReadBuffer {}
unsafe impl Sync for MappedReadBuffer {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderBlock<'a> {
    pub block_index: usize,
    pub offset: u64,
    pub file_size: u64,
    pub data: &'a [u8],
}

#[allow(dead_code)]
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

fn block_offset(
    thread_base: u64,
    block_id: u64,
    num_threads: u64,
    block_size: u64,
) -> std::io::Result<u64> {
    let stride = block_id
        .checked_mul(num_threads)
        .and_then(|value| value.checked_mul(block_size))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read offset calculation overflowed",
            )
        })?;
    thread_base.checked_add(stride).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "read offset calculation overflowed",
        )
    })
}

fn checked_output_offset(offset: u64, len: usize, output_len: usize) -> std::io::Result<usize> {
    let start = usize::try_from(offset).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "destination offset does not fit in usize",
        )
    })?;
    let end = start.checked_add(len).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "destination offset calculation overflowed",
        )
    })?;
    if end > output_len {
        return Err(std::io::Error::other(
            "read wrote beyond destination buffer",
        ));
    }
    Ok(start)
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
            .map(|idx| {
                block
                    .offset
                    .checked_add(idx as u64)
                    .expect("match offset should not overflow")
            })
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

fn grep_match_offsets(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    pattern: &[u8],
) -> std::io::Result<(Vec<u64>, u64)> {
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
            let pattern = pattern.to_vec();
            move |block| Ok::<_, std::io::Error>(collect_grep_scan_block(block, &pattern))
        },
    )?;

    let mut all_matches = Vec::new();
    for block in &blocks.blocks {
        all_matches.extend(block.matches.iter().copied());
    }
    for pair in blocks.blocks.windows(2) {
        all_matches.extend(find_boundary_matches(&pair[0], &pair[1], pattern));
    }
    all_matches.sort_unstable();
    all_matches.dedup();
    Ok((all_matches, blocks.bytes_read))
}

pub fn grep_match_offsets_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
    pattern: &[u8],
) -> std::io::Result<(Vec<u64>, u64)> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    grep_match_offsets(
        filename,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
        pattern,
    )
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
    if use_direct && len % 4096 == 0 {
        debug_assert_eq!(
            len % 4096,
            0,
            "Direct I/O requires a 4096-byte aligned read length"
        );
    }
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
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
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
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len = validate_read_result("read", current_offset, expected_len, result)?;
            if actual_len > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
                if !pattern.is_empty() {
                    let buf = &buffers[slot].as_slice()
                        [..std::cmp::min(actual_len, (block_size as usize) + pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(current_offset.checked_add(idx as u64).ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "grep match offset overflowed",
                            )
                        })?);
                    }
                }
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
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
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        let len = ((file_size - current_offset).min(block_size)) as usize;
        let output_offset = checked_output_offset(current_offset, len, output.len)?;
        let direct = should_use_direct_io(use_direct, current_offset, len, file_size);
        unsafe {
            let dst = output_slice_mut(&output, output_offset, len);
            let mut sqe = io_uring
                .prepare_sqe()
                .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
            if direct {
                sqe.prep_read(file_direct.as_raw_fd(), dst, current_offset);
            } else {
                sqe.prep_read(file.as_raw_fd(), dst, current_offset);
            }
            sqe.set_user_data(block_num as u64);
        }
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("load-to-memory", current_offset, expected_len, result)?;
            if actual_len > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                let len = ((file_size - next_offset).min(block_size)) as usize;
                let output_offset = checked_output_offset(next_offset, len, output.len)?;
                let direct = should_use_direct_io(use_direct, next_offset, len, file_size);
                unsafe {
                    let dst = output_slice_mut(&output, output_offset, len);
                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                        std::io::Error::other("io_uring submission queue is full")
                    })?;
                    if direct {
                        sqe.prep_read(file_direct.as_raw_fd(), dst, next_offset);
                    } else {
                        sqe.prep_read(file.as_raw_fd(), dst, next_offset);
                    }
                    sqe.set_user_data(block_num as u64);
                }
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
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
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
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("map-file-blocks", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[slot].as_slice()[..actual_len];
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

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
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
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
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
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("visit-file-blocks", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[slot].as_slice()[..actual_len];
                visitor(ReaderBlock {
                    block_index,
                    offset: current_offset,
                    file_size,
                    data: buf,
                })?;
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
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

    if file_len == 0 {
        return Ok(LoadedFile {
            data: LoadedData::Aligned(AlignedBuffer::new(0)),
            bytes_read: 0,
            params,
        });
    }

    if !params.use_direct {
        let file = File::open(filename)?;
        let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len)?);
        touch_pages(data.as_slice(), params.num_threads)?;
        return Ok(LoadedFile {
            data,
            bytes_read: file_size,
            params,
        });
    }

    let mut data = AlignedBuffer::new_uninit(file_len)?;
    let output = Arc::new(SharedOutput {
        ptr: data.as_mut_slice().as_mut_ptr(),
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
        data: LoadedData::Aligned(data),
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

#[allow(dead_code)]
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

    let (bytes_read, file_size) =
        visit_file_blocks_with_resolved_params(filename, params, visitor)?;
    Ok((bytes_read, file_size, params))
}

pub fn visit_file_blocks_with_resolved_params<F>(
    filename: &str,
    params: ResolvedReadParams,
    visitor: F,
) -> std::io::Result<(u64, u64)>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    validate_read_params(params)?;

    let file_size = std::fs::metadata(filename)?.len();
    if file_size == 0 {
        return Ok((0, 0));
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

    Ok((read_count.load(Ordering::SeqCst), file_size))
}

#[allow(dead_code)]
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
        let (all_matches, bytes_read) = grep_match_offsets(
            filename,
            num_threads_p,
            block_size_p,
            qd_p,
            num_threads_d,
            block_size_d,
            qd_d,
            io_mode,
            pattern.as_bytes(),
        )?;
        for m in all_matches {
            println!("{}:{}", m, pattern);
        }
        return Ok(bytes_read);
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
        assert_eq!(loaded.data.as_slice(), data.as_slice());

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

    #[test]
    fn block_offset_rejects_overflow() {
        let err = block_offset(u64::MAX - 7, 2, 8, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_output_offset_rejects_non_usize_offset() {
        let err = checked_output_offset(u64::MAX, 16, 32).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_output_offset_rejects_out_of_bounds_range() {
        let err = checked_output_offset(24, 16, 32).unwrap_err();
        assert!(err.to_string().contains("destination"));
    }

    #[test]
    fn output_slice_mut_writes_only_checked_window() {
        let mut backing = vec![0u8; 32];
        let shared = SharedOutput {
            ptr: backing.as_mut_ptr(),
            len: backing.len(),
        };
        let start = checked_output_offset(8, 12, backing.len()).unwrap();

        unsafe {
            output_slice_mut(&shared, start, 12).fill(0xAB);
        }

        assert!(backing[..8].iter().all(|byte| *byte == 0));
        assert!(backing[8..20].iter().all(|byte| *byte == 0xAB));
        assert!(backing[20..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn output_slice_mut_preserves_non_overlapping_regions() {
        let mut backing = vec![0u8; 24];
        let shared = SharedOutput {
            ptr: backing.as_mut_ptr(),
            len: backing.len(),
        };
        let left = checked_output_offset(0, 8, backing.len()).unwrap();
        let right = checked_output_offset(16, 8, backing.len()).unwrap();

        unsafe {
            output_slice_mut(&shared, left, 8).fill(0x11);
            output_slice_mut(&shared, right, 8).fill(0x22);
        }

        assert_eq!(&backing[..8], &[0x11; 8]);
        assert_eq!(&backing[8..16], &[0; 8]);
        assert_eq!(&backing[16..], &[0x22; 8]);
    }
}
unsafe fn output_slice_mut(output: &SharedOutput, offset: usize, len: usize) -> &mut [u8] {
    std::slice::from_raw_parts_mut(output.ptr.add(offset), len)
}

fn touch_pages(data: &[u8], num_threads: u64) -> std::io::Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let checksum = Arc::new(AtomicU64::new(0));
    let thread_count = num_threads.max(1) as usize;
    let shared = Arc::new(SharedOutput {
        ptr: data.as_ptr() as *mut u8,
        len: data.len(),
    });
    let mut threads = Vec::new();
    let scan_stride = 64usize;
    let chunk_size = data.len().div_ceil(thread_count);

    for thread_id in 0..thread_count {
        let checksum = Arc::clone(&checksum);
        let shared = Arc::clone(&shared);
        threads.push(std::thread::spawn(move || {
            let start = thread_id * chunk_size;
            let end = shared.len.min(start + chunk_size);
            let mut local = 0u64;
            let mut offset = start;

            while offset + scan_stride <= end {
                let value = unsafe { *shared.ptr.add(offset) as u64 };
                local = local.wrapping_add(value);
                offset += scan_stride;
            }
            if offset < end {
                let value = unsafe { *shared.ptr.add(end - 1) as u64 };
                local = local.wrapping_add(value);
            }
            checksum.fetch_add(local, Ordering::Relaxed);
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("page-touch worker thread panicked"))?;
    }
    black_box(checksum.load(Ordering::Relaxed));
    Ok(())
}
