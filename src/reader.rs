use crate::common::{AlignedBuffer, IOMode, ReadAutoStrategy, ReadPathKind};
use crate::config::{IOParams, LoadedConfig, MountInfo};
use crate::io_util::{
    expected_read_len, open_reader_files, validate_read_result, PendingReadSlots,
};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use memchr::memmem::Finder;
use std::fs::File;
use std::hint::black_box;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::os::unix::fs::FileExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct BufReader<R> {
    inner: std::io::BufReader<R>,
}

impl<R: Read> BufReader<R> {
    pub fn new(inner: R) -> Self {
        Self::with_capacity(1 << 20, inner)
    }

    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner: std::io::BufReader::with_capacity(capacity, inner),
        }
    }

    #[allow(dead_code)]
    pub fn get_ref(&self) -> &R {
        self.inner.get_ref()
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

fn auto_lift_mode_for_residency(first_page_resident: bool) -> IOMode {
    if first_page_resident {
        IOMode::PageCache
    } else {
        IOMode::Direct
    }
}

#[allow(dead_code)]
pub(crate) fn evict_file_cache(filename: &str) -> io::Result<()> {
    let file = File::open(filename)?;
    let result = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

pub(crate) fn warm_file_page_cache(filename: &str) -> io::Result<u64> {
    let file = File::open(filename)?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut buffer = vec![0_u8; 8 * 1024 * 1024];
    let mut total = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(total);
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("page-cache warm byte count overflowed"))?;
    }
}


fn auto_read_cache_state(filename: &str) -> ReadBenchmarkCacheState {
    match auto_lift_mode_for_residency(is_first_page_resident(filename).unwrap_or(false)) {
        IOMode::PageCache => ReadBenchmarkCacheState::Hot,
        IOMode::Direct | IOMode::Auto => ReadBenchmarkCacheState::Cold,
    }
}

fn choose_path_kind_for_state(
    strategy: ReadAutoStrategy,
    cache_state: ReadBenchmarkCacheState,
    file_size: u64,
) -> ReadPathKind {
    match cache_state {
        ReadBenchmarkCacheState::Hot => {
            if file_size >= strategy.hot_large_min_bytes {
                strategy.hot_large_path
            } else {
                strategy.hot_small_path
            }
        }
        ReadBenchmarkCacheState::Cold => {
            if file_size >= strategy.cold_large_min_bytes {
                strategy.cold_large_path
            } else {
                strategy.cold_small_path
            }
        }
    }
}

impl BufReader<File> {
    pub fn stdin() -> io::Result<Self> {
        let stdin_fd = unsafe { libc::dup(libc::STDIN_FILENO) };
        if stdin_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self::new(unsafe { File::from_raw_fd(stdin_fd) }))
    }
}

impl<R: Read> Read for BufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: Read> BufRead for BufReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt);
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedReadParams {
    pub use_direct: bool,
    pub num_threads: u64,
    pub block_size: u64,
    pub qd: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadBenchmarkVariant {
    SingleThreadPageCache,
    SingleThreadDirect,
    SingleThreadIoUring,
    QuickProbePageCache,
    MultiThreadCurrent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadBenchmarkCacheState {
    Cold,
    Hot,
}

impl ReadBenchmarkVariant {
    pub fn label(self) -> &'static str {
        match self {
            Self::SingleThreadPageCache => "st-page-cache",
            Self::SingleThreadDirect => "st-direct",
            Self::SingleThreadIoUring => "st-io-uring",
            Self::QuickProbePageCache => "quick-probe-page-cache",
            Self::MultiThreadCurrent => "mt-current",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadPhaseTimings {
    pub call_to_threads_created: Option<std::time::Duration>,
    pub call_to_first_submit: Option<std::time::Duration>,
    pub call_to_first_completion: Option<std::time::Duration>,
    pub call_to_wrapup_start: Option<std::time::Duration>,
    pub call_to_join_done: Option<std::time::Duration>,
}

impl ReadPhaseTimings {
    pub fn enabled(self) -> bool {
        self.call_to_threads_created.is_some()
            || self.call_to_first_submit.is_some()
            || self.call_to_first_completion.is_some()
            || self.call_to_wrapup_start.is_some()
            || self.call_to_join_done.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBenchmarkResult {
    pub bytes_read: u64,
    pub file_size: u64,
    pub elapsed: std::time::Duration,
    pub params: ResolvedReadParams,
    pub phase_timings: ReadPhaseTimings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisitFileMetrics {
    pub bytes_read: u64,
    pub file_size: u64,
    pub phase_timings: ReadPhaseTimings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedReadExecution {
    Simple(ResolvedReadParams),
    Threaded(ResolvedReadParams),
}

#[cfg(feature = "read-phase-timing")]
const READ_PHASE_UNSET: u64 = u64::MAX;

struct ReadPhaseTimingProbe {
    #[cfg(feature = "read-phase-timing")]
    start: std::time::Instant,
    #[cfg(feature = "read-phase-timing")]
    threads_created_ns: AtomicU64,
    #[cfg(feature = "read-phase-timing")]
    first_submit_ns: AtomicU64,
    #[cfg(feature = "read-phase-timing")]
    first_completion_ns: AtomicU64,
    #[cfg(feature = "read-phase-timing")]
    wrapup_start_ns: AtomicU64,
    #[cfg(feature = "read-phase-timing")]
    join_done_ns: AtomicU64,
}

impl ReadPhaseTimingProbe {
    fn new() -> Self {
        Self {
            #[cfg(feature = "read-phase-timing")]
            start: std::time::Instant::now(),
            #[cfg(feature = "read-phase-timing")]
            threads_created_ns: AtomicU64::new(READ_PHASE_UNSET),
            #[cfg(feature = "read-phase-timing")]
            first_submit_ns: AtomicU64::new(READ_PHASE_UNSET),
            #[cfg(feature = "read-phase-timing")]
            first_completion_ns: AtomicU64::new(READ_PHASE_UNSET),
            #[cfg(feature = "read-phase-timing")]
            wrapup_start_ns: AtomicU64::new(READ_PHASE_UNSET),
            #[cfg(feature = "read-phase-timing")]
            join_done_ns: AtomicU64::new(READ_PHASE_UNSET),
        }
    }

    #[cfg(feature = "read-phase-timing")]
    fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    #[cfg(feature = "read-phase-timing")]
    fn store_min(slot: &AtomicU64, value: u64) {
        let mut current = slot.load(Ordering::Relaxed);
        while value < current {
            match slot.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn note_threads_created(&self) {
        #[cfg(feature = "read-phase-timing")]
        Self::store_min(&self.threads_created_ns, self.elapsed_ns());
    }

    fn note_first_submit(&self) {
        #[cfg(feature = "read-phase-timing")]
        Self::store_min(&self.first_submit_ns, self.elapsed_ns());
    }

    fn note_first_completion(&self) {
        #[cfg(feature = "read-phase-timing")]
        Self::store_min(&self.first_completion_ns, self.elapsed_ns());
    }

    fn note_wrapup_start(&self) {
        #[cfg(feature = "read-phase-timing")]
        Self::store_min(&self.wrapup_start_ns, self.elapsed_ns());
    }

    fn note_join_done(&self) {
        #[cfg(feature = "read-phase-timing")]
        Self::store_min(&self.join_done_ns, self.elapsed_ns());
    }

    fn snapshot(&self) -> ReadPhaseTimings {
        #[cfg(feature = "read-phase-timing")]
        {
            fn load_duration(slot: &AtomicU64) -> Option<std::time::Duration> {
                let nanos = slot.load(Ordering::Relaxed);
                if nanos == READ_PHASE_UNSET {
                    None
                } else {
                    Some(std::time::Duration::from_nanos(nanos))
                }
            }

            return ReadPhaseTimings {
                call_to_threads_created: load_duration(&self.threads_created_ns),
                call_to_first_submit: load_duration(&self.first_submit_ns),
                call_to_first_completion: load_duration(&self.first_completion_ns),
                call_to_wrapup_start: load_duration(&self.wrapup_start_ns),
                call_to_join_done: load_duration(&self.join_done_ns),
            };
        }

        #[cfg(not(feature = "read-phase-timing"))]
        {
            ReadPhaseTimings::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadToMemoryMode {
    Auto,
    PagedSharedBuffer,
    Mmap,
    MmapReadPages,
    MultipleTargetBuffers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HugepageAdvice {
    Auto,
    Disabled,
}

const HUGEPAGE_MIN_FILE_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadToMemoryOptions {
    pub hugepages: HugepageAdvice,
    pub measure_unmap_time: bool,
}

impl Default for ReadToMemoryOptions {
    fn default() -> Self {
        Self {
            hugepages: HugepageAdvice::Auto,
            measure_unmap_time: false,
        }
    }
}

impl ReadToMemoryOptions {
    fn use_hugepages_for_len(self, len: usize) -> bool {
        match self.hugepages {
            HugepageAdvice::Auto => len >= HUGEPAGE_MIN_FILE_LEN,
            HugepageAdvice::Disabled => false,
        }
    }
}

pub fn resolve_to_memory_mode(
    filename: &str,
    io_mode: IOMode,
    requested_mode: ReadToMemoryMode,
) -> ReadToMemoryMode {
    match requested_mode {
        ReadToMemoryMode::Auto => match io_mode {
            IOMode::Direct => ReadToMemoryMode::PagedSharedBuffer,
            IOMode::PageCache => ReadToMemoryMode::Mmap,
            IOMode::Auto => {
                if is_first_page_resident(filename).unwrap_or(false) {
                    ReadToMemoryMode::Mmap
                } else {
                    ReadToMemoryMode::PagedSharedBuffer
                }
            }
        },
        other => other,
    }
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
    map_ptr: *mut libc::c_void,
    map_len: usize,
}

impl MappedReadBuffer {
    fn map(file: &File, len: usize, options: ReadToMemoryOptions) -> std::io::Result<Self> {
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
        if let Err(err) = advise_mapped_read_region(ptr, map_len, options) {
            unsafe {
                let _ = libc::munmap(ptr, map_len);
            }
            return Err(err);
        }
        Ok(Self {
            ptr: ptr.cast(),
            len,
            map_ptr: ptr,
            map_len,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[allow(unused)]
    pub fn mapped_len(&self) -> usize {
        self.map_len
    }

    #[allow(unused)]
    pub fn unmap_prefix(&mut self, bytes: usize) -> std::io::Result<usize> {
        if bytes == 0 || self.map_len == 0 {
            return Ok(0);
        }
        let page_size = 4096usize;
        let unmap_len = (bytes / page_size) * page_size;
        if unmap_len == 0 {
            return Ok(0);
        }
        let unmap_len = unmap_len.min(self.map_len);
        let rc = unsafe { libc::munmap(self.map_ptr, unmap_len) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        self.map_ptr = unsafe { self.map_ptr.add(unmap_len) };
        self.ptr = self.map_ptr.cast();
        self.map_len -= unmap_len;
        self.len = self.len.saturating_sub(unmap_len);
        Ok(unmap_len)
    }
}

impl Drop for MappedReadBuffer {
    fn drop(&mut self) {
        if self.map_len == 0 {
            return;
        }
        unsafe {
            let _ = libc::munmap(self.map_ptr, self.map_len);
        }
    }
}

fn madvise_best_effort(ptr: *mut libc::c_void, len: usize, advice: libc::c_int) -> io::Result<()> {
    if unsafe { libc::madvise(ptr, len, advice) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINVAL | libc::ENOSYS) => Ok(()),
        _ => Err(err),
    }
}

fn advise_mapped_read_region(
    ptr: *mut libc::c_void,
    len: usize,
    options: ReadToMemoryOptions,
) -> io::Result<()> {
    madvise_best_effort(ptr, len, libc::MADV_RANDOM)?;
    if options.use_hugepages_for_len(len) {
        madvise_best_effort(ptr, len, libc::MADV_HUGEPAGE)?;
    }
    Ok(())
}

unsafe impl Send for MappedReadBuffer {}
unsafe impl Sync for MappedReadBuffer {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockSpan {
    start_offset: u64,
    len: usize,
}

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
    match resolve_reader_execution_for_mode(config, mode, filename, io_mode)? {
        ResolvedReadExecution::Simple(params) | ResolvedReadExecution::Threaded(params) => Ok(params),
    }
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

fn submit_read_with_probe(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
    timing_probe: Option<&ReadPhaseTimingProbe>,
) -> std::io::Result<()> {
    submit_read(
        io_uring,
        file,
        file_direct,
        buffer,
        offset,
        block_id,
        use_direct,
        file_size,
    )?;
    if let Some(probe) = timing_probe {
        probe.note_first_submit();
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

fn read_file_single_thread_blocking(
    filename: &str,
    use_direct: bool,
    block_size: usize,
) -> io::Result<u64> {
    let (file, file_direct) = open_reader_files(filename, use_direct)?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    if file_size == 0 {
        return Ok(0);
    }

    let mut bytes_read = 0_u64;
    let mut offset = 0_u64;
    let mut buffer = AlignedBuffer::new(block_size);
    loop {
        let remaining = file_size.saturating_sub(offset);
        if remaining == 0 {
            return Ok(bytes_read);
        }
        let want = remaining.min(block_size as u64) as usize;
        let target = if should_use_direct_io(use_direct, offset, want, file_size) {
            &file_direct
        } else {
            &file
        };
        let read = target.read_at(&mut buffer.as_mut_slice()[..want], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blocking single-thread read reached EOF early at offset {} of {}",
                    offset, file_size
                ),
            ));
        }
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("single-thread byte count overflowed"))?;
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("single-thread offset overflowed"))?;
    }
}

fn benchmark_block_size(file_size: u64) -> usize {
    file_size.clamp(1, 1024 * 1024) as usize
}

fn benchmark_uring_qd(file_size: u64) -> usize {
    file_size.div_ceil(1024 * 1024).clamp(1, 4) as usize
}

fn read_file_simple_page_cache_from_probe(file: &mut File, file_size: u64) -> io::Result<u64> {
    let mut bytes_read = 64 * 1024_u64;
    let mut buffer = vec![0_u8; benchmark_block_size(file_size)];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(bytes_read);
        }
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("quick-probe byte count overflowed"))?;
    }
}

fn visit_file_blocks_simple<F>(
    filename: &str,
    params: ResolvedReadParams,
    visitor: F,
) -> io::Result<VisitFileMetrics>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()>,
{
    validate_read_params(params)?;
    let (file, file_direct) = open_reader_files(filename, params.use_direct)?;
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        return Ok(VisitFileMetrics {
            bytes_read: 0,
            file_size: 0,
            phase_timings: ReadPhaseTimings::default(),
        });
    }

    let mut bytes_read = 0_u64;
    let mut block_index = 0usize;
    let mut offset = 0_u64;
    let mut buffer = AlignedBuffer::new(params.block_size as usize);
    while offset < file_size {
        let remaining = file_size - offset;
        let want = remaining.min(params.block_size) as usize;
        let target = if should_use_direct_io(params.use_direct, offset, want, file_size) {
            &file_direct
        } else {
            &file
        };
        let read = target.read_at(&mut buffer.as_mut_slice()[..want], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("simple read reached EOF early at offset {} of {}", offset, file_size),
            ));
        }
        visitor(ReaderBlock {
            block_index,
            offset,
            file_size,
            data: &buffer.as_slice()[..read],
        })?;
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("simple visitor byte count overflowed"))?;
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("simple visitor offset overflowed"))?;
        block_index += 1;
    }

    Ok(VisitFileMetrics {
        bytes_read,
        file_size,
        phase_timings: ReadPhaseTimings::default(),
    })
}

fn choose_forced_path_kind(
    forced_simple: ReadPathKind,
    forced_threaded: ReadPathKind,
    small_candidate: ReadPathKind,
    large_candidate: ReadPathKind,
    file_size: u64,
    large_min_bytes: u64,
) -> ReadPathKind {
    if file_size >= large_min_bytes {
        match large_candidate {
            ReadPathKind::ThreadedPageCache | ReadPathKind::ThreadedDirect => forced_threaded,
            ReadPathKind::IoUringPageCache => ReadPathKind::IoUringPageCache,
            _ => forced_simple,
        }
    } else {
        match small_candidate {
            ReadPathKind::ThreadedPageCache | ReadPathKind::ThreadedDirect => forced_threaded,
            ReadPathKind::IoUringPageCache => ReadPathKind::IoUringPageCache,
            _ => forced_simple,
        }
    }
}

fn resolve_execution_for_path_kind(
    filename: &str,
    path_kind: ReadPathKind,
    file_size: u64,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<ResolvedReadExecution> {
    let params = match path_kind {
        ReadPathKind::SimplePageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::SimpleDirect => ResolvedReadParams {
            use_direct: true,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::IoUringPageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: benchmark_uring_qd(file_size),
        },
        ReadPathKind::ThreadedPageCache => {
            resolve_reader_params(filename, page_cache, direct, IOMode::PageCache)?
        }
        ReadPathKind::ThreadedDirect => {
            resolve_reader_params(filename, page_cache, direct, IOMode::Direct)?
        }
    };
    Ok(match path_kind {
        ReadPathKind::SimplePageCache | ReadPathKind::SimpleDirect => {
            ResolvedReadExecution::Simple(params)
        }
        ReadPathKind::IoUringPageCache
        | ReadPathKind::ThreadedPageCache
        | ReadPathKind::ThreadedDirect => ResolvedReadExecution::Threaded(params),
    })
}

fn resolve_reader_execution_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> io::Result<ResolvedReadExecution> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    let strategy = config.get_read_auto_strategy_for_path(filename);
    let mount_info = config.mount_info_for_path(filename);
    let file_size = std::fs::metadata(filename)?.len();
    let path_kind = match io_mode {
        IOMode::Direct => choose_forced_path_kind(
            ReadPathKind::SimpleDirect,
            ReadPathKind::ThreadedDirect,
            strategy.cold_small_path,
            strategy.cold_large_path,
            file_size,
            strategy.cold_large_min_bytes,
        ),
        IOMode::PageCache => choose_forced_path_kind(
            ReadPathKind::SimplePageCache,
            ReadPathKind::ThreadedPageCache,
            strategy.hot_small_path,
            strategy.hot_large_path,
            file_size,
            strategy.hot_large_min_bytes,
        ),
        IOMode::Auto => {
            if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
                ReadPathKind::SimpleDirect
            } else if file_size >= strategy.hot_large_min_bytes {
                match strategy.hot_large_path {
                    ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => {
                        ReadPathKind::SimpleDirect
                    }
                    other => other,
                }
            } else {
                match strategy.hot_small_path {
                    ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => {
                        ReadPathKind::SimpleDirect
                    }
                    other => other,
                }
            }
        }
    };
    resolve_execution_for_path_kind(filename, path_kind, file_size, &page_cache, &direct)
}

fn read_file_path_kind(
    pattern: &str,
    filename: &str,
    path_kind: ReadPathKind,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<u64> {
    match path_kind {
        ReadPathKind::SimplePageCache => {
            read_file(pattern, filename, 1, 1024 * 1024, 1, 1, 1024 * 1024, 1, IOMode::PageCache)
        }
        ReadPathKind::SimpleDirect => {
            read_file(pattern, filename, 1, 1024 * 1024, 1, 1, 1024 * 1024, 1, IOMode::Direct)
        }
        ReadPathKind::IoUringPageCache => read_file(
            pattern,
            filename,
            1,
            1024 * 1024,
            32,
            1,
            1024 * 1024,
            32,
            IOMode::PageCache,
        ),
        ReadPathKind::ThreadedPageCache => read_file(
            pattern,
            filename,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::PageCache,
        ),
        ReadPathKind::ThreadedDirect => read_file(
            pattern,
            filename,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::Direct,
        ),
    }
}

fn benchmark_quick_probe_page_cache(
    filename: &str,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<(u64, u64, ResolvedReadParams)> {
    if mount_info.is_some_and(|info| info.fstype == "zfs") {
        let file_size = File::open(filename)?.metadata()?.len();
        let bytes_read = read_file_path_kind("", filename, ReadPathKind::SimpleDirect, page_cache, direct)?;
        return Ok((
            bytes_read,
            file_size,
            ResolvedReadParams {
                use_direct: true,
                num_threads: 1,
                block_size: benchmark_block_size(file_size) as u64,
                qd: 1,
            },
        ));
    }

    let mut file = File::open(filename)?;
    let mut probe = vec![0_u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < probe.len() {
        let read = file.read(&mut probe[filled..])?;
        if read == 0 {
            let bytes_read = filled as u64;
            return Ok((
                bytes_read,
                bytes_read,
                ResolvedReadParams {
                    use_direct: false,
                    num_threads: 1,
                    block_size: benchmark_block_size(bytes_read) as u64,
                    qd: 1,
                },
            ));
        }
        filled += read;
    }

    let file_size = file.metadata()?.len();
    if file_size < strategy.hot_large_min_bytes {
        let bytes_read = read_file_simple_page_cache_from_probe(&mut file, file_size)?;
        return Ok((
            bytes_read,
            file_size,
            ResolvedReadParams {
                use_direct: false,
                num_threads: 1,
                block_size: benchmark_block_size(file_size) as u64,
                qd: 1,
            },
        ));
    }

    let selected_kind = match strategy.hot_large_path {
        ReadPathKind::SimplePageCache
        | ReadPathKind::IoUringPageCache
        | ReadPathKind::ThreadedPageCache => strategy.hot_large_path,
        ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => ReadPathKind::ThreadedPageCache,
    };
    let bytes_read = read_file_path_kind("", filename, selected_kind, page_cache, direct)?;
    let params = match selected_kind {
        ReadPathKind::SimplePageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::IoUringPageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: benchmark_uring_qd(file_size),
        },
        ReadPathKind::ThreadedPageCache => resolve_reader_params(filename, page_cache, direct, IOMode::PageCache)?,
        ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => unreachable!(),
    };
    Ok((bytes_read, file_size, params))
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

fn thread_loader_range(
    start_offset: u64,
    len: usize,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    output: Arc<SharedOutput>,
    use_direct: bool,
) -> std::io::Result<()> {
    if len == 0 {
        return Ok(());
    }

    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }

    let end_offset = start_offset
        .checked_add(len as u64)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed"))?;
    let mut block_num = 0u64;
    let mut inflight = 0usize;
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = start_offset
            .checked_add(block_num.checked_mul(block_size).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
            })?)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
            })?;
        if current_offset >= end_offset {
            break;
        }
        pending.reserve(slot, block_num)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            end_offset,
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
            let current_offset = start_offset
                .checked_add(block_id.checked_mul(block_size).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?;
            let expected_len = expected_read_len(end_offset, current_offset, block_size)?;
            let actual_len =
                validate_read_result("load-file-range", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let output_offset = checked_output_offset(
                    current_offset.saturating_sub(start_offset),
                    actual_len,
                    output.len,
                )?;
                unsafe {
                    let dst = output_slice_mut(&output, output_offset, actual_len);
                    dst.copy_from_slice(&buffers[slot].as_slice()[..actual_len]);
                }
                read_count.fetch_add(actual_len as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = start_offset
                .checked_add(block_num.checked_mul(block_size).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?;
            if next_offset < end_offset {
                pending.reserve(slot, block_num)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    end_offset,
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

fn resolve_load_file_request(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<(ResolvedReadParams, u64, usize)> {
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
    Ok((params, file_size, file_len))
}

fn load_file_to_shared_buffer(
    filename: &str,
    params: ResolvedReadParams,
    file_len: usize,
    options: ReadToMemoryOptions,
) -> std::io::Result<LoadedFile> {
    let mut data = AlignedBuffer::new_uninit(file_len)?;
    if options.use_hugepages_for_len(file_len) {
        madvise_best_effort(
            data.as_mut_slice().as_mut_ptr().cast(),
            file_len.max(1),
            libc::MADV_HUGEPAGE,
        )?;
    }
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

fn block_spans_for_threads(file_size: u64, block_size: u64, thread_count: usize) -> Vec<BlockSpan> {
    if file_size == 0 || thread_count == 0 {
        return Vec::new();
    }
    let block_count = file_size.div_ceil(block_size);
    let blocks_per_thread = block_count.div_ceil(thread_count as u64);
    let mut spans = Vec::new();
    for thread_id in 0..thread_count {
        let start_block = thread_id as u64 * blocks_per_thread;
        if start_block >= block_count {
            break;
        }
        let end_block = ((thread_id as u64 + 1) * blocks_per_thread).min(block_count);
        let start_offset = start_block * block_size;
        let end_offset = (end_block * block_size).min(file_size);
        spans.push(BlockSpan {
            start_offset,
            len: (end_offset - start_offset) as usize,
        });
    }
    spans
}

fn measure_file_load_multiple_targets(
    filename: &str,
    params: ResolvedReadParams,
    file_size: u64,
) -> std::io::Result<u64> {
    let spans = block_spans_for_threads(file_size, params.block_size, params.num_threads as usize);
    let read_count = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();

    for span in spans {
        let read_count = Arc::clone(&read_count);
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let mut target = AlignedBuffer::new_uninit(span.len)?;
            let output = Arc::new(SharedOutput {
                ptr: target.as_mut_slice().as_mut_ptr(),
                len: target.len(),
            });
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_loader_range(
                span.start_offset,
                span.len,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                output,
                params.use_direct,
            )?;
            black_box(target);
            Ok(())
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("multi-target read worker thread panicked"))??;
    }

    Ok(read_count.load(Ordering::SeqCst))
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
    timing_probe: Option<Arc<ReadPhaseTimingProbe>>,
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
        submit_read_with_probe(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            file_size,
            timing_probe.as_deref(),
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
            if let Some(probe) = timing_probe.as_deref() {
                probe.note_first_completion();
            }
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
                submit_read_with_probe(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    file_size,
                    timing_probe.as_deref(),
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        if inflight == 0 {
            if let Some(probe) = timing_probe.as_deref() {
                probe.note_wrapup_start();
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
    load_file_to_memory_with_mode(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
        ReadToMemoryMode::Auto,
        ReadToMemoryOptions::default(),
    )
}

pub fn load_file_to_memory_with_mode(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<LoadedFile> {
    let (params, file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(LoadedFile {
            data: LoadedData::Aligned(AlignedBuffer::new(0)),
            bytes_read: 0,
            params,
        });
    }

    let mode = resolve_to_memory_mode(filename, io_mode, mode);
    let loaded = match mode {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before loading"),
        ReadToMemoryMode::PagedSharedBuffer => {
            load_file_to_shared_buffer(filename, params, file_len, options)?
        }
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            LoadedFile {
                data: LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?),
                bytes_read: file_size,
                params,
            }
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            LoadedFile {
                data,
                bytes_read: file_size,
                params,
            }
        }
        ReadToMemoryMode::MultipleTargetBuffers => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "load_file_to_memory does not support --multiple-target-buffers; use measure_file_load_to_memory for that benchmarking mode",
            ));
        }
    };
    if loaded.bytes_read != file_size {
        return Err(std::io::Error::other(format!(
            "read loaded {} bytes but expected {}",
            loaded.bytes_read, file_size
        )));
    }
    Ok(loaded)
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

pub fn measure_file_load_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<u64> {
    let (params, file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(0);
    }

    match resolve_to_memory_mode(filename, io_mode, mode) {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before measuring"),
        ReadToMemoryMode::PagedSharedBuffer => {
            Ok(load_file_to_shared_buffer(filename, params, file_len, options)?.bytes_read)
        }
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            black_box(data.as_slice().len());
            Ok(file_size)
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            Ok(file_size)
        }
        ReadToMemoryMode::MultipleTargetBuffers => {
            measure_file_load_multiple_targets(filename, params, file_size)
        }
    }
}

pub fn prepare_file_load_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<Option<LoadedData>> {
    let (params, _file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(None);
    }

    match resolve_to_memory_mode(filename, io_mode, mode) {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before preparing"),
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            Ok(Some(LoadedData::Mapped(MappedReadBuffer::map(
                &file, file_len, options,
            )?)))
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            Ok(Some(data))
        }
        ReadToMemoryMode::PagedSharedBuffer | ReadToMemoryMode::MultipleTargetBuffers => Ok(None),
    }
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
    match resolve_reader_execution_for_mode(config, mode, filename, io_mode)? {
        ResolvedReadExecution::Simple(params) => {
            let file_size = std::fs::metadata(filename)?.len();
            if file_size == 0 {
                return Ok(MappedBlocks {
                    blocks: Vec::new(),
                    bytes_read: 0,
                    file_size,
                    params,
                });
            }
            let block_count = file_size.div_ceil(params.block_size) as usize;
            let mapper = Arc::new(mapper);
            let results = Arc::new(
                (0..block_count)
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>(),
            );
            let result_slots = Arc::clone(&results);
            let metrics = visit_file_blocks_simple(filename, params, move |block| {
                let value = mapper(block)?;
                *result_slots[block.block_index].lock().unwrap() = Some(value);
                Ok(())
            })?;
            let mut blocks = Vec::with_capacity(block_count);
            for (block_index, slot) in results.iter().enumerate() {
                let value = slot.lock().unwrap().take().ok_or_else(|| {
                    std::io::Error::other(format!(
                        "missing mapped result for block {}",
                        block_index
                    ))
                })?;
                blocks.push(value);
            }
            Ok(MappedBlocks {
                blocks,
                bytes_read: metrics.bytes_read,
                file_size: metrics.file_size,
                params,
            })
        }
        ResolvedReadExecution::Threaded(params) => {
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
                    std::io::Error::other(format!(
                        "missing mapped result for block {}",
                        block_index
                    ))
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
    }
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

    let metrics = visit_file_blocks_with_resolved_params(filename, params, visitor)?;
    Ok((metrics.bytes_read, metrics.file_size, params))
}

pub fn visit_file_blocks_with_resolved_params<F>(
    filename: &str,
    params: ResolvedReadParams,
    visitor: F,
) -> std::io::Result<VisitFileMetrics>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    validate_read_params(params)?;

    let file_size = std::fs::metadata(filename)?.len();
    if file_size == 0 {
        return Ok(VisitFileMetrics {
            bytes_read: 0,
            file_size: 0,
            phase_timings: ReadPhaseTimings::default(),
        });
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let visitor = Arc::new(visitor);
    let timing_probe = Arc::new(ReadPhaseTimingProbe::new());

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let filename = filename.to_string();
        let read_count = read_count.clone();
        let visitor = visitor.clone();
        let timing_probe = timing_probe.clone();
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
                Some(timing_probe),
            )
        }));
    }
    timing_probe.note_threads_created();

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }
    timing_probe.note_join_done();

    Ok(VisitFileMetrics {
        bytes_read: read_count.load(Ordering::SeqCst),
        file_size,
        phase_timings: timing_probe.snapshot(),
    })
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
    match resolve_reader_execution_for_mode(config, mode, filename, io_mode)? {
        ResolvedReadExecution::Simple(params) => {
            let metrics = visit_file_blocks_simple(filename, params, visitor)?;
            Ok((metrics.bytes_read, metrics.file_size, params))
        }
        ResolvedReadExecution::Threaded(params) => {
            let metrics = visit_file_blocks_with_resolved_params(filename, params, visitor)?;
            Ok((metrics.bytes_read, metrics.file_size, params))
        }
    }
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


pub fn read_file_auto_with_strategy(
    pattern: &str,
    filename: &str,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: IOParams,
    direct: IOParams,
) -> std::io::Result<u64> {
    if mount_info.is_some_and(|info| info.fstype == "zfs") {
        return read_file_path_kind(pattern, filename, ReadPathKind::SimpleDirect, &page_cache, &direct);
    }
    let file_size = std::fs::metadata(filename)?.len();
    let cache_state = auto_read_cache_state(filename);
    let path_kind = choose_path_kind_for_state(strategy, cache_state, file_size);
    read_file_path_kind(pattern, filename, path_kind, &page_cache, &direct)
}

pub fn benchmark_read_variant(
    filename: &str,
    variant: ReadBenchmarkVariant,
    cache_state: ReadBenchmarkCacheState,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: IOParams,
    direct: IOParams,
) -> io::Result<ReadBenchmarkResult> {
    let start = std::time::Instant::now();
    let file_size = std::fs::metadata(filename)?.len();
    let (bytes_read, file_size, params, phase_timings) = match variant {
        ReadBenchmarkVariant::SingleThreadPageCache => {
            let block_size = benchmark_block_size(file_size);
            let bytes_read = read_file_single_thread_blocking(filename, false, block_size)?;
            let params = ResolvedReadParams {
                use_direct: false,
                num_threads: 1,
                block_size: block_size as u64,
                qd: 1,
            };
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::SingleThreadDirect => {
            let block_size = benchmark_block_size(file_size);
            let bytes_read = read_file_single_thread_blocking(filename, true, block_size)?;
            let params = ResolvedReadParams {
                use_direct: true,
                num_threads: 1,
                block_size: block_size as u64,
                qd: 1,
            };
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::SingleThreadIoUring => {
            let block_size = benchmark_block_size(file_size) as u64;
            let qd = benchmark_uring_qd(file_size);
            let params = resolve_reader_params(
                filename,
                &IOParams {
                    num_threads: 1,
                    block_size,
                    qd,
                },
                &IOParams {
                    num_threads: 1,
                    block_size,
                    qd,
                },
                IOMode::PageCache,
            )?;
            let metrics =
                visit_file_blocks_with_resolved_params(filename, params, |_| Ok::<_, io::Error>(()))?;
            (metrics.bytes_read, metrics.file_size, params, metrics.phase_timings)
        }
        ReadBenchmarkVariant::QuickProbePageCache => {
            let (bytes_read, file_size, params) =
                benchmark_quick_probe_page_cache(filename, strategy, mount_info, &page_cache, &direct)?;
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::MultiThreadCurrent => {
            let io_mode = match cache_state {
                ReadBenchmarkCacheState::Cold => IOMode::Direct,
                ReadBenchmarkCacheState::Hot => IOMode::PageCache,
            };
            let params = resolve_reader_params(filename, &page_cache, &direct, io_mode)?;
            let metrics =
                visit_file_blocks_with_resolved_params(filename, params, |_| Ok::<_, io::Error>(()))?;
            (metrics.bytes_read, metrics.file_size, params, metrics.phase_timings)
        }
    };
    Ok(ReadBenchmarkResult {
        bytes_read,
        file_size,
        elapsed: start.elapsed(),
        params,
        phase_timings,
    })
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
    fn measure_file_load_to_memory_mmap_reports_full_length() {
        let path = unique_temp_file("fro-load-mmap");
        let data = (0..(256 * 1024 + 321))
            .map(|i| ((i * 13) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let bytes = measure_file_load_to_memory(
            path.to_str().unwrap(),
            2,
            128 * 1024,
            2,
            2,
            256 * 1024,
            2,
            IOMode::PageCache,
            ReadToMemoryMode::Mmap,
            ReadToMemoryOptions::default(),
        )
        .unwrap();

        assert_eq!(bytes, data.len() as u64);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn measure_file_load_to_memory_multiple_targets_reports_full_length() {
        let path = unique_temp_file("fro-load-multi-target");
        let data = (0..(512 * 1024 + 777))
            .map(|i| ((i * 29) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let bytes = measure_file_load_to_memory(
            path.to_str().unwrap(),
            4,
            128 * 1024,
            2,
            2,
            256 * 1024,
            2,
            IOMode::PageCache,
            ReadToMemoryMode::MultipleTargetBuffers,
            ReadToMemoryOptions::default(),
        )
        .unwrap();

        assert_eq!(bytes, data.len() as u64);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn mapped_read_buffer_unmaps_prefix_in_page_chunks() {
        let path = unique_temp_file("fro-load-mmap-unmap");
        let data = vec![0x5A; 8192];
        fs::write(&path, &data).unwrap();

        let file = File::open(&path).unwrap();
        let mut mapped =
            MappedReadBuffer::map(&file, data.len(), ReadToMemoryOptions::default()).unwrap();
        assert_eq!(mapped.as_slice().len(), 8192);

        let unmapped = mapped.unmap_prefix(5000).unwrap();
        assert_eq!(unmapped, 4096);
        assert_eq!(mapped.as_slice().len(), 4096);
        assert!(mapped.as_slice().iter().all(|&b| b == 0x5A));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_to_memory_defaults_use_auto_hugepages_policy() {
        let options = ReadToMemoryOptions::default();
        assert_eq!(options.hugepages, HugepageAdvice::Auto);
        assert!(!options.measure_unmap_time);
    }

    #[test]
    fn auto_hugepages_disable_small_files() {
        let options = ReadToMemoryOptions::default();
        assert!(!options.use_hugepages_for_len(HUGEPAGE_MIN_FILE_LEN - 1));
        assert!(options.use_hugepages_for_len(HUGEPAGE_MIN_FILE_LEN));
    }

    #[test]
    fn auto_to_memory_mode_uses_direct_loader_when_direct_is_forced() {
        assert_eq!(
            resolve_to_memory_mode("/dev/null", IOMode::Direct, ReadToMemoryMode::Auto),
            ReadToMemoryMode::PagedSharedBuffer
        );
    }

    #[test]
    fn auto_to_memory_mode_uses_mmap_when_page_cache_is_forced() {
        assert_eq!(
            resolve_to_memory_mode("/dev/null", IOMode::PageCache, ReadToMemoryMode::Auto),
            ReadToMemoryMode::Mmap
        );
    }

    #[test]
    fn auto_to_memory_mode_treats_empty_files_as_cached() {
        let path = unique_temp_file("fro-load-auto-empty");
        fs::write(&path, b"").unwrap();
        assert_eq!(
            resolve_to_memory_mode(path.to_str().unwrap(), IOMode::Auto, ReadToMemoryMode::Auto),
            ReadToMemoryMode::Mmap
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn auto_lift_mode_prefers_page_cache_for_resident_file() {
        assert!(auto_lift_mode_for_residency(false) == IOMode::Direct);
        assert!(auto_lift_mode_for_residency(true) == IOMode::PageCache);
    }

    #[test]
    fn benchmark_block_size_caps_at_one_megabyte() {
        assert_eq!(benchmark_block_size(4096), 4096);
        assert_eq!(benchmark_block_size(80 * 1024 * 1024), 1024 * 1024);
    }

    #[test]
    fn benchmark_uring_qd_scales_up_to_four() {
        assert_eq!(benchmark_uring_qd(4 * 1024), 1);
        assert_eq!(benchmark_uring_qd(1024 * 1024), 1);
        assert_eq!(benchmark_uring_qd(3 * 1024 * 1024), 3);
        assert_eq!(benchmark_uring_qd(80 * 1024 * 1024), 4);
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

fn read_all_bytes(data: &[u8], num_threads: u64) -> std::io::Result<()> {
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
    let chunk_size = data.len().div_ceil(thread_count);

    for thread_id in 0..thread_count {
        let checksum = Arc::clone(&checksum);
        let shared = Arc::clone(&shared);
        threads.push(std::thread::spawn(move || {
            let start = thread_id * chunk_size;
            let end = shared.len.min(start + chunk_size);
            let mut local = 0u64;
            for offset in start..end {
                let value = unsafe { *shared.ptr.add(offset) as u64 };
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

#[cfg(kani)]
mod kani_proofs {
    use super::auto_lift_mode_for_residency;
    use crate::common::IOMode;

    #[kani::proof]
    fn auto_lift_mode_matches_first_page_residency() {
        let first_page_resident: bool = kani::any();
        let mode = auto_lift_mode_for_residency(first_page_resident);
        if first_page_resident {
            assert_eq!(mode, IOMode::PageCache);
        } else {
            assert_eq!(mode, IOMode::Direct);
        }
    }
}
