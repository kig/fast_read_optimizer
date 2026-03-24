use crate::common::CopyStrategy;
use crate::config::load_config;
use crate::io_util::CopyOperationGuard;
use crate::reader::{evict_file_cache, load_file_to_memory_for_mode, warm_file_page_cache};
use crate::stream::{ParallelFile, ParallelReadReport, ParallelWriter};
use crate::writer::{
    self, copy_file_range_with_strategy as copy_range_internal, resolve_writer_params_for_mode,
    SequentialWriter,
};
use crate::IOMode;
use std::io;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn path_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCacheLiftBenchmarkReport {
    pub bytes_read: u64,
    pub checkpoint_1: Duration,
    pub checkpoint_2: Duration,
}

fn page_cache_lift_checkpoint_nanos(foreground_nanos: u64, background_nanos: u64) -> u64 {
    foreground_nanos.max(background_nanos)
}

fn duration_to_u64_nanos(duration: Duration) -> io::Result<u64> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| io::Error::other("benchmark duration overflowed u64 nanoseconds"))
}

pub fn open<P: AsRef<Path>>(path: P) -> io::Result<ParallelFile> {
    open_with_mode(path, IOMode::Auto)
}

#[cfg(test)]
mod tests {
    use super::page_cache_lift_checkpoint_nanos;

    #[test]
    fn page_cache_lift_checkpoint_never_precedes_foreground_completion() {
        assert_eq!(page_cache_lift_checkpoint_nanos(9, 3), 9);
        assert_eq!(page_cache_lift_checkpoint_nanos(9, 14), 14);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::page_cache_lift_checkpoint_nanos;

    #[kani::proof]
    fn checkpoint_2_is_monotonic_over_foreground_completion() {
        let foreground_nanos: u64 = kani::any();
        let background_nanos: u64 = kani::any();
        let checkpoint_2 = page_cache_lift_checkpoint_nanos(foreground_nanos, background_nanos);
        assert!(checkpoint_2 >= foreground_nanos);
        assert!(checkpoint_2 >= background_nanos || checkpoint_2 == foreground_nanos);
    }
}

pub fn open_with_mode<P: AsRef<Path>>(path: P, io_mode: IOMode) -> io::Result<ParallelFile> {
    let config = load_config(None);
    ParallelFile::open(&config, "read", path_str(path.as_ref())?, io_mode)
}

pub fn create<P: AsRef<Path>>(path: P) -> io::Result<SequentialWriter> {
    create_with_mode(path, IOMode::Auto)
}

pub fn create_with_mode<P: AsRef<Path>>(path: P, io_mode: IOMode) -> io::Result<SequentialWriter> {
    let config = load_config(None);
    let path = path_str(path.as_ref())?;
    let params = resolve_writer_params_for_mode(&config, "write", path, io_mode);
    SequentialWriter::create(path, params.qd, params.block_size, io_mode)
}

pub fn indexed_writer<P: AsRef<Path>>(path: P, block_count: usize) -> io::Result<ParallelWriter> {
    indexed_writer_with_mode(path, block_count, IOMode::Auto)
}

pub fn indexed_writer_with_mode<P: AsRef<Path>>(
    path: P,
    block_count: usize,
    io_mode: IOMode,
) -> io::Result<ParallelWriter> {
    let config = load_config(None);
    ParallelWriter::indexed(
        &config,
        "write",
        path_str(path.as_ref())?,
        io_mode,
        block_count,
    )
}

/// Create a fixed-size offset writer.
///
/// The destination is prepared to `total_size` bytes up front. Any regions that
/// are not explicitly written remain zero-filled on successful completion.
/// `report.bytes_written` counts only caller-provided bytes, not the zero-filled gaps.
pub fn offset_writer<P: AsRef<Path>>(path: P, total_size: u64) -> io::Result<ParallelWriter> {
    offset_writer_with_options(path, total_size, IOMode::Auto, true)
}

/// Create a fixed-size offset writer with an explicit I/O mode.
///
/// The destination is prepared to `total_size` bytes up front. Any regions that
/// are not explicitly written remain zero-filled on successful completion.
pub fn offset_writer_with_mode<P: AsRef<Path>>(
    path: P,
    total_size: u64,
    io_mode: IOMode,
) -> io::Result<ParallelWriter> {
    offset_writer_with_options(path, total_size, io_mode, true)
}

/// Create a fixed-size offset writer with explicit I/O mode and truncation policy.
///
/// When `truncate` is `true`, the file is recreated at exactly `total_size` bytes
/// before writes begin, so unwritten regions read back as zeroes. When `truncate`
/// is `false`, existing bytes outside the caller-written ranges are preserved,
/// and the file is only extended to `total_size` if needed.
pub fn offset_writer_with_options<P: AsRef<Path>>(
    path: P,
    total_size: u64,
    io_mode: IOMode,
    truncate: bool,
) -> io::Result<ParallelWriter> {
    let config = load_config(None);
    ParallelWriter::fixed_size_with_truncate(
        &config,
        "write",
        path_str(path.as_ref())?,
        io_mode,
        total_size,
        truncate,
    )
}

pub fn read_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    read_file_with_mode(path, IOMode::Auto)
}

pub fn read_file_with_mode<P: AsRef<Path>>(path: P, io_mode: IOMode) -> io::Result<Vec<u8>> {
    let config = load_config(None);
    Ok(
        load_file_to_memory_for_mode(&config, "read_to_memory", path_str(path.as_ref())?, io_mode)?
            .data
            .as_slice()
            .to_vec(),
    )
}

/// Benchmark a cold-start direct-to-memory load while warming the page cache in
/// parallel for the same file.
///
/// The call makes a best-effort cache eviction first, then starts:
/// - a foreground `read_to_memory` load forced to `IOMode::Direct`
/// - a background page-cache warm pass through the same file
///
/// `checkpoint_1` is when the application-owned direct load completes.
/// `checkpoint_2` is when the background page-cache warm is also complete.
pub fn benchmark_page_cache_lift<P: AsRef<Path>>(
    path: P,
) -> io::Result<PageCacheLiftBenchmarkReport> {
    let path = path.as_ref();
    let path_string = path_str(path)?.to_owned();
    evict_file_cache(&path_string)?;

    let start_barrier = Arc::new(Barrier::new(2));
    let background_barrier = Arc::clone(&start_barrier);
    let background_path = path_string.clone();
    let start = Instant::now();
    let background = thread::spawn(move || -> io::Result<(u64, u64)> {
        background_barrier.wait();
        let warmed = warm_file_page_cache(&background_path)?;
        Ok((warmed, duration_to_u64_nanos(start.elapsed())?))
    });

    let config = load_config(None);
    start_barrier.wait();
    let loaded =
        load_file_to_memory_for_mode(&config, "read_to_memory", &path_string, IOMode::Direct)?;
    let checkpoint_1_nanos = duration_to_u64_nanos(start.elapsed())?;
    let (background_warmed, background_nanos) = background
        .join()
        .map_err(|_| io::Error::other("background page-cache warm thread panicked"))??;

    if background_warmed != loaded.bytes_read {
        return Err(io::Error::other(format!(
            "background page-cache warm read {} bytes but foreground loaded {}",
            background_warmed, loaded.bytes_read
        )));
    }

    let checkpoint_2_nanos = page_cache_lift_checkpoint_nanos(checkpoint_1_nanos, background_nanos);
    Ok(PageCacheLiftBenchmarkReport {
        bytes_read: loaded.bytes_read,
        checkpoint_1: Duration::from_nanos(checkpoint_1_nanos),
        checkpoint_2: Duration::from_nanos(checkpoint_2_nanos),
    })
}

pub fn visit_blocks<P, F>(path: P, visit: F) -> io::Result<ParallelReadReport>
where
    P: AsRef<Path>,
    F: Fn(usize, &[u8]) -> io::Result<()> + Send + Sync + 'static,
{
    visit_blocks_with_mode(path, IOMode::Auto, visit)
}

pub fn visit_blocks_with_mode<P, F>(
    path: P,
    io_mode: IOMode,
    visit: F,
) -> io::Result<ParallelReadReport>
where
    P: AsRef<Path>,
    F: Fn(usize, &[u8]) -> io::Result<()> + Send + Sync + 'static,
{
    let file = open_with_mode(path, io_mode)?;
    file.foreach_block(visit)
}

pub fn write_file<P: AsRef<Path>>(path: P, data: &[u8]) -> io::Result<u64> {
    write_file_with_mode(path, data, IOMode::Auto)
}

pub fn write_file_range<P: AsRef<Path>>(
    path: P,
    data: &[u8],
    offset: usize,
    len: usize,
) -> io::Result<u64> {
    write_file_range_with_mode(path, data, offset, len, IOMode::Auto)
}

pub fn write_file_with_mode<P: AsRef<Path>>(
    path: P,
    data: &[u8],
    io_mode: IOMode,
) -> io::Result<u64> {
    let mut writer = create_with_mode(path, io_mode)?;
    writer.append(data)?;
    writer.flush()?;
    Ok(writer.bytes_written())
}

pub fn write_file_range_with_mode<P: AsRef<Path>>(
    path: P,
    data: &[u8],
    offset: usize,
    len: usize,
    io_mode: IOMode,
) -> io::Result<u64> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer range overflows"))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "buffer range {}..{} is out of bounds for {} bytes",
                offset,
                end,
                data.len()
            ),
        )
    })?;
    write_file_with_mode(path, slice, io_mode)
}

pub fn copy_file<S: AsRef<Path>, D: AsRef<Path>>(source: S, target: D) -> io::Result<u64> {
    copy_file_with_modes(source, target, IOMode::Auto, IOMode::Auto)
}

pub fn copy_file_via_memory<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
) -> io::Result<u64> {
    copy_file_via_memory_with_modes(source, target, IOMode::Auto, IOMode::Auto)
}

pub fn copy_file_via_memory_with_modes<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    let source = path_str(source.as_ref())?;
    let target = path_str(target.as_ref())?;
    let guard = CopyOperationGuard::new(source, target, true)?;
    let data = read_file_with_mode(source, io_mode_read)?;
    let copied = write_file_with_mode(target, &data, io_mode_write)?;
    guard.ensure_source_unchanged()?;
    Ok(copied)
}

pub fn copy_file_with_modes<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    let config = load_config(None);
    let source = path_str(source.as_ref())?;
    let target = path_str(target.as_ref())?;
    let guard = CopyOperationGuard::new(source, target, true)?;
    let page_cache = config.get_params_for_path("copy", false, target);
    let direct = config.get_params_for_path("copy", true, target);
    let copy_range = config.get_copy_range_params_for_path(target);
    let copied = writer::copy_file(
        source,
        target,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        copy_range.num_threads,
        copy_range.block_size,
        copy_range.qd,
        io_mode_read,
        io_mode_write,
    )?;
    guard.ensure_source_unchanged()?;
    Ok(copied)
}

pub fn copy_file_range_with_modes<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    source_offset: u64,
    dest_offset: u64,
    len: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    let config = load_config(None);
    let source = path_str(source.as_ref())?;
    let target = path_str(target.as_ref())?;
    let page_cache = config.get_params_for_path("copy", false, target);
    let direct = config.get_params_for_path("copy", true, target);
    let copy_range = config.get_copy_range_params_for_path(target);
    copy_range_internal(
        source,
        target,
        source_offset,
        dest_offset,
        len,
        truncate_target,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        copy_range.num_threads,
        copy_range.block_size,
        copy_range.qd,
        io_mode_read,
        io_mode_write,
        CopyStrategy::Threaded,
    )
}

pub fn optimal_block_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    optimal_block_size_with_mode(path, IOMode::Auto)
}

pub fn optimal_block_size_with_mode<P: AsRef<Path>>(path: P, io_mode: IOMode) -> io::Result<u64> {
    open_with_mode(path, io_mode)?.block_size()
}
