use crate::config::load_config;
use crate::io_util::CopyOperationGuard;
use crate::reader::load_file_to_memory_for_mode;
use crate::stream::{ParallelFile, ParallelReadReport, ParallelWriter};
use crate::writer::{
    self, copy_file_range as copy_range_internal, resolve_writer_params_for_mode, SequentialWriter,
};
use crate::IOMode;
use std::io;
use std::path::Path;

fn path_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

pub fn open<P: AsRef<Path>>(path: P) -> io::Result<ParallelFile> {
    open_with_mode(path, IOMode::Auto)
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
        load_file_to_memory_for_mode(&config, "read", path_str(path.as_ref())?, io_mode)?
            .data
            .as_slice()
            .to_vec(),
    )
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
    let copied = writer::copy_file(
        source,
        target,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
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
        io_mode_read,
        io_mode_write,
    )
}

pub fn optimal_block_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    optimal_block_size_with_mode(path, IOMode::Auto)
}

pub fn optimal_block_size_with_mode<P: AsRef<Path>>(path: P, io_mode: IOMode) -> io::Result<u64> {
    open_with_mode(path, io_mode)?.block_size()
}
