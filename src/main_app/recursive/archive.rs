use super::*;
use crate::common::{AlignedBuffer, IOMode};
use crate::config::{self, IOParams};
use crate::io_util::{sync_parent_directory, sync_path};
use crate::main_app::TarCompression;
use crate::writer::{copy_file_range_threaded, OffsetWriter};
use flate2::read::MultiGzDecoder;
use gzp::{deflate::Mgzip, ZBuilder};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;

const TAR_BLOCK_SIZE: u64 = 512;
const TAR_EOF_BLOCKS: u64 = TAR_BLOCK_SIZE * 2;
const TAR_COPY_BUFFER_SIZE: usize = 1024 * 1024;
const TAR_FAST_COPY_SENDFILE_CHUNK_SIZE: usize = 0x7fff_f000usize;
const TAR_SMALL_SLAB_TARGET_BYTES: usize = 512 * 1024;
const TAR_SMALL_WRITE_WORKERS: usize = 4;
const TAR_SMALL_WRITE_QD: usize = 4;

#[derive(Clone)]
enum TarEntryKind {
    RegularFile { size: u64, source_path: PathBuf },
    Directory,
    Symlink { target: Vec<u8> },
}

#[derive(Clone)]
struct TarEntry {
    archive_path: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
    kind: TarEntryKind,
    header_offset: u64,
    data_offset: u64,
}

#[derive(Clone)]
struct TarSlabTask {
    start_offset: u64,
    len: u64,
    entry_indices: Vec<usize>,
}

#[derive(Clone)]
struct TarLargeTask {
    entry_index: usize,
}

#[derive(Clone)]
enum TarPlannedTask {
    Slab(TarSlabTask),
    Large(TarLargeTask),
}

struct ReusableTarSlab {
    buffer: AlignedBuffer,
}

#[derive(Default)]
struct TarParallelCounters {
    small_slab_tasks_inflight: AtomicUsize,
    small_slab_entries_inflight: AtomicUsize,
    sendfile_streams_inflight: AtomicUsize,
    mt_copy_jobs_inflight: AtomicUsize,
    mt_copy_threads_inflight: AtomicUsize,
}

struct TarParallelSampler {
    done: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<io::Result<()>>>,
}

mod bench;
mod format;
mod manifest;
mod parallel;
mod reader;
pub(crate) use bench::bench_tar_archive_variant;

use self::format::*;
use self::manifest::*;
use self::parallel::*;

pub(crate) fn create_uncompressed_tar(
    source: &Path,
    output: &Path,
    verbose: bool,
) -> io::Result<u64> {
    let (entries, total_size) = collect_tar_manifest(source, output)?;
    let planned_tasks = plan_tar_tasks(&entries);
    if output == Path::new("/dev/null") {
        return stream_tar_to_dev_null(&entries, &planned_tasks, verbose);
    }
    let config = config::load_config(None);
    let output_str = output.to_string_lossy().into_owned();
    let copy_page_cache = config.get_params_for_path("copy", false, &output_str);
    let copy_direct = config.get_params_for_path("copy", true, &output_str);
    let output_file = prepare_tar_output(output, total_size)?;
    let output_path = output.to_path_buf();
    let entries = Arc::new(entries);
    let slab_tasks = Arc::new(
        planned_tasks
            .iter()
            .filter_map(|task| match task {
                TarPlannedTask::Slab(task) => Some(task.clone()),
                TarPlannedTask::Large(_) => None,
            })
            .collect::<Vec<_>>(),
    );
    let large_tasks = Arc::new(
        planned_tasks
            .iter()
            .filter_map(|task| match task {
                TarPlannedTask::Slab(_) => None,
                TarPlannedTask::Large(task) => Some(task.clone()),
            })
            .collect::<Vec<_>>(),
    );
    let next_slab = Arc::new(AtomicUsize::new(0));
    let next_large = Arc::new(AtomicUsize::new(0));
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let parallel_counters = Arc::new(TarParallelCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "tar-create",
            "items",
            sample_counters.clone(),
        ))
    } else {
        None
    };
    let parallel_sampler = if verbose {
        Some(TarParallelSampler::start(
            "tar-create",
            parallel_counters.clone(),
        ))
    } else {
        None
    };
    let slab_worker_count = TAR_SMALL_WRITE_WORKERS.min(slab_tasks.len().max(1));
    let large_worker_count = recursive_copy_large_worker_count().min(large_tasks.len().max(1));
    let mut threads = Vec::with_capacity(slab_worker_count.saturating_add(large_worker_count));
    for _ in 0..slab_worker_count {
        let entries = entries.clone();
        let slab_tasks = slab_tasks.clone();
        let next_slab = next_slab.clone();
        let output_path = output_path.clone();
        let sample_counters = sample_counters.clone();
        let parallel_counters = parallel_counters.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let output_path = output_path.to_string_lossy().into_owned();
            let mut writer = OffsetWriter::with_truncate(
                &output_path,
                total_size,
                TAR_SMALL_WRITE_QD,
                IOMode::Direct,
                false,
            )?;
            let mut slab = ReusableTarSlab::new()?;
            while let Some(task) = slab_tasks.get(next_slab.fetch_add(1, Ordering::SeqCst)) {
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_add(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_add(task.entry_indices.len(), Ordering::Relaxed);
                let slab_bytes = slab.fill(&entries, task)?;
                writer.write_at(task.start_offset, slab_bytes)?;
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_sub(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_sub(task.entry_indices.len(), Ordering::Relaxed);
                sample_counters.bytes.fetch_add(task.len, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
            }
            writer.flush()?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&output_path)?
                .sync_all()?;
            Ok(())
        }));
    }
    for _ in 0..large_worker_count {
        let entries = entries.clone();
        let large_tasks = large_tasks.clone();
        let next_large = next_large.clone();
        let output_path = output_path.clone();
        let sample_counters = sample_counters.clone();
        let copy_page_cache = copy_page_cache.clone();
        let copy_direct = copy_direct.clone();
        let parallel_counters = parallel_counters.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let output_path = output_path.to_string_lossy().into_owned();
            while let Some(task) = large_tasks.get(next_large.fetch_add(1, Ordering::SeqCst)) {
                let written = write_tar_large_entry(
                    &entries[task.entry_index],
                    &output_path,
                    &copy_page_cache,
                    &copy_direct,
                    Some(parallel_counters.as_ref()),
                )?;
                sample_counters.bytes.fetch_add(written, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("tar worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(sampler) = parallel_sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }

    let zero_blocks = [0u8; TAR_EOF_BLOCKS as usize];
    write_all_at(&output_file, total_size - TAR_EOF_BLOCKS, &zero_blocks)?;
    output_file.sync_all()?;
    sync_path(output)?;
    sync_parent_directory(output)?;
    if verbose {
        eprintln!(
            "tar create: entries={}, total_size={}",
            entries.len(),
            total_size
        );
    }
    Ok(total_size)
}

fn gzip_threads() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get().saturating_sub(1).min(8))
        .unwrap_or(0)
}

fn gzp_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("mgzip error: {err}"))
}

#[cfg(feature = "rapidgzip-backend")]
fn open_gzip_archive(path: &Path) -> io::Result<Box<dyn Read>> {
    Ok(Box::new(rapidgzip::Reader::open(path)?))
}

#[cfg(not(feature = "rapidgzip-backend"))]
fn open_gzip_archive(path: &Path) -> io::Result<Box<dyn Read>> {
    Ok(Box::new(MultiGzDecoder::new(fs::File::open(path)?)))
}

fn write_tar_stream<W: Write + ?Sized>(entries: &[TarEntry], writer: &mut W) -> io::Result<u64> {
    let mut total_bytes = 0_u64;
    let mut copy_buffer = vec![0u8; TAR_COPY_BUFFER_SIZE];
    let zero_block = [0u8; TAR_BLOCK_SIZE as usize];
    for entry in entries {
        let header = tar_header_bytes(entry)?;
        writer.write_all(&header)?;
        total_bytes = total_bytes.saturating_add(TAR_BLOCK_SIZE);
        if let TarEntryKind::RegularFile { size, source_path } = &entry.kind {
            let mut source = fs::File::open(source_path)?;
            let mut remaining = *size;
            while remaining > 0 {
                let chunk = remaining.min(copy_buffer.len() as u64) as usize;
                let read = source.read(&mut copy_buffer[..chunk])?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "short tar source read after {} of {} bytes from {}",
                            size.saturating_sub(remaining),
                            size,
                            source_path.display()
                        ),
                    ));
                }
                writer.write_all(&copy_buffer[..read])?;
                remaining = remaining.saturating_sub(read as u64);
                total_bytes = total_bytes.saturating_add(read as u64);
            }
            let padding = align_up(*size, TAR_BLOCK_SIZE).saturating_sub(*size);
            if padding > 0 {
                writer.write_all(&zero_block[..padding as usize])?;
                total_bytes = total_bytes.saturating_add(padding);
            }
        }
    }
    let eof_blocks = [0u8; TAR_EOF_BLOCKS as usize];
    writer.write_all(&eof_blocks)?;
    Ok(total_bytes.saturating_add(TAR_EOF_BLOCKS))
}

pub(crate) fn create_gzip_tar(source: &Path, output: &Path, verbose: bool) -> io::Result<u64> {
    let (entries, logical_size) = collect_tar_manifest(source, output)?;
    let output_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output)?;
    let mut writer = ZBuilder::<Mgzip, fs::File>::new()
        .num_threads(gzip_threads())
        .from_writer(output_file);
    let tar_bytes = write_tar_stream(&entries, &mut *writer)?;
    let output_file = writer.finish().map_err(gzp_error)?;
    output_file.sync_all()?;
    sync_path(output)?;
    sync_parent_directory(output)?;
    let archive_bytes = output_file.metadata()?.len();
    if verbose {
        eprintln!(
            "tar create (gzip): entries={}, tar_bytes={}, archive_bytes={}, manifest_total={}",
            entries.len(),
            tar_bytes,
            archive_bytes,
            logical_size
        );
    }
    Ok(archive_bytes)
}

pub(crate) fn list_uncompressed_tar(path: &Path, verbose: bool) -> io::Result<()> {
    reader::list_tar_archive(path, verbose)
}

pub(crate) fn list_gzip_tar(path: &Path, verbose: bool) -> io::Result<()> {
    let mut reader = open_gzip_archive(path)?;
    reader::list_tar_archive_reader(&mut reader, path, verbose)
}

pub(crate) fn extract_uncompressed_tar(
    path: &Path,
    destination: Option<&Path>,
    verbose: bool,
) -> io::Result<()> {
    reader::extract_tar_archive(path, destination, verbose)
}

pub(crate) fn extract_gzip_tar(
    path: &Path,
    destination: Option<&Path>,
    verbose: bool,
) -> io::Result<()> {
    let mut reader = open_gzip_archive(path)?;
    reader::extract_tar_archive_reader(&mut reader, path, destination, verbose)
}

pub(crate) fn create_tar_archive(
    source: &Path,
    output: &Path,
    verbose: bool,
    compression: TarCompression,
) -> io::Result<u64> {
    match compression {
        TarCompression::None => create_uncompressed_tar(source, output, verbose),
        TarCompression::Gzip => create_gzip_tar(source, output, verbose),
    }
}

pub(crate) fn list_tar_archive(
    path: &Path,
    verbose: bool,
    compression: TarCompression,
) -> io::Result<()> {
    match compression {
        TarCompression::None => list_uncompressed_tar(path, verbose),
        TarCompression::Gzip => list_gzip_tar(path, verbose),
    }
}

pub(crate) fn extract_tar_archive(
    path: &Path,
    destination: Option<&Path>,
    verbose: bool,
    compression: TarCompression,
) -> io::Result<()> {
    match compression {
        TarCompression::None => extract_uncompressed_tar(path, destination, verbose),
        TarCompression::Gzip => extract_gzip_tar(path, destination, verbose),
    }
}
