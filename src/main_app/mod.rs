use std::env;

use crate::block_hash::{
    default_hash_base, hash_file_blocks, hash_file_to_replicas, recover_file_with_copies,
    verify_file_with_replicas, BlockHashAlgorithm, RecoverMode,
};
use crate::common::{AlignedBuffer, CopyAutoMode, CopyStrategy, ReadAutoStrategy, ReadPathKind};
use crate::differ::{bench_diff_memory, bench_memcpy_memory, diff_files};
use crate::io_util::{
    direct_writer_supported, open_reader_files, sync_path, validate_read_result, CopyOperationGuard,
};
use crate::mincore::is_first_page_resident;
use crate::optimizer::run_optimizer;
use crate::reader::{
    benchmark_read_variant, load_file_to_memory, measure_file_load_to_memory,
    prepare_file_load_to_memory, read_file, read_file_auto_with_strategy, resolve_to_memory_mode,
    visit_file_blocks_for_mode, HugepageAdvice, ReadBenchmarkCacheState, ReadBenchmarkVariant,
    ReadToMemoryMode, ReadToMemoryOptions,
};
use crate::verified_copy::copy_file_verified_with_options_and_lock;
use crate::writer::{
    copy_file_range_syscall, copy_file_with_strategy, copy_file_with_strategy_and_truncate,
    overwrite_changed_chunks_direct, write_buffer, write_file,
};
use crate::{common, config, coreutils};
use iou::IoUring;
use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{symlink, FileExt, MetadataExt, PermissionsExt};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub(super) const FRO_VERSION: &str = env!("CARGO_PKG_VERSION");

mod cli;
mod copy_plan;
mod file_list;
mod help;
mod read_sweep;
mod recursive;
#[cfg(test)]
mod tests;
mod tuning;
mod util;

use self::copy_plan::{CopyRewriteMode, ResolvedCopyExecution};
use self::file_list::*;
pub(crate) use self::help::print_direct_command_help;
use self::help::*;
use self::read_sweep::*;
use self::tuning::*;
use self::util::*;

const PAGE_CACHE_PARAM_INDICES: [usize; 3] = [0, 1, 2];
const DIRECT_PARAM_INDICES: [usize; 3] = [3, 4, 5];
const COPY_RANGE_PARAM_INDICES: [usize; 3] = [6, 7, 8];
const RECURSIVE_COPY_SMALL_FILE_THRESHOLD: u64 = 16 << 20;
const RECURSIVE_COPY_THREADED_LANE_THRESHOLD: u64 = 80 << 20;
const RECURSIVE_COPY_MAX_LARGE_WORKERS: usize = 4;
const RECURSIVE_COPY_SMALL_WORKERS: usize = 32;
const THROUGHPUT_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Clone)]
struct RecursiveCopyContext {
    config: config::LoadedConfig,
    source_root: PathBuf,
    target_root: PathBuf,
    optimizer_params: [u64; 9],
    requested_strategy: CopyStrategy,
    rewrite_mode: CopyRewriteMode,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
    keep_target_size: bool,
    use_lock: bool,
    relative_copy_method: RelativeCopyMethod,
    verbose: bool,
    cp_compat: bool,
    cp_no_clobber: bool,
    preserve_timestamps: bool,
}

#[derive(Clone)]
struct RecursiveDirectoryTask {
    source_dir: PathBuf,
    target_dir: PathBuf,
}

#[derive(Clone)]
struct RecursiveFileTask {
    source_path: PathBuf,
    target_path: PathBuf,
    source_mode: u32,
    source_timestamps: recursive::PreservedTimestamps,
    resolved_copy: ResolvedCopyExecution,
    source_parent_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct RecursiveSmallFileTask {
    source_path: PathBuf,
    target_path: PathBuf,
    source_len: u64,
    source_mode: u32,
    source_timestamps: recursive::PreservedTimestamps,
    preserve_timestamps: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelativeCopyMethod {
    CopyFileRange,
    Sendfile,
}

#[derive(Clone)]
struct RecursiveReadDirectoryTask {
    source_dir: PathBuf,
}

#[derive(Clone)]
struct RecursiveReadFileTask {
    source_path: PathBuf,
}

struct RecursiveDirectoryQueue<T> {
    state: Mutex<RecursiveDirectoryQueueState<T>>,
    ready: Condvar,
}

struct RecursiveDirectoryQueueState<T> {
    queue: VecDeque<T>,
    active_workers: usize,
    waiting_workers: usize,
}

struct RecursiveTaskQueue<T> {
    state: Mutex<RecursiveTaskQueueState<T>>,
    ready: Condvar,
}

struct RecursiveTaskQueueState<T> {
    queue: VecDeque<T>,
    closed: bool,
}

struct RecursiveCopyStats {
    files_copied: AtomicU64,
    dirs_created: AtomicU64,
    symlinks_created: AtomicU64,
    bytes_copied: AtomicU64,
    items_completed: AtomicU64,
}

impl Default for RecursiveCopyStats {
    fn default() -> Self {
        Self {
            files_copied: AtomicU64::new(0),
            dirs_created: AtomicU64::new(0),
            symlinks_created: AtomicU64::new(0),
            bytes_copied: AtomicU64::new(0),
            items_completed: AtomicU64::new(0),
        }
    }
}

#[derive(Default)]
struct RecursiveReadStats {
    files_read: AtomicU64,
    bytes_read: AtomicU64,
}

struct FileListUringInflightRead {
    file: fs::File,
    file_direct: fs::File,
    slot_buffer_index: usize,
}

struct FileListUringSweepResult {
    inflight_per_thread: usize,
    total_bytes: u64,
    total_files: usize,
    elapsed_secs: f64,
}

#[derive(Clone, Copy)]
enum ManifestReadVariant {
    SingleThreadBlocking,
    MultiThreadBlocking { threads: usize },
    SingleThreadUring { qd: usize },
    MultiThreadUring { threads: usize, qd: usize },
}

impl ManifestReadVariant {
    fn label(self) -> String {
        match self {
            ManifestReadVariant::SingleThreadBlocking => "st-blocking".to_string(),
            ManifestReadVariant::MultiThreadBlocking { threads } => {
                format!("mt-blocking-{threads}t")
            }
            ManifestReadVariant::SingleThreadUring { qd } => format!("st-uring-qd{qd}"),
            ManifestReadVariant::MultiThreadUring { threads, qd } => {
                format!("mt-uring-{threads}t-qd{qd}")
            }
        }
    }
}

struct ManifestReadSweepResult {
    prefix_files: usize,
    variant: ManifestReadVariant,
    total_bytes: u64,
    total_files: usize,
    elapsed_secs: f64,
}

#[derive(Clone)]
struct ManifestCopyEntry {
    relative_path: PathBuf,
    size: u64,
    mode: u32,
}

struct ManifestCopyBenchmarkResult {
    entries: usize,
    bytes: u64,
    dirs_created: usize,
    dir_phase_secs: f64,
    file_phase_secs: f64,
    total_secs: f64,
    overlap_secs: Option<f64>,
    overlap_large_file_bytes: Option<u64>,
}

struct ThroughputSampler {
    done: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<io::Result<()>>>,
}

#[derive(Default)]
struct ThroughputSampleCounters {
    bytes: AtomicU64,
    units: AtomicU64,
}

#[cfg(feature = "read-phase-timing")]
#[derive(Default)]
struct RecursiveCopyLaneCounters {
    small_queued: AtomicU64,
    medium_queued: AtomicU64,
    large_queued: AtomicU64,
    small_done: AtomicU64,
    medium_done: AtomicU64,
    large_done: AtomicU64,
}

#[cfg(feature = "read-phase-timing")]
impl RecursiveCopyLaneCounters {
    fn note_queued(&self, lane: &'static str) {
        match lane {
            "small" => {
                self.small_queued.fetch_add(1, Ordering::Relaxed);
            }
            "medium" => {
                self.medium_queued.fetch_add(1, Ordering::Relaxed);
            }
            "large" => {
                self.large_queued.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn note_done(&self, lane: &'static str) {
        match lane {
            "small" => {
                self.small_done.fetch_add(1, Ordering::Relaxed);
            }
            "medium" => {
                self.medium_done.fetch_add(1, Ordering::Relaxed);
            }
            "large" => {
                self.large_done.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn snapshot_line(&self) -> String {
        format!(
            "lane-occupancy small={}/{} medium={}/{} large={}/{}",
            self.small_done.load(Ordering::Relaxed),
            self.small_queued.load(Ordering::Relaxed),
            self.medium_done.load(Ordering::Relaxed),
            self.medium_queued.load(Ordering::Relaxed),
            self.large_done.load(Ordering::Relaxed),
            self.large_queued.load(Ordering::Relaxed),
        )
    }
}

impl ThroughputSampler {
    fn start(
        label: &'static str,
        unit_label: &'static str,
        counters: Arc<ThroughputSampleCounters>,
    ) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let done_thread = done.clone();
        let handle = std::thread::spawn(move || -> io::Result<()> {
            let start = std::time::Instant::now();
            let mut last = start;
            let mut last_bytes = 0_u64;
            let mut last_units = 0_u64;
            loop {
                std::thread::sleep(THROUGHPUT_SAMPLE_INTERVAL);
                let now = std::time::Instant::now();
                let bytes_now = counters.bytes.load(Ordering::Relaxed);
                let units_now = counters.units.load(Ordering::Relaxed);
                let window = now.duration_since(last);
                let total = now.duration_since(start);
                let delta_bytes = bytes_now.saturating_sub(last_bytes);
                let delta_units = units_now.saturating_sub(last_units);
                let should_stop = done_thread.load(Ordering::Relaxed);
                if delta_bytes != 0 || delta_units != 0 || should_stop {
                    let window_secs = window.as_secs_f64().max(1e-9);
                    let total_secs = total.as_secs_f64().max(1e-9);
                    let window_gbps = delta_bytes as f64 / window_secs / 1e9;
                    let avg_gbps = bytes_now as f64 / total_secs / 1e9;
                    let unit_rate = delta_units as f64 / window_secs;
                    let mut stderr = std::io::stderr().lock();
                    writeln!(
                        stderr,
                        "{label} sample t={:.3}s bytes={} {}={} window={:.3} GB/s avg={:.3} GB/s {unit_label}/s={:.1}",
                        total_secs,
                        bytes_now,
                        unit_label,
                        units_now,
                        window_gbps,
                        avg_gbps,
                        unit_rate
                    )?;
                }
                if should_stop {
                    break;
                }
                last = now;
                last_bytes = bytes_now;
                last_units = units_now;
            }
            Ok(())
        });
        Self {
            done,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> io::Result<()> {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| io::Error::other("throughput sampler thread panicked"))??;
        }
        Ok(())
    }
}

impl<T> Default for RecursiveTaskQueue<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(RecursiveTaskQueueState {
                queue: VecDeque::new(),
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }
}

pub(crate) fn resolve_recursive_move_target(
    source_root: &Path,
    target: &Path,
) -> io::Result<PathBuf> {
    recursive::paths::resolve_recursive_copy_root(source_root, target)
}

pub(crate) fn remove_path_recursively(path: &Path, verbose: bool) -> io::Result<u64> {
    recursive::delete::run_recursive_delete(path, verbose)
}

pub(crate) fn move_directory_cross_filesystem(
    source_root: &Path,
    target_root: &Path,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let config = config::load_config(None);
    let target_str = target_root.to_string_lossy();
    let params_page_cache = config.get_params_for_path("copy", false, target_str.as_ref());
    let params_direct = config.get_params_for_path("copy", true, target_str.as_ref());
    let params_copy_range = config.get_copy_range_params_for_path(target_str.as_ref());
    let recursive_ctx = RecursiveCopyContext {
        config,
        source_root: source_root.to_path_buf(),
        target_root: target_root.to_path_buf(),
        optimizer_params: [
            params_page_cache.num_threads,
            params_page_cache.block_size,
            params_page_cache.qd as u64,
            params_direct.num_threads,
            params_direct.block_size,
            params_direct.qd as u64,
            params_copy_range.num_threads,
            params_copy_range.block_size,
            params_copy_range.qd as u64,
        ],
        requested_strategy: CopyStrategy::Auto,
        rewrite_mode: CopyRewriteMode::Auto,
        io_mode_read,
        io_mode_write,
        keep_target_size: false,
        use_lock: true,
        relative_copy_method: RelativeCopyMethod::CopyFileRange,
        verbose,
        cp_compat: false,
        cp_no_clobber: false,
        preserve_timestamps: false,
    };
    recursive::move_dir::run_recursive_move(recursive_ctx, verbose)
}

pub(crate) fn create_tar_archive(source: &Path, output: &Path, verbose: bool) -> io::Result<u64> {
    recursive::archive::create_uncompressed_tar(source, output, verbose)
}

pub(crate) fn list_tar_archive(path: &Path, verbose: bool) -> io::Result<()> {
    recursive::archive::list_uncompressed_tar(path, verbose)
}

pub(crate) fn extract_tar_archive(
    path: &Path,
    destination: Option<&Path>,
    verbose: bool,
) -> io::Result<()> {
    recursive::archive::extract_uncompressed_tar(path, destination, verbose)
}

pub(crate) fn bench_tar_archive(
    variant: &str,
    source: &Path,
    target: Option<&Path>,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
) -> io::Result<u64> {
    recursive::archive::bench_tar_archive_variant(
        variant,
        source,
        target,
        io_mode_read,
        io_mode_write,
    )
}

pub(super) fn main() {
    cli::main();
}
