use std::env;

mod block_hash;
mod common;
mod config;
mod coreutils;
mod differ;
mod io_util;
mod mincore;
mod optimizer;
mod reader;
#[allow(dead_code)]
mod stream;
mod verified_copy;
mod writer;

use block_hash::{
    default_hash_base, hash_file_blocks, hash_file_to_replicas, recover_file_with_copies,
    verify_file_with_replicas, BlockHashAlgorithm, RecoverMode,
};
use common::{CopyAutoMode, CopyStrategy};
use common::{AlignedBuffer, ReadAutoStrategy, ReadPathKind};
use differ::{bench_diff_memory, bench_memcpy_memory, diff_files};
use io_util::{
    direct_writer_supported, open_reader_files, sync_path, validate_read_result, CopyOperationGuard,
};
use iou::IoUring;
use mincore::is_first_page_resident;
use optimizer::run_optimizer;
use reader::{
    benchmark_read_variant, load_file_to_memory, measure_file_load_to_memory,
    prepare_file_load_to_memory, read_file, read_file_auto_with_strategy, resolve_to_memory_mode,
    visit_file_blocks_for_mode, HugepageAdvice, ReadBenchmarkCacheState, ReadBenchmarkVariant,
    ReadToMemoryMode, ReadToMemoryOptions,
};
use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use verified_copy::copy_file_verified_with_options_and_lock;
use writer::{
    copy_file_range_syscall, copy_file_with_strategy, copy_file_with_strategy_and_truncate,
    overwrite_changed_chunks_direct, write_buffer, write_file,
};

const FRO_VERSION: &str = env!("CARGO_PKG_VERSION");

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);
    let num: u64 = num_str.parse().ok()?;
    let mult = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return None,
    };
    num.checked_mul(mult)
}

fn unique_temp_file(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
}

fn format_phase_duration(duration: Option<std::time::Duration>) -> String {
    duration
        .map(|d| format!("{:.3} ms", d.as_secs_f64() * 1e3))
        .unwrap_or_else(|| "-".to_string())
}

fn write_sweep_fixture(path: &Path, size: usize) -> io::Result<()> {
    let mut file = fs::File::create(path)?;
    let mut remaining = size;
    let mut seed = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining > 0 {
        for byte in &mut buffer {
            *byte = ((seed.wrapping_mul(17).wrapping_add(23)) % 251) as u8;
            seed = seed.wrapping_add(1);
        }
        let chunk = remaining.min(buffer.len());
        file.write_all(&buffer[..chunk])?;
        remaining -= chunk;
    }
    file.sync_all()?;
    Ok(())
}

fn format_bytes_compact(size: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("GiB", 1024 * 1024 * 1024),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
        ("B", 1),
    ];
    for (suffix, unit) in UNITS {
        if size >= unit && size % unit == 0 {
            return format!("{}{}", size / unit, suffix);
        }
    }
    format!("{}B", size)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadSweepCacheState {
    Cold,
    Hot,
}

impl ReadSweepCacheState {
    fn label(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Hot => "hot",
        }
    }
}

fn prepare_read_sweep_cache(path: &Path, variant: ReadBenchmarkVariant, cache: ReadSweepCacheState) {
    let path_str = path.to_str().unwrap();
    match (variant, cache) {
        (ReadBenchmarkVariant::SingleThreadDirect, _) => {
            let _ = reader::evict_file_cache(path_str);
        }
        (_, ReadSweepCacheState::Cold) => {
            let _ = reader::evict_file_cache(path_str);
        }
        (_, ReadSweepCacheState::Hot) => {
            let _ = reader::warm_file_page_cache(path_str);
        }
    }
}

#[derive(Clone)]
struct ReadSweepRow {
    cache_state: ReadSweepCacheState,
    size: u64,
    variant: ReadBenchmarkVariant,
    gbps: f64,
    elapsed: f64,
    params: reader::ResolvedReadParams,
    phase_timings: reader::ReadPhaseTimings,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SmallFileThreadCacheState {
    Hot,
    Cold,
}

fn variant_to_path_kind(
    cache_state: ReadSweepCacheState,
    variant: ReadBenchmarkVariant,
) -> ReadPathKind {
    match (cache_state, variant) {
        (_, ReadBenchmarkVariant::SingleThreadPageCache) => ReadPathKind::SimplePageCache,
        (_, ReadBenchmarkVariant::SingleThreadDirect) => ReadPathKind::SimpleDirect,
        (_, ReadBenchmarkVariant::SingleThreadIoUring) => ReadPathKind::IoUringPageCache,
        (_, ReadBenchmarkVariant::QuickProbePageCache) => ReadPathKind::SimplePageCache,
        (ReadSweepCacheState::Hot, ReadBenchmarkVariant::MultiThreadCurrent) => {
            ReadPathKind::ThreadedPageCache
        }
        (ReadSweepCacheState::Cold, ReadBenchmarkVariant::MultiThreadCurrent) => {
            ReadPathKind::ThreadedDirect
        }
    }
}

fn path_kind_label(path: ReadPathKind) -> &'static str {
    match path {
        ReadPathKind::SimplePageCache => "simple-page-cache",
        ReadPathKind::SimpleDirect => "simple-direct",
        ReadPathKind::IoUringPageCache => "io-uring-page-cache",
        ReadPathKind::ThreadedPageCache => "threaded-page-cache",
        ReadPathKind::ThreadedDirect => "threaded-direct",
    }
}

fn infer_cache_strategy(rows: &[ReadSweepRow], cache_state: ReadSweepCacheState) -> (u64, ReadPathKind, ReadPathKind) {
    let mut by_size = std::collections::BTreeMap::<u64, Vec<&ReadSweepRow>>::new();
    for row in rows.iter().filter(|row| row.cache_state == cache_state) {
        by_size.entry(row.size).or_default().push(row);
    }
    let sizes = by_size.keys().copied().collect::<Vec<_>>();
    if sizes.is_empty() {
        return (
            0,
            ReadPathKind::SimplePageCache,
            ReadPathKind::SimplePageCache,
        );
    }

    let variants = [
        ReadPathKind::SimplePageCache,
        ReadPathKind::SimpleDirect,
        ReadPathKind::IoUringPageCache,
        match cache_state {
            ReadSweepCacheState::Hot => ReadPathKind::ThreadedPageCache,
            ReadSweepCacheState::Cold => ReadPathKind::ThreadedDirect,
        },
    ];

    let avg_for = |segment: &[u64], path_kind: ReadPathKind| -> f64 {
        if segment.is_empty() {
            return f64::NEG_INFINITY;
        }
        let mut total = 0.0;
        for size in segment {
            let row = by_size[size]
                .iter()
                .find(|row| variant_to_path_kind(cache_state, row.variant) == path_kind)
                .expect("path kind row should exist for each sweep size");
            total += row.gbps;
        }
        total / segment.len() as f64
    };

    let mut best_score = f64::NEG_INFINITY;
    let mut best = (
        sizes[0],
        variant_to_path_kind(cache_state, by_size[&sizes[0]][0].variant),
        variant_to_path_kind(
            cache_state,
            by_size[sizes.last().expect("sizes non-empty")][0].variant,
        ),
    );

    for split in 0..=sizes.len() {
        let small_sizes = &sizes[..split];
        let large_sizes = &sizes[split..];
        let small_path = variants
            .iter()
            .copied()
            .max_by(|a, b| avg_for(small_sizes, *a).partial_cmp(&avg_for(small_sizes, *b)).unwrap())
            .unwrap_or(variants[0]);
        let large_path = variants
            .iter()
            .copied()
            .max_by(|a, b| avg_for(large_sizes, *a).partial_cmp(&avg_for(large_sizes, *b)).unwrap())
            .unwrap_or(variants[0]);
        let mut score = 0.0;
        for size in small_sizes {
            score += avg_for(&[*size], small_path);
        }
        for size in large_sizes {
            score += avg_for(&[*size], large_path);
        }
        if score > best_score {
            let cutoff = if split >= sizes.len() {
                *sizes.last().unwrap()
            } else {
                sizes[split]
            };
            best_score = score;
            best = (cutoff, small_path, large_path);
        }
    }

    best
}

fn print_read_sweep_summary(rows: &[ReadSweepRow]) {
    println!();
    println!("summary\tcache\tsize\tfastest-variant\tfastest-path\tgbps");
    for cache_state in [ReadSweepCacheState::Cold, ReadSweepCacheState::Hot] {
        let mut sizes = rows
            .iter()
            .filter(|row| row.cache_state == cache_state)
            .map(|row| row.size)
            .collect::<Vec<_>>();
        sizes.sort_unstable();
        sizes.dedup();
        for size in sizes {
            if let Some(best) = rows
                .iter()
                .filter(|row| row.cache_state == cache_state && row.size == size)
                .max_by(|a, b| a.gbps.partial_cmp(&b.gbps).unwrap())
            {
                println!(
                    "summary\t{}\t{}\t{}\t{}\t{:.6}",
                    cache_state.label(),
                    size,
                    best.variant.label(),
                    path_kind_label(variant_to_path_kind(cache_state, best.variant)),
                    best.gbps
                );
            }
        }
    }
}

fn print_read_sweep_strategy(strategy: ReadAutoStrategy) {
    println!();
    println!("strategy\tstate\tsmall-path\tlarge-path\tcutoff-bytes");
    println!(
        "strategy\thot\t{}\t{}\t{}",
        path_kind_label(strategy.hot_small_path),
        path_kind_label(strategy.hot_large_path),
        strategy.hot_large_min_bytes
    );
    println!(
        "strategy\tcold\t{}\t{}\t{}",
        path_kind_label(strategy.cold_small_path),
        path_kind_label(strategy.cold_large_path),
        strategy.cold_large_min_bytes
    );
}

fn zfs_direct_read_strategy() -> ReadAutoStrategy {
    ReadAutoStrategy {
        hot_large_min_bytes: 1,
        cold_large_min_bytes: 1,
        hot_small_path: ReadPathKind::SimpleDirect,
        hot_large_path: ReadPathKind::SimpleDirect,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::SimpleDirect,
    }
}

fn print_read_sweep_table(rows: &[ReadSweepRow]) {
    let mut rendered = vec![vec![
        "cache".to_string(),
        "size".to_string(),
        "variant".to_string(),
        "bytes".to_string(),
        "elapsed_s".to_string(),
        "gbps".to_string(),
        "threads".to_string(),
        "block".to_string(),
        "qd".to_string(),
        "direct".to_string(),
    ]];
    for row in rows {
        rendered.push(vec![
            row.cache_state.label().to_string(),
            format_bytes_compact(row.size),
            row.variant.label().to_string(),
            row.size.to_string(),
            format!("{:.6}", row.elapsed),
            format!("{:.6}", row.gbps),
            row.params.num_threads.to_string(),
            format_bytes_compact(row.params.block_size),
            row.params.qd.to_string(),
            row.params.use_direct.to_string(),
        ]);
    }
    let widths = (0..rendered[0].len())
        .map(|col| rendered.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    for row in rendered {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

fn run_bench_read_sweep(config: &mut config::LoadedConfig) -> io::Result<()> {
    let sizes = [
        4 * 1024_u64,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
        16 * 1024 * 1024,
        32 * 1024 * 1024,
        64 * 1024 * 1024,
        80 * 1024 * 1024,
        256 * 1024 * 1024,
    ];
    let variants = [
        ReadBenchmarkVariant::SingleThreadPageCache,
        ReadBenchmarkVariant::SingleThreadDirect,
        ReadBenchmarkVariant::SingleThreadIoUring,
        ReadBenchmarkVariant::QuickProbePageCache,
        ReadBenchmarkVariant::MultiThreadCurrent,
    ];
    let cache_states = [ReadSweepCacheState::Cold, ReadSweepCacheState::Hot];
    let page_cache = config.get_params("read", false);
    let direct = config.get_params("read", true);
    let strategy_path = std::env::temp_dir().to_string_lossy().to_string();
    let mount_info = config.mount_info_for_path(&strategy_path);
    let strategy = if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
        zfs_direct_read_strategy()
    } else {
        config.get_read_auto_strategy()
    };

    let path = unique_temp_file("fro-read-sweep");
    let mut created = false;
    let mut previous_size = 0_u64;
    let mut rows = Vec::new();
    let result = (|| -> io::Result<()> {
        for size in sizes {
            if !created || size != previous_size {
                write_sweep_fixture(&path, size as usize)?;
                created = true;
                previous_size = size;
            }
            for cache_state in cache_states {
                for variant in variants {
                    prepare_read_sweep_cache(&path, variant, cache_state);
                    let result = benchmark_read_variant(
                        path.to_str().unwrap(),
                        variant,
                        match cache_state {
                            ReadSweepCacheState::Cold => ReadBenchmarkCacheState::Cold,
                            ReadSweepCacheState::Hot => ReadBenchmarkCacheState::Hot,
                        },
                        strategy,
                        mount_info.as_ref(),
                        page_cache.clone(),
                        direct.clone(),
                    )?;
                    let elapsed = result.elapsed.as_secs_f64();
                    let gbps = if elapsed > 0.0 {
                        result.bytes_read as f64 / elapsed / 1e9
                    } else {
                        0.0
                    };
                    rows.push(ReadSweepRow {
                        cache_state,
                        size,
                        variant,
                        gbps,
                        elapsed,
                        params: result.params,
                        phase_timings: result.phase_timings,
                    });
                    println!(
                        "result\t{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}",
                        cache_state.label(),
                        size,
                        variant.label(),
                        elapsed,
                        gbps,
                        result.params.num_threads,
                        result.params.block_size,
                        result.params.qd,
                        result.params.use_direct
                    );
                }
            }
        }
        rows.sort_by(|a, b| {
            a.cache_state
                .label()
                .cmp(b.cache_state.label())
                .then(a.size.cmp(&b.size))
                .then(a.variant.label().cmp(b.variant.label()))
        });
        println!();
        print_read_sweep_table(&rows);
        if rows.iter().any(|row| row.phase_timings.enabled()) {
            println!();
            println!(
                "phases\tcache\tsize\tvariant\tthreads-created\tfirst-submit\tfirst-completion\twrapup-start\tjoin-done"
            );
            for row in rows.iter().filter(|row| row.phase_timings.enabled()) {
                let timings = row.phase_timings;
                println!(
                    "phases\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    row.cache_state.label(),
                    row.size,
                    row.variant.label(),
                    format_phase_duration(timings.call_to_threads_created),
                    format_phase_duration(timings.call_to_first_submit),
                    format_phase_duration(timings.call_to_first_completion),
                    format_phase_duration(timings.call_to_wrapup_start),
                    format_phase_duration(timings.call_to_join_done),
                );
            }
        }
        print_read_sweep_summary(&rows);
        let (hot_cutoff, hot_small, hot_large) = infer_cache_strategy(&rows, ReadSweepCacheState::Hot);
        let (cold_cutoff, cold_small, cold_large) =
            infer_cache_strategy(&rows, ReadSweepCacheState::Cold);
        let strategy = ReadAutoStrategy {
            hot_large_min_bytes: hot_cutoff,
            cold_large_min_bytes: cold_cutoff,
            hot_small_path: hot_small,
            hot_large_path: hot_large,
            cold_small_path: cold_small,
            cold_large_path: cold_large,
        };
        let strategy = if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
            zfs_direct_read_strategy()
        } else {
            strategy
        };
        print_read_sweep_strategy(strategy);
        config.update_read_auto_strategy_for_path(&strategy_path, strategy);
        config.save();
        Ok(())
    })();
    let _ = fs::remove_file(&path);
    result
}

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
    resolved_copy: ResolvedCopyExecution,
}

#[derive(Clone)]
struct RecursiveSmallFileTask {
    source_path: PathBuf,
    target_path: PathBuf,
    source_len: u64,
    source_mode: u32,
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
            ManifestReadVariant::MultiThreadBlocking { threads } => format!("mt-blocking-{threads}t"),
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

fn collect_recursive_copy_manifest(
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<(Vec<RecursiveSmallFileTask>, Vec<RecursiveFileTask>)> {
    let mut stack = vec![RecursiveDirectoryTask {
        source_dir: ctx.source_root.clone(),
        target_dir: ctx.target_root.clone(),
    }];
    let mut small_tasks = Vec::new();
    let mut large_tasks = Vec::new();
    while let Some(task) = stack.pop() {
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&task.source_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let source_path = entry.path();
            let target_path = task.target_dir.join(entry.file_name());
            if file_type.is_dir() {
                let metadata = entry.metadata()?;
                let mode = metadata.permissions().mode();
                create_directory_like(mode, &target_path, stats, Some(sample_counters))?;
                child_dirs.push(RecursiveDirectoryTask {
                    source_dir: source_path,
                    target_dir: target_path,
                });
                continue;
            }
            if file_type.is_symlink() {
                copy_symlink_entry(&source_path, &target_path, stats)?;
                continue;
            }
            if !file_type.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "recursive copy only supports regular files, directories, and symlinks (saw {})",
                        source_path.display()
                    ),
                ));
            }
            let metadata = entry.metadata()?;
            let source_len = metadata.len();
            let source_mode = metadata.permissions().mode();
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                small_tasks.push(RecursiveSmallFileTask {
                    source_path,
                    target_path,
                    source_len,
                    source_mode,
                });
            } else {
                let resolved_copy =
                    resolve_recursive_large_copy_execution(ctx, &source_path, &target_path)?;
                large_tasks.push(RecursiveFileTask {
                    source_path,
                    target_path,
                    source_mode,
                    resolved_copy,
                });
            }
        }
        child_dirs.reverse();
        stack.extend(child_dirs);
    }
    Ok((small_tasks, large_tasks))
}

fn load_paths_from_manifest(path: &Path) -> io::Result<Vec<PathBuf>> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    let mut paths = Vec::new();
    loop {
        line.clear();
        let read = std::io::BufRead::read_line(&mut reader, &mut line)?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        paths.push(PathBuf::from(trimmed));
    }
    Ok(paths)
}

fn load_manifest_copy_entries(manifest: &Path, source_root: &Path) -> io::Result<Vec<ManifestCopyEntry>> {
    let paths = load_paths_from_manifest(manifest)?;
    let mut entries = Vec::with_capacity(paths.len());
    for source_path in paths {
        let metadata = fs::symlink_metadata(&source_path)?;
        if !metadata.file_type().is_file() {
            continue;
        }
        let relative_path = source_path.strip_prefix(source_root).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "manifest path {} is not under source root {}",
                    source_path.display(),
                    source_root.display()
                ),
            )
        })?;
        let relative_path = relative_path.to_path_buf();
        entries.push(ManifestCopyEntry {
            relative_path,
            size: metadata.len(),
            mode: metadata.permissions().mode(),
        });
    }
    Ok(entries)
}

fn create_manifest_target_dirs(
    entries: &[ManifestCopyEntry],
    target_root: &Path,
) -> io::Result<(usize, std::time::Duration)> {
    let start = std::time::Instant::now();
    let mut dirs = std::collections::BTreeSet::<PathBuf>::new();
    for entry in entries {
        let mut current = PathBuf::new();
        if let Some(parent) = entry.relative_path.parent() {
            for component in parent.components() {
                current.push(component.as_os_str());
                dirs.insert(current.clone());
            }
        }
    }
    for dir in &dirs {
        fs::create_dir_all(target_root.join(dir))?;
    }
    Ok((dirs.len(), start.elapsed()))
}

fn open_dir_fd(path: &Path) -> io::Result<fs::File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })?;
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

fn open_relative_fd(dir: &fs::File, relative: &Path, flags: i32, mode: libc::mode_t) -> io::Result<fs::File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_rel = CString::new(relative.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", relative.display()),
        )
    })?;
    let fd = unsafe { libc::openat(dir.as_raw_fd(), c_rel.as_ptr(), flags | libc::O_CLOEXEC, mode) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

fn open_relative_target_for_copy(
    dir: &fs::File,
    relative: &Path,
    mode: libc::mode_t,
) -> io::Result<(fs::File, bool)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_rel = CString::new(relative.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", relative.display()),
        )
    })?;
    let create_flags = libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC;
    let fd = unsafe { libc::openat(dir.as_raw_fd(), c_rel.as_ptr(), create_flags, mode) };
    if fd >= 0 {
        return Ok((unsafe { fs::File::from_raw_fd(fd) }, true));
    }
    let err = io::Error::last_os_error();
    if err.kind() != io::ErrorKind::AlreadyExists {
        return Err(err);
    }
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            c_rel.as_ptr(),
            libc::O_WRONLY | libc::O_TRUNC | libc::O_CLOEXEC,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((unsafe { fs::File::from_raw_fd(fd) }, false))
}

const FAST_COPY_SENDFILE_CHUNK_SIZE: usize = 0x7fff_f000usize;

fn copy_openat_via_sendfile(
    relative_path: &Path,
    source_len: u64,
    source: &fs::File,
    target: &fs::File,
) -> io::Result<u64> {
    let mut copied_total = 0_u64;
    let mut source_pos: libc::off_t = 0;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(FAST_COPY_SENDFILE_CHUNK_SIZE as u64) as usize;
        let copied = unsafe {
            libc::sendfile(
                target.as_raw_fd(),
                source.as_raw_fd(),
                &mut source_pos,
                chunk,
            )
        };
        if copied > 0 {
            copied_total = copied_total.saturating_add(copied as u64);
            continue;
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "sendfile/openat stopped early after {} of {} bytes for {}",
                    copied_total,
                    source_len,
                    relative_path.display()
                ),
            ));
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(io::Error::new(
            err.kind(),
            format!("sendfile/openat failed for {}: {}", relative_path.display(), err),
        ));
    }
    Ok(copied_total)
}

fn copy_openat_via_copy_file_range(
    relative_path: &Path,
    source_len: u64,
    source: &fs::File,
    target: &fs::File,
) -> io::Result<u64> {
    let mut source_pos: libc::loff_t = 0;
    let mut target_pos: libc::loff_t = 0;
    let mut copied_total = 0_u64;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(usize::MAX as u64) as usize;
        let copied = unsafe {
            libc::copy_file_range(
                source.as_raw_fd(),
                &mut source_pos,
                target.as_raw_fd(),
                &mut target_pos,
                chunk,
                0,
            )
        };
        if copied < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "copy_file_range/openat failed for {}: {}",
                    relative_path.display(),
                    err
                ),
            ));
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "copy_file_range/openat stopped early after {} of {} bytes for {}",
                    copied_total,
                    source_len,
                    relative_path.display()
                ),
            ));
        }
        copied_total = copied_total.saturating_add(copied as u64);
    }
    Ok(copied_total)
}

fn copy_small_file_openat(
    entry: &ManifestCopyEntry,
    source_root_fd: &fs::File,
    target_root_fd: &fs::File,
    method: RelativeCopyMethod,
) -> io::Result<u64> {
    let source = open_relative_fd(source_root_fd, &entry.relative_path, libc::O_RDONLY, 0)?;
    let (target, created) =
        open_relative_target_for_copy(target_root_fd, &entry.relative_path, entry.mode as libc::mode_t)?;
    let copied_total = match method {
        RelativeCopyMethod::CopyFileRange => {
            copy_openat_via_copy_file_range(&entry.relative_path, entry.size, &source, &target)?
        }
        RelativeCopyMethod::Sendfile => {
            copy_openat_via_sendfile(&entry.relative_path, entry.size, &source, &target)?
        }
    };
    if !created {
        let rc = unsafe { libc::fchmod(target.as_raw_fd(), entry.mode as libc::mode_t) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(copied_total)
}

fn copy_relative_file_openat(
    relative_path: &Path,
    source_len: u64,
    source_mode: u32,
    source_root_fd: &fs::File,
    target_root_fd: &fs::File,
    method: RelativeCopyMethod,
) -> io::Result<u64> {
    let source = open_relative_fd(source_root_fd, relative_path, libc::O_RDONLY, 0)?;
    let (target, created) =
        open_relative_target_for_copy(target_root_fd, relative_path, source_mode as libc::mode_t)?;
    let copied_total = match method {
        RelativeCopyMethod::CopyFileRange => {
            copy_openat_via_copy_file_range(relative_path, source_len, &source, &target)?
        }
        RelativeCopyMethod::Sendfile => {
            copy_openat_via_sendfile(relative_path, source_len, &source, &target)?
        }
    };
    if !created {
        let rc = unsafe { libc::fchmod(target.as_raw_fd(), source_mode as libc::mode_t) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(copied_total)
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ManualReadOverrides {
    threads: Option<u64>,
    block_size: Option<u64>,
    qd: Option<usize>,
}

impl ManualReadOverrides {
    fn any(self) -> bool {
        self.threads.is_some() || self.block_size.is_some() || self.qd.is_some()
    }
}

fn mark_optimizer_params(mask: &mut [bool], indices: &[usize], include_block_size: bool) {
    for &index in indices {
        if include_block_size || index % 3 != 1 {
            mask[index] = true;
        }
    }
}

fn active_optimizer_param_mask(
    mode: &str,
    io_mode: common::IOMode,
    io_mode_write: common::IOMode,
    via_memory: bool,
    copy_strategy: CopyStrategy,
) -> Vec<bool> {
    let mut mask = [false; 9];
    match mode {
        "read" | "grep" | "hash" | "diff" | "dual-read-bench" | "recursive-read-bench"
        | "file-list-read-bench" | "file-list-read-uring-bench"
        | "file-list-read-open-read-close-sweep"
        | "bench-recursive-small-file-threads" => {
            match io_mode {
                common::IOMode::Direct => {
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
                common::IOMode::PageCache => {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                }
                common::IOMode::Auto => {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
            }
        }
        "verify" | "recover" => match io_mode {
            common::IOMode::Direct => {
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, false);
            }
            common::IOMode::PageCache => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, false);
            }
            common::IOMode::Auto => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, false);
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, false);
            }
        },
        "write" => match io_mode_write {
            common::IOMode::PageCache => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
            }
            common::IOMode::Direct | common::IOMode::Auto => {
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
            }
        },
        "copy" => match copy_strategy {
            CopyStrategy::CopyFileRange => {
                mark_optimizer_params(&mut mask, &COPY_RANGE_PARAM_INDICES, true);
            }
            CopyStrategy::CopyFileRangeSingle | CopyStrategy::Reflink => {}
            CopyStrategy::Auto => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                mark_optimizer_params(&mut mask, &COPY_RANGE_PARAM_INDICES, true);
            }
            CopyStrategy::Threaded => {
                if io_mode_write == common::IOMode::PageCache
                    || io_mode == common::IOMode::PageCache
                {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                }
                if !(io_mode_write == common::IOMode::PageCache
                    && io_mode == common::IOMode::PageCache)
                {
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
            }
        },
        _ => mask.fill(true),
    }
    if mode == "copy"
        && (via_memory
            || matches!(
                copy_strategy,
                CopyStrategy::CopyFileRangeSingle | CopyStrategy::Reflink
            ))
    {
        mask.fill(false);
    }
    mask.to_vec()
}

fn freeze_read_override(
    start_params: &mut [u64],
    params_steps: &mut [u64],
    optimizer_mask: &mut [bool],
    indices: [usize; 2],
    value: u64,
) {
    for index in indices {
        start_params[index] = value;
        params_steps[index] = 1;
        optimizer_mask[index] = false;
    }
}

fn apply_manual_read_overrides(
    start_params: &mut [u64],
    params_steps: &mut [u64],
    optimizer_mask: &mut [bool],
    overrides: ManualReadOverrides,
) {
    if let Some(threads) = overrides.threads {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[0], DIRECT_PARAM_INDICES[0]],
            threads,
        );
    }
    if let Some(block_size) = overrides.block_size {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[1], DIRECT_PARAM_INDICES[1]],
            block_size,
        );
    }
    if let Some(qd) = overrides.qd {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[2], DIRECT_PARAM_INDICES[2]],
            qd as u64,
        );
    }
}

#[derive(Clone, Copy)]
struct ResolvedCopyExecution {
    copy_strategy: CopyStrategy,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
    diff_overwrite: bool,
    full_rewrite: bool,
    path_label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeuristicCopyPlan {
    DiffOverwrite,
    CachedReadDirectWrite,
    DirectReadDirectWrite,
    CopyFileRangeSingle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageRedundancy {
    Redundant,
    NonRedundant,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountInfoBrief {
    mount_point: String,
    fstype: String,
    mount_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ZpoolLeafState {
    state: Option<String>,
    vdev_path: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CopyRewriteMode {
    Auto,
    Diff,
    Full,
}

fn resolve_copy_execution(
    config: &config::LoadedConfig,
    source_path: &str,
    path: &str,
    requested_strategy: CopyStrategy,
    rewrite_mode: CopyRewriteMode,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
) -> io::Result<ResolvedCopyExecution> {
    if rewrite_mode == CopyRewriteMode::Diff {
        return Ok(if direct_writer_supported(path)? {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::Direct,
                diff_overwrite: true,
                full_rewrite: false,
                path_label: "forced diff-overwrite",
            }
        } else {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::CopyFileRangeSingle,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::PageCache,
                diff_overwrite: false,
                full_rewrite: false,
                path_label: "forced diff copy_file_range single fallback",
            }
        });
    }

    if requested_strategy != CopyStrategy::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: requested_strategy,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: match requested_strategy {
                CopyStrategy::Auto => "auto",
                CopyStrategy::Threaded => "threaded",
                CopyStrategy::CopyFileRange => "copy_file_range",
                CopyStrategy::CopyFileRangeSingle => "copy_file_range single",
                CopyStrategy::Reflink => "reflink",
            },
        });
    }

    if io_mode_read != common::IOMode::Auto || io_mode_write != common::IOMode::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: "explicit io-mode threaded",
        });
    }

    let resolved = match config.get_copy_auto_mode_for_path(path) {
        CopyAutoMode::PageCache => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config page-cache threaded",
        },
        CopyAutoMode::Direct => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::Direct,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config direct threaded",
        },
        CopyAutoMode::CopyFileRange => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::CopyFileRange,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config copy_file_range",
        },
        CopyAutoMode::Heuristic => {
            let (source_cached, target_cached, source_len, target_len) =
                inspect_copy_auto_state(source_path, path);
            let (redundancy, reflink_possible) = detect_copy_storage_policy(source_path, path);
            if rewrite_mode == CopyRewriteMode::Auto
                && redundancy == StorageRedundancy::Redundant
                && reflink_possible
            {
                return Ok(ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Reflink,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto redundant reflink",
                });
            }
            if redundancy == StorageRedundancy::NonRedundant {
                return Ok(choose_nonredundant_full_copy_plan(
                    source_cached,
                    source_len,
                    target_len,
                ));
            }
            let plan = if rewrite_mode != CopyRewriteMode::Full
                && should_prefer_cached_diff_overwrite(
                    source_cached,
                    target_cached,
                    source_len,
                    target_len,
                ) {
                if direct_writer_supported(path)? {
                    HeuristicCopyPlan::DiffOverwrite
                } else {
                    HeuristicCopyPlan::CopyFileRangeSingle
                }
            } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len)
            {
                HeuristicCopyPlan::CachedReadDirectWrite
            } else {
                HeuristicCopyPlan::DirectReadDirectWrite
            };

            match plan {
                HeuristicCopyPlan::DiffOverwrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: true,
                    full_rewrite: false,
                    path_label: "auto diff-overwrite",
                },
                HeuristicCopyPlan::CachedReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto cached-read direct-write",
                },
                HeuristicCopyPlan::DirectReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::Direct,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto direct threaded",
                },
                HeuristicCopyPlan::CopyFileRangeSingle => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::CopyFileRangeSingle,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto copy_file_range single fallback",
                },
            }
        }
    };

    Ok(resolved)
}

fn inspect_copy_auto_state(
    source_path: &str,
    target_path: &str,
) -> (bool, bool, Option<u64>, Option<u64>) {
    let source_cached = Ok(true) == is_first_page_resident(source_path);
    let target_cached = Ok(true) == is_first_page_resident(target_path);
    let source_len = std::fs::metadata(source_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    let target_len = std::fs::metadata(target_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    (source_cached, target_cached, source_len, target_len)
}

fn target_is_similar_size(source_len: Option<u64>, target_len: Option<u64>) -> bool {
    const TARGET_SIZE_THRESHOLD_PERCENT: u64 = 70;

    let (Some(source_len), Some(target_len)) = (source_len, target_len) else {
        return false;
    };

    if source_len == 0 {
        return target_len == 0;
    }

    target_len.saturating_mul(100).saturating_div(source_len) >= TARGET_SIZE_THRESHOLD_PERCENT
}

#[cfg(kani)]
mod kani_proofs {
    use super::target_is_similar_size;

    #[kani::proof]
    fn zero_length_source_is_only_similar_to_zero_length_target() {
        let target_len: Option<u64> = kani::any();
        let expected = matches!(target_len, Some(0));
        assert_eq!(target_is_similar_size(Some(0), target_len), expected);
    }

    #[kani::proof]
    fn missing_sizes_are_never_similar() {
        let source_len: Option<u64> = kani::any();
        let target_len: Option<u64> = kani::any();
        kani::assume(source_len.is_none() || target_len.is_none());
        assert!(!target_is_similar_size(source_len, target_len));
    }
}

fn should_prefer_cached_diff_overwrite(
    source_cached: bool,
    target_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_cached && target_is_similar_size(source_len, target_len)
}

fn should_prefer_cached_read_direct_write(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_is_similar_size(source_len, target_len)
}

fn path_starts_with_mount(path: &str, mount_point: &str) -> bool {
    if mount_point == "/" {
        return path.starts_with('/');
    }
    if path == mount_point {
        return true;
    }
    path.strip_prefix(mount_point)
        .is_some_and(|rest| rest.starts_with('/'))
}

fn path_for_mount_lookup(path: &Path) -> Option<String> {
    let absolute = if path.exists() {
        fs::canonicalize(path).ok()?
    } else if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    Some(absolute.to_string_lossy().into_owned())
}

fn mount_info_for_path(path: &Path) -> Option<MountInfoBrief> {
    let path = path_for_mount_lookup(path)?;
    let data = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<MountInfoBrief> = None;
    let mut best_len = 0usize;
    for line in data.lines() {
        let (lhs, rhs) = line.split_once(" - ")?;
        let left_fields: Vec<&str> = lhs.split_whitespace().collect();
        if left_fields.len() < 5 {
            continue;
        }
        let mount_point = left_fields[4];
        if !path_starts_with_mount(&path, mount_point) {
            continue;
        }
        let right_fields: Vec<&str> = rhs.split_whitespace().collect();
        if right_fields.len() < 2 {
            continue;
        }
        if mount_point.len() > best_len {
            best_len = mount_point.len();
            best = Some(MountInfoBrief {
                mount_point: mount_point.to_string(),
                fstype: right_fields[0].to_string(),
                mount_source: right_fields[1].to_string(),
            });
        }
    }
    best
}

fn filesystem_supports_reflink(fstype: &str) -> bool {
    matches!(fstype, "btrfs" | "xfs" | "ocfs2" | "bcachefs")
}

fn base_block_name_from_devpath(devpath: &Path) -> Option<String> {
    let canon = fs::canonicalize(devpath).ok()?;
    let name = canon.file_name()?.to_string_lossy().to_string();
    let sys = Path::new("/sys/class/block").join(&name);
    if sys.join("partition").exists() {
        let real = fs::read_link(&sys).ok()?;
        let real_abs = if real.is_absolute() {
            real
        } else {
            Path::new("/sys/class/block").join(real)
        };
        let parent = real_abs.parent()?;
        return Some(parent.file_name()?.to_string_lossy().to_string());
    }
    Some(name)
}

fn read_sysfs_trimmed(path: &Path) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn md_storage_redundancy(base_block: &str) -> StorageRedundancy {
    let md = Path::new("/sys/class/block").join(base_block).join("md");
    let Some(level) = read_sysfs_trimmed(&md.join("level")) else {
        return StorageRedundancy::Unknown;
    };
    let degraded = read_sysfs_trimmed(&md.join("degraded"))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let redundant = match level.as_str() {
        "raid0" | "linear" => return StorageRedundancy::NonRedundant,
        "raid1" | "raid4" | "raid5" | "raid6" | "raid10" => true,
        _ => false,
    };
    if !redundant {
        return StorageRedundancy::Unknown;
    }
    if degraded == 0 {
        StorageRedundancy::Redundant
    } else {
        StorageRedundancy::NonRedundant
    }
}

fn is_zpool_group_name(name: &str) -> bool {
    name.starts_with("mirror-")
        || name.starts_with("raidz")
        || name == "logs"
        || name == "log"
        || name == "cache"
        || name == "spares"
        || name.starts_with("spare-")
        || name == "special"
        || name.starts_with("replacing")
}

fn parse_zpool_status_leaves(out: &str) -> Vec<ZpoolLeafState> {
    let mut in_config = false;
    let mut in_table = false;
    let mut saw_pool = false;
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut leaves = Vec::new();

    for line in out.lines() {
        let trimmed = line.trim();
        if trimmed == "config:" {
            in_config = true;
            continue;
        }
        if !in_config {
            continue;
        }
        if trimmed.starts_with("errors:") {
            break;
        }
        if trimmed.starts_with("NAME") && trimmed.contains("STATE") {
            in_table = true;
            continue;
        }
        if !in_table || trimmed.is_empty() {
            continue;
        }

        let indent = line.chars().take_while(|c| c.is_whitespace()).count();
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next().map(str::to_string) else {
            continue;
        };
        let state = parts.next().map(str::to_string);

        while let Some((last_indent, _)) = stack.last() {
            if *last_indent >= indent {
                stack.pop();
            } else {
                break;
            }
        }

        if !saw_pool {
            saw_pool = true;
            stack.push((indent, name));
            continue;
        }

        if is_zpool_group_name(&name) {
            stack.push((indent, name));
            continue;
        }

        leaves.push(ZpoolLeafState {
            state,
            vdev_path: stack.iter().skip(1).map(|(_, name)| name.clone()).collect(),
        });
    }

    leaves
}

fn zfs_storage_redundancy_from_status(out: &str) -> StorageRedundancy {
    let leaves = parse_zpool_status_leaves(out);
    if leaves.is_empty() {
        return StorageRedundancy::Unknown;
    }
    let has_mirror = leaves.iter().any(|leaf| {
        leaf.vdev_path
            .iter()
            .any(|name| name.starts_with("mirror-"))
    });
    if has_mirror {
        return if leaves.iter().any(|leaf| {
            leaf.vdev_path
                .iter()
                .any(|name| name.starts_with("mirror-"))
                && leaf.state.as_deref() != Some("ONLINE")
        }) {
            StorageRedundancy::NonRedundant
        } else {
            StorageRedundancy::Redundant
        };
    }

    let has_raidz = leaves
        .iter()
        .any(|leaf| leaf.vdev_path.iter().any(|name| name.starts_with("raidz")));
    if has_raidz {
        return if leaves.iter().any(|leaf| {
            leaf.vdev_path.iter().any(|name| name.starts_with("raidz"))
                && leaf.state.as_deref() != Some("ONLINE")
        }) {
            StorageRedundancy::NonRedundant
        } else {
            StorageRedundancy::Redundant
        };
    }

    StorageRedundancy::NonRedundant
}

fn zfs_storage_redundancy(dataset: &str) -> StorageRedundancy {
    let pool = dataset.split('/').next().unwrap_or(dataset);
    let output = Command::new("zpool")
        .args(["status", "-P", pool])
        .output()
        .ok();
    let Some(output) = output else {
        return StorageRedundancy::Unknown;
    };
    if !output.status.success() {
        return StorageRedundancy::Unknown;
    }
    zfs_storage_redundancy_from_status(&String::from_utf8_lossy(&output.stdout))
}

fn mount_storage_redundancy(info: &MountInfoBrief) -> StorageRedundancy {
    if info.fstype == "zfs" {
        return zfs_storage_redundancy(&info.mount_source);
    }
    if info.mount_source.starts_with("/dev/") {
        if let Some(base) = base_block_name_from_devpath(Path::new(&info.mount_source)) {
            if base.starts_with("md") {
                return md_storage_redundancy(&base);
            }
        }
    }
    StorageRedundancy::Unknown
}

fn detect_copy_storage_policy(source_path: &str, target_path: &str) -> (StorageRedundancy, bool) {
    let source_mount = mount_info_for_path(Path::new(source_path));
    let target_mount = mount_info_for_path(Path::new(target_path));
    let redundancy = target_mount
        .as_ref()
        .map(mount_storage_redundancy)
        .unwrap_or(StorageRedundancy::Unknown);
    let reflink_possible = match (source_mount.as_ref(), target_mount.as_ref()) {
        (Some(source), Some(target))
            if source.mount_point == target.mount_point
                && source.fstype == target.fstype
                && filesystem_supports_reflink(&target.fstype) =>
        {
            true
        }
        _ => false,
    };
    (redundancy, reflink_possible)
}

fn choose_nonredundant_full_copy_plan(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> ResolvedCopyExecution {
    if should_prefer_cached_read_direct_write(source_cached, source_len, target_len) {
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: true,
            path_label: "auto nonredundant cached-read direct-write",
        }
    } else {
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::Direct,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: true,
            path_label: "auto nonredundant direct threaded",
        }
    }
}

fn io_mode_label(io_mode: common::IOMode) -> &'static str {
    match io_mode {
        common::IOMode::Auto => "auto",
        common::IOMode::PageCache => "page-cache",
        common::IOMode::Direct => "direct",
    }
}

fn describe_copy_path(
    resolved_copy: ResolvedCopyExecution,
    via_memory: bool,
    keep_target_size: bool,
) -> String {
    if via_memory {
        return format!(
            "copy path: via-memory [read={}, write={}]",
            io_mode_label(resolved_copy.io_mode_read),
            io_mode_label(resolved_copy.io_mode_write)
        );
    }

    let mut details = vec![
        format!("strategy={}", resolved_copy.path_label),
        format!("read={}", io_mode_label(resolved_copy.io_mode_read)),
        format!("write={}", io_mode_label(resolved_copy.io_mode_write)),
    ];
    if resolved_copy.full_rewrite {
        details.push("rewrite=full".to_string());
    }
    if resolved_copy.diff_overwrite && !keep_target_size {
        details.push("delta=changed-chunks".to_string());
    }
    if keep_target_size {
        details.push("target=keep-size".to_string());
    }
    format!("copy path: {}", details.join(", "))
}

impl<T> Default for RecursiveDirectoryQueue<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(RecursiveDirectoryQueueState {
                queue: VecDeque::new(),
                active_workers: 0,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<T> RecursiveDirectoryQueue<T> {
    fn enqueue(&self, tasks: impl IntoIterator<Item = T>) {
        let mut state = self.state.lock().unwrap();
        let mut added = false;
        for task in tasks {
            state.queue.push_back(task);
            added = true;
        }
        if added {
            self.ready.notify_all();
        }
    }

    fn enqueue_one(&self, task: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(task);
        self.ready.notify_one();
    }

    fn claim(&self, stop: &AtomicBool) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(task) = state.queue.pop_front() {
                state.active_workers += 1;
                return Some(task);
            }
            if state.active_workers == 0 {
                return None;
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    fn complete_claim(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_workers = state.active_workers.saturating_sub(1);
        self.ready.notify_all();
    }

    fn wake_all(&self) {
        self.ready.notify_all();
    }
}

impl<T> RecursiveTaskQueue<T> {
    fn enqueue(&self, task: T) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(io::Error::other("recursive copy queue closed"));
        }
        state.queue.push_back(task);
        self.ready.notify_one();
        Ok(())
    }

    fn claim(&self, stop: &AtomicBool) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(task) = state.queue.pop_front() {
                return Some(task);
            }
            if state.closed {
                return None;
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.ready.notify_all();
    }

    fn wake_all(&self) {
        self.ready.notify_all();
    }
}

const FILE_LIST_URING_THREAD_COUNT: usize = 32;
const FILE_LIST_URING_INFLIGHT_SWEEP: [usize; 5] = [32, 64, 128, 256, 512];
const FILE_LIST_URING_SLOT_BUFFER_SIZE: usize = 4096;
const MANIFEST_READ_PREFIX_SWEEP: [usize; 5] = [512, 2048, 8192, 16384, 32768];
const MANIFEST_MT_BLOCKING_THREADS: [usize; 3] = [4, 16, 32];
const MANIFEST_ST_URING_QDS: [usize; 4] = [32, 64, 128, 256];
const MANIFEST_MT_URING_CONFIGS: [(usize, usize); 4] = [(4, 32), (8, 32), (16, 64), (32, 64)];

fn file_list_uring_should_use_direct(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
) -> bool {
    match io_mode {
        common::IOMode::Direct => true,
        common::IOMode::PageCache => false,
        common::IOMode::Auto => {
            if config
                .mount_info_for_path(manifest_path)
                .as_ref()
                .is_some_and(|info| info.fstype == "zfs")
            {
                return true;
            }
            let strategy = config.get_read_auto_strategy_for_path(manifest_path);
            matches!(
                strategy.hot_small_path,
                ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect
            ) || matches!(
                strategy.cold_small_path,
                ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect
            )
        }
    }
}

fn raise_nofile_soft_limit(verbose: bool) {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let get_result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) };
    if get_result != 0 {
        if verbose {
            let err = io::Error::last_os_error();
            eprintln!("warning: failed to read RLIMIT_NOFILE: {err}");
        }
        return;
    }
    if limits.rlim_cur >= limits.rlim_max {
        return;
    }
    let updated = libc::rlimit {
        rlim_cur: limits.rlim_max,
        rlim_max: limits.rlim_max,
    };
    let set_result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &updated) };
    if set_result != 0 && verbose {
        let err = io::Error::last_os_error();
        eprintln!(
            "warning: failed to raise RLIMIT_NOFILE from {} to {}: {err}",
            limits.rlim_cur, limits.rlim_max
        );
    }
}

fn file_list_uring_claim_path(
    files: &[PathBuf],
    next_index: &AtomicUsize,
) -> Option<PathBuf> {
    let index = next_index.fetch_add(1, Ordering::Relaxed);
    files.get(index).cloned()
}

fn prepare_file_list_uring_read(path: PathBuf, use_direct: bool, slot_buffer_index: usize) -> io::Result<Option<FileListUringInflightRead>> {
    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let (file, file_direct) = open_reader_files(path_str, use_direct)?;
    Ok(Some(FileListUringInflightRead { file, file_direct, slot_buffer_index }))
}

fn submit_file_list_uring_read(
    io_uring: &mut IoUring,
    slot: usize,
    read: &mut FileListUringInflightRead,
    slot_buffers: &mut [AlignedBuffer],
    use_direct: bool,
) -> io::Result<()> {
    let buffer = slot_buffers
        .get_mut(read.slot_buffer_index)
        .ok_or_else(|| io::Error::other("missing slot buffer for file-list io_uring read"))?;
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
        if use_direct {
            sqe.prep_read(read.file_direct.as_raw_fd(), buffer.as_mut_slice(), 0);
        } else {
            sqe.prep_read(read.file.as_raw_fd(), buffer.as_mut_slice(), 0);
        }
        sqe.set_user_data(slot as u64);
    }
    Ok(())
}

fn file_list_uring_fill_slots(
    io_uring: &mut IoUring,
    slots: &mut [Option<FileListUringInflightRead>],
    slot_buffers: &mut [AlignedBuffer],
    files: &[PathBuf],
    next_index: &AtomicUsize,
    use_direct: bool,
    _stats: &RecursiveReadStats,
    _sample_counters: &ThroughputSampleCounters,
) -> io::Result<usize> {
    let mut queued = 0usize;
    for (slot_index, slot) in slots.iter_mut().enumerate() {
        if slot.is_some() {
            continue;
        }
        loop {
            let Some(path) = file_list_uring_claim_path(files, next_index) else {
                break;
            };
            match prepare_file_list_uring_read(path, use_direct, slot_index)? {
                Some(mut read) => {
                    submit_file_list_uring_read(io_uring, slot_index, &mut read, slot_buffers, use_direct)?;
                    *slot = Some(read);
                    queued += 1;
                    break;
                }
                None => unreachable!("prepare_file_list_uring_read always returns Some"),
            }
        }
    }
    Ok(queued)
}

fn wait_for_ready_slots(io_uring: &mut IoUring) -> io::Result<Vec<(usize, u32)>> {
    let first = io_uring.wait_for_cqe().map_err(io::Error::other)?;
    let mut ready = vec![(
        usize::try_from(first.user_data())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot id overflowed"))?,
        first.result()?,
    )];
    while io_uring.cq_ready() > 0 {
        let cq = io_uring
            .peek_for_cqe()
            .ok_or_else(|| io::Error::other("completion queue reported ready but no CQE was available"))?;
        ready.push((
            usize::try_from(cq.user_data())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot id overflowed"))?,
            cq.result()?,
        ));
    }
    Ok(ready)
}

fn file_list_uring_worker(
    files: Arc<Vec<PathBuf>>,
    next_index: Arc<AtomicUsize>,
    inflight_per_thread: usize,
    use_direct: bool,
    stats: Arc<RecursiveReadStats>,
    sample_counters: Arc<ThroughputSampleCounters>,
) -> io::Result<()> {
    let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
    let mut slot_buffers = std::iter::repeat_with(|| AlignedBuffer::new(FILE_LIST_URING_SLOT_BUFFER_SIZE))
        .take(inflight_per_thread)
        .collect::<Vec<_>>();
    let mut slots = std::iter::repeat_with(|| None)
        .take(inflight_per_thread)
        .collect::<Vec<Option<FileListUringInflightRead>>>();
    let mut inflight = file_list_uring_fill_slots(
        &mut io_uring,
        &mut slots,
        &mut slot_buffers,
        &files,
        &next_index,
        use_direct,
        &stats,
        &sample_counters,
    )?;
    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(io::Error::other)?;
    loop {
        for (slot_index, result) in wait_for_ready_slots(&mut io_uring)? {
            let _read = slots
                .get_mut(slot_index)
                .and_then(Option::take)
                .ok_or_else(|| io::Error::other("completed slot had no active file read"))?;
            let actual_len = validate_read_result(
                "file-list-uring-read",
                0,
                FILE_LIST_URING_SLOT_BUFFER_SIZE,
                result,
            )?;
            if actual_len > 0 {
                sample_counters
                    .bytes
                    .fetch_add(actual_len as u64, Ordering::Relaxed);
                stats
                    .bytes_read
                    .fetch_add(actual_len as u64, Ordering::Relaxed);
            }
            stats.files_read.fetch_add(1, Ordering::Relaxed);
            sample_counters.units.fetch_add(1, Ordering::Relaxed);
            inflight = inflight.saturating_sub(1);
        }
        inflight += file_list_uring_fill_slots(
            &mut io_uring,
            &mut slots,
            &mut slot_buffers,
            &files,
            &next_index,
            use_direct,
            &stats,
            &sample_counters,
        )?;
        if inflight == 0 {
            return Ok(());
        }
        io_uring.submit_sqes().map_err(io::Error::other)?;
    }
}

fn run_file_list_uring_bench_once(
    files: Arc<Vec<PathBuf>>,
    inflight_per_thread: usize,
    use_direct: bool,
    verbose: bool,
) -> io::Result<FileListUringSweepResult> {
    let start = std::time::Instant::now();
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    if verbose {
        eprintln!(
            "file-list-read-uring-bench run threads={} inflight/thread={} direct={}",
            FILE_LIST_URING_THREAD_COUNT, inflight_per_thread, use_direct
        );
    }
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "file-list-read-uring-bench",
            "files",
            sample_counters.clone(),
        ))
    } else {
        None
    };
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::with_capacity(FILE_LIST_URING_THREAD_COUNT);
    for _ in 0..FILE_LIST_URING_THREAD_COUNT {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        let sample_counters = sample_counters.clone();
        threads.push(std::thread::spawn(move || {
            file_list_uring_worker(
                files,
                next_index,
                inflight_per_thread,
                use_direct,
                stats,
                sample_counters,
            )
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("file-list io_uring read worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(FileListUringSweepResult {
        inflight_per_thread,
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

fn print_file_list_uring_sweep_results(results: &[FileListUringSweepResult]) {
    let mut rows = vec![vec![
        "inflight/thread".to_string(),
        "time(s)".to_string(),
        "GB/s".to_string(),
        "files/s".to_string(),
        "bytes".to_string(),
        "files".to_string(),
    ]];
    for result in results {
        rows.push(vec![
            result.inflight_per_thread.to_string(),
            format!("{:.4}", result.elapsed_secs),
            format!("{:.3}", result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9),
            format!("{:.1}", result.total_files as f64 / result.elapsed_secs.max(1e-9)),
            result.total_bytes.to_string(),
            result.total_files.to_string(),
        ]);
    }
    let column_count = rows[0].len();
    let widths = (0..column_count)
        .map(|index| rows.iter().map(|row| row[index].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, cell)| format!("{cell:<width$}", width = widths[index]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

fn load_manifest_prefix(files: &[PathBuf], prefix_files: usize) -> Arc<Vec<PathBuf>> {
    Arc::new(files.iter().take(prefix_files.min(files.len())).cloned().collect())
}


fn run_manifest_blocking_worker(
    files: Arc<Vec<PathBuf>>,
    next_index: Arc<AtomicUsize>,
    use_direct: bool,
    stats: Arc<RecursiveReadStats>,
) -> io::Result<()> {
    let mut buffer = AlignedBuffer::new(FILE_LIST_URING_SLOT_BUFFER_SIZE);
    loop {
        let index = next_index.fetch_add(1, Ordering::Relaxed);
        let Some(path) = files.get(index) else {
            return Ok(());
        };
        let path_str = path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path is not valid UTF-8: {}", path.display()),
            )
        })?;
        let (file, file_direct) = open_reader_files(path_str, use_direct)?;
        let target = if use_direct { &file_direct } else { &file };
        let read = target.read_at(buffer.as_mut_slice(), 0)?;
        stats.files_read.fetch_add(1, Ordering::Relaxed);
        stats.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
    }
}

fn read_small_file_probe_then_fallback(
    config: &config::LoadedConfig,
    path: &Path,
    io_mode: common::IOMode,
) -> io::Result<u64> {
    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let mut file = fs::File::open(path)?;
    let mut probe = vec![0_u8; 64 * 1024];
    let read = file.read(&mut probe)?;
    if read < probe.len() {
        return Ok(read as u64);
    }
    let (bytes_read, _file_size, _params) =
        visit_file_blocks_for_mode(config, "read", path_str, io_mode, |_| Ok(()))?;
    Ok(bytes_read)
}

fn run_manifest_blocking_once(
    files: Arc<Vec<PathBuf>>,
    threads: usize,
    use_direct: bool,
) -> io::Result<ManifestReadSweepResult> {
    let start = std::time::Instant::now();
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(threads.max(1));
    for _ in 0..threads.max(1) {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        handles.push(std::thread::spawn(move || {
            run_manifest_blocking_worker(files, next_index, use_direct, stats)
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| io::Error::other("manifest blocking worker panicked"))??;
    }
    Ok(ManifestReadSweepResult {
        prefix_files: files.len(),
        variant: if threads <= 1 {
            ManifestReadVariant::SingleThreadBlocking
        } else {
            ManifestReadVariant::MultiThreadBlocking { threads }
        },
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

fn run_manifest_uring_once(
    files: Arc<Vec<PathBuf>>,
    threads: usize,
    qd: usize,
    use_direct: bool,
) -> io::Result<ManifestReadSweepResult> {
    let start = std::time::Instant::now();
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(threads.max(1));
    for _ in 0..threads.max(1) {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        handles.push(std::thread::spawn(move || {
            file_list_uring_worker(
                files,
                next_index,
                qd,
                use_direct,
                stats,
                Arc::new(ThroughputSampleCounters::default()),
            )
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| io::Error::other("manifest io_uring worker panicked"))??;
    }
    Ok(ManifestReadSweepResult {
        prefix_files: files.len(),
        variant: if threads <= 1 {
            ManifestReadVariant::SingleThreadUring { qd }
        } else {
            ManifestReadVariant::MultiThreadUring { threads, qd }
        },
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

fn print_manifest_read_sweep_results(results: &[ManifestReadSweepResult]) {
    let mut rows = vec![vec![
        "prefix_files".to_string(),
        "variant".to_string(),
        "time(s)".to_string(),
        "GB/s".to_string(),
        "files/s".to_string(),
        "bytes".to_string(),
    ]];
    for result in results {
        rows.push(vec![
            result.prefix_files.to_string(),
            result.variant.label(),
            format!("{:.4}", result.elapsed_secs),
            format!("{:.3}", result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9),
            format!("{:.1}", result.total_files as f64 / result.elapsed_secs.max(1e-9)),
            result.total_bytes.to_string(),
        ]);
    }
    let column_count = rows[0].len();
    let widths = (0..column_count)
        .map(|index| rows.iter().map(|row| row[index].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, cell)| format!("{cell:<width$}", width = widths[index]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

fn bench_recursive_small_file_threads(
    config: &mut config::LoadedConfig,
    path: &str,
    io_mode: common::IOMode,
    verbose: bool,
    save_config: bool,
    cache_state_override: Option<SmallFileThreadCacheState>,
) -> io::Result<u64> {
    let root = Path::new(path);
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bench-recursive-small-file-threads requires a directory root, got {}", root.display()),
        ));
    }
    let mount_path = path;
    let is_hot = cache_state_override
        .map(|state| state == SmallFileThreadCacheState::Hot)
        .unwrap_or_else(|| is_first_page_resident(path).unwrap_or(false));
    match cache_state_override {
        Some(SmallFileThreadCacheState::Hot) => {
            let _ = reader::warm_file_page_cache(path);
        }
        Some(SmallFileThreadCacheState::Cold) => {
            let _ = reader::evict_file_cache(path);
        }
        None => {}
    }
    let sweep = [8_u64, 13, 16, 24, 32, 55, 64, 96, 128];
    let page_cache = config.get_params_for_path("read", false, path);
    let direct = config.get_params_for_path("read", true, path);
    let mut rows = Vec::new();
    for threads in sweep {
        let start = std::time::Instant::now();
        let bytes = bench_recursive_read(
            config,
            path,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            io_mode,
            verbose,
            Some(threads),
        )?;
        let elapsed = start.elapsed().as_secs_f64();
        rows.push((threads, bytes, elapsed));
        println!(
            "result\tcache={}\tthreads={}\ttime={:.4}s\tgbps={:.3}",
            if is_hot { "hot" } else { "cold" },
            threads,
            elapsed,
            bytes as f64 / elapsed.max(1e-9) / 1e9
        );
    }
    let best = rows
        .iter()
        .min_by(|a, b| a.2.total_cmp(&b.2))
        .ok_or_else(|| io::Error::other("recursive small-file thread sweep produced no results"))?;
    println!(
        "bench-recursive-small-file-threads best cache={} threads={} {:.4}s {:.3} GB/s",
        if is_hot { "hot" } else { "cold" },
        best.0,
        best.2,
        best.1 as f64 / best.2.max(1e-9) / 1e9
    );
    if save_config {
        let mut tuned = config.get_recursive_small_file_threads_for_path(mount_path);
        if is_hot {
            tuned.hot = best.0;
        } else {
            tuned.cold = best.0;
        }
        config.update_recursive_small_file_threads_for_path(mount_path, tuned);
        config.save();
        println!(
            "saved recursive_small_file_threads for {}: hot={}, cold={}",
            mount_path, tuned.hot, tuned.cold
        );
    }
    Ok(best.1)
}

fn current_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn prospective_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.exists() {
        return path.canonicalize();
    }
    let absolute = current_absolute_path(path)?;
    let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.canonicalize()?.join(
        absolute
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing target name"))?,
    ))
}

fn resolve_recursive_copy_root(source_root: &Path, target: &Path) -> io::Result<PathBuf> {
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(target.join(
            source_root
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source root has no final path component"))?,
        )),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy target must be a directory or a missing path when copying a directory recursively",
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(target.to_path_buf()),
        Err(err) => Err(err),
    }
}

fn ensure_recursive_target_not_inside_source(
    source_root: &Path,
    target_root: &Path,
) -> io::Result<()> {
    let source_abs = source_root.canonicalize()?;
    let target_abs = prospective_absolute_path(target_root)?;
    if target_abs == source_abs || target_abs.starts_with(&source_abs) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to copy directory {} into itself via {}",
                source_root.display(),
                target_root.display()
            ),
        ));
    }
    Ok(())
}

fn ensure_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
}

fn ensure_removed_non_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("target path {} is a directory", path.display()),
        )),
        Ok(_) => fs::remove_file(path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn create_directory_like(
    source_mode: u32,
    target_dir: &Path,
    stats: &RecursiveCopyStats,
    sample_counters: Option<&ThroughputSampleCounters>,
) -> io::Result<()> {
    match fs::symlink_metadata(target_dir) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "target path {} exists and is not a directory",
                        target_dir.display()
                    ),
                ));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(target_dir)?;
            stats.dirs_created.fetch_add(1, Ordering::Relaxed);
            stats.items_completed.fetch_add(1, Ordering::Relaxed);
            if let Some(counters) = sample_counters {
                counters.units.fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(err) => return Err(err),
    }
    fs::set_permissions(target_dir, fs::Permissions::from_mode(source_mode))
}

fn copy_symlink_entry(
    source_path: &Path,
    target_path: &Path,
    stats: &RecursiveCopyStats,
) -> io::Result<()> {
    ensure_parent_directory(target_path)?;
    ensure_removed_non_directory(target_path)?;
    let link_target = fs::read_link(source_path)?;
    symlink(&link_target, target_path)?;
    stats.symlinks_created.fetch_add(1, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn execute_recursive_file_copy(
    task: &RecursiveFileTask,
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
    sample_counters: Option<&ThroughputSampleCounters>,
) -> io::Result<()> {
    let source_path = task.source_path.to_string_lossy();
    let target_path = task.target_path.to_string_lossy();
    let guard = CopyOperationGuard::new(&source_path, &target_path, ctx.use_lock)?;
    let copied = if task.resolved_copy.diff_overwrite && !ctx.keep_target_size {
        let diff_scan = ctx
            .config
            .get_params_for_path("diff", false, target_path.as_ref());
        overwrite_changed_chunks_direct(
            &source_path,
            &target_path,
            diff_scan.num_threads,
            diff_scan.block_size,
            diff_scan.qd,
            ctx.optimizer_params[3],
            ctx.optimizer_params[4],
            ctx.optimizer_params[5] as usize,
        )?
    } else {
        copy_file_with_strategy_and_truncate(
            &source_path,
            &target_path,
            ctx.optimizer_params[0],
            ctx.optimizer_params[1],
            ctx.optimizer_params[2] as usize,
            ctx.optimizer_params[3],
            ctx.optimizer_params[4],
            ctx.optimizer_params[5] as usize,
            ctx.optimizer_params[6],
            ctx.optimizer_params[7],
            ctx.optimizer_params[8] as usize,
            task.resolved_copy.io_mode_read,
            task.resolved_copy.io_mode_write,
            task.resolved_copy.copy_strategy,
            !ctx.keep_target_size,
        )?
    };
    guard.ensure_source_unchanged()?;
    fs::set_permissions(
        &task.target_path,
        fs::Permissions::from_mode(task.source_mode),
    )?;
    stats.files_copied.fetch_add(1, Ordering::Relaxed);
    stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    if let Some(counters) = sample_counters {
        counters.bytes.fetch_add(copied, Ordering::Relaxed);
        counters.units.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn recursive_copy_dir_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

fn recursive_copy_large_worker_count() -> usize {
    if let Ok(value) = std::env::var("FRO_RECURSIVE_COPY_LARGE_WORKERS") {
        if let Ok(parsed) = value.parse::<usize>() {
            if parsed > 0 {
                return parsed;
            }
        }
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(RECURSIVE_COPY_MAX_LARGE_WORKERS)
        .max(1)
}

fn recursive_copy_small_worker_count() -> usize {
    if let Ok(value) = std::env::var("FRO_RECURSIVE_COPY_SMALL_WORKERS") {
        if let Ok(parsed) = value.parse::<usize>() {
            if parsed > 0 {
                return parsed;
            }
        }
    }
    RECURSIVE_COPY_SMALL_WORKERS
}

fn recursive_read_dir_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

fn recursive_read_file_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

fn recursive_read_file_worker_count_with_override(override_threads: Option<u64>) -> usize {
    override_threads
        .and_then(|threads| usize::try_from(threads).ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(recursive_read_file_worker_count)
}

fn recursive_small_file_worker_count_for_path(
    config: &config::LoadedConfig,
    path: &str,
    override_threads: Option<u64>,
) -> usize {
    if let Some(threads) = override_threads {
        return recursive_read_file_worker_count_with_override(Some(threads));
    }
    let tuned = config.get_recursive_small_file_threads_for_path(path);
    let cache_state = if is_first_page_resident(path).unwrap_or(false) {
        tuned.hot
    } else {
        tuned.cold
    };
    recursive_read_file_worker_count_with_override(Some(cache_state))
}

fn recursive_copy_uses_small_file_range(ctx: &RecursiveCopyContext, source_len: u64) -> bool {
    let cutoff = std::env::var("FRO_RECURSIVE_COPY_SMALL_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(RECURSIVE_COPY_SMALL_FILE_THRESHOLD);
    source_len < cutoff
        && ctx.rewrite_mode == CopyRewriteMode::Auto
        && !ctx.keep_target_size
        && !matches!(
            ctx.requested_strategy,
            CopyStrategy::Threaded | CopyStrategy::Reflink
        )
        && ctx.io_mode_read != common::IOMode::Direct
        && ctx.io_mode_write != common::IOMode::Direct
}

fn recursive_copy_uses_threaded_large_lane(ctx: &RecursiveCopyContext, source_len: u64) -> bool {
    let cutoff = std::env::var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(RECURSIVE_COPY_THREADED_LANE_THRESHOLD);
    source_len >= cutoff
        || ctx.requested_strategy == CopyStrategy::Threaded
        || ctx.requested_strategy == CopyStrategy::Reflink
}

fn resolve_recursive_large_copy_execution(
    ctx: &RecursiveCopyContext,
    source_path: &Path,
    target_path: &Path,
) -> io::Result<ResolvedCopyExecution> {
    let source_str = source_path.to_string_lossy();
    let target_str = target_path.to_string_lossy();
    let resolved = resolve_copy_execution(
        &ctx.config,
        &source_str,
        &target_str,
        ctx.requested_strategy,
        ctx.rewrite_mode,
        ctx.io_mode_read,
        ctx.io_mode_write,
    )?;
    if ctx.requested_strategy == CopyStrategy::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: resolved.io_mode_read,
            io_mode_write: resolved.io_mode_write,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "recursive large threaded",
        });
    }
    Ok(resolved)
}

fn execute_recursive_small_file_copy(
    task: &RecursiveSmallFileTask,
    use_lock: bool,
    stats: &RecursiveCopyStats,
    sample_counters: Option<&ThroughputSampleCounters>,
) -> io::Result<()> {
    let source_str = task.source_path.to_string_lossy();
    let target_str = task.target_path.to_string_lossy();
    let guard = CopyOperationGuard::new(&source_str, &target_str, use_lock)?;
    let copied = copy_file_range_syscall(
        &source_str,
        &target_str,
        0,
        0,
        task.source_len,
        true,
        common::IOMode::PageCache,
        common::IOMode::PageCache,
    )?;
    guard.ensure_source_unchanged()?;
    fs::set_permissions(&task.target_path, fs::Permissions::from_mode(task.source_mode))?;
    stats.files_copied.fetch_add(1, Ordering::Relaxed);
    stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    if let Some(counters) = sample_counters {
        counters.bytes.fetch_add(copied, Ordering::Relaxed);
        counters.units.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn walk_recursive_copy_subtree(
    start: RecursiveDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveDirectoryTask>,
    large_queue: &RecursiveTaskQueue<RecursiveFileTask>,
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
    sample_counters: &ThroughputSampleCounters,
    #[cfg(feature = "read-phase-timing")] lane_counters: &RecursiveCopyLaneCounters,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start];
    let source_root_fd = open_dir_fd(&ctx.source_root)?;
    let target_root_fd = open_dir_fd(&ctx.target_root)?;
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&task.source_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let source_path = entry.path();
            let target_path = task.target_dir.join(entry.file_name());
            if file_type.is_dir() {
                let metadata = entry.metadata()?;
                let mode = metadata.permissions().mode();
                create_directory_like(mode, &target_path, stats, Some(sample_counters))?;
                child_dirs.push(RecursiveDirectoryTask {
                    source_dir: source_path,
                    target_dir: target_path,
                });
                continue;
            }
            if file_type.is_symlink() {
                copy_symlink_entry(&source_path, &target_path, stats)?;
                continue;
            }
            if !file_type.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "recursive copy only supports regular files, directories, and symlinks (saw {})",
                        source_path.display()
                    ),
                ));
            }
            let metadata = entry.metadata()?;
            let source_len = metadata.len();
            let source_mode = metadata.permissions().mode();
            let relative_path = source_path
                .strip_prefix(&ctx.source_root)
                .map_err(|_| io::Error::other("recursive copy path escaped source root"))?
                .to_path_buf();
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                let copied = copy_relative_file_openat(
                    &relative_path,
                    source_len,
                    source_mode,
                    &source_root_fd,
                    &target_root_fd,
                    ctx.relative_copy_method,
                )?;
                stats.files_copied.fetch_add(1, Ordering::Relaxed);
                stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
                stats.items_completed.fetch_add(1, Ordering::Relaxed);
                sample_counters.bytes.fetch_add(copied, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "read-phase-timing")]
                {
                    lane_counters.note_queued("small");
                    lane_counters.note_done("small");
                }
            } else if recursive_copy_uses_threaded_large_lane(ctx, source_len) {
                let resolved_copy =
                    resolve_recursive_large_copy_execution(ctx, &source_path, &target_path)?;
                large_queue.enqueue(RecursiveFileTask {
                    source_path,
                    target_path,
                    source_mode,
                    resolved_copy,
                })?;
                #[cfg(feature = "read-phase-timing")]
                lane_counters.note_queued("large");
            } else {
                let copied = copy_relative_file_openat(
                    &relative_path,
                    source_len,
                    source_mode,
                    &source_root_fd,
                    &target_root_fd,
                    ctx.relative_copy_method,
                )?;
                stats.files_copied.fetch_add(1, Ordering::Relaxed);
                stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
                stats.items_completed.fetch_add(1, Ordering::Relaxed);
                sample_counters.bytes.fetch_add(copied, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "read-phase-timing")]
                {
                    lane_counters.note_queued("medium");
                    lane_counters.note_done("medium");
                }
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    Ok(())
}

fn walk_recursive_read_subtree(
    start: RecursiveReadDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveReadDirectoryTask>,
    file_queue: &RecursiveTaskQueue<RecursiveReadFileTask>,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start];
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&task.source_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                child_dirs.push(RecursiveReadDirectoryTask { source_dir: path });
            } else if file_type.is_file() {
                file_queue.enqueue(RecursiveReadFileTask { source_path: path })?;
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            for task in child_dirs {
                dir_queue.enqueue_one(task);
            }
            stack.push(local_dir);
        }
    }
    Ok(())
}

fn run_recursive_copy(ctx: RecursiveCopyContext, verbose: bool) -> io::Result<u64> {
    let source_meta = fs::symlink_metadata(&ctx.source_root)?;
    if !source_meta.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recursive copy requires a directory source",
        ));
    }
    ensure_recursive_target_not_inside_source(&ctx.source_root, &ctx.target_root)?;
    let stats = Arc::new(RecursiveCopyStats::default());
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    create_directory_like(
        source_meta.permissions().mode(),
        &ctx.target_root,
        &stats,
        Some(sample_counters.as_ref()),
    )?;
    let source_mount = mount_info_for_path(&ctx.source_root);
    let target_mount = mount_info_for_path(&ctx.target_root);
    let relative_copy_method = match (source_mount.as_ref(), target_mount.as_ref()) {
        (Some(source), Some(target))
            if source.mount_point == target.mount_point
                && source.fstype == target.fstype
                && source.mount_source == target.mount_source =>
        {
            RelativeCopyMethod::CopyFileRange
        }
        _ => RelativeCopyMethod::Sendfile,
    };
    let mut ctx = ctx;
    ctx.relative_copy_method = relative_copy_method;
    let dir_queue = Arc::new(RecursiveDirectoryQueue::<RecursiveDirectoryTask>::default());
    let large_queue = Arc::new(RecursiveTaskQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    #[cfg(feature = "read-phase-timing")]
    let lane_counters = Arc::new(RecursiveCopyLaneCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start("recursive-copy", "items", sample_counters.clone()))
    } else {
        None
    };

    dir_queue.enqueue_one(RecursiveDirectoryTask {
        source_dir: ctx.source_root.clone(),
        target_dir: ctx.target_root.clone(),
    });

    let worker_count = recursive_copy_dir_worker_count();
    let large_worker_count = recursive_copy_large_worker_count();

    let mut walk_threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let dir_queue = dir_queue.clone();
        let large_queue = large_queue.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        #[cfg(feature = "read-phase-timing")]
        let lane_counters = lane_counters.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_copy_subtree(
                    task,
                    &dir_queue,
                    &large_queue,
                    &ctx,
                    &stats,
                    sample_counters.as_ref(),
                    #[cfg(feature = "read-phase-timing")]
                    lane_counters.as_ref(),
                    &stop,
                );
                dir_queue.complete_claim();
                if let Err(err) = result {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
                    large_queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut large_threads = Vec::with_capacity(large_worker_count);
    for _ in 0..large_worker_count {
        let queue = large_queue.clone();
        let dir_queue = dir_queue.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        #[cfg(feature = "read-phase-timing")]
        let lane_counters = lane_counters.clone();
        large_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = queue.claim(&stop) {
                if let Err(err) =
                    execute_recursive_file_copy(&task, &ctx, &stats, Some(sample_counters.as_ref()))
                {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
                    queue.wake_all();
                    return Err(err);
                }
                #[cfg(feature = "read-phase-timing")]
                lane_counters.note_done("large");
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in walk_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("recursive copy walk worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    large_queue.close();

    for thread in large_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("recursive copy file worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    let bytes_copied = stats.bytes_copied.load(Ordering::Relaxed);
    if verbose {
        eprintln!(
            "recursive copy: dirs_created={}, files_copied={}, symlinks_created={}, bytes_copied={}",
            stats.dirs_created.load(Ordering::Relaxed),
            stats.files_copied.load(Ordering::Relaxed),
            stats.symlinks_created.load(Ordering::Relaxed),
            bytes_copied
        );
        #[cfg(feature = "read-phase-timing")]
        eprintln!("{}", lane_counters.snapshot_line());
    }
    Ok(bytes_copied)
}

fn run_split_manifest_recursive_copy(ctx: RecursiveCopyContext, verbose: bool) -> io::Result<u64> {
    let source_meta = fs::symlink_metadata(&ctx.source_root)?;
    if !source_meta.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recursive copy requires a directory source",
        ));
    }
    ensure_recursive_target_not_inside_source(&ctx.source_root, &ctx.target_root)?;
    let stats = Arc::new(RecursiveCopyStats::default());
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    create_directory_like(
        source_meta.permissions().mode(),
        &ctx.target_root,
        &stats,
        Some(sample_counters.as_ref()),
    )?;
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "split-manifest-recursive-copy",
            "items",
            sample_counters.clone(),
        ))
    } else {
        None
    };

    let (small_tasks, large_tasks) =
        collect_recursive_copy_manifest(&ctx, stats.as_ref(), sample_counters.as_ref())?;

    let small_queue = Arc::new(RecursiveTaskQueue::default());
    let large_queue = Arc::new(RecursiveTaskQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    for task in small_tasks {
        small_queue.enqueue(task)?;
    }
    for task in large_tasks {
        large_queue.enqueue(task)?;
    }
    small_queue.close();
    large_queue.close();

    let small_worker_count = recursive_copy_small_worker_count();
    let large_worker_count = recursive_copy_large_worker_count();

    let mut small_threads = Vec::with_capacity(small_worker_count);
    for _ in 0..small_worker_count {
        let queue = small_queue.clone();
        let large_queue = large_queue.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        small_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = queue.claim(&stop) {
                if let Err(err) = execute_recursive_small_file_copy(
                    &task,
                    ctx.use_lock,
                    &stats,
                    Some(sample_counters.as_ref()),
                ) {
                    stop.store(true, Ordering::SeqCst);
                    queue.wake_all();
                    large_queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut large_threads = Vec::with_capacity(large_worker_count);
    for _ in 0..large_worker_count {
        let queue = large_queue.clone();
        let small_queue = small_queue.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        large_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = queue.claim(&stop) {
                if let Err(err) =
                    execute_recursive_file_copy(&task, &ctx, &stats, Some(sample_counters.as_ref()))
                {
                    stop.store(true, Ordering::SeqCst);
                    small_queue.wake_all();
                    queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in small_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("split-manifest recursive copy small worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    for thread in large_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("split-manifest recursive copy large worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }

    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    let bytes_copied = stats.bytes_copied.load(Ordering::Relaxed);
    if verbose {
        eprintln!(
            "split-manifest recursive copy: dirs_created={}, files_copied={}, symlinks_created={}, bytes_copied={}",
            stats.dirs_created.load(Ordering::Relaxed),
            stats.files_copied.load(Ordering::Relaxed),
            stats.symlinks_created.load(Ordering::Relaxed),
            bytes_copied
        );
    }
    Ok(bytes_copied)
}

fn bench_recursive_read(
    config: &config::LoadedConfig,
    path: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: common::IOMode,
    verbose: bool,
    file_worker_override: Option<u64>,
) -> io::Result<u64> {
    let root = Path::new(path);
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("recursive-read-bench requires a directory root, got {}", root.display()),
        ));
    }
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found under {}", root.display()),
        ));
    }

    let start = std::time::Instant::now();
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "recursive-read-bench",
            "files",
            sample_counters.clone(),
        ))
    } else {
        None
    };
    let mut read_config = config.clone();
    read_config.update_params_for_path(
        "read",
        false,
        path,
        config::IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
    );
    read_config.update_params_for_path(
        "read",
        true,
        path,
        config::IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
    );
    let stats = Arc::new(RecursiveReadStats::default());
    let dir_queue = Arc::new(RecursiveDirectoryQueue::<RecursiveReadDirectoryTask>::default());
    let file_queue = Arc::new(RecursiveTaskQueue::<RecursiveReadFileTask>::default());
    let stop = Arc::new(AtomicBool::new(false));
    dir_queue.enqueue_one(RecursiveReadDirectoryTask {
        source_dir: root.to_path_buf(),
    });

    let dir_worker_count = recursive_read_dir_worker_count();
    let file_worker_count =
        recursive_small_file_worker_count_for_path(&read_config, path, file_worker_override);

    let mut walk_threads = Vec::with_capacity(dir_worker_count);
    for _ in 0..dir_worker_count {
        let dir_queue = dir_queue.clone();
        let file_queue = file_queue.clone();
        let stop = stop.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_read_subtree(task, &dir_queue, &file_queue, &stop);
                dir_queue.complete_claim();
                if let Err(err) = result {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
                    file_queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut file_threads = Vec::with_capacity(file_worker_count);
    for _ in 0..file_worker_count {
        let file_queue = file_queue.clone();
        let dir_queue = dir_queue.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        let sample_counters = sample_counters.clone();
        let read_config = read_config.clone();
        let file_io_mode = io_mode;
        file_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = file_queue.claim(&stop) {
                let bytes_read =
                    read_small_file_probe_then_fallback(&read_config, &task.source_path, file_io_mode)?;
                sample_counters.bytes.fetch_add(bytes_read, Ordering::Relaxed);
                stats.files_read.fetch_add(1, Ordering::Relaxed);
                stats.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                if bytes_read == 0 {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
                    file_queue.wake_all();
                    return Err(io::Error::other(format!(
                        "recursive-read-bench observed zero bytes for file {}",
                        task.source_path.display()
                    )));
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in walk_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("recursive read walk worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    file_queue.close();
    for thread in file_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("recursive read file worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    let total_bytes = stats.bytes_read.load(Ordering::Relaxed);
    let total_files = stats.files_read.load(Ordering::Relaxed) as usize;
    if total_files == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found under {}", root.display()),
        ));
    }
    let elapsed = start.elapsed().as_secs_f64();
    if verbose {
        eprintln!(
            "recursive-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s ({:.1} files/s)",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9,
            total_files as f64 / elapsed.max(1e-9)
        );
    } else {
        println!(
            "recursive-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9
        );
    }
    Ok(total_bytes)
}

fn bench_file_list_read(
    config: &config::LoadedConfig,
    manifest_path: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = load_paths_from_manifest(Path::new(manifest_path))?;
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }

    let start = std::time::Instant::now();
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "file-list-read-bench",
            "files",
            sample_counters.clone(),
        ))
    } else {
        None
    };
    let mut read_config = config.clone();
    read_config.update_params_for_path(
        "read",
        false,
        manifest_path,
        config::IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
    );
    read_config.update_params_for_path(
        "read",
        true,
        manifest_path,
        config::IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
    );
    let stats = Arc::new(RecursiveReadStats::default());
    let file_queue = Arc::new(RecursiveTaskQueue::<RecursiveReadFileTask>::default());
    let stop = Arc::new(AtomicBool::new(false));
    for source_path in files {
        file_queue.enqueue(RecursiveReadFileTask { source_path })?;
    }
    file_queue.close();

    let file_worker_count = recursive_read_file_worker_count();
    let mut file_threads = Vec::with_capacity(file_worker_count);
    for _ in 0..file_worker_count {
        let file_queue = file_queue.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        let sample_counters = sample_counters.clone();
        let read_config = read_config.clone();
        file_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = file_queue.claim(&stop) {
                let sample_counters_for_file = sample_counters.clone();
                let file_str = task.source_path.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("path is not valid UTF-8: {}", task.source_path.display()),
                    )
                })?;
                let (bytes_read, _file_size, _params) = visit_file_blocks_for_mode(
                    &read_config,
                    "read",
                    file_str,
                    io_mode,
                    move |block| {
                        sample_counters_for_file
                            .bytes
                            .fetch_add(block.data.len() as u64, Ordering::Relaxed);
                        Ok(())
                    },
                )?;
                stats.files_read.fetch_add(1, Ordering::Relaxed);
                stats.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                if bytes_read == 0 && fs::metadata(&task.source_path)?.len() != 0 {
                    stop.store(true, Ordering::SeqCst);
                    file_queue.wake_all();
                    return Err(io::Error::other(format!(
                        "file-list-read-bench observed zero bytes for non-empty file {}",
                        task.source_path.display()
                    )));
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in file_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("file-list read worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    let total_bytes = stats.bytes_read.load(Ordering::Relaxed);
    let total_files = stats.files_read.load(Ordering::Relaxed) as usize;
    let elapsed = start.elapsed().as_secs_f64();
    if verbose {
        eprintln!(
            "file-list-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s ({:.1} files/s)",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9,
            total_files as f64 / elapsed.max(1e-9)
        );
    } else {
        println!(
            "file-list-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9
        );
    }
    Ok(total_bytes)
}

fn bench_file_list_read_uring(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = Arc::new(load_paths_from_manifest(Path::new(manifest_path))?);
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }
    raise_nofile_soft_limit(verbose);
    let use_direct = file_list_uring_should_use_direct(config, manifest_path, io_mode);
    let mut results = Vec::with_capacity(FILE_LIST_URING_INFLIGHT_SWEEP.len());
    for inflight_per_thread in FILE_LIST_URING_INFLIGHT_SWEEP {
        let result = run_file_list_uring_bench_once(
            files.clone(),
            inflight_per_thread,
            use_direct,
            verbose,
        )?;
        eprintln!(
            "result\tinflight/thread={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
            result.inflight_per_thread,
            result.elapsed_secs,
            result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
            result.total_files as f64 / result.elapsed_secs.max(1e-9)
        );
        results.push(result);
    }
    print_file_list_uring_sweep_results(&results);
    let best = results
        .iter()
        .min_by(|a, b| a.elapsed_secs.total_cmp(&b.elapsed_secs))
        .ok_or_else(|| io::Error::other("file-list io_uring sweep produced no results"))?;
    println!(
        "file-list-read-uring-bench best inflight/thread={} {} bytes across {} files in {:.4} s, {:.3} GB/s ({:.1} files/s)",
        best.inflight_per_thread,
        best.total_bytes,
        best.total_files,
        best.elapsed_secs,
        best.total_bytes as f64 / best.elapsed_secs.max(1e-9) / 1e9,
        best.total_files as f64 / best.elapsed_secs.max(1e-9)
    );
    Ok(best.total_bytes)
}

fn bench_file_list_read_open_read_close_sweep(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = load_paths_from_manifest(Path::new(manifest_path))?;
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }
    raise_nofile_soft_limit(verbose);
    let use_direct = file_list_uring_should_use_direct(config, manifest_path, io_mode);
    let mut results = Vec::new();
    let prefix_sweep = MANIFEST_READ_PREFIX_SWEEP
        .iter()
        .copied()
        .filter(|count| *count <= files.len())
        .chain(std::iter::once(files.len()))
        .collect::<Vec<_>>();
    for prefix_files in prefix_sweep {
        let prefix = load_manifest_prefix(&files, prefix_files);
        let st = run_manifest_blocking_once(prefix.clone(), 1, use_direct)?;
        eprintln!(
            "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
            st.prefix_files,
            st.variant.label(),
            st.elapsed_secs,
            st.total_bytes as f64 / st.elapsed_secs.max(1e-9) / 1e9,
            st.total_files as f64 / st.elapsed_secs.max(1e-9)
        );
        results.push(st);

        for threads in MANIFEST_MT_BLOCKING_THREADS {
            let result = run_manifest_blocking_once(prefix.clone(), threads, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }

        for qd in MANIFEST_ST_URING_QDS {
            let result = run_manifest_uring_once(prefix.clone(), 1, qd, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }

        for (threads, qd) in MANIFEST_MT_URING_CONFIGS {
            let result = run_manifest_uring_once(prefix.clone(), threads, qd, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }
    }

    print_manifest_read_sweep_results(&results);
    let best = results
        .iter()
        .filter(|result| result.prefix_files == files.len())
        .min_by(|a, b| a.elapsed_secs.total_cmp(&b.elapsed_secs))
        .ok_or_else(|| io::Error::other("manifest open-read-close sweep produced no results"))?;
    println!(
        "file-list-read-open-read-close-sweep best prefix_files={} variant={} {} bytes across {} files in {:.4} s, {:.3} GB/s ({:.1} files/s)",
        best.prefix_files,
        best.variant.label(),
        best.total_bytes,
        best.total_files,
        best.elapsed_secs,
        best.total_bytes as f64 / best.elapsed_secs.max(1e-9) / 1e9,
        best.total_files as f64 / best.elapsed_secs.max(1e-9)
    );
    Ok(best.total_bytes)
}

fn bench_manifest_recursive_copy(
    manifest_path: &str,
    source_root: &str,
    target_root: &str,
    overlap_large_file: Option<&str>,
    verbose: bool,
) -> io::Result<u64> {
    let manifest = Path::new(manifest_path);
    let source_root = Path::new(source_root);
    let target_root = Path::new(target_root);
    if target_root.exists() {
        fs::remove_dir_all(target_root)?;
    }
    fs::create_dir_all(target_root)?;

    let entries = load_manifest_copy_entries(manifest, source_root)?;
    if entries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found in manifest {}", manifest.display()),
        ));
    }
    let total_bytes = entries.iter().map(|entry| entry.size).sum::<u64>();

    let overall_start = std::time::Instant::now();
    let (dirs_created, dir_elapsed) = create_manifest_target_dirs(&entries, target_root)?;

    let source_root_fd = open_dir_fd(source_root)?;
    let target_root_fd = open_dir_fd(target_root)?;
    let file_start = std::time::Instant::now();

    let overlap_handle = overlap_large_file.map(|large_src| {
        let large_src = large_src.to_string();
        let large_dst = target_root.join(".fro_manifest_overlap_large_copy.bin");
        std::thread::spawn(move || {
            let large_src_str = large_src;
            let large_dst_str = large_dst.display().to_string();
            let start = std::time::Instant::now();
            let status = Command::new(env::current_exe()?)
                .arg("copy")
                .arg("-n")
                .arg("1")
                .arg(&large_src_str)
                .arg(&large_dst_str)
                .status()?;
            if !status.success() {
                return Err(io::Error::other(format!(
                    "overlap fro copy failed with status {status}"
                )));
            }
            let copied = fs::metadata(&large_dst)?.len();
            Ok::<(u64, std::time::Duration), io::Error>((copied, start.elapsed()))
        })
    });

    for entry in &entries {
        copy_small_file_openat(
            entry,
            &source_root_fd,
            &target_root_fd,
            RelativeCopyMethod::CopyFileRange,
        )?;
    }
    let file_elapsed = file_start.elapsed();

    let (overlap_bytes, overlap_elapsed) = match overlap_handle {
        Some(handle) => {
            let (bytes, elapsed) = handle
                .join()
                .map_err(|_| io::Error::other("overlap large-file copy worker panicked"))??;
            (Some(bytes), Some(elapsed))
        }
        None => (None, None),
    };

    let result = ManifestCopyBenchmarkResult {
        entries: entries.len(),
        bytes: total_bytes,
        dirs_created,
        dir_phase_secs: dir_elapsed.as_secs_f64(),
        file_phase_secs: file_elapsed.as_secs_f64(),
        total_secs: overall_start.elapsed().as_secs_f64(),
        overlap_secs: overlap_elapsed.map(|d| d.as_secs_f64()),
        overlap_large_file_bytes: overlap_bytes,
    };

    println!(
        "manifest-recursive-copy {} bytes across {} files: dirs={} dir_phase={:.4}s file_phase={:.4}s total={:.4}s file_gbps={:.3}",
        result.bytes,
        result.entries,
        result.dirs_created,
        result.dir_phase_secs,
        result.file_phase_secs,
        result.total_secs,
        result.bytes as f64 / result.file_phase_secs.max(1e-9) / 1e9
    );
    if let (Some(bytes), Some(secs)) = (result.overlap_large_file_bytes, result.overlap_secs) {
        println!(
            "manifest-recursive-copy overlap-large-file {} bytes in {:.4}s {:.3} GB/s",
            bytes,
            secs,
            bytes as f64 / secs.max(1e-9) / 1e9
        );
    }
    if verbose {
        eprintln!(
            "manifest-recursive-copy details: source_root={} target_root={} manifest={}",
            source_root.display(),
            target_root.display(),
            manifest.display()
        );
    }
    Ok(result.bytes)
}

fn print_verify_report(report: &block_hash::VerifyReport) {
    println!(
        "verify: loaded {}/3 hash replicas, ok_blocks={}, bad_blocks={}",
        report.loaded_manifests,
        report.ok_blocks,
        report.bad_blocks.len()
    );
    for issue in &report.bad_blocks {
        println!(
            "block {}: {}",
            issue.block_index,
            issue.decision.status_message()
        );
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some(message)
    } else if let Some(message) = payload.downcast_ref::<String>() {
        Some(message.as_str())
    } else {
        None
    }
}

fn is_broken_pipe_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

fn is_broken_pipe_panic(payload: &(dyn std::any::Any + Send)) -> bool {
    panic_payload_message(payload).is_some_and(|message| message.contains("Broken pipe"))
}

#[derive(Clone, Copy)]
struct CommandHelp {
    name: &'static str,
    usage: &'static str,
    summary: &'static str,
    notes: &'static [&'static str],
    examples: &'static [(&'static str, &'static str)],
}

fn is_help_flag(arg: &str) -> bool {
    arg == "--help" || arg == "-h"
}

fn is_version_flag(arg: &str) -> bool {
    arg == "--version"
}

fn print_version(program: &str) {
    println!("{program} {FRO_VERSION}");
}

fn command_help(name: &str) -> Option<CommandHelp> {
    match name {
        "read" => Some(CommandHelp {
            name: "read",
            usage: "read [--auto-lift] [--to-memory] [--paged-shared-buffer|--mmap|--mmap-read-pages|--multiple-target-buffers] [--threads N] [--qd N] [--blocksize SIZE] [--disable-hugepages] [--measure-unmap-time] [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Striped multi-threaded file read for measuring raw throughput on one file.",
            notes: &[
                "Use -n 1 for one measured run with the current tuned parameters.",
                "Use -s together with --direct or --no-direct to save the best result back to config.",
                "--auto-lift starts cold files on the direct path while a background thread warms the page cache for later iterations in the same process.",
                "--to-memory defaults to an auto backend: mmap when the first page looks cached, otherwise the direct/shared-buffer loader.",
                "--paged-shared-buffer forces the old shared destination-buffer loader for read --to-memory.",
                "--mmap maps the file instead of reading into a destination buffer; --mmap-read-pages also walks the mapped bytes in userspace.",
                "--multiple-target-buffers gives each reader thread its own destination buffer with no consolidation step.",
                "--to-memory uses hugepage advice automatically for files >= 64 MiB; smaller files default to non-hugepages. --disable-hugepages turns advice off entirely for the mapped or destination buffer backing.",
                "--measure-unmap-time keeps mmap teardown inside the timed region for --mmap and --mmap-read-pages.",
                "--threads, --qd, and --blocksize override the read-side tuned params so you can do one-off perf sweeps without editing fro.json.",
            ],
            examples: &[
                (
                    "Measure direct-IO read throughput once",
                    "read --direct -n 1 /mnt/fast/bigfile.dat",
                ),
                (
                    "Let read --to-memory auto-pick mmap vs direct based on cache state",
                    "read --to-memory -n 1 /mnt/fast/bigfile.dat",
                ),
                (
                    "Start cold reads on direct IO, then flip later iterations onto page cache",
                    "read --auto-lift -n 100 /mnt/fast/bigfile.dat",
                ),
                (
                    "Map a hot file and read all mapped bytes",
                    "read --to-memory --mmap-read-pages --no-direct -n 1 /mnt/fast/bigfile.dat",
                ),
            ],
        }),
        "grep" => Some(CommandHelp {
            name: "grep",
            usage: "grep [--auto-lift] [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <pattern> <filename>",
            summary: "Read plus literal byte-substring search over one file.",
            notes: &[
                "This is a literal substring search, not a regex engine.",
                "Matches are printed as offset:pattern.",
                "--auto-lift starts cold files on the direct path while a background thread warms the page cache for later iterations in the same process.",
            ],
            examples: &[
                (
                    "Scan a file in page cache for a literal marker string",
                    "grep --no-direct -n 1 needle /mnt/fast/bigfile.dat",
                ),
                (
                    "Start cold and improve over repeated scans of the same file",
                    "grep --auto-lift -n 100 needle /mnt/fast/bigfile.dat",
                ),
            ],
        }),
        "cat" => Some(CommandHelp {
            name: "cat",
            usage: "cat [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print one or more files using fro's fast read path.",
            notes: &["Useful as a compatibility wrapper over the same IO-mode flags as fro reads."],
            examples: &[("Print two files", "cat a.txt b.txt")],
        }),
        "base64" => Some(CommandHelp {
            name: "base64",
            usage: "base64 [-d|--decode] [-i|--ignore-garbage] [-w cols|--wrap=cols] [--auto|--no-direct|--direct] [file]",
            summary: "Encode or decode one file or stdin using RFC 4648 base64.",
            notes: &[
                "Without a file operand, or when the file is -, base64 reads standard input.",
                "Encoding wraps at 76 columns by default; use -w 0 to disable wrapping.",
                "--ignore-garbage only affects decode mode.",
            ],
            examples: &[
                ("Encode stdin without wrapping", "base64 -w 0 < input.bin"),
                ("Decode one file", "base64 -d payload.b64"),
            ],
        }),
        "cmp" => Some(CommandHelp {
            name: "cmp",
            usage: "cmp [--auto|--no-direct|--direct] <file1> <file2>",
            summary: "Compare two files using fro's diff engine and GNU cmp-style reporting.",
            notes: &["Exits nonzero on mismatch or size difference."],
            examples: &[("Compare two files", "cmp a.bin b.bin")],
        }),
        "fgrep" => Some(CommandHelp {
            name: "fgrep",
            usage: "fgrep [-n] [--auto|--no-direct|--direct] <pattern> <file> [file ...]",
            summary: "Literal line-oriented grep on top of fro's fast substring scanner.",
            notes: &["Matches GNU grep -F visible behavior for the covered compatibility matrix."],
            examples: &[("Print matching lines with numbers", "fgrep -n needle notes.txt")],
        }),
        "find" => Some(CommandHelp {
            name: "find",
            usage: "find [path ...]",
            summary: "Walk one or more directory trees and print every encountered path.",
            notes: &["This first correctness slice does not guarantee output ordering."],
            examples: &[("Walk the current tree", "find ."), ("Walk two roots", "find src tests")],
        }),
        "du" => Some(CommandHelp {
            name: "du",
            usage: "du [-s] [-a] [path ...]",
            summary: "Report disk usage from filesystem block counts for files and directories.",
            notes: &[
                "Without -s, directory arguments print descendant directory totals plus the root total.",
                "-a includes non-directory entries in the output.",
            ],
            examples: &[("Summarize one tree", "du -s ."), ("Print all entries in src", "du -a src")],
        }),
        "tac" => Some(CommandHelp {
            name: "tac",
            usage: "tac [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print files with line order reversed within each file.",
            notes: &[],
            examples: &[("Reverse one file by line", "tac notes.txt")],
        }),
        "wc" => Some(CommandHelp {
            name: "wc",
            usage: "wc [-l] [-w] [-c] [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Count lines, words, and bytes using fro block visitors.",
            notes: &["Without -l/-w/-c, prints all three counts."],
            examples: &[("Count lines and words", "wc -l -w notes.txt")],
        }),
        "dd" => Some(CommandHelp {
            name: "dd",
            usage: "dd if=<input> of=<output> [bs=<size>] [count=<blocks>] [skip=<blocks>] [seek=<blocks>] [iflag=direct] [oflag=direct] [conv=notrunc,fsync] [status=none|progress]",
            summary: "Copy byte ranges with dd-style operands on top of fro I/O primitives.",
            notes: &[
                "Whole-file copies without offset/count flags use the tuned copy path directly.",
                "Compatibility currently focuses on the covered operands from tests/dd_example_compat.rs.",
            ],
            examples: &[("Copy five 4 KiB blocks with no summary", "dd if=src.bin of=dst.bin bs=4K count=5 status=none")],
        }),
        "cksum" => Some(CommandHelp {
            name: "cksum",
            usage: "cksum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "POSIX cksum compatibility wrapper on top of fro file reads.",
            notes: &["TODO: replace the current CRC32 path with a fast-crc32-grade implementation."],
            examples: &[("Print POSIX CRC32 and size", "cksum archive.tar")],
        }),
        "b3sum" => Some(CommandHelp {
            name: "b3sum",
            usage: "b3sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print BLAKE3 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with BLAKE3", "b3sum bigfile.dat")],
        }),
        "b2sum" => Some(CommandHelp {
            name: "b2sum",
            usage: "b2sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print BLAKE2b-512 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with BLAKE2b-512", "b2sum bigfile.dat")],
        }),
        "md5sum" => Some(CommandHelp {
            name: "md5sum",
            usage: "md5sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print MD5 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with MD5", "md5sum bigfile.dat")],
        }),
        "sha224sum" => Some(CommandHelp {
            name: "sha224sum",
            usage: "sha224sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print SHA-224 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with SHA-224", "sha224sum bigfile.dat")],
        }),
        "sha256sum" => Some(CommandHelp {
            name: "sha256sum",
            usage: "sha256sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print SHA-256 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with SHA-256", "sha256sum bigfile.dat")],
        }),
        "sha384sum" => Some(CommandHelp {
            name: "sha384sum",
            usage: "sha384sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print SHA-384 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with SHA-384", "sha384sum bigfile.dat")],
        }),
        "sha512sum" => Some(CommandHelp {
            name: "sha512sum",
            usage: "sha512sum [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print SHA-512 digests for one or more files.",
            notes: &[],
            examples: &[("Hash one file with SHA-512", "sha512sum bigfile.dat")],
        }),
        "shred" => Some(CommandHelp {
            name: "shred",
            usage: "shred [-n passes] [-z] [-u] [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Overwrite files with random or zero patterns, optionally removing them.",
            notes: &["This compatibility surface currently focuses on the covered basic flags."],
            examples: &[("Zero a file once and keep it", "shred -n 0 -z scratch.bin")],
        }),
        "write" => Some(CommandHelp {
            name: "write",
            usage: "write [--create <size>] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Write a file using the tuned pipeline, optionally creating and sizing it first.",
            notes: &[
                "Without --create, the existing file size is used.",
                "--direct/--no-direct control the read-side planner mode; --direct-write/--no-direct-write control the write side.",
            ],
            examples: &[
                (
                    "Create a fresh 64 MiB file and fill it through the write path",
                    "write --create 64MiB --no-direct -n 1 out.bin",
                ),
                (
                    "Rewrite an existing file with direct writes",
                    "write --direct-write -n 1 out.bin",
                ),
            ],
        }),
        "copy" | "copy-via-memory" => Some(CommandHelp {
            name: "copy",
            usage: "copy [--recursive|-r|-R] [--via-memory] [--keep-target-size] [--diff|--full] [--copy-file-range|--copy-file-range-single|--threaded-copy|--reflink] [--no-lock] [--verify|--verify-diff] [--hash] [--xxh3|--sha256] [--hash-base path] [-q|--quiet] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <source> <target>",
            summary: "Copy one file, or recursively copy one directory tree, using the tuned read/write pipeline.",
            notes: &[
                "--direct/--no-direct/--auto control source reads.",
                "--direct-write/--no-direct-write/--auto-write control destination writes.",
                "--recursive (or -r/-R) enables directory-tree copies; the destination behaves like cp -r, so an existing destination directory receives the source basename as a child.",
                "--copy-file-range uses the tunable multi-call copy_file_range(2) strategy with its own optimizer params.",
                "--copy-file-range-single forces the one-call copy_file_range(2) baseline for benchmarking.",
                "--threaded-copy forces the existing tuned striped io_uring copy path.",
                "--reflink requests a CoW clone/reflink when the filesystem supports it; this is fast but does not promise physically independent storage blocks.",
                "--diff forces chunked diff-and-overwrite copy when supported; --full disables diffing and always rewrites the full file.",
                "Without either flag, plain copy uses copy auto mode: when source and target share a reflink-capable filesystem and the target storage topology is positively identified as redundant, auto prefers reflink; when the target topology is positively identified as non-redundant (for example RAID0, ZFS stripe, or a degraded mirror), auto forces a real full copy; otherwise it falls back to the existing cache-aware threaded heuristic.",
                "--keep-target-size preserves an already-sized destination instead of re-truncating/re-preallocating it; this is mainly useful for best-case benchmarking.",
                "Copy takes an advisory shared lock on the source and an advisory exclusive lock on the destination by default; use --no-lock to skip that cooperative locking.",
                "--via-memory loads the whole source file into RAM first, then writes that buffer to the destination.",
                "--verify hashes the source, copies into a temporary sibling, fsyncs and verifies that file, then renames it into place without leaving sidecars by default.",
                "--hash with --verify leaves durable sidecars at the destination and at the source if the source did not already have sidecars, and syncs their parent directories too.",
                "--verify-diff fsyncs the destination and then runs a diff pass instead of block-hash verification.",
                "For non-verified copy modes, fro also checks whether the source file's size/mtime/ctime changed during the operation and fails if it did.",
                "When using --via-memory, tune read and write separately instead of saving copy params.",
                "Verification success is reported to stderr unless --quiet is used.",
            ],
            examples: &[
                (
                    "Copy in.bin to out.bin",
                    "copy in.bin out.bin",
                ),
                (
                    "Recursively copy a tree into an existing destination directory",
                    "copy --recursive srcdir outdir",
                ),
                (
                    "Read through page cache but force direct writes to the destination",
                    "copy --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Benchmark the kernel copy_file_range syscall path directly",
                    "copy --copy-file-range-single --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Optimize the chunked copy_file_range path separately",
                    "copy --copy-file-range --no-direct -n 32 -s in.bin out.bin",
                ),
                (
                    "Force the existing striped io_uring copy path for comparison",
                    "copy --threaded-copy --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Request a CoW reflink/soft copy when the filesystem supports it",
                    "copy --reflink --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Load the whole source into RAM, then flush it with direct writes",
                    "copy --via-memory --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Skip advisory locking when cooperating lock semantics would get in the way",
                    "copy --no-lock --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file through a verified temp target swap without leaving sidecars",
                    "copy --verify --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file, verify it, and leave source/destination sidecars",
                    "copy --verify --hash --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file and run a diff pass after fsync",
                    "copy --verify-diff --no-direct in.bin out.bin",
                ),
            ],
        }),
        "diff" => Some(CommandHelp {
            name: "diff",
            usage: "diff [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Compare two files and report the first mismatch.",
            notes: &["Exits nonzero on mismatch."],
            examples: &[(
                "Check whether two large files are byte-identical",
                "diff --direct a.bin b.bin",
            )],
        }),
        "dual-read-bench" => Some(CommandHelp {
            name: "dual-read-bench",
            usage: "dual-read-bench [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Read two files like diff, but treat it as a throughput benchmark rather than a mismatch-reporting tool.",
            notes: &["Useful when you want the read pressure of diff without stopping to explain a mismatch."],
            examples: &[(
                "Benchmark reading two files from page cache",
                "dual-read-bench --no-direct -n 1 a.bin b.bin",
            )],
        }),
        "recursive-read-bench" => Some(CommandHelp {
            name: "recursive-read-bench",
            usage: "recursive-read-bench [--auto|--no-direct|--direct] [-v] <directory>",
            summary: "Read every byte of every regular file in a directory tree and report aggregate throughput.",
            notes: &[
                "This is intended as a read-side roofline for recursive copy/cp measurements.",
                "Symlinks and non-regular files are skipped.",
            ],
            examples: &[(
                "Benchmark the page-cache read roofline of a source tree",
                "recursive-read-bench --no-direct /data/tree",
            )],
        }),
        "file-list-read-bench" => Some(CommandHelp {
            name: "file-list-read-bench",
            usage: "file-list-read-bench [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Read every file named in a newline-delimited manifest and report aggregate throughput without tree-walk cost.",
            notes: &[
                "Use this to separate traversal cost from pure many-small-file read throughput.",
                "Each non-empty line in the manifest is treated as one path.",
            ],
            examples: &[(
                "Benchmark reading files listed by a prior find pass",
                "find /data/tree -type f > files.txt && file-list-read-bench --no-direct files.txt",
            )],
        }),
        "file-list-read-uring-bench" => Some(CommandHelp {
            name: "file-list-read-uring-bench",
            usage: "file-list-read-uring-bench [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Sweep many-small-file io_uring reads from a manifest using 32 worker threads and varying in-flight file counts.",
            notes: &[
                "Each worker issues separate whole-file reads via its own io_uring.",
                "Sweeps in-flight files per thread over 32, 64, 128, 256, and 512.",
            ],
            examples: &[(
                "Benchmark a many-open-files io_uring approach against a prior find manifest",
                "find /data/tree -type f > files.txt && file-list-read-uring-bench --auto files.txt",
            )],
        }),
        "file-list-read-open-read-close-sweep" => Some(CommandHelp {
            name: "file-list-read-open-read-close-sweep",
            usage: "file-list-read-open-read-close-sweep [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Compare open-read-close manifest readers across blocking and io_uring variants over file-count prefixes.",
            notes: &[
                "Runs single-thread, multi-thread, single-thread io_uring, and multi-thread io_uring variants.",
                "Useful for finding the file-count inflection points between reader designs.",
            ],
            examples: &[(
                "Sweep manifest reader variants over small-file prefixes",
                "find /data/tree -type f > files.txt && file-list-read-open-read-close-sweep --auto files.txt",
            )],
        }),
        "manifest-recursive-copy-bench" => Some(CommandHelp {
            name: "manifest-recursive-copy-bench",
            usage: "manifest-recursive-copy-bench [--overlap-large-file PATH] [-v] <manifest> <source_root> <target_root>",
            summary: "Benchmark manifest-driven recursive copy with separate directory-build and file-copy phase timing.",
            notes: &[
                "The manifest must list regular files under <source_root>.",
                "Keep source and target on the same mount when benchmarking the openat+copy_file_range small-file phase.",
                "--overlap-large-file runs one additional large-file copy in parallel with the file-copy phase.",
            ],
            examples: &[(
                "Time recursive copy phases and overlap one large file",
                "manifest-recursive-copy-bench --overlap-large-file /data/ilmari_cache/fro-test/coreutils-1g.bin files.txt /data/tree /data/out",
            )],
        }),
        "split-manifest-recursive-copy-bench" => Some(CommandHelp {
            name: "split-manifest-recursive-copy-bench",
            usage: "split-manifest-recursive-copy-bench [-v] <source_dir> <target_dir>",
            summary: "Benchmark a recursive copy design that first builds the full manifest, then dispatches copy work.",
            notes: &[
                "Uses the same recursive copy lanes as copy --recursive, but delays file-copy dispatch until the full manifest is built.",
                "This exists to compare walk-as-you-go scheduling against a split manifest-build -> dispatch design.",
            ],
            examples: &[(
                "Benchmark split-manifest recursive copy on one tree",
                "split-manifest-recursive-copy-bench /data/tree /data/out",
            )],
        }),
        "bench-recursive-small-file-threads" => Some(CommandHelp {
            name: "bench-recursive-small-file-threads",
            usage: "bench-recursive-small-file-threads [--auto|--no-direct|--direct] [--hot|--cold] [-v] [-s] <directory>",
            summary: "Sweep recursive small-file worker counts for the current cache state and optionally save the per-mount winner.",
            notes: &[
                "The current cache state is inferred from the directory path's first-page residency.",
                "When used with --save, only the hot or cold slot for the current mount is updated.",
            ],
            examples: &[(
                "Tune recursive small-file worker count for the current mount/cache state",
                "bench-recursive-small-file-threads --auto -s /data/tree",
            )],
        }),
        "bench-read-sweep" => Some(CommandHelp {
            name: "bench-read-sweep",
            usage: "bench-read-sweep",
            summary: "Sweep single-file read performance across size buckets for simple ST, ST io_uring, and current MT readers.",
            notes: &[
                "Uses temporary files sized 4 KiB through 256 MiB.",
                "Reports effective GB/s and, when built with the read-phase-timing feature, MT phase timestamps.",
            ],
            examples: &[(
                "Run the reader crossover sweep",
                "bench-read-sweep",
            )],
        }),
        "hash" => Some(CommandHelp {
            name: "hash",
            usage: "hash [--auto|--no-direct|--direct] [--xxh3|--sha256] [--hash-only] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Hash a file in parallel 1 MiB blocks, hash the hashes, and write three JSON sidecar replicas.",
            notes: &[
                "Default sidecar base is <file>.fro-hash.",
                "Use --xxh3 or --sha256 to choose the sidecar digest algorithm. (NB: this is not sha256sum-compatible.)",
                "Use --hash-only to only print the filename and hash of hashes.",
                "Default -n for hash is 1.",
            ],
            examples: &[(
                "Create block-hash sidecars for one large file",
                "hash --no-direct bigfile.dat",
            ),
            (
                "Get a SHA256 hash of block hashes for easy file comparisons",
                "hash --sha256 --hash-only bigfile.dat",
            )],
        }),
        "verify" => Some(CommandHelp {
            name: "verify",
            usage: "verify [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Re-hash a file, compare it to its sidecars, and report bad blocks.",
            notes: &[
                "Read-only command.",
                "verify follows the hash type stored in the sidecar manifest.",
                "On a clean file with intact sidecars, verify hashes the file once and stops.",
            ],
            examples: &[(
                "Scrub one file against its block-hash sidecars",
                "verify --no-direct bigfile.dat",
            )],
        }),
        "recover" => Some(CommandHelp {
            name: "recover",
            usage: "recover [--auto|--no-direct|--direct] [--fast] [--in-place-all] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <target> <copy1> [copy2 ...]",
            summary: "Repair corrupted 1 MiB blocks using one or more full-file replicas.",
            notes: &[
                "Default recover rewrites only the first file; later files are read-only sources.",
                "--fast behaves like verify on the first file unless corruption forces a full multi-file scan.",
                "--in-place-all attempts to repair every input file and refresh broken sidecars.",
                "recover follows each file's stored sidecar hash type.",
            ],
            examples: &[
                (
                    "Repair a target file from one clean copy",
                    "recover --no-direct target.bin backup.bin",
                ),
                (
                    "Use verify-like fast scrub behavior and only fall back to full recovery if needed",
                    "recover --fast --no-direct target.bin backup.bin",
                ),
            ],
        }),
        "bench-diff" => Some(CommandHelp {
            name: "bench-diff",
            usage: "bench-diff",
            summary: "In-memory diff microbenchmark used by the benchmark harness.",
            notes: &["This is mainly for development and regression tracking."],
            examples: &[("Run the in-memory diff microbenchmark", "bench-diff")],
        }),
        "bench-memcpy" => Some(CommandHelp {
            name: "bench-memcpy",
            usage: "bench-memcpy [--size <bytes>] [--threads <count>]",
            summary: "In-memory memcpy microbenchmark for establishing the RAM copy ceiling.",
            notes: &[
                "Defaults to --size 4GiB and --threads 32.",
                "Reports effective bandwidth as source read plus destination write bytes.",
            ],
            examples: &[(
                "Benchmark a 4 GiB to 4 GiB memcpy with 32 threads",
                "bench-memcpy --size 4GiB --threads 32",
            )],
        }),
        "bench-base64-encode" => Some(CommandHelp {
            name: "bench-base64-encode",
            usage: "bench-base64-encode [-n iterations] [--variant auto|scalar|spmd|shuffle]",
            summary: "Hot-loop the in-memory 12 KiB -> 16 KiB base64 encode kernel on one core.",
            notes: &[
                "Uses a fixed 12 KiB source buffer and fixed 16 KiB destination buffer.",
                "Reports iterations per second and effective input GB/s per core.",
                "Use --variant to compare scalar, AVX2 SPMD, and AVX2 shuffle-unpack kernels.",
            ],
            examples: &[(
                "Run one million kernel iterations",
                "bench-base64-encode -n 1000000",
            )],
        }),
        "bench-base64-decode" => Some(CommandHelp {
            name: "bench-base64-decode",
            usage: "bench-base64-decode [-n iterations] [--variant auto|scalar|avx2]",
            summary: "Hot-loop the in-memory 16 KiB -> 12 KiB base64 decode kernel on one core.",
            notes: &[
                "Uses a fixed 16 KiB encoded buffer generated from a fixed 12 KiB source buffer.",
                "Reports iterations per second and effective decoded GB/s per core.",
                "Use --variant to compare scalar and AVX2 decode kernels.",
            ],
            examples: &[(
                "Run one million decode kernel iterations",
                "bench-base64-decode -n 1000000",
            )],
        }),
        "bench-base64-decode-detect-fallback" => Some(CommandHelp {
            name: "bench-base64-decode-detect-fallback",
            usage: "bench-base64-decode-detect-fallback [-n iterations] [--variant auto|scalar|avx2]",
            summary: "Hot-loop decode with a pre-scan that falls back to the wrapped/dirty path when needed.",
            notes: &[
                "Uses clean unwrapped base64 generated from the fixed 12 KiB source buffer.",
                "Measures the cost of checking for garbage/newlines before choosing the fast decode kernel.",
            ],
            examples: &[(
                "Run one million detect+fallback decode iterations",
                "bench-base64-decode-detect-fallback -n 1000000 --variant avx2",
            )],
        }),
        "bench-base64-wrapped-encode" => Some(CommandHelp {
            name: "bench-base64-wrapped-encode",
            usage: "bench-base64-wrapped-encode [-n iterations] [--wrap COLS]",
            summary: "Hot-loop the wrapped base64 encode path on one core.",
            notes: &[
                "Uses the wrapped slow-path implementation over a fixed 12 KiB source buffer.",
                "Reports iterations per second and effective input GB/s per core.",
            ],
            examples: &[(
                "Run one million wrapped encode iterations at 76 columns",
                "bench-base64-wrapped-encode -n 1000000 --wrap 76",
            )],
        }),
        "bench-base64-wrapped-decode" => Some(CommandHelp {
            name: "bench-base64-wrapped-decode",
            usage: "bench-base64-wrapped-decode [-n iterations] [--ignore-garbage]",
            summary: "Hot-loop the wrapped/dirty base64 decode reorganization path on one core.",
            notes: &[
                "Uses wrapped base64 generated from the fixed 12 KiB source buffer.",
                "Reports iterations per second and effective decoded GB/s per core.",
            ],
            examples: &[(
                "Run one million wrapped decode iterations",
                "bench-base64-wrapped-decode -n 1000000",
            )],
        }),
        "bench-mmap-write" => Some(CommandHelp {
            name: "bench-mmap-write",
            usage: "bench-mmap-write <filename>",
            summary: "Memory-mapped write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the mmap write microbenchmark against an existing file",
                "bench-mmap-write out.bin",
            )],
        }),
        "bench-write" => Some(CommandHelp {
            name: "bench-write",
            usage: "bench-write <filename>",
            summary: "Plain write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the plain write microbenchmark against an existing file",
                "bench-write out.bin",
            )],
        }),
        _ => None,
    }
}

fn print_command_help(program: &str, help: CommandHelp) {
    println!("{} - {}", help.name, help.summary);
    println!();
    println!("USAGE:");
    println!("  {} {}", program, help.usage);
    if !help.notes.is_empty() {
        println!();
        println!("NOTES:");
        for note in help.notes {
            println!("  - {}", note);
        }
    }
    if !help.examples.is_empty() {
        println!();
        println!("EXAMPLES:");
        for (description, command) in help.examples {
            println!("  {}", description);
            println!("    {} {}", program, command);
        }
    }
}

fn print_general_help(program: &str) {
    println!("fast_read_optimizer (fro)");
    println!(
        "High-throughput Linux file IO utilities with companion benchmark and optimizer tooling."
    );
    println!();
    println!("USAGE:");
    println!("  {} <command> [options]", program);
    println!("  {} <command> --help", program);
    println!();
    println!("Utilities:");
    for (name, summary) in [
        ("cat", "print files using the fro read path"),
        ("base64", "encode or decode base64 data"),
        ("cmp", "compare two files using the fro diff engine"),
        ("dd", "copy byte ranges with dd-style operands"),
        ("fgrep", "literal line-oriented grep compatibility wrapper"),
        ("find", "walk directory trees and print every path"),
        ("du", "report disk usage from filesystem block counts"),
        ("grep", "search for a literal byte substring while reading"),
        ("tac", "print files in reverse line order"),
        ("wc", "count lines, words, and bytes"),
        ("cksum", "POSIX cksum compatibility wrapper"),
        ("b3sum", "print BLAKE3 digests"),
        ("b2sum", "print BLAKE2b-512 digests"),
        ("md5sum", "print MD5 digests"),
        ("sha224sum", "print SHA-224 digests"),
        ("sha256sum", "print SHA-256 digests"),
        ("sha384sum", "print SHA-384 digests"),
        ("sha512sum", "print SHA-512 digests"),
        ("shred", "overwrite files with patterns"),
        (
            "write",
            "rewrite or create a file through the tuned write path",
        ),
        (
            "copy",
            "copy one file to another with tuned read/write settings",
        ),
        ("diff", "compare two files and report the first mismatch"),
        (
            "recursive-read-bench",
            "read every byte of every file in a tree",
        ),
        (
            "file-list-read-bench",
            "read every file named in a manifest",
        ),
        (
            "file-list-read-uring-bench",
            "sweep many-small-file io_uring manifest reads",
        ),
        (
            "file-list-read-open-read-close-sweep",
            "compare manifest open-read-close reader variants",
        ),
        (
            "manifest-recursive-copy-bench",
            "benchmark manifest-driven recursive copy phases",
        ),
        (
            "split-manifest-recursive-copy-bench",
            "benchmark split manifest-build recursive copy",
        ),
        (
            "bench-recursive-small-file-threads",
            "sweep recursive small-file worker counts",
        ),
        ("hash", "write 1 MiB block-hash sidecars"),
        ("verify", "scrub a file against its block-hash sidecars"),
        (
            "recover",
            "repair corrupted blocks from one or more replicas",
        ),
    ] {
        println!("  {:<16} {}", name, summary);
    }
    println!();
    println!("Benchmarks:");
    println!("  read               measure striped file read throughput");
    println!("  dual-read-bench    benchmark the read pressure of diff");
    println!("  recursive-read-bench benchmark aggregate read throughput of a tree");
    println!("  file-list-read-bench benchmark aggregate read throughput from a file manifest");
    println!("  file-list-read-uring-bench sweep io_uring aggregate throughput from a file manifest");
    println!("  file-list-read-open-read-close-sweep compare manifest reader variants across file-count prefixes");
    println!("  manifest-recursive-copy-bench benchmark manifest-driven recursive copy phase timing");
    println!("  split-manifest-recursive-copy-bench benchmark split manifest-build recursive copy timing");
    println!("  bench-recursive-small-file-threads sweep recursive small-file worker counts and save hot/cold per mount");
    println!("  bench-read-sweep  sweep read variants across file sizes");
    println!("  fro-optimize       tune configs for one or more commands / mounts");
    println!("  fro-benchmark      run the regression benchmark suite");
    println!("  bench-diff         in-memory diff microbenchmark");
    println!("  bench-memcpy       in-memory memcpy microbenchmark");
    println!("  bench-base64-encode base64 encode kernel microbenchmark");
    println!("  bench-base64-decode base64 decode kernel microbenchmark");
    println!("  bench-base64-wrapped-encode wrapped base64 encode path microbenchmark");
    println!("  bench-base64-wrapped-decode wrapped base64 decode path microbenchmark");
    println!("  bench-mmap-write   mmap write microbenchmark");
    println!("  bench-write        plain write microbenchmark");
    println!();
    println!("Common flags:");
    println!("  --auto | --no-direct | --direct");
    println!("  --auto-write | --no-direct-write | --direct-write");
    println!("  -n <iterations>    use -n 1 for one measured run with current tuned params");
    println!("  -s, --save         save tuned params when forcing --direct or --no-direct");
    println!("  -c, --config PATH  override config path");
    println!("  -v, --verbose      print more about the current run");
    println!();
    println!("Coreutils compatibility names:");
    println!(
        "  cp cmp dd fgrep find du cat base64 tac wc cksum b3sum b2sum md5sum sha224sum sha256sum sha384sum sha512sum shred"
    );
    println!("  (use as `fro <name> ...` or invoke via argv[0] multicall)");
    println!();
    println!("Related tools:");
    println!("  ./target/release/fro-optimize --help");
    println!("  ./target/release/fro-benchmark --help");
    println!();
    println!(
        "Config resolution (when -c is not provided): $FRO_CONFIG, then ~/.fro/fro.json, then /etc/fro.json"
    );
}

fn try_main() -> io::Result<i32> {
    let raw_args: Vec<String> = env::args().collect();
    if let Some(code) = coreutils::try_run_multicall(&raw_args)? {
        return Ok(code);
    }
    if raw_args
        .get(1)
        .is_some_and(|arg| is_version_flag(arg.as_str()))
    {
        print_version(raw_args[0].as_str());
        return Ok(0);
    }
    let args = coreutils::rewrite_subcommand_alias(coreutils::rewrite_alias_args(raw_args));
    if args.len() < 2 || is_help_flag(args[1].as_str()) {
        print_general_help(args[0].as_str());
        return Ok(0);
    }
    if args.len() >= 3 && is_help_flag(args[2].as_str()) {
        if let Some(help) = command_help(args[1].as_str()) {
            print_command_help(args[0].as_str(), help);
        } else {
            eprintln!("Unknown command: {}", args[1]);
            println!();
            print_general_help(args[0].as_str());
        }
        return Ok(0);
    }
    if let Some(code) =
        coreutils::try_run_subcommand(args[0].as_str(), args[1].as_str(), &args[2..])?
    {
        return Ok(code);
    }
    let legacy_copy_via_memory = args[1] == "copy-via-memory";
    let mode = if legacy_copy_via_memory {
        "copy"
    } else {
        args[1].as_str()
    };
    let mut io_mode = common::IOMode::Auto;
    let mut io_mode_write = common::IOMode::Auto;
    let mut to_memory = false;
    let mut auto_lift = false;
    let mut to_memory_mode = ReadToMemoryMode::Auto;
    let mut to_memory_options = ReadToMemoryOptions::default();
    let mut manual_read_overrides = ManualReadOverrides::default();
    let mut via_memory = legacy_copy_via_memory;
    let mut verify_copy = false;
    let mut verify_copy_diff = false;
    let mut recursive_copy = false;
    let mut persist_verification_hashes = false;
    let mut quiet = false;
    let mut no_lock = false;
    let mut keep_target_size = false;
    let mut force_diff_copy = false;
    let mut force_full_copy = false;
    let mut force_copy_file_range = false;
    let mut force_copy_file_range_single = false;
    let mut force_threaded_copy = false;
    let mut force_reflink = false;
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut extra_paths: Vec<String> = Vec::new();
    let mut hash_base: Option<&str> = None;
    let mut recover_mode = RecoverMode::Standard;
    let mut hash_type = BlockHashAlgorithm::Xxh3;
    let mut hash_only = false;
    let mut recover_fast_requested = false;
    let mut recover_in_place_all_requested = false;
    let mut create_size: Option<u64> = None;
    let mut iterations = if mode == "read" {
        1000
    } else if mode == "bench-base64-encode"
        || mode == "bench-base64-decode"
        || mode == "bench-base64-decode-detect-fallback"
        || mode == "bench-base64-wrapped-encode"
        || mode == "bench-base64-wrapped-decode"
    {
        1_000_000
    } else {
        1
    };
    let mut save_config = false;
    let mut config_path: Option<&str> = None;
    let mut bench_size: Option<u64> = None;
    let mut bench_threads: Option<usize> = None;
    let mut base64_kernel = coreutils::Base64EncodeKernel::Auto;
    let mut base64_decode_kernel = coreutils::Base64DecodeKernel::Auto;
    let mut base64_wrap_cols: usize = 76;
    let mut base64_ignore_garbage = false;
    let mut small_file_thread_cache_state: Option<SmallFileThreadCacheState> = None;
    let mut overlap_large_file: Option<&str> = None;

    let mut i = 2;
    let mut end_flags = false;
    while i < args.len() {
        let is_flag = !end_flags && args[i].starts_with("-");
        if is_flag {
            if args[i] == "--" {
                end_flags = true;
            } else if args[i] == "--help" {
                if let Some(help) = command_help(args[1].as_str()) {
                    print_command_help(args[0].as_str(), help);
                } else {
                    eprintln!("Unknown command: {}", args[1]);
                    println!();
                    print_general_help(args[0].as_str());
                }
                return Ok(0);
            } else if args[i] == "-c" || args[i] == "--config" {
                i += 1;
                if i < args.len() {
                    config_path = Some(args[i].as_str());
                }
            } else if args[i] == "--hash-base" {
                i += 1;
                if i < args.len() {
                    hash_base = Some(args[i].as_str());
                }
            } else if args[i] == "--overlap-large-file" {
                i += 1;
                if i < args.len() {
                    overlap_large_file = Some(args[i].as_str());
                }
            } else if args[i] == "--size" {
                i += 1;
                if i < args.len() {
                    bench_size = parse_size(args[i].as_str()).or_else(|| {
                        eprintln!("Invalid --size: {}", args[i]);
                        None
                    });
                    if bench_size.is_none() {
                        return Ok(1);
                    }
                }
            } else if args[i] == "--variant" {
                i += 1;
                if i < args.len() {
                    if mode == "bench-base64-encode" {
                        base64_kernel = coreutils::parse_base64_encode_kernel(args[i].as_str())?;
                    } else if mode == "bench-base64-decode"
                        || mode == "bench-base64-decode-detect-fallback"
                    {
                        base64_decode_kernel =
                            coreutils::parse_base64_decode_kernel(args[i].as_str())?;
                    } else {
                        eprintln!("--variant is only supported for bench-base64-encode/decode");
                        return Ok(1);
                    }
                }
            } else if args[i] == "--wrap" {
                i += 1;
                if i < args.len() {
                    base64_wrap_cols = args[i].parse().map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid wrap size: {}", err),
                        )
                    })?;
                }
            } else if args[i] == "--ignore-garbage" {
                base64_ignore_garbage = true;
            } else if args[i] == "--hot" {
                small_file_thread_cache_state = Some(SmallFileThreadCacheState::Hot);
            } else if args[i] == "--cold" {
                small_file_thread_cache_state = Some(SmallFileThreadCacheState::Cold);
            } else if args[i] == "--threads" {
                i += 1;
                if i < args.len() {
                    if mode == "bench-memcpy" {
                        bench_threads = Some(args[i].parse().map_err(|err| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid thread count: {}", err),
                            )
                        })?);
                    } else {
                        manual_read_overrides.threads = Some(args[i].parse().map_err(|err| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid thread count: {}", err),
                            )
                        })?);
                    }
                }
            } else if args[i] == "--qd" {
                i += 1;
                if i < args.len() {
                    manual_read_overrides.qd = Some(args[i].parse().map_err(|err| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("invalid qd: {}", err))
                    })?);
                }
            } else if args[i] == "--blocksize" {
                i += 1;
                if i < args.len() {
                    manual_read_overrides.block_size = parse_size(args[i].as_str()).or_else(|| {
                        eprintln!("Invalid --blocksize: {}", args[i]);
                        None
                    });
                    if manual_read_overrides.block_size.is_none() {
                        return Ok(1);
                    }
                }
            } else if args[i] == "--create" {
                i += 1;
                if i < args.len() {
                    create_size = parse_size(args[i].as_str()).or_else(|| {
                        eprintln!("Invalid --create size: {}", args[i]);
                        None
                    });
                    if create_size.is_none() {
                        return Ok(1);
                    }
                }
            } else if args[i] == "--fast" {
                recover_fast_requested = true;
                recover_mode = RecoverMode::Fast;
            } else if args[i] == "--in-place-all" {
                recover_in_place_all_requested = true;
                recover_mode = RecoverMode::InPlaceAll;
            } else if args[i] == "--sha256" {
                hash_type = BlockHashAlgorithm::Sha256;
            } else if args[i] == "--xxh3" {
                hash_type = BlockHashAlgorithm::Xxh3;
            } else if args[i] == "--hash-only" {
                hash_only = true;
            } else if args[i] == "--direct" {
                io_mode = common::IOMode::Direct;
                io_mode_write = common::IOMode::Direct;
            } else if args[i] == "--no-direct" {
                io_mode = common::IOMode::PageCache;
                io_mode_write = common::IOMode::PageCache;
            } else if args[i] == "--auto" {
                io_mode = common::IOMode::Auto;
                io_mode_write = common::IOMode::Auto;
            } else if args[i] == "--direct-write" {
                io_mode_write = common::IOMode::Direct;
            } else if args[i] == "--no-direct-write" {
                io_mode_write = common::IOMode::PageCache;
            } else if args[i] == "--auto-write" {
                io_mode_write = common::IOMode::Auto;
            } else if args[i] == "--to-memory" {
                to_memory = true;
            } else if args[i] == "--auto-lift" {
                auto_lift = true;
            } else if args[i] == "--paged-shared-buffer" {
                to_memory_mode = ReadToMemoryMode::PagedSharedBuffer;
            } else if args[i] == "--mmap" {
                to_memory_mode = ReadToMemoryMode::Mmap;
            } else if args[i] == "--mmap-read-pages" {
                to_memory_mode = ReadToMemoryMode::MmapReadPages;
            } else if args[i] == "--multiple-target-buffers" {
                to_memory_mode = ReadToMemoryMode::MultipleTargetBuffers;
            } else if args[i] == "--disable-hugepages" {
                to_memory_options.hugepages = HugepageAdvice::Disabled;
            } else if args[i] == "--measure-unmap-time" {
                to_memory_options.measure_unmap_time = true;
            } else if args[i] == "--via-memory" {
                via_memory = true;
            } else if args[i] == "-r" || args[i] == "-R" || args[i] == "--recursive" {
                recursive_copy = true;
            } else if args[i] == "--verify" || args[i] == "--verified" {
                verify_copy = true;
            } else if args[i] == "--verify-diff" {
                verify_copy_diff = true;
            } else if args[i] == "--hash" {
                persist_verification_hashes = true;
            } else if args[i] == "--no-lock" {
                no_lock = true;
            } else if args[i] == "--keep-target-size" {
                keep_target_size = true;
            } else if args[i] == "--diff" {
                force_diff_copy = true;
            } else if args[i] == "--full" {
                force_full_copy = true;
            } else if args[i] == "--copy-file-range" {
                force_copy_file_range = true;
            } else if args[i] == "--copy-file-range-single" {
                force_copy_file_range_single = true;
            } else if args[i] == "--threaded-copy" {
                force_threaded_copy = true;
            } else if args[i] == "--reflink" {
                force_reflink = true;
            } else if args[i] == "-q" || args[i] == "--quiet" {
                quiet = true;
            } else if args[i] == "-v" || args[i] == "--verbose" {
                verbose = true;
            } else if args[i] == "-s" || args[i] == "--save" {
                save_config = true;
            } else if args[i] == "-n" {
                i += 1;
                if i < args.len() {
                    iterations = args[i].parse().map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid number of iterations: {}", err),
                        )
                    })?;
                }
            } else {
                eprintln!("Unknown flag for {}: {}", args[0], args[i]);
                println!();
                if let Some(help) = command_help(args[0].as_str()) {
                    print_command_help(args[0].as_str(), help);
                }
                return Ok(0);
            }
        } else if mode == "copy"
            || mode == "diff"
            || mode == "dual-read-bench"
            || mode == "split-manifest-recursive-copy-bench"
        {
            if source.is_none() {
                source = Some(args[i].as_str());
            } else if filename == "" {
                filename = args[i].as_str();
            }
        } else if mode == "manifest-recursive-copy-bench" {
            if filename == "" {
                filename = args[i].as_str();
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "recover" {
            if filename == "" {
                filename = args[i].as_str();
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "grep" {
            if pattern == "" {
                pattern = args[i].as_str();
            } else {
                filename = args[i].as_str();
            }
        } else {
            filename = args[i].as_str();
        }
        i += 1;
    }
    if mode == "bench-diff" {
        bench_diff_memory(16, 1024 * 1024);
        return Ok(0);
    }
    if mode == "bench-read-sweep" {
        let mut config = config::load_config(config_path);
        run_bench_read_sweep(&mut config)?;
        return Ok(0);
    }
    if mode == "bench-memcpy" {
        let total_size = bench_size.unwrap_or(4 * 1024 * 1024 * 1024);
        let num_threads = bench_threads.unwrap_or(32);
        if num_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--threads must be greater than zero",
            ));
        }
        let total_size = usize::try_from(total_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bench size does not fit in usize: {}", total_size),
            )
        })?;
        bench_memcpy_memory(num_threads, total_size);
        return Ok(0);
    }
    if mode == "bench-base64-encode" {
        coreutils::bench_base64_encode(iterations as u64, base64_kernel)?;
        return Ok(0);
    }
    if mode == "bench-base64-decode" {
        coreutils::bench_base64_decode(iterations as u64, base64_decode_kernel)?;
        return Ok(0);
    }
    if mode == "bench-base64-decode-detect-fallback" {
        coreutils::bench_base64_decode_detect_fallback(iterations as u64, base64_decode_kernel)?;
        return Ok(0);
    }
    if mode == "bench-base64-wrapped-encode" {
        coreutils::bench_base64_wrapped_encode(iterations as u64, base64_wrap_cols)?;
        return Ok(0);
    }
    if mode == "bench-base64-wrapped-decode" {
        coreutils::bench_base64_wrapped_decode(iterations as u64, base64_ignore_garbage)?;
        return Ok(0);
    }
    if mode == "bench-mmap-write" {
        if filename == "" {
            println!("Filename missing");
            return Ok(1);
        }
        writer::bench_mmap_write(filename);
        return Ok(0);
    }
    if mode == "bench-write" {
        if filename == "" {
            println!("Filename missing");
            return Ok(1);
        }
        writer::bench_write(filename);
        return Ok(0);
    }

    if filename == "" {
        println!("Filename missing");
        return Ok(1);
    }
    if to_memory && mode != "read" {
        println!("--to-memory is only supported for read");
        return Ok(1);
    }
    if auto_lift && mode != "read" && mode != "grep" {
        println!("--auto-lift is only supported for read and grep");
        return Ok(1);
    }
    if auto_lift && to_memory {
        println!("--auto-lift is not supported with read --to-memory");
        return Ok(1);
    }
    if auto_lift && io_mode != common::IOMode::Auto {
        println!("--auto-lift chooses between direct and page-cache itself; do not combine it with --auto, --no-direct, or --direct");
        return Ok(1);
    }
    if to_memory_mode != ReadToMemoryMode::Auto && !to_memory {
        println!(
            "--paged-shared-buffer, --mmap, --mmap-read-pages, and --multiple-target-buffers require read --to-memory"
        );
        return Ok(1);
    }
    if matches!(to_memory_options.hugepages, HugepageAdvice::Disabled) && !to_memory {
        println!("--disable-hugepages requires read --to-memory");
        return Ok(1);
    }
    if to_memory_options.measure_unmap_time && !to_memory {
        println!("--measure-unmap-time requires read --to-memory");
        return Ok(1);
    }
    if matches!(
        to_memory_mode,
        ReadToMemoryMode::Mmap | ReadToMemoryMode::MmapReadPages
    ) && io_mode == common::IOMode::Direct
    {
        println!("--mmap and --mmap-read-pages are not supported with --direct");
        return Ok(1);
    }
    if manual_read_overrides.any()
        && mode != "read"
        && mode != "recursive-read-bench"
        && mode != "file-list-read-bench"
        && mode != "file-list-read-uring-bench"
        && mode != "file-list-read-open-read-close-sweep"
        && mode != "manifest-recursive-copy-bench"
    {
        println!("--threads, --qd, and --blocksize overrides are only supported for read-style benchmarks");
        return Ok(1);
    }
    if via_memory && mode != "copy" {
        println!("--via-memory is only supported for copy");
        return Ok(1);
    }
    if recursive_copy && mode != "copy" {
        println!("--recursive is only supported for copy");
        return Ok(1);
    }
    if (force_copy_file_range
        || force_copy_file_range_single
        || force_threaded_copy
        || force_reflink)
        && mode != "copy"
    {
        println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink are only supported for copy"
        );
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && mode != "copy" {
        println!("--verify and --verify-diff are only supported for copy");
        return Ok(1);
    }
    if usize::from(force_copy_file_range)
        + usize::from(force_copy_file_range_single)
        + usize::from(force_threaded_copy)
        + usize::from(force_reflink)
        > 1
    {
        println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink cannot be combined"
        );
        return Ok(1);
    }
    if no_lock && mode != "copy" {
        println!("--no-lock is only supported for copy");
        return Ok(1);
    }
    if keep_target_size && mode != "copy" {
        println!("--keep-target-size is only supported for copy");
        return Ok(1);
    }
    if (force_diff_copy || force_full_copy) && mode != "copy" {
        println!("--diff and --full are only supported for copy");
        return Ok(1);
    }
    if force_diff_copy && force_full_copy {
        println!("copy --diff and --full cannot be combined");
        return Ok(1);
    }
    if verify_copy && verify_copy_diff {
        println!("--verify and --verify-diff cannot be used together");
        return Ok(1);
    }
    if persist_verification_hashes && !verify_copy {
        println!("--hash is only supported for copy --verify");
        return Ok(1);
    }
    if via_memory && save_config {
        println!("copy --via-memory does not support --save; tune read and write separately");
        return Ok(1);
    }
    if keep_target_size && (via_memory || verify_copy || verify_copy_diff) {
        println!("copy --keep-target-size is only supported for plain streaming copy");
        return Ok(1);
    }
    if force_diff_copy && (via_memory || verify_copy || verify_copy_diff) {
        println!("copy --diff is only supported for plain streaming copy");
        return Ok(1);
    }
    if force_diff_copy && (force_copy_file_range || force_copy_file_range_single || force_reflink) {
        println!("copy --diff cannot be combined with --copy-file-range, --copy-file-range-single, or --reflink");
        return Ok(1);
    }
    if force_copy_file_range && via_memory {
        println!("copy --copy-file-range cannot be used with --via-memory");
        return Ok(1);
    }
    if force_copy_file_range_single && via_memory {
        println!("copy --copy-file-range-single cannot be used with --via-memory");
        return Ok(1);
    }
    if force_threaded_copy && via_memory {
        println!("copy --threaded-copy cannot be used with --via-memory");
        return Ok(1);
    }
    if force_reflink && via_memory {
        println!("copy --reflink cannot be used with --via-memory");
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && save_config {
        println!(
            "copy verification modes do not support --save; tune copy and verification separately"
        );
        return Ok(1);
    }
    if force_copy_file_range_single && save_config {
        println!("copy --copy-file-range-single does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if force_diff_copy && save_config {
        println!("copy --diff does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if force_reflink && save_config {
        println!("copy --reflink does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && iterations > 1 {
        println!("copy verification modes require -n 1");
        return Ok(1);
    }
    if recursive_copy && iterations > 1 {
        println!("copy --recursive currently requires -n 1");
        return Ok(1);
    }
    if force_reflink && iterations > 1 {
        println!("copy --reflink requires -n 1");
        return Ok(1);
    }
    if force_copy_file_range_single && iterations > 1 {
        println!("copy --copy-file-range-single benchmarks the fixed one-call path; use --copy-file-range to optimize the tunable multi-call mode");
        return Ok(1);
    }
    if verify_copy_diff && hash_base.is_some() {
        println!("copy --verify-diff does not use --hash-base");
        return Ok(1);
    }
    if (force_copy_file_range || force_copy_file_range_single)
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        println!("copy --copy-file-range and --copy-file-range-single do not support direct read/write modes");
        return Ok(1);
    }
    if force_reflink
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        println!("copy --reflink does not support direct read/write modes");
        return Ok(1);
    }
    if recursive_copy && via_memory {
        println!("copy --recursive does not support --via-memory yet");
        return Ok(1);
    }
    if recursive_copy && (verify_copy || verify_copy_diff) {
        println!("copy --recursive does not support verification modes yet");
        return Ok(1);
    }
    if recursive_copy && save_config {
        println!("copy --recursive does not support --save yet");
        return Ok(1);
    }

    if mode == "recover" && extra_paths.is_empty() {
        println!("At least one recovery copy is required");
        return Ok(1);
    }
    if mode == "manifest-recursive-copy-bench" && extra_paths.len() != 2 {
        println!("manifest-recursive-copy-bench requires <manifest> <source_root> <target_root>");
        return Ok(1);
    }
    if mode != "write" && create_size.is_some() {
        println!("--create is only supported for write");
        return Ok(1);
    }
    if mode == "recover" && recover_fast_requested && recover_in_place_all_requested {
        println!("--fast and --in-place-all cannot be used together");
        return Ok(1);
    }

    let mut config = config::load_config(config_path);
    let config_mode = match mode {
        "read" if to_memory => "read_to_memory",
        "recursive-read-bench"
        | "file-list-read-bench"
        | "file-list-read-uring-bench"
        | "file-list-read-open-read-close-sweep"
        | "manifest-recursive-copy-bench"
        | "bench-recursive-small-file-threads" => "read",
        "recover" => "verify",
        "hash" | "verify" => mode,
        _ => mode,
    };

    let context_path = filename;

    let params_page_cache = config.get_params_for_path(config_mode, false, context_path);
    let params_direct = config.get_params_for_path(config_mode, true, context_path);
    let params_copy_range = config.get_copy_range_params_for_path(context_path);

    let num_threads_pc = params_page_cache.num_threads;
    let qd_pc = params_page_cache.qd;

    let num_threads_direct = params_direct.num_threads;
    let qd_direct = params_direct.qd;

    // We reverse the scaling factor logic here since run_optimizer multiplies by bsf*1024
    let base_block_size_pc = params_page_cache.block_size / (4 * 1024);
    let base_block_size_direct = params_direct.block_size / (256 * 1024);
    let base_block_size_copy_range = params_copy_range.block_size / (256 * 1024);

    let mut start_params = vec![
        num_threads_pc,
        base_block_size_pc,
        qd_pc as u64,
        num_threads_direct,
        base_block_size_direct,
        qd_direct as u64,
        params_copy_range.num_threads,
        base_block_size_copy_range,
        params_copy_range.qd as u64,
    ];
    let mut params_steps = vec![1, 4 * 1024, 1, 1, 256 * 1024, 1, 1, 256 * 1024, 1];

    let mode_name = mode;
    let copy_strategy = if force_copy_file_range {
        CopyStrategy::CopyFileRange
    } else if force_copy_file_range_single {
        CopyStrategy::CopyFileRangeSingle
    } else if force_reflink {
        CopyStrategy::Reflink
    } else if force_threaded_copy || via_memory {
        CopyStrategy::Threaded
    } else {
        CopyStrategy::Auto
    };
    let copy_rewrite_mode = if force_diff_copy {
        CopyRewriteMode::Diff
    } else if force_full_copy {
        CopyRewriteMode::Full
    } else {
        CopyRewriteMode::Auto
    };
    let mut optimizer_mask =
        active_optimizer_param_mask(mode, io_mode, io_mode_write, via_memory, copy_strategy);
    if mode == "read"
        || mode == "recursive-read-bench"
        || mode == "file-list-read-bench"
        || mode == "file-list-read-uring-bench"
        || mode == "file-list-read-open-read-close-sweep"
        || mode == "manifest-recursive-copy-bench"
        || mode == "bench-recursive-small-file-threads"
    {
        apply_manual_read_overrides(
            &mut start_params,
            &mut params_steps,
            &mut optimizer_mask,
            manual_read_overrides,
        );
    }
    let verbose = verbose || mode == "read" || mode == "write";
    if verbose {
        eprintln!("Opening file {} for {}", filename, mode);
    }

    let mut exit_code = 0;
    let hash_base_owned = hash_base.map(|s| s.to_string());
    let extra_paths_owned = extra_paths;
    let read_auto_strategy = config.get_read_auto_strategy_for_path(context_path);
    let read_mount_info = config.mount_info_for_path(context_path);
    let params_page_cache_for_path = config.get_params_for_path(config_mode, false, context_path);
    let params_direct_for_path = config.get_params_for_path(config_mode, true, context_path);

    let mode_callback = |p: &[u64]| {
        if mode == "read" && to_memory {
            measure_file_load_to_memory(
                filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                to_memory_mode,
                to_memory_options,
            )
        } else if mode == "read" || mode == "grep" {
            if auto_lift {
                read_file_auto_with_strategy(
                    pattern,
                    filename,
                    read_auto_strategy,
                    read_mount_info.as_ref(),
                    params_page_cache_for_path.clone(),
                    params_direct_for_path.clone(),
                )
            } else {
                read_file(
                    pattern,
                    filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )
            }
        } else if mode == "recursive-read-bench" {
            bench_recursive_read(
                &config,
                filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                verbose,
                manual_read_overrides.threads,
            )
        } else if mode == "file-list-read-bench" {
            bench_file_list_read(
                &config,
                filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                verbose,
            )
        } else if mode == "file-list-read-uring-bench" {
            bench_file_list_read_uring(&config, filename, io_mode, verbose)
        } else if mode == "file-list-read-open-read-close-sweep" {
            bench_file_list_read_open_read_close_sweep(&config, filename, io_mode, verbose)
        } else if mode == "manifest-recursive-copy-bench" {
            bench_manifest_recursive_copy(
                filename,
                extra_paths_owned[0].as_str(),
                extra_paths_owned[1].as_str(),
                overlap_large_file,
                verbose,
            )
        } else if mode == "bench-recursive-small-file-threads" {
            bench_recursive_small_file_threads(
                &mut config,
                filename,
                io_mode,
                verbose,
                save_config,
                small_file_thread_cache_state,
            )
        } else if mode == "hash" {
            if hash_only || iterations > 1 {
                let manifest = hash_file_blocks(
                    filename,
                    hash_type,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )?;
                if iterations == 1 {
                    println!("{}  {}", manifest.hash_of_hashes, filename);
                }
                Ok(manifest.bytes_hashed)
            } else {
                let manifest = hash_file_to_replicas(
                    filename,
                    hash_base_owned.as_deref(),
                    hash_type,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )?;
                println!(
                    "wrote {} {:?} hash blocks ({} bytes each) to {}.[0-2].json",
                    manifest.block_hashes.len(),
                    manifest.hash_type,
                    manifest.block_size,
                    hash_base_owned
                        .as_deref()
                        .unwrap_or(&default_hash_base(filename))
                );
                Ok(manifest.bytes_hashed)
            }
        } else if mode == "verify" {
            let report = verify_file_with_replicas(
                filename,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )?;
            if iterations == 1 {
                print_verify_report(&report);
            }
            if iterations == 1 && !report.bad_blocks.is_empty() {
                exit_code = 1;
            }
            Ok(report.bytes_hashed)
        } else if mode == "recover" {
            let report = recover_file_with_copies(
                filename,
                &extra_paths_owned,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                recover_mode,
            )?;
            println!(
                "recover: repaired_blocks={}, repaired_files={}, sidecars_refreshed={}, failed_blocks={}, used_fast_path={}, fell_back_to_full_scan={}",
                report.repaired_blocks,
                report.repaired_files,
                report.sidecars_refreshed,
                report.failed_blocks.len()
                ,
                report.used_fast_path,
                report.fell_back_to_full_scan
            );
            for issue in &report.failed_blocks {
                println!(
                    "file {} ({}), block {}: {}",
                    issue.file_index,
                    issue.file_path,
                    issue.block_index,
                    issue.decision.status_message()
                );
            }
            if !report.failed_blocks.is_empty() {
                println!(
                    "recover could not fully repair all requested files; add more clean replicas or inspect the failed block reasons above"
                );
                exit_code = 1;
            } else if report.repaired_blocks == 0 {
                println!("recover: no block writes were needed");
            }
            Ok(report.bytes_hashed)
        } else if mode == "write" {
            write_file(
                filename,
                create_size,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode_write,
            )
        } else if mode == "copy" || mode == "split-manifest-recursive-copy-bench" {
            if let Some(src) = source {
                if recursive_copy || mode == "split-manifest-recursive-copy-bench" {
                    let source_root = PathBuf::from(src);
                    let source_metadata = fs::symlink_metadata(&source_root)?;
                    if !source_metadata.file_type().is_dir() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            if mode == "copy" {
                                "copy --recursive requires a directory source"
                            } else {
                                "split-manifest-recursive-copy-bench requires a directory source"
                            },
                        ));
                    }
                    let target_root =
                        resolve_recursive_copy_root(&source_root, Path::new(filename))?;
                    let optimizer_params = std::array::from_fn(|index| p[index]);
                    let recursive_ctx = RecursiveCopyContext {
                        config: config.clone(),
                        source_root,
                        target_root,
                        optimizer_params,
                        requested_strategy: copy_strategy,
                        rewrite_mode: copy_rewrite_mode,
                        io_mode_read: io_mode,
                        io_mode_write,
                        keep_target_size,
                        use_lock: !no_lock,
                        relative_copy_method: RelativeCopyMethod::CopyFileRange,
                    };
                    if mode == "split-manifest-recursive-copy-bench" {
                        run_split_manifest_recursive_copy(recursive_ctx, verbose)
                    } else {
                        run_recursive_copy(recursive_ctx, verbose)
                    }
                } else {
                    let resolved_copy = resolve_copy_execution(
                        &config,
                        src,
                        filename,
                        copy_strategy,
                        copy_rewrite_mode,
                        io_mode,
                        io_mode_write,
                    )?;
                    if verbose {
                        eprintln!(
                            "{}",
                            describe_copy_path(resolved_copy, via_memory, keep_target_size)
                        );
                    }
                    if verify_copy {
                        let target_hash_base_owned = if persist_verification_hashes {
                            Some(
                                hash_base_owned
                                    .clone()
                                    .unwrap_or_else(|| default_hash_base(filename)),
                            )
                        } else {
                            None
                        };
                        let report = copy_file_verified_with_options_and_lock(
                            src,
                            filename,
                            resolved_copy.io_mode_read,
                            resolved_copy.io_mode_write,
                            hash_type,
                            via_memory,
                            target_hash_base_owned.as_deref(),
                            resolved_copy.copy_strategy,
                            !no_lock,
                        )?;
                        if !quiet {
                            eprintln!(
                            "copy verify: success; verified_blocks={}, repaired_blocks={}, used_recovery={}, hash_type={:?}, sidecars_written={}",
                            report.verified_blocks,
                            report.repaired_blocks,
                            report.used_recovery,
                            report.hash_type,
                            report.hashes_persisted
                        );
                        }
                        Ok(report.bytes_copied)
                    } else if verify_copy_diff {
                        let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                        let copied = if via_memory {
                            let read_page_cache =
                                config.get_params_for_path("read_to_memory", false, src);
                            let read_direct =
                                config.get_params_for_path("read_to_memory", true, src);
                            let loaded = load_file_to_memory(
                                src,
                                read_page_cache.num_threads,
                                read_page_cache.block_size,
                                read_page_cache.qd,
                                read_direct.num_threads,
                                read_direct.block_size,
                                read_direct.qd,
                                io_mode,
                            )?;
                            let write_page_cache =
                                config.get_params_for_path("write", false, filename);
                            let write_direct = config.get_params_for_path("write", true, filename);
                            write_buffer(
                                filename,
                                &loaded.data,
                                write_page_cache.num_threads,
                                write_page_cache.block_size,
                                write_page_cache.qd,
                                write_direct.num_threads,
                                write_direct.block_size,
                                write_direct.qd,
                                resolved_copy.io_mode_write,
                            )?
                        } else {
                            copy_file_with_strategy(
                                src,
                                filename,
                                p[0],
                                p[1],
                                p[2] as usize,
                                p[3],
                                p[4],
                                p[5] as usize,
                                p[6],
                                p[7],
                                p[8] as usize,
                                resolved_copy.io_mode_read,
                                resolved_copy.io_mode_write,
                                resolved_copy.copy_strategy,
                            )?
                        };
                        sync_path(filename)?;
                        guard.ensure_source_unchanged()?;
                        let diff_page_cache = config.get_params_for_path("diff", false, filename);
                        let diff_direct = config.get_params_for_path("diff", true, filename);
                        let diff_res = diff_files(
                            src,
                            filename,
                            diff_page_cache.num_threads,
                            diff_page_cache.block_size,
                            diff_page_cache.qd,
                            diff_direct.num_threads,
                            diff_direct.block_size,
                            diff_direct.qd,
                            io_mode,
                            false,
                            true,
                        )?;
                        if diff_res != 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "copy verify-diff found a mismatch at byte offset {}",
                                    diff_res
                                ),
                            ));
                        }
                        guard.ensure_source_unchanged()?;
                        if !quiet {
                            eprintln!("copy verify-diff: success");
                        }
                        Ok(copied)
                    } else if via_memory {
                        let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                        let read_page_cache =
                            config.get_params_for_path("read_to_memory", false, src);
                        let read_direct = config.get_params_for_path("read_to_memory", true, src);
                        let loaded = load_file_to_memory(
                            src,
                            read_page_cache.num_threads,
                            read_page_cache.block_size,
                            read_page_cache.qd,
                            read_direct.num_threads,
                            read_direct.block_size,
                            read_direct.qd,
                            io_mode,
                        )?;
                        let write_page_cache = config.get_params_for_path("write", false, filename);
                        let write_direct = config.get_params_for_path("write", true, filename);
                        let copied = write_buffer(
                            filename,
                            &loaded.data,
                            write_page_cache.num_threads,
                            write_page_cache.block_size,
                            write_page_cache.qd,
                            write_direct.num_threads,
                            write_direct.block_size,
                            write_direct.qd,
                            resolved_copy.io_mode_write,
                        )?;
                        guard.ensure_source_unchanged()?;
                        Ok(copied)
                    } else {
                        let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                        let copied = if resolved_copy.diff_overwrite && !keep_target_size {
                            let diff_scan = config.get_params_for_path("diff", false, filename);
                            overwrite_changed_chunks_direct(
                                src,
                                filename,
                                diff_scan.num_threads,
                                diff_scan.block_size,
                                diff_scan.qd,
                                p[3],
                                p[4],
                                p[5] as usize,
                            )?
                        } else {
                            copy_file_with_strategy_and_truncate(
                                src,
                                filename,
                                p[0],
                                p[1],
                                p[2] as usize,
                                p[3],
                                p[4],
                                p[5] as usize,
                                p[6],
                                p[7],
                                p[8] as usize,
                                resolved_copy.io_mode_read,
                                resolved_copy.io_mode_write,
                                resolved_copy.copy_strategy,
                                !keep_target_size,
                            )?
                        };
                        guard.ensure_source_unchanged()?;
                        Ok(copied)
                    }
                }
            } else {
                eprintln!("Copy is missing a destination path.");
                Ok(1)
            }
        } else if mode == "diff" || mode == "dual-read-bench" {
            let s1 = std::fs::metadata(source.unwrap())?.len();
            let s2 = std::fs::metadata(filename)?.len();
            if s1 != s2 {
                if verbose {
                    eprintln!("Files have different sizes: {} != {}", s1, s2);
                }
                if mode == "diff" {
                    exit_code = 1;
                }
            }

            if exit_code == 0 {
                let bench_only = mode == "dual-read-bench";
                let size = std::fs::File::open(filename)?.metadata()?.len();
                let res = diff_files(
                    source.unwrap(),
                    filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                    bench_only,
                    true,
                )?;
                if res != 0 && mode == "diff" {
                    exit_code = 1;
                }
                Ok(size * 2)
            } else {
                Ok(0)
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid mode {}", mode),
            ))
        }
    };

    let effective_to_memory_mode = if mode == "read" && to_memory {
        Some(resolve_to_memory_mode(filename, io_mode, to_memory_mode))
    } else {
        None
    };

    if mode == "read"
        && to_memory
        && iterations == 1
        && effective_to_memory_mode.is_some_and(|mode| {
            matches!(
                mode,
                ReadToMemoryMode::Mmap | ReadToMemoryMode::MmapReadPages
            )
        })
        && !to_memory_options.measure_unmap_time
    {
        let single_run_params = start_params
            .iter()
            .zip(params_steps.iter())
            .map(|(value, scale)| value * scale)
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        let prepared = prepare_file_load_to_memory(
            filename,
            single_run_params[0],
            single_run_params[1],
            single_run_params[2] as usize,
            single_run_params[3],
            single_run_params[4],
            single_run_params[5] as usize,
            io_mode,
            to_memory_mode,
            to_memory_options,
        )?;
        let bytes = std::fs::metadata(filename)?.len();
        let elapsed = start.elapsed().as_secs_f64();
        eprintln!(
            "{} {} bytes in {:.4} s, {:.1} GB/s, {:?}",
            mode_name,
            bytes,
            elapsed,
            bytes as f64 / elapsed / 1e9,
            &single_run_params[..6]
        );
        drop(prepared);
        if save_config {
            match io_mode {
                common::IOMode::Auto => {}
                _ => {
                    let direct = io_mode == common::IOMode::Direct;
                    let off = if direct { 3 } else { 0 };
                    config.update_params_for_path(
                        config_mode,
                        direct,
                        context_path,
                        config::IOParams {
                            num_threads: single_run_params[off],
                            block_size: single_run_params[off + 1],
                            qd: single_run_params[off + 2] as usize,
                        },
                    );
                    config.save();
                }
            }
        }
        return Ok(0);
    }

    let best_params = run_optimizer(
        mode_name,
        start_params,
        params_steps,
        optimizer_mask,
        iterations,
        verbose,
        mode_callback,
    )?;

    if mode == "verify" && iterations > 1 {
        let report = verify_file_with_replicas(
            filename,
            hash_base_owned.as_deref(),
            best_params[0],
            best_params[1],
            best_params[2] as usize,
            best_params[3],
            best_params[4],
            best_params[5] as usize,
            io_mode,
        )?;
        print_verify_report(&report);
        if !report.bad_blocks.is_empty() {
            exit_code = 1;
        }
    }

    if save_config {
        match (mode, copy_strategy, io_mode) {
            ("copy", CopyStrategy::CopyFileRange, _) => {
                config.update_copy_range_params_for_path(
                    context_path,
                    config::IOParams {
                        num_threads: best_params[6],
                        block_size: best_params[7],
                        qd: best_params[8] as usize,
                    },
                );
                config.save();
            }
            (_, _, common::IOMode::Auto) => {}
            _ => {
                let direct = io_mode == common::IOMode::Direct;
                let off = if direct { 3 } else { 0 };
                config.update_params_for_path(
                    config_mode,
                    direct,
                    context_path,
                    config::IOParams {
                        num_threads: best_params[off + 0],
                        block_size: best_params[off + 1],
                        qd: best_params[off + 2] as usize,
                    },
                );
                config.save();
            }
        }
    }

    if exit_code != 0 {
        return Ok(exit_code);
    }
    Ok(0)
}

fn main() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if is_broken_pipe_panic(info.payload()) {
            return;
        }
        default_hook(info);
    }));

    match std::panic::catch_unwind(try_main) {
        Ok(Ok(code)) if code == 0 => {}
        Ok(Ok(code)) => std::process::exit(code),
        Ok(Err(err)) => {
            if is_broken_pipe_error(&err) {
                std::process::exit(0);
            }
            let _ = writeln!(io::stderr().lock(), "Error: {}", err);
            std::process::exit(1);
        }
        Err(payload) => {
            if is_broken_pipe_panic(payload.as_ref()) {
                std::process::exit(0);
            }
            std::panic::resume_unwind(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        active_optimizer_param_mask, apply_manual_read_overrides,
        choose_nonredundant_full_copy_plan, describe_copy_path, parse_zpool_status_leaves,
        should_prefer_cached_diff_overwrite, should_prefer_cached_read_direct_write,
        target_is_similar_size, zfs_storage_redundancy_from_status, HeuristicCopyPlan,
        ManualReadOverrides, ResolvedCopyExecution, StorageRedundancy,
    };
    use crate::common::{CopyStrategy, IOMode};

    fn heuristic_plan(
        source_cached: bool,
        target_cached: bool,
        source_len: Option<u64>,
        target_len: Option<u64>,
        direct_write_supported: bool,
    ) -> HeuristicCopyPlan {
        if should_prefer_cached_diff_overwrite(source_cached, target_cached, source_len, target_len)
        {
            if direct_write_supported {
                HeuristicCopyPlan::DiffOverwrite
            } else {
                HeuristicCopyPlan::CopyFileRangeSingle
            }
        } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len) {
            HeuristicCopyPlan::CachedReadDirectWrite
        } else {
            HeuristicCopyPlan::DirectReadDirectWrite
        }
    }

    #[test]
    fn nonredundant_policy_forces_full_copy_path() {
        let cached = choose_nonredundant_full_copy_plan(true, Some(1024), Some(1024));
        assert!(cached.copy_strategy == CopyStrategy::Threaded);
        assert!(cached.io_mode_read == IOMode::PageCache);
        assert!(cached.io_mode_write == IOMode::Direct);
        assert!(cached.full_rewrite);
        assert!(!cached.diff_overwrite);

        let cold = choose_nonredundant_full_copy_plan(false, Some(1024), Some(1024));
        assert!(cold.copy_strategy == CopyStrategy::Threaded);
        assert!(cold.io_mode_read == IOMode::Direct);
        assert!(cold.io_mode_write == IOMode::Direct);
        assert!(cold.full_rewrite);
        assert!(!cold.diff_overwrite);
    }

    #[test]
    fn parse_zpool_status_tracks_vdev_paths() {
        let leaves = parse_zpool_status_leaves(
            "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          mirror-0                  ONLINE       0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       ONLINE       0     0     0\n\nerrors: No known data errors\n",
        );
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].vdev_path, vec!["mirror-0".to_string()]);
        assert_eq!(leaves[0].state.as_deref(), Some("ONLINE"));
    }

    #[test]
    fn zfs_redundancy_marks_healthy_mirror_redundant() {
        let status = "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          mirror-0                  ONLINE       0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       ONLINE       0     0     0\n\nerrors: No known data errors\n";
        assert_eq!(
            zfs_storage_redundancy_from_status(status),
            StorageRedundancy::Redundant
        );
    }

    #[test]
    fn zfs_redundancy_marks_degraded_mirror_nonredundant() {
        let status = "  pool: tank\n state: DEGRADED\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        DEGRADED     0     0     0\n          mirror-0                  DEGRADED     0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       UNAVAIL      0     0     0  was /dev/disk/by-id/b\n\nerrors: No known data errors\n";
        assert_eq!(
            zfs_storage_redundancy_from_status(status),
            StorageRedundancy::NonRedundant
        );
    }

    #[test]
    fn zfs_redundancy_marks_stripe_nonredundant() {
        let status = "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          /dev/disk/by-id/a         ONLINE       0     0     0\n          /dev/disk/by-id/b         ONLINE       0     0     0\n\nerrors: No known data errors\n";
        assert_eq!(
            zfs_storage_redundancy_from_status(status),
            StorageRedundancy::NonRedundant
        );
    }

    #[test]
    fn verify_skips_block_size_mutations() {
        assert_eq!(
            active_optimizer_param_mask(
                "verify",
                IOMode::PageCache,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, false, true, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "verify",
                IOMode::Direct,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, true, false, true, false, false, false]
        );
    }

    #[test]
    fn write_only_mutates_write_side_params() {
        assert_eq!(
            active_optimizer_param_mask(
                "write",
                IOMode::Auto,
                IOMode::PageCache,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, true, true, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, true, true, true, true, true, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Auto,
                true,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Direct,
                true,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::PageCache,
                false,
                CopyStrategy::CopyFileRange
            ),
            vec![false, false, false, false, false, false, true, true, true]
        );
    }

    #[test]
    fn manual_read_overrides_freeze_both_read_param_sets() {
        let mut start_params = vec![8, 16, 2, 12, 32, 4, 1, 64, 1];
        let mut params_steps = vec![1, 4096, 1, 1, 262144, 1, 1, 262144, 1];
        let mut mask = vec![true; 9];
        apply_manual_read_overrides(
            &mut start_params,
            &mut params_steps,
            &mut mask,
            ManualReadOverrides {
                threads: Some(5),
                block_size: Some(131072),
                qd: Some(7),
            },
        );
        assert_eq!(&start_params[..6], &[5, 131072, 7, 5, 131072, 7]);
        assert_eq!(&params_steps[..6], &[1, 1, 1, 1, 1, 1]);
        assert_eq!(&mask[..6], &[false, false, false, false, false, false]);
    }

    #[test]
    fn cached_read_direct_write_requires_hot_close_sized_target() {
        assert!(should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(1024)
        ));
        assert!(should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(900)
        ));
        assert!(should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(2048)
        ));
        assert!(should_prefer_cached_read_direct_write(
            true,
            Some(0),
            Some(0)
        ));
        assert!(!should_prefer_cached_read_direct_write(
            false,
            Some(1024),
            Some(1024)
        ));
        assert!(!should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(716)
        ));
        assert!(!should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            None
        ));
    }

    #[test]
    fn cached_diff_overwrite_requires_hot_similar_sized_files() {
        assert!(should_prefer_cached_diff_overwrite(
            true,
            true,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            true,
            false,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            false,
            true,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            true,
            true,
            Some(1024),
            Some(600)
        ));
    }

    #[test]
    fn zero_length_source_is_only_similar_to_zero_length_target() {
        assert!(target_is_similar_size(Some(0), Some(0)));
        assert!(!target_is_similar_size(Some(0), Some(1)));
    }

    #[test]
    fn heuristic_plan_prefers_diff_overwrite_then_copy_file_range_single_fallback() {
        assert_eq!(
            heuristic_plan(true, true, Some(1024), Some(900), true),
            HeuristicCopyPlan::DiffOverwrite
        );
        assert_eq!(
            heuristic_plan(true, true, Some(1024), Some(900), false),
            HeuristicCopyPlan::CopyFileRangeSingle
        );
        assert_eq!(
            heuristic_plan(true, false, Some(1024), Some(900), true),
            HeuristicCopyPlan::CachedReadDirectWrite
        );
        assert_eq!(
            heuristic_plan(false, false, Some(1024), Some(900), true),
            HeuristicCopyPlan::DirectReadDirectWrite
        );
    }

    #[test]
    fn describe_copy_path_reports_diff_overwrite_details() {
        let path = describe_copy_path(
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: IOMode::PageCache,
                io_mode_write: IOMode::Direct,
                diff_overwrite: true,
                full_rewrite: false,
                path_label: "auto diff-overwrite",
            },
            false,
            false,
        );
        assert!(path.contains("strategy=auto diff-overwrite"));
        assert!(path.contains("read=page-cache"));
        assert!(path.contains("write=direct"));
        assert!(path.contains("delta=changed-chunks"));
    }

    #[test]
    fn describe_copy_path_reports_via_memory_path() {
        let path = describe_copy_path(
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: IOMode::PageCache,
                io_mode_write: IOMode::Direct,
                diff_overwrite: false,
                full_rewrite: false,
                path_label: "auto cached-read direct-write",
            },
            true,
            false,
        );
        assert_eq!(
            path,
            "copy path: via-memory [read=page-cache, write=direct]"
        );
    }
}
