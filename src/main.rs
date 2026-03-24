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
use differ::{bench_diff_memory, bench_memcpy_memory, diff_files};
use io_util::{direct_writer_supported, sync_path, CopyOperationGuard};
use mincore::is_first_page_resident;
use optimizer::run_optimizer;
use reader::visit_file_blocks;
use reader::{
    load_file_to_memory, measure_file_load_to_memory, prepare_file_load_to_memory, read_file,
    read_file_auto_lift, resolve_to_memory_mode, HugepageAdvice, ReadToMemoryMode,
    ReadToMemoryOptions,
};
use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

const PAGE_CACHE_PARAM_INDICES: [usize; 3] = [0, 1, 2];
const DIRECT_PARAM_INDICES: [usize; 3] = [3, 4, 5];
const COPY_RANGE_PARAM_INDICES: [usize; 3] = [6, 7, 8];
const RECURSIVE_COPY_LARGE_FILE_THRESHOLD: u64 = 8 << 20;
const RECURSIVE_COPY_MAX_LARGE_WORKERS: usize = 2;

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

#[derive(Default)]
struct RecursiveDirectoryQueue {
    state: Mutex<RecursiveDirectoryQueueState>,
    ready: Condvar,
}

#[derive(Default)]
struct RecursiveDirectoryQueueState {
    queue: VecDeque<RecursiveDirectoryTask>,
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
}

impl Default for RecursiveCopyStats {
    fn default() -> Self {
        Self {
            files_copied: AtomicU64::new(0),
            dirs_created: AtomicU64::new(0),
            symlinks_created: AtomicU64::new(0),
            bytes_copied: AtomicU64::new(0),
        }
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
        "read" | "grep" | "hash" | "diff" | "dual-read-bench" | "recursive-read-bench" => {
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

impl RecursiveDirectoryQueue {
    fn enqueue(&self, tasks: impl IntoIterator<Item = RecursiveDirectoryTask>) {
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

    fn enqueue_one(&self, task: RecursiveDirectoryTask) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(task);
        self.ready.notify_one();
    }

    fn claim(&self, stop: &AtomicBool) -> Option<RecursiveDirectoryTask> {
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
    Ok(())
}

fn execute_recursive_file_copy(
    task: &RecursiveFileTask,
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
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
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(RECURSIVE_COPY_MAX_LARGE_WORKERS)
        .max(1)
}

fn recursive_copy_uses_small_file_range(ctx: &RecursiveCopyContext, source_len: u64) -> bool {
    source_len < RECURSIVE_COPY_LARGE_FILE_THRESHOLD
        && ctx.rewrite_mode == CopyRewriteMode::Auto
        && !ctx.keep_target_size
        && !matches!(
            ctx.requested_strategy,
            CopyStrategy::Threaded | CopyStrategy::Reflink
        )
        && ctx.io_mode_read != common::IOMode::Direct
        && ctx.io_mode_write != common::IOMode::Direct
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
    source_path: &Path,
    target_path: &Path,
    source_len: u64,
    source_mode: u32,
    use_lock: bool,
    stats: &RecursiveCopyStats,
) -> io::Result<()> {
    let source_str = source_path.to_string_lossy();
    let target_str = target_path.to_string_lossy();
    let guard = CopyOperationGuard::new(&source_str, &target_str, use_lock)?;
    let copied = copy_file_range_syscall(
        &source_str,
        &target_str,
        0,
        0,
        source_len,
        true,
        common::IOMode::PageCache,
        common::IOMode::PageCache,
    )?;
    guard.ensure_source_unchanged()?;
    fs::set_permissions(target_path, fs::Permissions::from_mode(source_mode))?;
    stats.files_copied.fetch_add(1, Ordering::Relaxed);
    stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
    Ok(())
}

fn walk_recursive_copy_subtree(
    start: RecursiveDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue,
    large_queue: &RecursiveTaskQueue<RecursiveFileTask>,
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
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
            let source_path = entry.path();
            let target_path = task.target_dir.join(entry.file_name());
            let metadata = fs::symlink_metadata(&source_path)?;
            let file_type = metadata.file_type();
            if file_type.is_dir() {
                let mode = metadata.permissions().mode();
                create_directory_like(mode, &target_path, stats)?;
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
            let source_len = metadata.len();
            let source_mode = metadata.permissions().mode();
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                execute_recursive_small_file_copy(
                    &source_path,
                    &target_path,
                    source_len,
                    source_mode,
                    ctx.use_lock,
                    stats,
                )?;
            } else {
                let resolved_copy =
                    resolve_recursive_large_copy_execution(ctx, &source_path, &target_path)?;
                large_queue.enqueue(RecursiveFileTask {
                    source_path,
                    target_path,
                    source_mode,
                    resolved_copy,
                })?;
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
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
    create_directory_like(source_meta.permissions().mode(), &ctx.target_root, &stats)?;
    let dir_queue = Arc::new(RecursiveDirectoryQueue::default());
    let large_queue = Arc::new(RecursiveTaskQueue::default());
    let stop = Arc::new(AtomicBool::new(false));

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
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_copy_subtree(
                    task,
                    &dir_queue,
                    &large_queue,
                    &ctx,
                    &stats,
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
        large_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = queue.claim(&stop) {
                if let Err(err) = execute_recursive_file_copy(&task, &ctx, &stats) {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
                    queue.wake_all();
                    return Err(err);
                }
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
    }
    Ok(bytes_copied)
}

fn collect_regular_files_recursive(root: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_file() {
        out.push(root.to_path_buf());
        return Ok(());
    }
    if !metadata.file_type().is_dir() {
        return Ok(());
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_dir() {
                stack.push(path);
            } else if metadata.file_type().is_file() {
                out.push(path);
            }
        }
    }
    Ok(())
}

fn bench_recursive_read(
    path: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let root = Path::new(path);
    let mut files = Vec::new();
    collect_regular_files_recursive(root, &mut files)?;
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found under {}", root.display()),
        ));
    }

    let start = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    for file in &files {
        let file_str = file.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path is not valid UTF-8: {}", file.display()),
            )
        })?;
        let (bytes_read, _file_size, _params) = visit_file_blocks(
            file_str,
            num_threads_p,
            block_size_p,
            qd_p,
            num_threads_d,
            block_size_d,
            qd_d,
            io_mode,
            |_block| Ok(()),
        )?;
        total_bytes = total_bytes.saturating_add(bytes_read);
        if bytes_read == 0 && fs::metadata(file)?.len() != 0 {
            return Err(io::Error::other(format!(
                "recursive-read-bench observed zero bytes for non-empty file {}",
                file.display()
            )));
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    if verbose {
        eprintln!(
            "recursive-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s",
            total_bytes,
            files.len(),
            elapsed,
            total_bytes as f64 / elapsed / 1e9
        );
    } else {
        println!(
            "recursive-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s",
            total_bytes,
            files.len(),
            elapsed,
            total_bytes as f64 / elapsed / 1e9
        );
    }
    Ok(total_bytes)
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
    println!("  fro-optimize       tune configs for one or more commands / mounts");
    println!("  fro-benchmark      run the regression benchmark suite");
    println!("  bench-diff         in-memory diff microbenchmark");
    println!("  bench-memcpy       in-memory memcpy microbenchmark");
    println!("  bench-base64-encode base64 encode kernel microbenchmark");
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
    if raw_args.get(1).is_some_and(|arg| is_version_flag(arg.as_str())) {
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
    } else if mode == "bench-base64-encode" {
        1_000_000
    } else {
        1
    };
    let mut save_config = false;
    let mut config_path: Option<&str> = None;
    let mut bench_size: Option<u64> = None;
    let mut bench_threads: Option<usize> = None;
    let mut base64_kernel = coreutils::Base64EncodeKernel::Auto;

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
                    } else {
                        eprintln!("--variant is only supported for bench-base64-encode");
                        return Ok(1);
                    }
                }
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
        } else if mode == "copy" || mode == "diff" || mode == "dual-read-bench" {
            if source.is_none() {
                source = Some(args[i].as_str());
            } else if filename == "" {
                filename = args[i].as_str();
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
    if manual_read_overrides.any() && mode != "read" {
        println!("--threads, --qd, and --blocksize overrides are only supported for read");
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
        "recursive-read-bench" => "read",
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
    if mode == "read" || mode == "recursive-read-bench" {
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
                read_file_auto_lift(
                    pattern,
                    filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
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
        } else if mode == "copy" {
            if let Some(src) = source {
                if recursive_copy {
                    let source_root = PathBuf::from(src);
                    let source_metadata = fs::symlink_metadata(&source_root)?;
                    if !source_metadata.file_type().is_dir() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "copy --recursive requires a directory source",
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
                    };
                    run_recursive_copy(recursive_ctx, verbose)
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
