use super::*;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy)]
pub(super) struct ResolvedCopyExecution {
    pub(super) copy_strategy: CopyStrategy,
    pub(super) io_mode_read: common::IOMode,
    pub(super) io_mode_write: common::IOMode,
    pub(super) diff_overwrite: bool,
    pub(super) full_rewrite: bool,
    pub(super) path_label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HeuristicCopyPlan {
    DiffOverwrite,
    LowLatencyCopyFileRangeSingle,
    CachedReadDirectWrite,
    DirectReadDirectWrite,
    CopyFileRangeSingle,
}

const LOW_LATENCY_COPY_FILE_RANGE_SINGLE_THRESHOLD: u64 = 256 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyRewriteMode {
    Auto,
    Diff,
    Full,
}

pub(super) fn resolve_copy_execution(
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

    let resolved = match config.get_copy_auto_mode_for_config_path(path) {
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
            } else if should_prefer_low_latency_copy_file_range_single(
                source_path,
                path,
                source_len,
            ) {
                HeuristicCopyPlan::LowLatencyCopyFileRangeSingle
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
                HeuristicCopyPlan::LowLatencyCopyFileRangeSingle => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::CopyFileRangeSingle,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto low-latency copy_file_range single",
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

pub(super) fn inspect_copy_auto_state(
    source_path: &str,
    target_path: &str,
) -> (bool, bool, Option<u64>, Option<u64>) {
    let source_cached = Ok(true) == is_edge_pages_resident(source_path);
    let target_cached = Ok(true) == is_edge_pages_resident(target_path);
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

pub(super) fn target_is_similar_size(source_len: Option<u64>, target_len: Option<u64>) -> bool {
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

pub(super) fn should_prefer_cached_diff_overwrite(
    source_cached: bool,
    target_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_cached && target_is_similar_size(source_len, target_len)
}

pub(super) fn should_prefer_cached_read_direct_write(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_is_similar_size(source_len, target_len)
}

fn low_latency_copy_file_range_single_threshold() -> u64 {
    std::env::var("FRO_COPY_LOW_LATENCY_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(LOW_LATENCY_COPY_FILE_RANGE_SINGLE_THRESHOLD)
}

fn same_device_for_copy_file_range(source_path: &str, target_path: &str) -> bool {
    let Ok(source_meta) = std::fs::metadata(source_path) else {
        return false;
    };
    if !source_meta.file_type().is_file() {
        return false;
    }
    let target_path = std::path::Path::new(target_path);
    if let Ok(target_meta) = std::fs::metadata(target_path) {
        return target_meta.file_type().is_file() && target_meta.dev() == source_meta.dev();
    }
    let Some(parent) = target_path.parent() else {
        return false;
    };
    let Ok(parent_meta) = std::fs::metadata(parent) else {
        return false;
    };
    parent_meta.is_dir() && parent_meta.dev() == source_meta.dev()
}

pub(super) fn should_prefer_low_latency_copy_file_range_single(
    source_path: &str,
    target_path: &str,
    source_len: Option<u64>,
) -> bool {
    let Some(source_len) = source_len else {
        return false;
    };
    source_len <= low_latency_copy_file_range_single_threshold()
        && same_device_for_copy_file_range(source_path, target_path)
}

pub(super) fn io_mode_label(io_mode: common::IOMode) -> &'static str {
    match io_mode {
        common::IOMode::Auto => "auto",
        common::IOMode::PageCache => "page-cache",
        common::IOMode::Direct => "direct",
    }
}

pub(super) fn describe_copy_path(
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
