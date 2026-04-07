use super::*;

pub(super) fn recursive_copy_dir_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

pub(super) fn recursive_copy_large_worker_count() -> usize {
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

pub(super) fn recursive_copy_small_worker_count() -> usize {
    if let Ok(value) = std::env::var("FRO_RECURSIVE_COPY_SMALL_WORKERS") {
        if let Ok(parsed) = value.parse::<usize>() {
            if parsed > 0 {
                return parsed;
            }
        }
    }
    RECURSIVE_COPY_SMALL_WORKERS
}

pub(crate) fn recursive_read_dir_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

pub(crate) fn recursive_read_file_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

pub(crate) fn recursive_read_file_worker_count_with_override(
    override_threads: Option<u64>,
) -> usize {
    override_threads
        .and_then(|threads| usize::try_from(threads).ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(recursive_read_file_worker_count)
}

pub(crate) fn recursive_small_file_worker_count_for_path(
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

pub(super) fn recursive_copy_uses_small_file_range(
    ctx: &RecursiveCopyContext,
    source_len: u64,
) -> bool {
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

pub(super) fn recursive_copy_uses_threaded_large_lane(
    ctx: &RecursiveCopyContext,
    source_len: u64,
) -> bool {
    let cutoff = std::env::var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(RECURSIVE_COPY_THREADED_LANE_THRESHOLD);
    source_len >= cutoff
        || ctx.requested_strategy == CopyStrategy::Threaded
        || ctx.requested_strategy == CopyStrategy::Reflink
}

pub(super) fn resolve_recursive_large_copy_execution(
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
