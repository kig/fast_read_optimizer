use super::*;
use crate::config;
use crate::main_app::copy_plan::{resolve_copy_execution, CopyRewriteMode, ResolvedCopyExecution};

pub(crate) mod archive;
pub(super) mod bench;
pub(crate) mod delete;
pub(crate) mod move_dir;
pub(super) mod openat;
pub(super) mod paths;

#[derive(Clone, Copy)]
pub(super) struct PreservedTimestamps {
    atime_sec: i64,
    atime_nsec: i64,
    mtime_sec: i64,
    mtime_nsec: i64,
}

#[derive(Clone)]
struct RecursiveDirectoryMetadataTask {
    target_path: PathBuf,
    timestamps: PreservedTimestamps,
}

pub(super) fn preserved_timestamps_from_metadata(metadata: &fs::Metadata) -> PreservedTimestamps {
    PreservedTimestamps {
        atime_sec: metadata.atime(),
        atime_nsec: metadata.atime_nsec(),
        mtime_sec: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
    }
}

fn set_path_timestamps(
    path: &Path,
    timestamps: PreservedTimestamps,
    nofollow_symlink: bool,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })?;
    let times = [
        libc::timespec {
            tv_sec: timestamps.atime_sec,
            tv_nsec: timestamps.atime_nsec,
        },
        libc::timespec {
            tv_sec: timestamps.mtime_sec,
            tv_nsec: timestamps.mtime_nsec,
        },
    ];
    let flags = if nofollow_symlink {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), flags) };
    if rc == 0 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

pub(super) fn preserve_file_timestamps(source_path: &Path, target_path: &Path) -> io::Result<()> {
    let metadata = fs::metadata(source_path)?;
    set_path_timestamps(
        target_path,
        preserved_timestamps_from_metadata(&metadata),
        false,
    )
}

fn finalize_directory_timestamps(tasks: &[RecursiveDirectoryMetadataTask]) -> io::Result<()> {
    let mut sorted = tasks.to_vec();
    sorted.sort_by_key(|task| std::cmp::Reverse(task.target_path.components().count()));
    for task in sorted {
        set_path_timestamps(&task.target_path, task.timestamps, false)?;
    }
    Ok(())
}

fn maybe_print_verbose_copy(
    verbose: bool,
    cp_compat: bool,
    source_path: &Path,
    target_path: &Path,
) {
    if verbose && cp_compat {
        println!("'{}' -> '{}'", source_path.display(), target_path.display());
    }
}

fn collect_recursive_copy_manifest(
    ctx: &RecursiveCopyContext,
    stats: &RecursiveCopyStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<(
    Vec<RecursiveDirectoryMetadataTask>,
    Vec<RecursiveSmallFileTask>,
    Vec<RecursiveFileTask>,
)> {
    let mut stack = vec![RecursiveDirectoryTask {
        source_dir: ctx.source_root.clone(),
        target_dir: ctx.target_root.clone(),
    }];
    let mut dir_tasks = Vec::new();
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
                if ctx.preserve_timestamps {
                    dir_tasks.push(RecursiveDirectoryMetadataTask {
                        target_path: target_path.clone(),
                        timestamps: preserved_timestamps_from_metadata(&metadata),
                    });
                }
                child_dirs.push(RecursiveDirectoryTask {
                    source_dir: source_path,
                    target_dir: target_path,
                });
                continue;
            }
            if file_type.is_symlink() {
                if ctx.cp_no_clobber && skip_copy_destination(&source_path, &target_path, false)? {
                    continue;
                }
                copy_symlink_entry(
                    &source_path,
                    &target_path,
                    stats,
                    false,
                    false,
                    ctx.preserve_timestamps,
                )?;
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
            let source_timestamps = preserved_timestamps_from_metadata(&metadata);
            if ctx.cp_no_clobber && skip_copy_destination(&source_path, &target_path, false)? {
                continue;
            }
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                small_tasks.push(RecursiveSmallFileTask {
                    source_path,
                    target_path,
                    source_len,
                    source_mode,
                    source_timestamps,
                    preserve_timestamps: ctx.preserve_timestamps,
                });
            } else {
                let resolved_copy =
                    resolve_recursive_large_copy_execution(ctx, &source_path, &target_path)?;
                large_tasks.push(RecursiveFileTask {
                    source_path,
                    target_path,
                    source_mode,
                    source_timestamps,
                    resolved_copy,
                    source_parent_dir: None,
                });
            }
        }
        child_dirs.reverse();
        stack.extend(child_dirs);
    }
    Ok((dir_tasks, small_tasks, large_tasks))
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
    pub(super) fn enqueue(&self, tasks: impl IntoIterator<Item = T>) {
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

    pub(super) fn enqueue_one(&self, task: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(task);
        self.ready.notify_one();
    }

    pub(super) fn claim(&self, stop: &AtomicBool) -> Option<T> {
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

    pub(super) fn complete_claim(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_workers = state.active_workers.saturating_sub(1);
        self.ready.notify_all();
    }

    pub(super) fn wake_all(&self) {
        self.ready.notify_all();
    }
}

impl<T> RecursiveTaskQueue<T> {
    pub(super) fn enqueue_batch(&self, tasks: impl IntoIterator<Item = T>) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(io::Error::other("recursive copy queue closed"));
        }
        let mut added = false;
        for task in tasks {
            state.queue.push_back(task);
            added = true;
        }
        if added {
            self.ready.notify_all();
        }
        Ok(())
    }

    pub(super) fn enqueue(&self, task: T) -> io::Result<()> {
        self.enqueue_batch(std::iter::once(task))
    }

    pub(super) fn claim(&self, stop: &AtomicBool) -> Option<T> {
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

    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.ready.notify_all();
    }

    pub(super) fn wake_all(&self) {
        self.ready.notify_all();
    }
}

fn ensure_recursive_target_not_inside_source(
    source_root: &Path,
    target_root: &Path,
) -> io::Result<()> {
    let source_abs = source_root.canonicalize()?;
    let target_abs = paths::prospective_absolute_path(target_root)?;
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

pub(super) fn create_directory_like(
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

pub(super) fn copy_symlink_entry(
    source_path: &Path,
    target_path: &Path,
    stats: &RecursiveCopyStats,
    verbose: bool,
    cp_compat: bool,
    preserve_timestamps: bool,
) -> io::Result<()> {
    ensure_parent_directory(target_path)?;
    ensure_removed_non_directory(target_path)?;
    let link_target = fs::read_link(source_path)?;
    symlink(&link_target, target_path)?;
    if preserve_timestamps {
        let metadata = fs::symlink_metadata(source_path)?;
        set_path_timestamps(
            target_path,
            preserved_timestamps_from_metadata(&metadata),
            true,
        )?;
    }
    maybe_print_verbose_copy(verbose, cp_compat, source_path, target_path);
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
    if ctx.preserve_timestamps {
        set_path_timestamps(&task.target_path, task.source_timestamps, false)?;
    }
    maybe_print_verbose_copy(
        ctx.verbose,
        ctx.cp_compat,
        &task.source_path,
        &task.target_path,
    );
    stats.files_copied.fetch_add(1, Ordering::Relaxed);
    stats.bytes_copied.fetch_add(copied, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    if let Some(counters) = sample_counters {
        counters.bytes.fetch_add(copied, Ordering::Relaxed);
        counters.units.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

use scheduling::{
    recursive_copy_dir_worker_count, recursive_copy_large_worker_count,
    recursive_copy_small_worker_count, recursive_copy_uses_small_file_range,
    recursive_copy_uses_threaded_large_lane, recursive_read_dir_worker_count,
    recursive_small_file_worker_count_for_path, resolve_recursive_large_copy_execution,
};

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
    fs::set_permissions(
        &task.target_path,
        fs::Permissions::from_mode(task.source_mode),
    )?;
    if task.preserve_timestamps {
        set_path_timestamps(&task.target_path, task.source_timestamps, false)?;
    }
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
    dir_metadata_tasks: &Mutex<Vec<RecursiveDirectoryMetadataTask>>,
    #[cfg(feature = "read-phase-timing")] lane_counters: &RecursiveCopyLaneCounters,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start];
    let source_root_fd = openat::open_dir_fd(&ctx.source_root)?;
    let target_root_fd = openat::open_dir_fd(&ctx.target_root)?;
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
                if ctx.preserve_timestamps {
                    dir_metadata_tasks
                        .lock()
                        .unwrap()
                        .push(RecursiveDirectoryMetadataTask {
                            target_path: target_path.clone(),
                            timestamps: preserved_timestamps_from_metadata(&metadata),
                        });
                }
                child_dirs.push(RecursiveDirectoryTask {
                    source_dir: source_path,
                    target_dir: target_path,
                });
                continue;
            }
            if file_type.is_symlink() {
                if ctx.cp_no_clobber && skip_copy_destination(&source_path, &target_path, false)? {
                    continue;
                }
                copy_symlink_entry(
                    &source_path,
                    &target_path,
                    stats,
                    ctx.verbose,
                    ctx.cp_compat,
                    ctx.preserve_timestamps,
                )?;
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
            let source_timestamps = preserved_timestamps_from_metadata(&metadata);
            if ctx.cp_no_clobber && skip_copy_destination(&source_path, &target_path, false)? {
                continue;
            }
            let relative_path = source_path
                .strip_prefix(&ctx.source_root)
                .map_err(|_| io::Error::other("recursive copy path escaped source root"))?
                .to_path_buf();
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                let copied = openat::copy_relative_file_openat(
                    &relative_path,
                    source_len,
                    source_mode,
                    &source_root_fd,
                    &target_root_fd,
                    ctx.relative_copy_method,
                )?;
                if ctx.preserve_timestamps {
                    set_path_timestamps(&target_path, source_timestamps, false)?;
                }
                maybe_print_verbose_copy(ctx.verbose, ctx.cp_compat, &source_path, &target_path);
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
                    source_timestamps,
                    resolved_copy,
                    source_parent_dir: None,
                })?;
                #[cfg(feature = "read-phase-timing")]
                lane_counters.note_queued("large");
            } else {
                let copied = openat::copy_relative_file_openat(
                    &relative_path,
                    source_len,
                    source_mode,
                    &source_root_fd,
                    &target_root_fd,
                    ctx.relative_copy_method,
                )?;
                if ctx.preserve_timestamps {
                    set_path_timestamps(&target_path, source_timestamps, false)?;
                }
                maybe_print_verbose_copy(ctx.verbose, ctx.cp_compat, &source_path, &target_path);
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

pub(super) fn walk_recursive_read_subtree(
    start: RecursiveReadDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveReadDirectoryTask>,
    file_queue: &RecursiveTaskQueue<RecursiveReadFileTask>,
    stop: &AtomicBool,
) -> io::Result<()> {
    const RECURSIVE_READ_FILE_BATCH_SIZE: usize = 256;

    fn flush_recursive_read_batch(
        file_queue: &RecursiveTaskQueue<RecursiveReadFileTask>,
        pending_files: &mut Vec<RecursiveReadFileTask>,
    ) -> io::Result<()> {
        if pending_files.is_empty() {
            return Ok(());
        }
        file_queue.enqueue_batch(std::mem::take(pending_files))
    }

    let mut stack = vec![start];
    let mut pending_files = Vec::with_capacity(RECURSIVE_READ_FILE_BATCH_SIZE);
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
                pending_files.push(RecursiveReadFileTask { source_path: path });
                if pending_files.len() >= RECURSIVE_READ_FILE_BATCH_SIZE {
                    flush_recursive_read_batch(file_queue, &mut pending_files)?;
                }
            }
        }
        flush_recursive_read_batch(file_queue, &mut pending_files)?;
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    flush_recursive_read_batch(file_queue, &mut pending_files)?;
    Ok(())
}

#[cfg(test)]
mod tests;
pub(crate) mod split_manifest;
pub(crate) mod scheduling;

pub(super) fn run_recursive_copy(ctx: RecursiveCopyContext, verbose: bool) -> io::Result<u64> {
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
    let directory_timestamps = Arc::new(Mutex::new(Vec::new()));
    create_directory_like(
        source_meta.permissions().mode(),
        &ctx.target_root,
        &stats,
        Some(sample_counters.as_ref()),
    )?;
    if ctx.preserve_timestamps {
        directory_timestamps
            .lock()
            .unwrap()
            .push(RecursiveDirectoryMetadataTask {
                target_path: ctx.target_root.clone(),
                timestamps: preserved_timestamps_from_metadata(&source_meta),
            });
    }
    let source_mount = config::mount_info_for_path(&ctx.source_root.to_string_lossy());
    let target_mount = config::mount_info_for_path(&ctx.target_root.to_string_lossy());
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
        Some(ThroughputSampler::start(
            "recursive-copy",
            "items",
            sample_counters.clone(),
        ))
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
        let directory_timestamps = directory_timestamps.clone();
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
                    directory_timestamps.as_ref(),
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
    if ctx.preserve_timestamps {
        let tasks = directory_timestamps.lock().unwrap().clone();
        finalize_directory_timestamps(&tasks)?;
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
