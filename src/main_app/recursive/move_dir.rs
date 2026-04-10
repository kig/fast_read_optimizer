use super::*;
use std::collections::HashMap;

#[derive(Clone)]
struct RecursiveMoveDirectoryTask {
    source_dir: PathBuf,
    target_dir: PathBuf,
    parent_source_dir: Option<PathBuf>,
}

#[derive(Default)]
struct RecursiveMoveStats {
    files_moved: AtomicU64,
    dirs_created: AtomicU64,
    dirs_removed: AtomicU64,
    symlinks_moved: AtomicU64,
    bytes_moved: AtomicU64,
    items_completed: AtomicU64,
}

struct PendingMoveDir {
    source_dir: PathBuf,
    remaining_children: usize,
    parent_source_dir: Option<PathBuf>,
}

fn finish_move_directory(
    source_dir: PathBuf,
    parent_source_dir: Option<PathBuf>,
    pending: &Mutex<HashMap<PathBuf, PendingMoveDir>>,
    stats: &RecursiveMoveStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<()> {
    let mut next = Some((source_dir, parent_source_dir));
    while let Some((source_dir, parent_source_dir)) = next.take() {
        fs::remove_dir(&source_dir)?;
        stats.dirs_removed.fetch_add(1, Ordering::Relaxed);
        stats.items_completed.fetch_add(1, Ordering::Relaxed);
        sample_counters.units.fetch_add(1, Ordering::Relaxed);
        let Some(parent) = parent_source_dir else {
            continue;
        };
        let mut locked = pending.lock().unwrap();
        let parent_state = locked.get_mut(&parent).ok_or_else(|| {
            io::Error::other(format!(
                "recursive move lost parent pending state for {}",
                parent.display()
            ))
        })?;
        parent_state.remaining_children = parent_state.remaining_children.saturating_sub(1);
        if parent_state.remaining_children == 0 {
            let state = locked.remove(&parent).ok_or_else(|| {
                io::Error::other(format!(
                    "recursive move could not remove completed parent state for {}",
                    parent.display()
                ))
            })?;
            next = Some((state.source_dir, state.parent_source_dir));
        }
    }
    Ok(())
}

fn move_symlink_entry(
    source_path: &Path,
    target_path: &Path,
    stats: &RecursiveMoveStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<()> {
    ensure_parent_directory(target_path)?;
    ensure_removed_non_directory(target_path)?;
    let link_target = fs::read_link(source_path)?;
    symlink(&link_target, target_path)?;
    fs::remove_file(source_path)?;
    stats.symlinks_moved.fetch_add(1, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    sample_counters.units.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn move_small_file_entry(
    relative_path: &Path,
    source_path: &Path,
    source_len: u64,
    source_mode: u32,
    source_root_fd: &fs::File,
    target_root_fd: &fs::File,
    method: RelativeCopyMethod,
    stats: &RecursiveMoveStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<()> {
    let copied = openat::copy_relative_file_openat(
        relative_path,
        source_len,
        source_mode,
        source_root_fd,
        target_root_fd,
        method,
    )?;
    fs::remove_file(source_path)?;
    stats.files_moved.fetch_add(1, Ordering::Relaxed);
    stats.bytes_moved.fetch_add(copied, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    sample_counters.bytes.fetch_add(copied, Ordering::Relaxed);
    sample_counters.units.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn execute_recursive_file_move(
    task: &RecursiveFileTask,
    ctx: &RecursiveCopyContext,
    pending: &Mutex<HashMap<PathBuf, PendingMoveDir>>,
    stats: &RecursiveMoveStats,
    sample_counters: Option<&ThroughputSampleCounters>,
) -> io::Result<()> {
    let source_path = task.source_path.to_string_lossy();
    let target_path = task.target_path.to_string_lossy();
    let guard = CopyOperationGuard::new(&source_path, &target_path, ctx.use_lock)?;
    let copied = copy_file_with_strategy_and_truncate(
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
        true,
    )?;
    guard.ensure_source_unchanged()?;
    fs::set_permissions(
        &task.target_path,
        fs::Permissions::from_mode(task.source_mode),
    )?;
    fs::remove_file(&task.source_path)?;
    stats.files_moved.fetch_add(1, Ordering::Relaxed);
    stats.bytes_moved.fetch_add(copied, Ordering::Relaxed);
    stats.items_completed.fetch_add(1, Ordering::Relaxed);
    if let Some(counters) = sample_counters {
        counters.bytes.fetch_add(copied, Ordering::Relaxed);
        counters.units.fetch_add(1, Ordering::Relaxed);
        if let Some(parent_dir) = &task.source_parent_dir {
            let ready = {
                let mut locked = pending.lock().unwrap();
                let parent_state = locked.get_mut(parent_dir).ok_or_else(|| {
                    io::Error::other(format!(
                        "recursive move lost pending state for parent {}",
                        parent_dir.display()
                    ))
                })?;
                parent_state.remaining_children = parent_state.remaining_children.saturating_sub(1);
                if parent_state.remaining_children == 0 {
                    let state = locked.remove(parent_dir).ok_or_else(|| {
                        io::Error::other(format!(
                            "recursive move could not remove completed parent state for {}",
                            parent_dir.display()
                        ))
                    })?;
                    Some((state.source_dir, state.parent_source_dir))
                } else {
                    None
                }
            };
            if let Some((source_dir, parent_source_dir)) = ready {
                finish_move_directory(source_dir, parent_source_dir, pending, stats, counters)?;
            }
        }
    }
    Ok(())
}

fn walk_recursive_move_subtree(
    start: RecursiveMoveDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveMoveDirectoryTask>,
    large_queue: &RecursiveTaskQueue<RecursiveFileTask>,
    pending: &Mutex<HashMap<PathBuf, PendingMoveDir>>,
    ctx: &RecursiveCopyContext,
    stats: &RecursiveMoveStats,
    sample_counters: &ThroughputSampleCounters,
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
        let mut large_file_tasks = Vec::new();
        let mut pending_children = 0usize;
        for entry in fs::read_dir(&task.source_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let source_path = entry.path();
            let target_path = task.target_dir.join(entry.file_name());
            if file_type.is_dir() {
                let metadata = entry.metadata()?;
                let mode = metadata.permissions().mode();
                create_directory_like(mode, &target_path, &RecursiveCopyStats::default(), None)?;
                stats.dirs_created.fetch_add(1, Ordering::Relaxed);
                stats.items_completed.fetch_add(1, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                pending_children += 1;
                child_dirs.push(RecursiveMoveDirectoryTask {
                    source_dir: source_path,
                    target_dir: target_path,
                    parent_source_dir: Some(task.source_dir.clone()),
                });
                continue;
            }
            if file_type.is_symlink() {
                move_symlink_entry(&source_path, &target_path, stats, sample_counters)?;
                continue;
            }
            if !file_type.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "recursive move only supports regular files, directories, and symlinks (saw {})",
                        source_path.display()
                    ),
                ));
            }
            let metadata = entry.metadata()?;
            let source_len = metadata.len();
            let source_mode = metadata.permissions().mode();
            let source_timestamps = super::preserved_timestamps_from_metadata(&metadata);
            let relative_path = source_path
                .strip_prefix(&ctx.source_root)
                .map_err(|_| io::Error::other("recursive move path escaped source root"))?
                .to_path_buf();
            if recursive_copy_uses_small_file_range(ctx, source_len) {
                move_small_file_entry(
                    &relative_path,
                    &source_path,
                    source_len,
                    source_mode,
                    &source_root_fd,
                    &target_root_fd,
                    ctx.relative_copy_method,
                    stats,
                    sample_counters,
                )?;
            } else {
                let resolved_copy =
                    resolve_recursive_large_copy_execution(ctx, &source_path, &target_path)?;
                pending_children += 1;
                large_file_tasks.push(RecursiveFileTask {
                    source_path,
                    target_path,
                    source_mode,
                    source_timestamps,
                    resolved_copy,
                    source_parent_dir: Some(task.source_dir.clone()),
                });
            }
        }
        if pending_children == 0 {
            finish_move_directory(
                task.source_dir.clone(),
                task.parent_source_dir.clone(),
                pending,
                stats,
                sample_counters,
            )?;
            continue;
        }
        pending.lock().unwrap().insert(
            task.source_dir.clone(),
            PendingMoveDir {
                source_dir: task.source_dir.clone(),
                remaining_children: pending_children,
                parent_source_dir: task.parent_source_dir.clone(),
            },
        );
        for file_task in large_file_tasks {
            large_queue.enqueue(file_task)?;
        }
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    Ok(())
}

pub(crate) fn run_recursive_move(ctx: RecursiveCopyContext, verbose: bool) -> io::Result<u64> {
    let source_meta = fs::symlink_metadata(&ctx.source_root)?;
    if !source_meta.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recursive move requires a directory source",
        ));
    }
    ensure_recursive_target_not_inside_source(&ctx.source_root, &ctx.target_root)?;
    fs::create_dir_all(&ctx.target_root)?;
    fs::set_permissions(
        &ctx.target_root,
        fs::Permissions::from_mode(source_meta.permissions().mode()),
    )?;

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

    let stats = Arc::new(RecursiveMoveStats::default());
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let dir_queue = Arc::new(RecursiveDirectoryQueue::<RecursiveMoveDirectoryTask>::default());
    let large_queue = Arc::new(RecursiveTaskQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    let pending = Arc::new(Mutex::new(HashMap::<PathBuf, PendingMoveDir>::new()));
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "recursive-move",
            "items",
            sample_counters.clone(),
        ))
    } else {
        None
    };

    dir_queue.enqueue_one(RecursiveMoveDirectoryTask {
        source_dir: ctx.source_root.clone(),
        target_dir: ctx.target_root.clone(),
        parent_source_dir: None,
    });

    let worker_count = recursive_copy_dir_worker_count();
    let large_worker_count = recursive_copy_large_worker_count();

    let mut walk_threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let dir_queue = dir_queue.clone();
        let large_queue = large_queue.clone();
        let pending = pending.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_move_subtree(
                    task,
                    &dir_queue,
                    &large_queue,
                    pending.as_ref(),
                    &ctx,
                    &stats,
                    sample_counters.as_ref(),
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
        let pending = pending.clone();
        let ctx = ctx.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        large_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = queue.claim(&stop) {
                if let Err(err) = execute_recursive_file_move(
                    &task,
                    &ctx,
                    pending.as_ref(),
                    &stats,
                    Some(sample_counters.as_ref()),
                ) {
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
            .map_err(|_| io::Error::other("recursive move walk worker panicked"))?
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
            .map_err(|_| io::Error::other("recursive move file worker panicked"))?
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

    let bytes_moved = stats.bytes_moved.load(Ordering::Relaxed);
    if verbose {
        fro::cio_eprintln!(
            "recursive move: dirs_created={}, dirs_removed={}, files_moved={}, symlinks_moved={}, bytes_moved={}",
            stats.dirs_created.load(Ordering::Relaxed),
            stats.dirs_removed.load(Ordering::Relaxed),
            stats.files_moved.load(Ordering::Relaxed),
            stats.symlinks_moved.load(Ordering::Relaxed),
            bytes_moved
        );
    }
    Ok(bytes_moved)
}
