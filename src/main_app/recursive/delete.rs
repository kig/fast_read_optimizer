use super::*;
use std::collections::HashMap;

#[derive(Clone)]
struct RecursiveDeleteDirectoryTask {
    dir: PathBuf,
    parent_dir: Option<PathBuf>,
}

#[derive(Default)]
struct RecursiveDeleteStats {
    files_removed: AtomicU64,
    dirs_removed: AtomicU64,
    symlinks_removed: AtomicU64,
    bytes_removed: AtomicU64,
    items_completed: AtomicU64,
}

struct PendingDeleteDir {
    remaining_children: usize,
    parent_dir: Option<PathBuf>,
}

fn finish_delete_directory(
    dir: PathBuf,
    parent_dir: Option<PathBuf>,
    pending: &Mutex<HashMap<PathBuf, PendingDeleteDir>>,
    stats: &RecursiveDeleteStats,
    sample_counters: &ThroughputSampleCounters,
) -> io::Result<()> {
    let mut next = Some((dir, parent_dir));
    while let Some((dir, parent_dir)) = next.take() {
        fs::remove_dir(&dir)?;
        stats.dirs_removed.fetch_add(1, Ordering::Relaxed);
        stats.items_completed.fetch_add(1, Ordering::Relaxed);
        sample_counters.units.fetch_add(1, Ordering::Relaxed);
        let Some(parent) = parent_dir else {
            continue;
        };
        let mut locked = pending.lock().unwrap();
        let parent_state = locked.get_mut(&parent).ok_or_else(|| {
            io::Error::other(format!(
                "recursive delete lost parent pending state for {}",
                parent.display()
            ))
        })?;
        parent_state.remaining_children = parent_state.remaining_children.saturating_sub(1);
        if parent_state.remaining_children == 0 {
            let state = locked.remove(&parent).ok_or_else(|| {
                io::Error::other(format!(
                    "recursive delete could not remove completed parent state for {}",
                    parent.display()
                ))
            })?;
            next = Some((parent, state.parent_dir));
        }
    }
    Ok(())
}

fn walk_recursive_delete_subtree(
    start: RecursiveDeleteDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveDeleteDirectoryTask>,
    pending: &Mutex<HashMap<PathBuf, PendingDeleteDir>>,
    stats: &RecursiveDeleteStats,
    sample_counters: &ThroughputSampleCounters,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start];
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&task.dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                child_dirs.push(RecursiveDeleteDirectoryTask {
                    dir: path,
                    parent_dir: Some(task.dir.clone()),
                });
                continue;
            }
            if file_type.is_symlink() {
                fs::remove_file(&path)?;
                stats.symlinks_removed.fetch_add(1, Ordering::Relaxed);
                stats.items_completed.fetch_add(1, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let removed_bytes = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            fs::remove_file(&path)?;
            stats.files_removed.fetch_add(1, Ordering::Relaxed);
            stats
                .bytes_removed
                .fetch_add(removed_bytes, Ordering::Relaxed);
            stats.items_completed.fetch_add(1, Ordering::Relaxed);
            sample_counters
                .bytes
                .fetch_add(removed_bytes, Ordering::Relaxed);
            sample_counters.units.fetch_add(1, Ordering::Relaxed);
        }
        let pending_children = child_dirs.len();
        if pending_children == 0 {
            finish_delete_directory(
                task.dir.clone(),
                task.parent_dir.clone(),
                pending,
                stats,
                sample_counters,
            )?;
            continue;
        }
        pending.lock().unwrap().insert(
            task.dir.clone(),
            PendingDeleteDir {
                remaining_children: pending_children,
                parent_dir: task.parent_dir.clone(),
            },
        );
        if let Some(local_dir) = child_dirs.pop() {
            for task in child_dirs {
                dir_queue.enqueue_one(task);
            }
            stack.push(local_dir);
        }
    }
    Ok(())
}

pub(crate) fn run_recursive_delete(root: &Path, verbose: bool) -> io::Result<u64> {
    let source_meta = fs::symlink_metadata(root)?;
    if !source_meta.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recursive delete requires a directory root",
        ));
    }

    let stats = Arc::new(RecursiveDeleteStats::default());
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let dir_queue = Arc::new(RecursiveDirectoryQueue::<RecursiveDeleteDirectoryTask>::default());
    let stop = Arc::new(AtomicBool::new(false));
    let pending = Arc::new(Mutex::new(HashMap::<PathBuf, PendingDeleteDir>::new()));
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "recursive-delete",
            "items",
            sample_counters.clone(),
        ))
    } else {
        None
    };

    dir_queue.enqueue_one(RecursiveDeleteDirectoryTask {
        dir: root.to_path_buf(),
        parent_dir: None,
    });

    let worker_count = recursive_copy_dir_worker_count();
    let mut walk_threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let dir_queue = dir_queue.clone();
        let pending = pending.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_delete_subtree(
                    task,
                    &dir_queue,
                    pending.as_ref(),
                    &stats,
                    sample_counters.as_ref(),
                    &stop,
                );
                dir_queue.complete_claim();
                if let Err(err) = result {
                    stop.store(true, Ordering::SeqCst);
                    dir_queue.wake_all();
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
            .map_err(|_| io::Error::other("recursive delete walk worker panicked"))?
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

    let bytes_removed = stats.bytes_removed.load(Ordering::Relaxed);
    if verbose {
        eprintln!(
            "recursive delete: dirs_removed={}, files_removed={}, symlinks_removed={}, bytes_removed={}",
            stats.dirs_removed.load(Ordering::Relaxed),
            stats.files_removed.load(Ordering::Relaxed),
            stats.symlinks_removed.load(Ordering::Relaxed),
            bytes_removed
        );
    }
    Ok(bytes_removed)
}
