use super::*;

#[derive(Clone)]
struct RecursiveDeleteDirectoryTask {
    dir: PathBuf,
}

#[derive(Default)]
struct RecursiveDeleteStats {
    files_removed: AtomicU64,
    dirs_removed: AtomicU64,
    symlinks_removed: AtomicU64,
    bytes_removed: AtomicU64,
    items_completed: AtomicU64,
}

fn walk_recursive_delete_subtree(
    start: RecursiveDeleteDirectoryTask,
    dir_queue: &RecursiveDirectoryQueue<RecursiveDeleteDirectoryTask>,
    directories: &Mutex<Vec<PathBuf>>,
    stats: &RecursiveDeleteStats,
    sample_counters: &ThroughputSampleCounters,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start];
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        directories.lock().unwrap().push(task.dir.clone());
        let mut child_dirs = Vec::new();
        for entry in fs::read_dir(&task.dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                child_dirs.push(RecursiveDeleteDirectoryTask { dir: path });
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
            stats.bytes_removed.fetch_add(removed_bytes, Ordering::Relaxed);
            stats.items_completed.fetch_add(1, Ordering::Relaxed);
            sample_counters.bytes.fetch_add(removed_bytes, Ordering::Relaxed);
            sample_counters.units.fetch_add(1, Ordering::Relaxed);
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
    let directories = Arc::new(Mutex::new(Vec::new()));
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
    });

    let worker_count = recursive_copy_dir_worker_count();
    let mut walk_threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let dir_queue = dir_queue.clone();
        let directories = directories.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let sample_counters = sample_counters.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_delete_subtree(
                    task,
                    &dir_queue,
                    directories.as_ref(),
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

    if first_error.is_none() {
        let mut dirs = directories.lock().unwrap().clone();
        dirs.sort_by(|a, b| {
            b.components()
                .count()
                .cmp(&a.components().count())
                .then_with(|| b.cmp(a))
        });
        for dir in dirs {
            fs::remove_dir(&dir)?;
            stats.dirs_removed.fetch_add(1, Ordering::Relaxed);
            stats.items_completed.fetch_add(1, Ordering::Relaxed);
            sample_counters.units.fetch_add(1, Ordering::Relaxed);
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
