use super::*;
use std::collections::HashMap;

type DeviceIdResolver = dyn Fn(&Path, &fs::Metadata) -> u64 + Send + Sync;
type SkippedDirHandler = dyn Fn(&Path) + Send + Sync;

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
    root_device: Option<u64>,
    device_id_resolver: &DeviceIdResolver,
    on_skipped_dir: &SkippedDirHandler,
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
                if let Some(root_device) = root_device {
                    let metadata = entry.metadata()?;
                    if device_id_resolver(&path, &metadata) != root_device {
                        on_skipped_dir(&path);
                        continue;
                    }
                }
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
            if !child_dirs.is_empty() {
                dir_queue.enqueue(child_dirs);
            }
            stack.push(local_dir);
        }
    }
    Ok(())
}

fn run_recursive_delete_with_options(
    root: &Path,
    verbose: bool,
    root_device: Option<u64>,
    device_id_resolver: Arc<DeviceIdResolver>,
    on_skipped_dir: Arc<SkippedDirHandler>,
) -> io::Result<u64> {
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
        let device_id_resolver = device_id_resolver.clone();
        let on_skipped_dir = on_skipped_dir.clone();
        walk_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = dir_queue.claim(&stop) {
                let result = walk_recursive_delete_subtree(
                    task,
                    &dir_queue,
                    pending.as_ref(),
                    &stats,
                    sample_counters.as_ref(),
                    root_device,
                    device_id_resolver.as_ref(),
                    on_skipped_dir.as_ref(),
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
        fro::cio_eprintln!(
            "recursive delete: dirs_removed={}, files_removed={}, symlinks_removed={}, bytes_removed={}",
            stats.dirs_removed.load(Ordering::Relaxed),
            stats.files_removed.load(Ordering::Relaxed),
            stats.symlinks_removed.load(Ordering::Relaxed),
            bytes_removed
        );
    }
    Ok(bytes_removed)
}

pub(crate) fn run_recursive_delete(root: &Path, verbose: bool) -> io::Result<u64> {
    run_recursive_delete_with_options(
        root,
        verbose,
        None,
        Arc::new(|_, metadata| metadata.dev()),
        Arc::new(|_| {}),
    )
}

pub(crate) fn run_recursive_delete_one_file_system<F>(
    root: &Path,
    verbose: bool,
    root_device: u64,
    on_skipped_dir: F,
) -> io::Result<u64>
where
    F: Fn(&Path) + Send + Sync + 'static,
{
    run_recursive_delete_with_options(
        root,
        verbose,
        Some(root_device),
        Arc::new(|_, metadata| metadata.dev()),
        Arc::new(on_skipped_dir),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_recursive_delete_test_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from("target")
            .join("test-artifacts")
            .join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn run_recursive_delete_removes_wide_tree_and_counts_regular_file_bytes() {
        let root = unique_recursive_delete_test_dir("recursive-delete-wide");
        fs::create_dir_all(&root).unwrap();

        let mut expected_bytes = 0_u64;
        for dir_index in 0..24 {
            let dir = root.join(format!("dir-{dir_index}"));
            let nested = dir.join("nested");
            fs::create_dir_all(&nested).unwrap();

            let top_bytes = vec![b'a' + (dir_index % 26) as u8; 64 + dir_index];
            expected_bytes += top_bytes.len() as u64;
            fs::write(dir.join("top.bin"), top_bytes).unwrap();

            let nested_bytes = vec![b'z' - (dir_index % 26) as u8; 128 + dir_index];
            expected_bytes += nested_bytes.len() as u64;
            fs::write(nested.join("deep.bin"), nested_bytes).unwrap();

            symlink("nested/deep.bin", dir.join("deep-link")).unwrap();
        }

        let removed_bytes = run_recursive_delete(&root, false).unwrap();

        assert_eq!(removed_bytes, expected_bytes);
        assert!(!root.exists());
    }

    #[test]
    fn run_recursive_delete_one_file_system_skips_directories_on_other_devices() {
        let root = unique_recursive_delete_test_dir("recursive-delete-one-file-system");
        let skipped = root.join("skipped-mount");
        let removed = root.join("removed-dir");
        fs::create_dir_all(skipped.join("nested")).unwrap();
        fs::create_dir_all(removed.join("nested")).unwrap();
        fs::write(skipped.join("nested/keep.txt"), b"keep\n").unwrap();
        fs::write(removed.join("nested/delete.txt"), b"delete\n").unwrap();

        let root_device = fs::metadata(&root).unwrap().dev();
        let alternate_device = root_device + 1;
        let skipped_for_resolver = skipped.clone();
        let skipped_dirs = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let skipped_dirs_for_handler = skipped_dirs.clone();
        let removed_bytes = run_recursive_delete_with_options(
            &root,
            false,
            Some(root_device),
            Arc::new(move |path, metadata| {
                if path == skipped_for_resolver {
                    alternate_device
                } else {
                    metadata.dev()
                }
            }),
            Arc::new(move |path| {
                skipped_dirs_for_handler
                    .lock()
                    .unwrap()
                    .push(path.to_path_buf());
            }),
        )
        .unwrap_err();

        assert_eq!(removed_bytes.kind(), io::ErrorKind::DirectoryNotEmpty);
        assert!(skipped.exists());
        assert!(!removed.exists());
        let mut seen = skipped_dirs.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, vec![skipped]);

        fs::remove_dir_all(&root).unwrap();
    }
}
