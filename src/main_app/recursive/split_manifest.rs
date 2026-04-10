use super::*;

pub(super) struct SplitManifestRecursiveCopyBenchmarkResult {
    pub(super) bytes_copied: u64,
    pub(super) files_copied: u64,
    pub(super) dirs_created: u64,
    pub(super) symlinks_created: u64,
    pub(super) small_file_tasks: usize,
    pub(super) large_file_tasks: usize,
    pub(super) manifest_phase_secs: f64,
    pub(super) copy_phase_secs: f64,
    pub(super) total_secs: f64,
}

pub(crate) fn run_split_manifest_recursive_copy(
    ctx: RecursiveCopyContext,
    verbose: bool,
) -> io::Result<u64> {
    let result = run_split_manifest_recursive_copy_with_result(ctx, verbose)?;
    fro::cio_println!(
        "split-manifest-recursive-copy {} bytes across {} files: dirs={} symlinks={} small={} large={} manifest_phase={:.4}s copy_phase={:.4}s total={:.4}s file_gbps={:.3}",
        result.bytes_copied,
        result.files_copied,
        result.dirs_created,
        result.symlinks_created,
        result.small_file_tasks,
        result.large_file_tasks,
        result.manifest_phase_secs,
        result.copy_phase_secs,
        result.total_secs,
        result.bytes_copied as f64 / result.copy_phase_secs.max(1e-9) / 1e9
    );
    Ok(result.bytes_copied)
}

pub(super) fn run_split_manifest_recursive_copy_with_result(
    ctx: RecursiveCopyContext,
    verbose: bool,
) -> io::Result<SplitManifestRecursiveCopyBenchmarkResult> {
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
    let mut root_dir_tasks = Vec::new();
    if ctx.preserve_timestamps {
        root_dir_tasks.push(RecursiveDirectoryMetadataTask {
            target_path: ctx.target_root.clone(),
            timestamps: preserved_timestamps_from_metadata(&source_meta),
        });
    }
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "split-manifest-recursive-copy",
            "items",
            sample_counters.clone(),
        ))
    } else {
        None
    };

    let overall_start = std::time::Instant::now();
    let manifest_start = std::time::Instant::now();
    let (mut dir_tasks, small_tasks, large_tasks) =
        collect_recursive_copy_manifest(&ctx, stats.as_ref(), sample_counters.as_ref())?;
    let manifest_elapsed = manifest_start.elapsed();
    root_dir_tasks.append(&mut dir_tasks);

    let small_file_tasks = small_tasks.len();
    let large_file_tasks = large_tasks.len();
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

    let copy_start = std::time::Instant::now();
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
    if ctx.preserve_timestamps {
        finalize_directory_timestamps(&root_dir_tasks)?;
    }
    let bytes_copied = stats.bytes_copied.load(Ordering::Relaxed);
    let copy_elapsed = copy_start.elapsed();
    let result = SplitManifestRecursiveCopyBenchmarkResult {
        bytes_copied,
        files_copied: stats.files_copied.load(Ordering::Relaxed),
        dirs_created: stats.dirs_created.load(Ordering::Relaxed),
        symlinks_created: stats.symlinks_created.load(Ordering::Relaxed),
        small_file_tasks,
        large_file_tasks,
        manifest_phase_secs: manifest_elapsed.as_secs_f64(),
        copy_phase_secs: copy_elapsed.as_secs_f64(),
        total_secs: overall_start.elapsed().as_secs_f64(),
    };
    if verbose {
        fro::cio_eprintln!(
            "split-manifest recursive copy: dirs_created={}, files_copied={}, symlinks_created={}, small_tasks={}, large_tasks={}, bytes_copied={}, manifest_phase={:.4}s, copy_phase={:.4}s",
            result.dirs_created,
            result.files_copied,
            result.symlinks_created,
            result.small_file_tasks,
            result.large_file_tasks,
            result.bytes_copied,
            result.manifest_phase_secs,
            result.copy_phase_secs
        );
    }
    Ok(result)
}
