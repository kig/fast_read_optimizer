use super::*;
use crate::main_app::file_list::read_small_file_probe_then_fallback;
use crate::reader;

pub(in crate::main_app) fn bench_recursive_small_file_threads(
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
            format!(
                "bench-recursive-small-file-threads requires a directory root, got {}",
                root.display()
            ),
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

pub(in crate::main_app) fn bench_recursive_read(
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
            format!(
                "recursive-read-bench requires a directory root, got {}",
                root.display()
            ),
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
                let bytes_read = read_small_file_probe_then_fallback(
                    &read_config,
                    &task.source_path,
                    file_io_mode,
                )?;
                sample_counters
                    .bytes
                    .fetch_add(bytes_read, Ordering::Relaxed);
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
