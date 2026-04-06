use super::*;

pub(super) fn load_manifest_prefix(files: &[PathBuf], prefix_files: usize) -> Arc<Vec<PathBuf>> {
    Arc::new(
        files
            .iter()
            .take(prefix_files.min(files.len()))
            .cloned()
            .collect(),
    )
}

pub(super) fn run_manifest_blocking_worker(
    files: Arc<Vec<PathBuf>>,
    next_index: Arc<AtomicUsize>,
    use_direct: bool,
    stats: Arc<RecursiveReadStats>,
) -> io::Result<()> {
    let mut buffer = AlignedBuffer::new(FILE_LIST_URING_SLOT_BUFFER_SIZE);
    loop {
        let index = next_index.fetch_add(1, Ordering::Relaxed);
        let Some(path) = files.get(index) else {
            return Ok(());
        };
        let path_str = path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path is not valid UTF-8: {}", path.display()),
            )
        })?;
        let (file, file_direct) = open_reader_files(path_str, use_direct)?;
        let target = if use_direct { &file_direct } else { &file };
        let read = target.read_at(buffer.as_mut_slice(), 0)?;
        stats.files_read.fetch_add(1, Ordering::Relaxed);
        stats.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
    }
}

pub(crate) fn read_small_file_probe_then_fallback(
    config: &config::LoadedConfig,
    path: &Path,
    io_mode: common::IOMode,
) -> io::Result<u64> {
    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let mut file = fs::File::open(path)?;
    let mut probe = vec![0_u8; 64 * 1024];
    let read = file.read(&mut probe)?;
    if read < probe.len() {
        return Ok(read as u64);
    }
    let (bytes_read, _file_size, _params) =
        visit_file_blocks_for_mode(config, "read", path_str, io_mode, |_| Ok(()))?;
    Ok(bytes_read)
}

pub(super) fn run_manifest_blocking_once(
    files: Arc<Vec<PathBuf>>,
    threads: usize,
    use_direct: bool,
) -> io::Result<ManifestReadSweepResult> {
    let start = std::time::Instant::now();
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(threads.max(1));
    for _ in 0..threads.max(1) {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        handles.push(std::thread::spawn(move || {
            run_manifest_blocking_worker(files, next_index, use_direct, stats)
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| io::Error::other("manifest blocking worker panicked"))??;
    }
    Ok(ManifestReadSweepResult {
        prefix_files: files.len(),
        variant: if threads <= 1 {
            ManifestReadVariant::SingleThreadBlocking
        } else {
            ManifestReadVariant::MultiThreadBlocking { threads }
        },
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

pub(super) fn run_manifest_uring_once(
    files: Arc<Vec<PathBuf>>,
    threads: usize,
    qd: usize,
    use_direct: bool,
) -> io::Result<ManifestReadSweepResult> {
    let start = std::time::Instant::now();
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(threads.max(1));
    for _ in 0..threads.max(1) {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        handles.push(std::thread::spawn(move || {
            file_list_uring_worker(
                files,
                next_index,
                qd,
                use_direct,
                stats,
                Arc::new(ThroughputSampleCounters::default()),
            )
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| io::Error::other("manifest io_uring worker panicked"))??;
    }
    Ok(ManifestReadSweepResult {
        prefix_files: files.len(),
        variant: if threads <= 1 {
            ManifestReadVariant::SingleThreadUring { qd }
        } else {
            ManifestReadVariant::MultiThreadUring { threads, qd }
        },
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

pub(super) fn print_manifest_read_sweep_results(results: &[ManifestReadSweepResult]) {
    let mut rows = vec![vec![
        "prefix_files".to_string(),
        "variant".to_string(),
        "time(s)".to_string(),
        "GB/s".to_string(),
        "files/s".to_string(),
        "bytes".to_string(),
    ]];
    for result in results {
        rows.push(vec![
            result.prefix_files.to_string(),
            result.variant.label(),
            format!("{:.4}", result.elapsed_secs),
            format!(
                "{:.3}",
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9
            ),
            format!(
                "{:.1}",
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            ),
            result.total_bytes.to_string(),
        ]);
    }
    let column_count = rows[0].len();
    let widths = (0..column_count)
        .map(|index| rows.iter().map(|row| row[index].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, cell)| format!("{cell:<width$}", width = widths[index]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

pub(crate) fn bench_file_list_read(
    config: &config::LoadedConfig,
    manifest_path: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = load_paths_from_manifest(Path::new(manifest_path))?;
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }

    let start = std::time::Instant::now();
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "file-list-read-bench",
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
        manifest_path,
        config::IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
    );
    read_config.update_params_for_path(
        "read",
        true,
        manifest_path,
        config::IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
    );
    let stats = Arc::new(RecursiveReadStats::default());
    let file_queue = Arc::new(RecursiveTaskQueue::<RecursiveReadFileTask>::default());
    let stop = Arc::new(AtomicBool::new(false));
    for source_path in files {
        file_queue.enqueue(RecursiveReadFileTask { source_path })?;
    }
    file_queue.close();

    let file_worker_count = recursive_read_file_worker_count();
    let mut file_threads = Vec::with_capacity(file_worker_count);
    for _ in 0..file_worker_count {
        let file_queue = file_queue.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        let sample_counters = sample_counters.clone();
        let read_config = read_config.clone();
        file_threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(task) = file_queue.claim(&stop) {
                let sample_counters_for_file = sample_counters.clone();
                let file_str = task.source_path.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("path is not valid UTF-8: {}", task.source_path.display()),
                    )
                })?;
                let (bytes_read, _file_size, _params) = visit_file_blocks_for_mode(
                    &read_config,
                    "read",
                    file_str,
                    io_mode,
                    move |block| {
                        sample_counters_for_file
                            .bytes
                            .fetch_add(block.data.len() as u64, Ordering::Relaxed);
                        Ok(())
                    },
                )?;
                stats.files_read.fetch_add(1, Ordering::Relaxed);
                stats.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
                if bytes_read == 0 && fs::metadata(&task.source_path)?.len() != 0 {
                    stop.store(true, Ordering::SeqCst);
                    file_queue.wake_all();
                    return Err(io::Error::other(format!(
                        "file-list-read-bench observed zero bytes for non-empty file {}",
                        task.source_path.display()
                    )));
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in file_threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("file-list read worker panicked"))?
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
    let elapsed = start.elapsed().as_secs_f64();
    if verbose {
        eprintln!(
            "file-list-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s ({:.1} files/s)",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9,
            total_files as f64 / elapsed.max(1e-9)
        );
    } else {
        println!(
            "file-list-read-bench {} bytes across {} files in {:.4} s, {:.1} GB/s",
            total_bytes,
            total_files,
            elapsed,
            total_bytes as f64 / elapsed / 1e9
        );
    }
    Ok(total_bytes)
}

pub(crate) fn bench_file_list_read_uring(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = Arc::new(load_paths_from_manifest(Path::new(manifest_path))?);
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }
    raise_nofile_soft_limit(verbose);
    let use_direct = file_list_uring_should_use_direct(config, manifest_path, io_mode);
    let mut results = Vec::with_capacity(FILE_LIST_URING_INFLIGHT_SWEEP.len());
    for inflight_per_thread in FILE_LIST_URING_INFLIGHT_SWEEP {
        let result = run_file_list_uring_bench_once(
            files.clone(),
            inflight_per_thread,
            use_direct,
            verbose,
        )?;
        eprintln!(
            "result\tinflight/thread={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
            result.inflight_per_thread,
            result.elapsed_secs,
            result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
            result.total_files as f64 / result.elapsed_secs.max(1e-9)
        );
        results.push(result);
    }
    print_file_list_uring_sweep_results(&results);
    let best = results
        .iter()
        .min_by(|a, b| a.elapsed_secs.total_cmp(&b.elapsed_secs))
        .ok_or_else(|| io::Error::other("file-list io_uring sweep produced no results"))?;
    println!(
        "file-list-read-uring-bench best inflight/thread={} {} bytes across {} files in {:.4} s, {:.3} GB/s ({:.1} files/s)",
        best.inflight_per_thread,
        best.total_bytes,
        best.total_files,
        best.elapsed_secs,
        best.total_bytes as f64 / best.elapsed_secs.max(1e-9) / 1e9,
        best.total_files as f64 / best.elapsed_secs.max(1e-9)
    );
    Ok(best.total_bytes)
}

pub(crate) fn bench_file_list_read_open_read_close_sweep(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
    verbose: bool,
) -> io::Result<u64> {
    let files = load_paths_from_manifest(Path::new(manifest_path))?;
    if files.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no file paths found in manifest {}", manifest_path),
        ));
    }
    raise_nofile_soft_limit(verbose);
    let use_direct = file_list_uring_should_use_direct(config, manifest_path, io_mode);
    let mut results = Vec::new();
    let prefix_sweep = MANIFEST_READ_PREFIX_SWEEP
        .iter()
        .copied()
        .filter(|count| *count <= files.len())
        .chain(std::iter::once(files.len()))
        .collect::<Vec<_>>();
    for prefix_files in prefix_sweep {
        let prefix = load_manifest_prefix(&files, prefix_files);
        let st = run_manifest_blocking_once(prefix.clone(), 1, use_direct)?;
        eprintln!(
            "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
            st.prefix_files,
            st.variant.label(),
            st.elapsed_secs,
            st.total_bytes as f64 / st.elapsed_secs.max(1e-9) / 1e9,
            st.total_files as f64 / st.elapsed_secs.max(1e-9)
        );
        results.push(st);

        for threads in MANIFEST_MT_BLOCKING_THREADS {
            let result = run_manifest_blocking_once(prefix.clone(), threads, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }

        for qd in MANIFEST_ST_URING_QDS {
            let result = run_manifest_uring_once(prefix.clone(), 1, qd, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }

        for (threads, qd) in MANIFEST_MT_URING_CONFIGS {
            let result = run_manifest_uring_once(prefix.clone(), threads, qd, use_direct)?;
            eprintln!(
                "result\tprefix={}\tvariant={}\ttime={:.4}s\tgbps={:.3}\tfiles/s={:.1}",
                result.prefix_files,
                result.variant.label(),
                result.elapsed_secs,
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9,
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            );
            results.push(result);
        }
    }

    print_manifest_read_sweep_results(&results);
    let best = results
        .iter()
        .filter(|result| result.prefix_files == files.len())
        .min_by(|a, b| a.elapsed_secs.total_cmp(&b.elapsed_secs))
        .ok_or_else(|| io::Error::other("manifest open-read-close sweep produced no results"))?;
    println!(
        "file-list-read-open-read-close-sweep best prefix_files={} variant={} {} bytes across {} files in {:.4} s, {:.3} GB/s ({:.1} files/s)",
        best.prefix_files,
        best.variant.label(),
        best.total_bytes,
        best.total_files,
        best.elapsed_secs,
        best.total_bytes as f64 / best.elapsed_secs.max(1e-9) / 1e9,
        best.total_files as f64 / best.elapsed_secs.max(1e-9)
    );
    Ok(best.total_bytes)
}
