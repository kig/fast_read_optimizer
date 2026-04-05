use super::*;
use crate::main_app::recursive::openat::{copy_small_file_openat, open_dir_fd};
use crate::main_app::recursive::recursive_read_file_worker_count;

pub(super) fn load_paths_from_manifest(path: &Path) -> io::Result<Vec<PathBuf>> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    let mut paths = Vec::new();
    loop {
        line.clear();
        let read = std::io::BufRead::read_line(&mut reader, &mut line)?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        paths.push(PathBuf::from(trimmed));
    }
    Ok(paths)
}

pub(super) fn load_manifest_copy_entries(manifest: &Path, source_root: &Path) -> io::Result<Vec<ManifestCopyEntry>> {
    let paths = load_paths_from_manifest(manifest)?;
    let mut entries = Vec::with_capacity(paths.len());
    for source_path in paths {
        let metadata = fs::symlink_metadata(&source_path)?;
        if !metadata.file_type().is_file() {
            continue;
        }
        let relative_path = source_path.strip_prefix(source_root).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "manifest path {} is not under source root {}",
                    source_path.display(),
                    source_root.display()
                ),
            )
        })?;
        let relative_path = relative_path.to_path_buf();
        entries.push(ManifestCopyEntry {
            relative_path,
            size: metadata.len(),
            mode: metadata.permissions().mode(),
        });
    }
    Ok(entries)
}

pub(super) fn create_manifest_target_dirs(
    entries: &[ManifestCopyEntry],
    target_root: &Path,
) -> io::Result<(usize, std::time::Duration)> {
    let start = std::time::Instant::now();
    let mut dirs = std::collections::BTreeSet::<PathBuf>::new();
    for entry in entries {
        let mut current = PathBuf::new();
        if let Some(parent) = entry.relative_path.parent() {
            for component in parent.components() {
                current.push(component.as_os_str());
                dirs.insert(current.clone());
            }
        }
    }
    for dir in &dirs {
        fs::create_dir_all(target_root.join(dir))?;
    }
    Ok((dirs.len(), start.elapsed()))
}

pub(super) const FILE_LIST_URING_THREAD_COUNT: usize = 32;
pub(super) const FILE_LIST_URING_INFLIGHT_SWEEP: [usize; 5] = [32, 64, 128, 256, 512];
pub(super) const FILE_LIST_URING_SLOT_BUFFER_SIZE: usize = 4096;
pub(super) const MANIFEST_READ_PREFIX_SWEEP: [usize; 5] = [512, 2048, 8192, 16384, 32768];
pub(super) const MANIFEST_MT_BLOCKING_THREADS: [usize; 3] = [4, 16, 32];
pub(super) const MANIFEST_ST_URING_QDS: [usize; 4] = [32, 64, 128, 256];
pub(super) const MANIFEST_MT_URING_CONFIGS: [(usize, usize); 4] = [(4, 32), (8, 32), (16, 64), (32, 64)];

pub(super) fn file_list_uring_should_use_direct(
    config: &config::LoadedConfig,
    manifest_path: &str,
    io_mode: common::IOMode,
) -> bool {
    match io_mode {
        common::IOMode::Direct => true,
        common::IOMode::PageCache => false,
        common::IOMode::Auto => {
            if config
                .mount_info_for_path(manifest_path)
                .as_ref()
                .is_some_and(|info| info.fstype == "zfs")
            {
                return true;
            }
            let strategy = config.get_read_auto_strategy_for_path(manifest_path);
            matches!(
                strategy.hot_small_path,
                ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect
            ) || matches!(
                strategy.cold_small_path,
                ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect
            )
        }
    }
}

pub(super) fn raise_nofile_soft_limit(verbose: bool) {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let get_result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) };
    if get_result != 0 {
        if verbose {
            let err = io::Error::last_os_error();
            eprintln!("warning: failed to read RLIMIT_NOFILE: {err}");
        }
        return;
    }
    if limits.rlim_cur >= limits.rlim_max {
        return;
    }
    let updated = libc::rlimit {
        rlim_cur: limits.rlim_max,
        rlim_max: limits.rlim_max,
    };
    let set_result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &updated) };
    if set_result != 0 && verbose {
        let err = io::Error::last_os_error();
        eprintln!(
            "warning: failed to raise RLIMIT_NOFILE from {} to {}: {err}",
            limits.rlim_cur, limits.rlim_max
        );
    }
}

pub(super) fn file_list_uring_claim_path(
    files: &[PathBuf],
    next_index: &AtomicUsize,
) -> Option<PathBuf> {
    let index = next_index.fetch_add(1, Ordering::Relaxed);
    files.get(index).cloned()
}

pub(super) fn prepare_file_list_uring_read(path: PathBuf, use_direct: bool, slot_buffer_index: usize) -> io::Result<Option<FileListUringInflightRead>> {
    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let (file, file_direct) = open_reader_files(path_str, use_direct)?;
    Ok(Some(FileListUringInflightRead { file, file_direct, slot_buffer_index }))
}

pub(super) fn submit_file_list_uring_read(
    io_uring: &mut IoUring,
    slot: usize,
    read: &mut FileListUringInflightRead,
    slot_buffers: &mut [AlignedBuffer],
    use_direct: bool,
) -> io::Result<()> {
    let buffer = slot_buffers
        .get_mut(read.slot_buffer_index)
        .ok_or_else(|| io::Error::other("missing slot buffer for file-list io_uring read"))?;
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
        if use_direct {
            sqe.prep_read(read.file_direct.as_raw_fd(), buffer.as_mut_slice(), 0);
        } else {
            sqe.prep_read(read.file.as_raw_fd(), buffer.as_mut_slice(), 0);
        }
        sqe.set_user_data(slot as u64);
    }
    Ok(())
}

pub(super) fn file_list_uring_fill_slots(
    io_uring: &mut IoUring,
    slots: &mut [Option<FileListUringInflightRead>],
    slot_buffers: &mut [AlignedBuffer],
    files: &[PathBuf],
    next_index: &AtomicUsize,
    use_direct: bool,
    _stats: &RecursiveReadStats,
    _sample_counters: &ThroughputSampleCounters,
) -> io::Result<usize> {
    let mut queued = 0usize;
    for (slot_index, slot) in slots.iter_mut().enumerate() {
        if slot.is_some() {
            continue;
        }
        loop {
            let Some(path) = file_list_uring_claim_path(files, next_index) else {
                break;
            };
            match prepare_file_list_uring_read(path, use_direct, slot_index)? {
                Some(mut read) => {
                    submit_file_list_uring_read(io_uring, slot_index, &mut read, slot_buffers, use_direct)?;
                    *slot = Some(read);
                    queued += 1;
                    break;
                }
                None => unreachable!("prepare_file_list_uring_read always returns Some"),
            }
        }
    }
    Ok(queued)
}

pub(super) fn wait_for_ready_slots(io_uring: &mut IoUring) -> io::Result<Vec<(usize, u32)>> {
    let first = io_uring.wait_for_cqe().map_err(io::Error::other)?;
    let mut ready = vec![(
        usize::try_from(first.user_data())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot id overflowed"))?,
        first.result()?,
    )];
    while io_uring.cq_ready() > 0 {
        let cq = io_uring
            .peek_for_cqe()
            .ok_or_else(|| io::Error::other("completion queue reported ready but no CQE was available"))?;
        ready.push((
            usize::try_from(cq.user_data())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot id overflowed"))?,
            cq.result()?,
        ));
    }
    Ok(ready)
}

pub(super) fn file_list_uring_worker(
    files: Arc<Vec<PathBuf>>,
    next_index: Arc<AtomicUsize>,
    inflight_per_thread: usize,
    use_direct: bool,
    stats: Arc<RecursiveReadStats>,
    sample_counters: Arc<ThroughputSampleCounters>,
) -> io::Result<()> {
    let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
    let mut slot_buffers = std::iter::repeat_with(|| AlignedBuffer::new(FILE_LIST_URING_SLOT_BUFFER_SIZE))
        .take(inflight_per_thread)
        .collect::<Vec<_>>();
    let mut slots = std::iter::repeat_with(|| None)
        .take(inflight_per_thread)
        .collect::<Vec<Option<FileListUringInflightRead>>>();
    let mut inflight = file_list_uring_fill_slots(
        &mut io_uring,
        &mut slots,
        &mut slot_buffers,
        &files,
        &next_index,
        use_direct,
        &stats,
        &sample_counters,
    )?;
    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(io::Error::other)?;
    loop {
        for (slot_index, result) in wait_for_ready_slots(&mut io_uring)? {
            let _read = slots
                .get_mut(slot_index)
                .and_then(Option::take)
                .ok_or_else(|| io::Error::other("completed slot had no active file read"))?;
            let actual_len = validate_read_result(
                "file-list-uring-read",
                0,
                FILE_LIST_URING_SLOT_BUFFER_SIZE,
                result,
            )?;
            if actual_len > 0 {
                sample_counters
                    .bytes
                    .fetch_add(actual_len as u64, Ordering::Relaxed);
                stats
                    .bytes_read
                    .fetch_add(actual_len as u64, Ordering::Relaxed);
            }
            stats.files_read.fetch_add(1, Ordering::Relaxed);
            sample_counters.units.fetch_add(1, Ordering::Relaxed);
            inflight = inflight.saturating_sub(1);
        }
        inflight += file_list_uring_fill_slots(
            &mut io_uring,
            &mut slots,
            &mut slot_buffers,
            &files,
            &next_index,
            use_direct,
            &stats,
            &sample_counters,
        )?;
        if inflight == 0 {
            return Ok(());
        }
        io_uring.submit_sqes().map_err(io::Error::other)?;
    }
}

pub(super) fn run_file_list_uring_bench_once(
    files: Arc<Vec<PathBuf>>,
    inflight_per_thread: usize,
    use_direct: bool,
    verbose: bool,
) -> io::Result<FileListUringSweepResult> {
    let start = std::time::Instant::now();
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    if verbose {
        eprintln!(
            "file-list-read-uring-bench run threads={} inflight/thread={} direct={}",
            FILE_LIST_URING_THREAD_COUNT, inflight_per_thread, use_direct
        );
    }
    let sampler = if verbose {
        Some(ThroughputSampler::start(
            "file-list-read-uring-bench",
            "files",
            sample_counters.clone(),
        ))
    } else {
        None
    };
    let stats = Arc::new(RecursiveReadStats::default());
    let next_index = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::with_capacity(FILE_LIST_URING_THREAD_COUNT);
    for _ in 0..FILE_LIST_URING_THREAD_COUNT {
        let files = files.clone();
        let next_index = next_index.clone();
        let stats = stats.clone();
        let sample_counters = sample_counters.clone();
        threads.push(std::thread::spawn(move || {
            file_list_uring_worker(
                files,
                next_index,
                inflight_per_thread,
                use_direct,
                stats,
                sample_counters,
            )
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("file-list io_uring read worker panicked"))?
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
    Ok(FileListUringSweepResult {
        inflight_per_thread,
        total_bytes: stats.bytes_read.load(Ordering::Relaxed),
        total_files: stats.files_read.load(Ordering::Relaxed) as usize,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

pub(super) fn print_file_list_uring_sweep_results(results: &[FileListUringSweepResult]) {
    let mut rows = vec![vec![
        "inflight/thread".to_string(),
        "time(s)".to_string(),
        "GB/s".to_string(),
        "files/s".to_string(),
        "bytes".to_string(),
        "files".to_string(),
    ]];
    for result in results {
        rows.push(vec![
            result.inflight_per_thread.to_string(),
            format!("{:.4}", result.elapsed_secs),
            format!("{:.3}", result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9),
            format!("{:.1}", result.total_files as f64 / result.elapsed_secs.max(1e-9)),
            result.total_bytes.to_string(),
            result.total_files.to_string(),
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

pub(super) fn load_manifest_prefix(files: &[PathBuf], prefix_files: usize) -> Arc<Vec<PathBuf>> {
    Arc::new(files.iter().take(prefix_files.min(files.len())).cloned().collect())
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

pub(super) fn read_small_file_probe_then_fallback(
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
            format!("{:.3}", result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9),
            format!("{:.1}", result.total_files as f64 / result.elapsed_secs.max(1e-9)),
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

pub(super) fn bench_file_list_read(
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

pub(super) fn bench_file_list_read_uring(
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

pub(super) fn bench_file_list_read_open_read_close_sweep(
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

pub(super) fn bench_manifest_recursive_copy(
    manifest_path: &str,
    source_root: &str,
    target_root: &str,
    overlap_large_file: Option<&str>,
    verbose: bool,
) -> io::Result<u64> {
    let manifest = Path::new(manifest_path);
    let source_root = Path::new(source_root);
    let target_root = Path::new(target_root);
    if target_root.exists() {
        fs::remove_dir_all(target_root)?;
    }
    fs::create_dir_all(target_root)?;

    let entries = load_manifest_copy_entries(manifest, source_root)?;
    if entries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found in manifest {}", manifest.display()),
        ));
    }
    let total_bytes = entries.iter().map(|entry| entry.size).sum::<u64>();

    let overall_start = std::time::Instant::now();
    let (dirs_created, dir_elapsed) = create_manifest_target_dirs(&entries, target_root)?;

    let source_root_fd = open_dir_fd(source_root)?;
    let target_root_fd = open_dir_fd(target_root)?;
    let file_start = std::time::Instant::now();

    let overlap_handle = overlap_large_file.map(|large_src| {
        let large_src = large_src.to_string();
        let large_dst = target_root.join(".fro_manifest_overlap_large_copy.bin");
        std::thread::spawn(move || {
            let large_src_str = large_src;
            let large_dst_str = large_dst.display().to_string();
            let start = std::time::Instant::now();
            let status = Command::new(env::current_exe()?)
                .arg("copy")
                .arg("-n")
                .arg("1")
                .arg(&large_src_str)
                .arg(&large_dst_str)
                .status()?;
            if !status.success() {
                return Err(io::Error::other(format!(
                    "overlap fro copy failed with status {status}"
                )));
            }
            let copied = fs::metadata(&large_dst)?.len();
            Ok::<(u64, std::time::Duration), io::Error>((copied, start.elapsed()))
        })
    });

    for entry in &entries {
        copy_small_file_openat(
            entry,
            &source_root_fd,
            &target_root_fd,
            RelativeCopyMethod::CopyFileRange,
        )?;
    }
    let file_elapsed = file_start.elapsed();

    let (overlap_bytes, overlap_elapsed) = match overlap_handle {
        Some(handle) => {
            let (bytes, elapsed) = handle
                .join()
                .map_err(|_| io::Error::other("overlap large-file copy worker panicked"))??;
            (Some(bytes), Some(elapsed))
        }
        None => (None, None),
    };

    let result = ManifestCopyBenchmarkResult {
        entries: entries.len(),
        bytes: total_bytes,
        dirs_created,
        dir_phase_secs: dir_elapsed.as_secs_f64(),
        file_phase_secs: file_elapsed.as_secs_f64(),
        total_secs: overall_start.elapsed().as_secs_f64(),
        overlap_secs: overlap_elapsed.map(|d| d.as_secs_f64()),
        overlap_large_file_bytes: overlap_bytes,
    };

    println!(
        "manifest-recursive-copy {} bytes across {} files: dirs={} dir_phase={:.4}s file_phase={:.4}s total={:.4}s file_gbps={:.3}",
        result.bytes,
        result.entries,
        result.dirs_created,
        result.dir_phase_secs,
        result.file_phase_secs,
        result.total_secs,
        result.bytes as f64 / result.file_phase_secs.max(1e-9) / 1e9
    );
    if let (Some(bytes), Some(secs)) = (result.overlap_large_file_bytes, result.overlap_secs) {
        println!(
            "manifest-recursive-copy overlap-large-file {} bytes in {:.4}s {:.3} GB/s",
            bytes,
            secs,
            bytes as f64 / secs.max(1e-9) / 1e9
        );
    }
    if verbose {
        eprintln!(
            "manifest-recursive-copy details: source_root={} target_root={} manifest={}",
            source_root.display(),
            target_root.display(),
            manifest.display()
        );
    }
    Ok(result.bytes)
}
