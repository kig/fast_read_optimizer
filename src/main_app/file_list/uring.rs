use super::*;

pub(super) const FILE_LIST_URING_THREAD_COUNT: usize = 32;
pub(super) const FILE_LIST_URING_INFLIGHT_SWEEP: [usize; 5] = [32, 64, 128, 256, 512];
pub(super) const FILE_LIST_URING_SLOT_BUFFER_SIZE: usize = 4096;
pub(super) const MANIFEST_READ_PREFIX_SWEEP: [usize; 5] = [512, 2048, 8192, 16384, 32768];
pub(super) const MANIFEST_MT_BLOCKING_THREADS: [usize; 3] = [4, 16, 32];
pub(super) const MANIFEST_ST_URING_QDS: [usize; 4] = [32, 64, 128, 256];
pub(super) const MANIFEST_MT_URING_CONFIGS: [(usize, usize); 4] =
    [(4, 32), (8, 32), (16, 64), (32, 64)];

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

pub(super) fn prepare_file_list_uring_read(
    path: PathBuf,
    use_direct: bool,
    slot_buffer_index: usize,
) -> io::Result<Option<FileListUringInflightRead>> {
    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })?;
    let (file, file_direct) = open_reader_files(path_str, use_direct)?;
    Ok(Some(FileListUringInflightRead {
        file,
        file_direct,
        slot_buffer_index,
    }))
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
                    submit_file_list_uring_read(
                        io_uring,
                        slot_index,
                        &mut read,
                        slot_buffers,
                        use_direct,
                    )?;
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
        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
            io::Error::other("completion queue reported ready but no CQE was available")
        })?;
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
    let mut slot_buffers =
        std::iter::repeat_with(|| AlignedBuffer::new(FILE_LIST_URING_SLOT_BUFFER_SIZE))
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
            format!(
                "{:.3}",
                result.total_bytes as f64 / result.elapsed_secs.max(1e-9) / 1e9
            ),
            format!(
                "{:.1}",
                result.total_files as f64 / result.elapsed_secs.max(1e-9)
            ),
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
