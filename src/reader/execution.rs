use super::*;

pub(super) fn should_use_direct_io(use_direct: bool, offset: u64, len: usize, file_size: u64) -> bool {
    if use_direct && len % 4096 == 0 {
        debug_assert_eq!(
            len % 4096,
            0,
            "Direct I/O requires a 4096-byte aligned read length"
        );
    }
    use_direct && (offset % 4096 == 0) && (len % 4096 == 0) && (offset + len as u64 <= file_size)
}

pub(super) fn submit_read(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
) -> std::io::Result<()> {
    let direct = should_use_direct_io(use_direct, offset, buffer.as_slice().len(), file_size);
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
        if direct {
            sqe.prep_read(file_direct.as_raw_fd(), buffer.as_mut_slice(), offset);
        } else {
            sqe.prep_read(file.as_raw_fd(), buffer.as_mut_slice(), offset);
        }
        sqe.set_user_data(block_id);
    }
    Ok(())
}

pub(super) fn submit_read_with_probe(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
    timing_probe: Option<&ReadPhaseTimingProbe>,
) -> std::io::Result<()> {
    submit_read(
        io_uring,
        file,
        file_direct,
        buffer,
        offset,
        block_id,
        use_direct,
        file_size,
    )?;
    if let Some(probe) = timing_probe {
        probe.note_first_submit();
    }
    Ok(())
}

pub(super) fn wait_for_ready(io_uring: &mut IoUring) -> std::io::Result<Vec<(u64, u32)>> {
    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
    let mut ready = vec![(cq.user_data(), cq.result()?)];

    while io_uring.cq_ready() > 0 {
        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
            std::io::Error::other("completion queue reported ready but no CQE was available")
        })?;
        ready.push((cq.user_data(), cq.result()?));
    }

    Ok(ready)
}

pub(super) fn read_file_single_thread_blocking(
    filename: &str,
    use_direct: bool,
    block_size: usize,
) -> io::Result<u64> {
    let (file, file_direct) = open_reader_files(filename, use_direct)?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    if file_size == 0 {
        return Ok(0);
    }

    let mut bytes_read = 0_u64;
    let mut offset = 0_u64;
    let mut buffer = AlignedBuffer::new(block_size);
    loop {
        let remaining = file_size.saturating_sub(offset);
        if remaining == 0 {
            return Ok(bytes_read);
        }
        let want = remaining.min(block_size as u64) as usize;
        let target = if should_use_direct_io(use_direct, offset, want, file_size) {
            &file_direct
        } else {
            &file
        };
        let read = target.read_at(&mut buffer.as_mut_slice()[..want], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blocking single-thread read reached EOF early at offset {} of {}",
                    offset, file_size
                ),
            ));
        }
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("single-thread byte count overflowed"))?;
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("single-thread offset overflowed"))?;
    }
}

pub(super) fn benchmark_block_size(file_size: u64) -> usize {
    file_size.clamp(1, 1024 * 1024) as usize
}

pub(super) fn benchmark_uring_qd(file_size: u64) -> usize {
    file_size.div_ceil(1024 * 1024).clamp(1, 4) as usize
}

fn read_file_simple_page_cache_from_probe(file: &mut File, file_size: u64) -> io::Result<u64> {
    let mut bytes_read = 64 * 1024_u64;
    let mut buffer = vec![0_u8; benchmark_block_size(file_size)];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(bytes_read);
        }
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("quick-probe byte count overflowed"))?;
    }
}

pub(super) fn visit_file_blocks_simple<F>(
    filename: &str,
    params: ResolvedReadParams,
    visitor: F,
) -> io::Result<VisitFileMetrics>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()>,
{
    validate_read_params(params)?;
    let (file, file_direct) = open_reader_files(filename, params.use_direct)?;
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        return Ok(VisitFileMetrics {
            bytes_read: 0,
            file_size: 0,
            phase_timings: ReadPhaseTimings::default(),
        });
    }

    let mut bytes_read = 0_u64;
    let mut block_index = 0usize;
    let mut offset = 0_u64;
    let mut buffer = AlignedBuffer::new(params.block_size as usize);
    while offset < file_size {
        let remaining = file_size - offset;
        let want = remaining.min(params.block_size) as usize;
        let target = if should_use_direct_io(params.use_direct, offset, want, file_size) {
            &file_direct
        } else {
            &file
        };
        let read = target.read_at(&mut buffer.as_mut_slice()[..want], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("simple read reached EOF early at offset {} of {}", offset, file_size),
            ));
        }
        visitor(ReaderBlock {
            block_index,
            offset,
            file_size,
            data: &buffer.as_slice()[..read],
        })?;
        bytes_read = bytes_read
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("simple visitor byte count overflowed"))?;
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("simple visitor offset overflowed"))?;
        block_index += 1;
    }

    Ok(VisitFileMetrics {
        bytes_read,
        file_size,
        phase_timings: ReadPhaseTimings::default(),
    })
}

fn choose_forced_path_kind(
    forced_simple: ReadPathKind,
    forced_threaded: ReadPathKind,
    small_candidate: ReadPathKind,
    large_candidate: ReadPathKind,
    file_size: u64,
    large_min_bytes: u64,
) -> ReadPathKind {
    if file_size >= large_min_bytes {
        match large_candidate {
            ReadPathKind::ThreadedPageCache | ReadPathKind::ThreadedDirect => forced_threaded,
            ReadPathKind::IoUringPageCache => ReadPathKind::IoUringPageCache,
            _ => forced_simple,
        }
    } else {
        match small_candidate {
            ReadPathKind::ThreadedPageCache | ReadPathKind::ThreadedDirect => forced_threaded,
            ReadPathKind::IoUringPageCache => ReadPathKind::IoUringPageCache,
            _ => forced_simple,
        }
    }
}

fn resolve_execution_for_path_kind(
    filename: &str,
    path_kind: ReadPathKind,
    file_size: u64,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<ResolvedReadExecution> {
    let params = match path_kind {
        ReadPathKind::SimplePageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::SimpleDirect => ResolvedReadParams {
            use_direct: true,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::IoUringPageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: benchmark_uring_qd(file_size),
        },
        ReadPathKind::ThreadedPageCache => {
            resolve_reader_params(filename, page_cache, direct, IOMode::PageCache)?
        }
        ReadPathKind::ThreadedDirect => {
            resolve_reader_params(filename, page_cache, direct, IOMode::Direct)?
        }
    };
    Ok(match path_kind {
        ReadPathKind::SimplePageCache | ReadPathKind::SimpleDirect => {
            ResolvedReadExecution::Simple(params)
        }
        ReadPathKind::IoUringPageCache
        | ReadPathKind::ThreadedPageCache
        | ReadPathKind::ThreadedDirect => ResolvedReadExecution::Threaded(params),
    })
}

pub(super) fn resolve_reader_execution_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> io::Result<ResolvedReadExecution> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    let strategy = config.get_read_auto_strategy_for_path(filename);
    let mount_info = config.mount_info_for_path(filename);
    let file_size = std::fs::metadata(filename)?.len();
    let path_kind = match io_mode {
        IOMode::Direct => choose_forced_path_kind(
            ReadPathKind::SimpleDirect,
            ReadPathKind::ThreadedDirect,
            strategy.cold_small_path,
            strategy.cold_large_path,
            file_size,
            strategy.cold_large_min_bytes,
        ),
        IOMode::PageCache => choose_forced_path_kind(
            ReadPathKind::SimplePageCache,
            ReadPathKind::ThreadedPageCache,
            strategy.hot_small_path,
            strategy.hot_large_path,
            file_size,
            strategy.hot_large_min_bytes,
        ),
        IOMode::Auto => {
            if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
                ReadPathKind::SimpleDirect
            } else if file_size >= strategy.hot_large_min_bytes {
                match strategy.hot_large_path {
                    ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => {
                        ReadPathKind::SimpleDirect
                    }
                    other => other,
                }
            } else {
                match strategy.hot_small_path {
                    ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => {
                        ReadPathKind::SimpleDirect
                    }
                    other => other,
                }
            }
        }
    };
    resolve_execution_for_path_kind(filename, path_kind, file_size, &page_cache, &direct)
}

pub(super) fn read_file_path_kind(
    pattern: &str,
    filename: &str,
    path_kind: ReadPathKind,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<u64> {
    match path_kind {
        ReadPathKind::SimplePageCache => {
            read_file(pattern, filename, 1, 1024 * 1024, 1, 1, 1024 * 1024, 1, IOMode::PageCache)
        }
        ReadPathKind::SimpleDirect => {
            read_file(pattern, filename, 1, 1024 * 1024, 1, 1, 1024 * 1024, 1, IOMode::Direct)
        }
        ReadPathKind::IoUringPageCache => read_file(
            pattern,
            filename,
            1,
            1024 * 1024,
            32,
            1,
            1024 * 1024,
            32,
            IOMode::PageCache,
        ),
        ReadPathKind::ThreadedPageCache => read_file(
            pattern,
            filename,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::PageCache,
        ),
        ReadPathKind::ThreadedDirect => read_file(
            pattern,
            filename,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::Direct,
        ),
    }
}

pub(super) fn benchmark_quick_probe_page_cache(
    filename: &str,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: &IOParams,
    direct: &IOParams,
) -> io::Result<(u64, u64, ResolvedReadParams)> {
    if mount_info.is_some_and(|info| info.fstype == "zfs") {
        let file_size = File::open(filename)?.metadata()?.len();
        let bytes_read = read_file_path_kind("", filename, ReadPathKind::SimpleDirect, page_cache, direct)?;
        return Ok((
            bytes_read,
            file_size,
            ResolvedReadParams {
                use_direct: true,
                num_threads: 1,
                block_size: benchmark_block_size(file_size) as u64,
                qd: 1,
            },
        ));
    }

    let mut file = File::open(filename)?;
    let mut probe = vec![0_u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < probe.len() {
        let read = file.read(&mut probe[filled..])?;
        if read == 0 {
            let bytes_read = filled as u64;
            return Ok((
                bytes_read,
                bytes_read,
                ResolvedReadParams {
                    use_direct: false,
                    num_threads: 1,
                    block_size: benchmark_block_size(bytes_read) as u64,
                    qd: 1,
                },
            ));
        }
        filled += read;
    }

    let file_size = file.metadata()?.len();
    if file_size < strategy.hot_large_min_bytes {
        let bytes_read = read_file_simple_page_cache_from_probe(&mut file, file_size)?;
        return Ok((
            bytes_read,
            file_size,
            ResolvedReadParams {
                use_direct: false,
                num_threads: 1,
                block_size: benchmark_block_size(file_size) as u64,
                qd: 1,
            },
        ));
    }

    let selected_kind = match strategy.hot_large_path {
        ReadPathKind::SimplePageCache
        | ReadPathKind::IoUringPageCache
        | ReadPathKind::ThreadedPageCache => strategy.hot_large_path,
        ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => ReadPathKind::ThreadedPageCache,
    };
    let bytes_read = read_file_path_kind("", filename, selected_kind, page_cache, direct)?;
    let params = match selected_kind {
        ReadPathKind::SimplePageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: 1,
        },
        ReadPathKind::IoUringPageCache => ResolvedReadParams {
            use_direct: false,
            num_threads: 1,
            block_size: benchmark_block_size(file_size) as u64,
            qd: benchmark_uring_qd(file_size),
        },
        ReadPathKind::ThreadedPageCache => resolve_reader_params(filename, page_cache, direct, IOMode::PageCache)?,
        ReadPathKind::SimpleDirect | ReadPathKind::ThreadedDirect => unreachable!(),
    };
    Ok((bytes_read, file_size, params))
}
