use super::*;
use super::execution::{benchmark_block_size, benchmark_quick_probe_page_cache, benchmark_uring_qd, read_file_path_kind, read_file_single_thread_blocking, resolve_reader_execution_for_mode, visit_file_blocks_simple};
use super::workers::{load_file_to_shared_buffer, measure_file_load_multiple_targets, resolve_load_file_request, thread_map_blocks, thread_visit_blocks};

#[allow(dead_code)]
pub fn load_file_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<LoadedFile> {
    load_file_to_memory_with_mode(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
        ReadToMemoryMode::Auto,
        ReadToMemoryOptions::default(),
    )
}

pub fn load_file_to_memory_with_mode(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<LoadedFile> {
    let (params, file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(LoadedFile {
            data: LoadedData::Aligned(AlignedBuffer::new(0)),
            bytes_read: 0,
            params,
        });
    }

    let mode = resolve_to_memory_mode(filename, io_mode, mode);
    let loaded = match mode {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before loading"),
        ReadToMemoryMode::PagedSharedBuffer => {
            load_file_to_shared_buffer(filename, params, file_len, options)?
        }
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            LoadedFile {
                data: LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?),
                bytes_read: file_size,
                params,
            }
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            LoadedFile {
                data,
                bytes_read: file_size,
                params,
            }
        }
        ReadToMemoryMode::MultipleTargetBuffers => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "load_file_to_memory does not support --multiple-target-buffers; use measure_file_load_to_memory for that benchmarking mode",
            ));
        }
    };
    if loaded.bytes_read != file_size {
        return Err(std::io::Error::other(format!(
            "read loaded {} bytes but expected {}",
            loaded.bytes_read, file_size
        )));
    }
    Ok(loaded)
}

#[allow(dead_code)]
pub fn load_file_to_memory_for_mode(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
) -> std::io::Result<LoadedFile> {
    let page_cache = config.get_params_for_path(mode, false, filename);
    let direct = config.get_params_for_path(mode, true, filename);
    load_file_to_memory(
        filename,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
    )
}

pub fn measure_file_load_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<u64> {
    let (params, file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(0);
    }

    match resolve_to_memory_mode(filename, io_mode, mode) {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before measuring"),
        ReadToMemoryMode::PagedSharedBuffer => {
            Ok(load_file_to_shared_buffer(filename, params, file_len, options)?.bytes_read)
        }
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            black_box(data.as_slice().len());
            Ok(file_size)
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            Ok(file_size)
        }
        ReadToMemoryMode::MultipleTargetBuffers => {
            measure_file_load_multiple_targets(filename, params, file_size)
        }
    }
}

pub fn prepare_file_load_to_memory(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mode: ReadToMemoryMode,
    options: ReadToMemoryOptions,
) -> std::io::Result<Option<LoadedData>> {
    let (params, _file_size, file_len) = resolve_load_file_request(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;

    if file_len == 0 {
        return Ok(None);
    }

    match resolve_to_memory_mode(filename, io_mode, mode) {
        ReadToMemoryMode::Auto => unreachable!("auto mode should be resolved before preparing"),
        ReadToMemoryMode::Mmap => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            Ok(Some(LoadedData::Mapped(MappedReadBuffer::map(
                &file, file_len, options,
            )?)))
        }
        ReadToMemoryMode::MmapReadPages => {
            if io_mode == IOMode::Direct {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--mmap-read-pages is not supported with --direct",
                ));
            }
            let file = File::open(filename)?;
            let data = LoadedData::Mapped(MappedReadBuffer::map(&file, file_len, options)?);
            read_all_bytes(data.as_slice(), params.num_threads)?;
            Ok(Some(data))
        }
        ReadToMemoryMode::PagedSharedBuffer | ReadToMemoryMode::MultipleTargetBuffers => Ok(None),
    }
}

pub fn map_file_blocks<T, F>(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    mapper: F,
) -> std::io::Result<MappedBlocks<T>>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    let params = resolve_reader_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;

    let file_size = std::fs::metadata(filename)?.len();
    let block_count = if file_size == 0 {
        0
    } else {
        file_size.div_ceil(params.block_size) as usize
    };
    if block_count == 0 {
        return Ok(MappedBlocks {
            blocks: Vec::new(),
            bytes_read: 0,
            file_size,
            params,
        });
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let results = Arc::new(
        (0..block_count)
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>(),
    );
    let mapper = Arc::new(mapper);

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let filename = filename.to_string();
        let read_count = read_count.clone();
        let results = results.clone();
        let mapper = mapper.clone();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_map_blocks(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                results,
                mapper,
                params.use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }

    let mut blocks = Vec::with_capacity(block_count);
    for (block_index, slot) in results.iter().enumerate() {
        let value = slot.lock().unwrap().take().ok_or_else(|| {
            std::io::Error::other(format!("missing mapped result for block {}", block_index))
        })?;
        blocks.push(value);
    }

    Ok(MappedBlocks {
        blocks,
        bytes_read: read_count.load(Ordering::SeqCst),
        file_size,
        params,
    })
}

#[allow(dead_code)]
pub fn map_file_blocks_for_mode<T, F>(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
    mapper: F,
) -> std::io::Result<MappedBlocks<T>>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    match resolve_reader_execution_for_mode(config, mode, filename, io_mode)? {
        ResolvedReadExecution::Simple(params) => {
            let file_size = std::fs::metadata(filename)?.len();
            if file_size == 0 {
                return Ok(MappedBlocks {
                    blocks: Vec::new(),
                    bytes_read: 0,
                    file_size,
                    params,
                });
            }
            let block_count = file_size.div_ceil(params.block_size) as usize;
            let mapper = Arc::new(mapper);
            let results = Arc::new(
                (0..block_count)
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>(),
            );
            let result_slots = Arc::clone(&results);
            let metrics = visit_file_blocks_simple(filename, params, move |block| {
                let value = mapper(block)?;
                *result_slots[block.block_index].lock().unwrap() = Some(value);
                Ok(())
            })?;
            let mut blocks = Vec::with_capacity(block_count);
            for (block_index, slot) in results.iter().enumerate() {
                let value = slot.lock().unwrap().take().ok_or_else(|| {
                    std::io::Error::other(format!(
                        "missing mapped result for block {}",
                        block_index
                    ))
                })?;
                blocks.push(value);
            }
            Ok(MappedBlocks {
                blocks,
                bytes_read: metrics.bytes_read,
                file_size: metrics.file_size,
                params,
            })
        }
        ResolvedReadExecution::Threaded(params) => {
            let file_size = std::fs::metadata(filename)?.len();
            let block_count = if file_size == 0 {
                0
            } else {
                file_size.div_ceil(params.block_size) as usize
            };
            if block_count == 0 {
                return Ok(MappedBlocks {
                    blocks: Vec::new(),
                    bytes_read: 0,
                    file_size,
                    params,
                });
            }
            let read_count = Arc::new(AtomicU64::new(0));
            let results = Arc::new(
                (0..block_count)
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>(),
            );
            let mapper = Arc::new(mapper);
            let mut threads = vec![];
            for thread_id in 0..params.num_threads {
                let filename = filename.to_string();
                let read_count = read_count.clone();
                let results = results.clone();
                let mapper = mapper.clone();
                threads.push(std::thread::spawn(move || -> std::io::Result<()> {
                    let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
                    let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
                    thread_map_blocks(
                        thread_id,
                        params.num_threads,
                        params.block_size,
                        params.qd,
                        &mut file,
                        &mut file_direct,
                        &mut io_uring,
                        read_count,
                        results,
                        mapper,
                        params.use_direct,
                    )
                }));
            }
            for thread in threads {
                thread
                    .join()
                    .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
            }
            let mut blocks = Vec::with_capacity(block_count);
            for (block_index, slot) in results.iter().enumerate() {
                let value = slot.lock().unwrap().take().ok_or_else(|| {
                    std::io::Error::other(format!(
                        "missing mapped result for block {}",
                        block_index
                    ))
                })?;
                blocks.push(value);
            }
            Ok(MappedBlocks {
                blocks,
                bytes_read: read_count.load(Ordering::SeqCst),
                file_size,
                params,
            })
        }
    }
}

pub fn visit_file_blocks<F>(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    visitor: F,
) -> std::io::Result<(u64, u64, ResolvedReadParams)>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    let params = resolve_reader_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;

    let metrics = visit_file_blocks_with_resolved_params(filename, params, visitor)?;
    Ok((metrics.bytes_read, metrics.file_size, params))
}

pub fn visit_file_blocks_with_resolved_params<F>(
    filename: &str,
    params: ResolvedReadParams,
    visitor: F,
) -> std::io::Result<VisitFileMetrics>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    validate_read_params(params)?;

    let file_size = std::fs::metadata(filename)?.len();
    if file_size == 0 {
        return Ok(VisitFileMetrics {
            bytes_read: 0,
            file_size: 0,
            phase_timings: ReadPhaseTimings::default(),
        });
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let visitor = Arc::new(visitor);
    let timing_probe = Arc::new(ReadPhaseTimingProbe::new());

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let filename = filename.to_string();
        let read_count = read_count.clone();
        let visitor = visitor.clone();
        let timing_probe = timing_probe.clone();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_visit_blocks(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                visitor,
                params.use_direct,
                Some(timing_probe),
            )
        }));
    }
    timing_probe.note_threads_created();

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }
    timing_probe.note_join_done();

    Ok(VisitFileMetrics {
        bytes_read: read_count.load(Ordering::SeqCst),
        file_size,
        phase_timings: timing_probe.snapshot(),
    })
}

#[allow(dead_code)]
pub fn visit_file_blocks_for_mode<F>(
    config: &LoadedConfig,
    mode: &str,
    filename: &str,
    io_mode: IOMode,
    visitor: F,
) -> std::io::Result<(u64, u64, ResolvedReadParams)>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    match resolve_reader_execution_for_mode(config, mode, filename, io_mode)? {
        ResolvedReadExecution::Simple(params) => {
            let metrics = visit_file_blocks_simple(filename, params, visitor)?;
            Ok((metrics.bytes_read, metrics.file_size, params))
        }
        ResolvedReadExecution::Threaded(params) => {
            let metrics = visit_file_blocks_with_resolved_params(filename, params, visitor)?;
            Ok((metrics.bytes_read, metrics.file_size, params))
        }
    }
}

pub fn read_file(
    pattern: &str,
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<u64> {
    if !pattern.is_empty() {
        let (all_matches, bytes_read) = grep_match_offsets(
            filename,
            num_threads_p,
            block_size_p,
            qd_p,
            num_threads_d,
            block_size_d,
            qd_d,
            io_mode,
            pattern.as_bytes(),
        )?;
        for m in all_matches {
            println!("{}:{}", m, pattern);
        }
        return Ok(bytes_read);
    }
    let (bytes_read, _, _) = visit_file_blocks(
        filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
        |_| Ok::<_, std::io::Error>(()),
    )?;
    Ok(bytes_read)
}


pub fn read_file_auto_with_strategy(
    pattern: &str,
    filename: &str,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: IOParams,
    direct: IOParams,
) -> std::io::Result<u64> {
    if mount_info.is_some_and(|info| info.fstype == "zfs") {
        return read_file_path_kind(pattern, filename, ReadPathKind::SimpleDirect, &page_cache, &direct);
    }
    let file_size = std::fs::metadata(filename)?.len();
    let cache_state = auto_read_cache_state(filename);
    let path_kind = choose_path_kind_for_state(strategy, cache_state, file_size);
    read_file_path_kind(pattern, filename, path_kind, &page_cache, &direct)
}

pub fn benchmark_read_variant(
    filename: &str,
    variant: ReadBenchmarkVariant,
    cache_state: ReadBenchmarkCacheState,
    strategy: ReadAutoStrategy,
    mount_info: Option<&MountInfo>,
    page_cache: IOParams,
    direct: IOParams,
) -> io::Result<ReadBenchmarkResult> {
    let start = std::time::Instant::now();
    let file_size = std::fs::metadata(filename)?.len();
    let (bytes_read, file_size, params, phase_timings) = match variant {
        ReadBenchmarkVariant::SingleThreadPageCache => {
            let block_size = benchmark_block_size(file_size);
            let bytes_read = read_file_single_thread_blocking(filename, false, block_size)?;
            let params = ResolvedReadParams {
                use_direct: false,
                num_threads: 1,
                block_size: block_size as u64,
                qd: 1,
            };
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::SingleThreadDirect => {
            let block_size = benchmark_block_size(file_size);
            let bytes_read = read_file_single_thread_blocking(filename, true, block_size)?;
            let params = ResolvedReadParams {
                use_direct: true,
                num_threads: 1,
                block_size: block_size as u64,
                qd: 1,
            };
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::SingleThreadIoUring => {
            let block_size = benchmark_block_size(file_size) as u64;
            let qd = benchmark_uring_qd(file_size);
            let params = resolve_reader_params(
                filename,
                &IOParams {
                    num_threads: 1,
                    block_size,
                    qd,
                },
                &IOParams {
                    num_threads: 1,
                    block_size,
                    qd,
                },
                IOMode::PageCache,
            )?;
            let metrics =
                visit_file_blocks_with_resolved_params(filename, params, |_| Ok::<_, io::Error>(()))?;
            (metrics.bytes_read, metrics.file_size, params, metrics.phase_timings)
        }
        ReadBenchmarkVariant::QuickProbePageCache => {
            let (bytes_read, file_size, params) =
                benchmark_quick_probe_page_cache(filename, strategy, mount_info, &page_cache, &direct)?;
            (bytes_read, file_size, params, ReadPhaseTimings::default())
        }
        ReadBenchmarkVariant::MultiThreadCurrent => {
            let io_mode = match cache_state {
                ReadBenchmarkCacheState::Cold => IOMode::Direct,
                ReadBenchmarkCacheState::Hot => IOMode::PageCache,
            };
            let params = resolve_reader_params(filename, &page_cache, &direct, io_mode)?;
            let metrics =
                visit_file_blocks_with_resolved_params(filename, params, |_| Ok::<_, io::Error>(()))?;
            (metrics.bytes_read, metrics.file_size, params, metrics.phase_timings)
        }
    };
    Ok(ReadBenchmarkResult {
        bytes_read,
        file_size,
        elapsed: start.elapsed(),
        params,
        phase_timings,
    })
}

