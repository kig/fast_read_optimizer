use super::execution::{
    is_terminal_tail, should_use_direct_io, submit_read, submit_read_with_probe, wait_for_ready,
};
use super::*;
use crate::io_util::note_direct_unaligned_fallback;
use crate::uring_util::io_uring_available;

fn read_exact_range_blocking(
    file: &File,
    file_direct: &File,
    output: &mut [u8],
    start_offset: u64,
    block_size: u64,
    use_direct: bool,
) -> std::io::Result<u64> {
    let mut filled = 0usize;
    let chunk_size = block_size.max(4096);
    while filled < output.len() {
        let chunk_len = (output.len() - filled).min(chunk_size as usize);
        let offset = start_offset + filled as u64;
        let aligned_read = (offset % 4096 == 0) && (chunk_len as u64 == chunk_size);
        let range_end = start_offset + output.len() as u64;
        if use_direct && !aligned_read && !is_terminal_tail(offset, chunk_len, range_end) {
            note_direct_unaligned_fallback("read", offset, chunk_len);
        }
        let source = if use_direct && aligned_read {
            file_direct
        } else {
            file
        };
        let read = source.read_at(&mut output[filled..filled + chunk_len], offset)?;
        if read != chunk_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "short blocking read at offset {}: expected {} bytes, got {}",
                    offset, chunk_len, read
                ),
            ));
        }
        filled += read;
    }
    Ok(filled as u64)
}

fn load_file_to_shared_buffer_blocking(
    filename: &str,
    params: ResolvedReadParams,
    file_len: usize,
    options: ReadToMemoryOptions,
) -> std::io::Result<LoadedFile> {
    let mut data = AlignedBuffer::new_uninit(file_len)?;
    if options.use_hugepages_for_len(file_len) {
        madvise_best_effort(
            data.as_mut_slice().as_mut_ptr().cast(),
            file_len.max(1),
            fro::os::MADV_HUGEPAGE,
        )?;
    }
    let (file, file_direct) = open_reader_files(filename, params.use_direct)?;
    let bytes_read = read_exact_range_blocking(
        &file,
        &file_direct,
        data.as_mut_slice(),
        0,
        params.block_size,
        params.use_direct,
    )?;
    Ok(LoadedFile {
        data: LoadedData::Aligned(data),
        bytes_read,
        params,
    })
}

#[allow(dead_code)]
pub(super) fn thread_reader(
    thread_id: u64,
    pattern: String,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    use_direct: bool,
) -> std::io::Result<Vec<u64>> {
    let mut matches = Vec::new();
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(matches);
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    let finder = Finder::new(pattern.as_bytes());

    loop {
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len = validate_read_result("read", current_offset, expected_len, result)?;
            if actual_len > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
                if !pattern.is_empty() {
                    let buf = &buffers[slot].as_slice()
                        [..std::cmp::min(actual_len, (block_size as usize) + pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(current_offset.checked_add(idx as u64).ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "grep match offset overflowed",
                            )
                        })?);
                    }
                }
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(matches);
        }
    }
}

#[allow(dead_code)]
pub(super) fn thread_loader(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    output: Arc<SharedOutput>,
    use_direct: bool,
) -> std::io::Result<()> {
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        let len = ((file_size - current_offset).min(block_size)) as usize;
        let output_offset = checked_output_offset(current_offset, len, output.len)?;
        let direct = should_use_direct_io(use_direct, current_offset, len, file_size);
        unsafe {
            let dst = output_slice_mut(&output, output_offset, len);
            let mut sqe = io_uring
                .prepare_sqe()
                .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
            if direct {
                sqe.prep_read(file_direct.as_raw_fd(), dst, current_offset);
            } else {
                sqe.prep_read(file.as_raw_fd(), dst, current_offset);
            }
            sqe.set_user_data(block_num as u64);
        }
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (block_id, result) in wait_for_ready(io_uring)? {
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("load-to-memory", current_offset, expected_len, result)?;
            if actual_len > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                let len = ((file_size - next_offset).min(block_size)) as usize;
                let output_offset = checked_output_offset(next_offset, len, output.len)?;
                let direct = should_use_direct_io(use_direct, next_offset, len, file_size);
                unsafe {
                    let dst = output_slice_mut(&output, output_offset, len);
                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                        std::io::Error::other("io_uring submission queue is full")
                    })?;
                    if direct {
                        sqe.prep_read(file_direct.as_raw_fd(), dst, next_offset);
                    } else {
                        sqe.prep_read(file.as_raw_fd(), dst, next_offset);
                    }
                    sqe.set_user_data(block_num as u64);
                }
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

pub(super) fn thread_loader_range(
    start_offset: u64,
    len: usize,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    output: Arc<SharedOutput>,
    use_direct: bool,
) -> std::io::Result<()> {
    if len == 0 {
        return Ok(());
    }

    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }

    let file_size = file.seek(SeekFrom::End(0))?;
    let end_offset = start_offset
        .checked_add(len as u64)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed"))?;
    let effective_end = end_offset.min(file_size);
    if start_offset >= effective_end {
        return Ok(());
    }
    let mut block_num = 0u64;
    let mut inflight = 0usize;
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = start_offset
            .checked_add(block_num.checked_mul(block_size).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
            })?)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
            })?;
        if current_offset >= effective_end {
            break;
        }
        pending.reserve(slot, block_num)?;
        submit_range_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            effective_end,
            file_size,
            block_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = start_offset
                .checked_add(block_id.checked_mul(block_size).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?;
            let expected_len =
                expected_range_read_len(file_size, effective_end, current_offset, block_size)?;
            let actual_len =
                validate_read_result("load-file-range", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let output_offset = checked_output_offset(
                    current_offset.saturating_sub(start_offset),
                    actual_len,
                    output.len,
                )?;
                unsafe {
                    let dst = output_slice_mut(&output, output_offset, actual_len);
                    dst.copy_from_slice(&buffers[slot].as_slice()[..actual_len]);
                }
                read_count.fetch_add(actual_len as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = start_offset
                .checked_add(block_num.checked_mul(block_size).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range overflowed")
                })?;
            if next_offset < effective_end {
                pending.reserve(slot, block_num)?;
                submit_range_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    effective_end,
                    file_size,
                    block_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

fn expected_range_read_len(
    file_size: u64,
    end_offset: u64,
    offset: u64,
    block_size: u64,
) -> std::io::Result<usize> {
    let effective_end = file_size.min(end_offset);
    expected_read_len(effective_end, offset, block_size)
}

fn submit_range_read(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    end_offset: u64,
    file_size: u64,
    block_size: u64,
) -> std::io::Result<()> {
    let len = expected_range_read_len(file_size, end_offset, offset, block_size)?;
    let direct = should_use_direct_io(use_direct, offset, len, file_size);
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
        if direct {
            sqe.prep_read(
                file_direct.as_raw_fd(),
                &mut buffer.as_mut_slice()[..len],
                offset,
            );
        } else {
            sqe.prep_read(file.as_raw_fd(), &mut buffer.as_mut_slice()[..len], offset);
        }
        sqe.set_user_data(block_id);
    }
    Ok(())
}

pub(super) fn resolve_load_file_request(
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<(ResolvedReadParams, u64, usize)> {
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
    let file_len = usize::try_from(file_size).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "file is too large to fit in memory on this platform: {}",
                file_size
            ),
        )
    })?;
    Ok((params, file_size, file_len))
}

pub(super) fn load_file_to_shared_buffer(
    filename: &str,
    params: ResolvedReadParams,
    file_len: usize,
    options: ReadToMemoryOptions,
) -> std::io::Result<LoadedFile> {
    if !io_uring_available(1024)? {
        return load_file_to_shared_buffer_blocking(filename, params, file_len, options);
    }
    let mut data = AlignedBuffer::new_uninit(file_len)?;
    if options.use_hugepages_for_len(file_len) {
        madvise_best_effort(
            data.as_mut_slice().as_mut_ptr().cast(),
            file_len.max(1),
            fro::os::MADV_HUGEPAGE,
        )?;
    }
    let output = Arc::new(SharedOutput {
        ptr: data.as_mut_slice().as_mut_ptr(),
        len: data.len(),
    });
    let read_count = Arc::new(AtomicU64::new(0));

    let mut threads = vec![];
    for thread_id in 0..params.num_threads {
        let output = output.clone();
        let read_count = read_count.clone();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_loader(
                thread_id,
                params.num_threads,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                output,
                params.use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("read worker thread panicked"))??;
    }

    Ok(LoadedFile {
        data: LoadedData::Aligned(data),
        bytes_read: read_count.load(Ordering::SeqCst),
        params,
    })
}

fn block_spans_for_threads(file_size: u64, block_size: u64, thread_count: usize) -> Vec<BlockSpan> {
    if file_size == 0 || thread_count == 0 {
        return Vec::new();
    }
    let block_count = file_size.div_ceil(block_size);
    let blocks_per_thread = block_count.div_ceil(thread_count as u64);
    let mut spans = Vec::new();
    for thread_id in 0..thread_count {
        let start_block = thread_id as u64 * blocks_per_thread;
        if start_block >= block_count {
            break;
        }
        let end_block = ((thread_id as u64 + 1) * blocks_per_thread).min(block_count);
        let start_offset = start_block * block_size;
        let end_offset = (end_block * block_size).min(file_size);
        spans.push(BlockSpan {
            start_offset,
            len: (end_offset - start_offset) as usize,
        });
    }
    spans
}

pub(super) fn measure_file_load_multiple_targets(
    filename: &str,
    params: ResolvedReadParams,
    file_size: u64,
) -> std::io::Result<u64> {
    let spans = block_spans_for_threads(file_size, params.block_size, params.num_threads as usize);
    if !io_uring_available(1024)? {
        let read_count = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for span in spans {
            let read_count = Arc::clone(&read_count);
            let filename = filename.to_string();
            threads.push(std::thread::spawn(move || -> std::io::Result<()> {
                let mut target = AlignedBuffer::new_uninit(span.len)?;
                let (file, file_direct) = open_reader_files(&filename, params.use_direct)?;
                let bytes = read_exact_range_blocking(
                    &file,
                    &file_direct,
                    target.as_mut_slice(),
                    span.start_offset,
                    params.block_size,
                    params.use_direct,
                )?;
                read_count.fetch_add(bytes, Ordering::SeqCst);
                black_box(target);
                Ok(())
            }));
        }
        for thread in threads {
            thread
                .join()
                .map_err(|_| std::io::Error::other("multi-target read worker thread panicked"))??;
        }
        return Ok(read_count.load(Ordering::SeqCst));
    }
    let read_count = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();

    for span in spans {
        let read_count = Arc::clone(&read_count);
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> std::io::Result<()> {
            let mut target = AlignedBuffer::new_uninit(span.len)?;
            let output = Arc::new(SharedOutput {
                ptr: target.as_mut_slice().as_mut_ptr(),
                len: target.len(),
            });
            let (mut file, mut file_direct) = open_reader_files(&filename, params.use_direct)?;
            let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
            thread_loader_range(
                span.start_offset,
                span.len,
                params.block_size,
                params.qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                output,
                params.use_direct,
            )?;
            black_box(target);
            Ok(())
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| std::io::Error::other("multi-target read worker thread panicked"))??;
    }

    Ok(read_count.load(Ordering::SeqCst))
}

pub(super) fn thread_map_blocks<T, F>(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    results: Arc<Vec<Mutex<Option<T>>>>,
    mapper: Arc<F>,
    use_direct: bool,
) -> std::io::Result<()>
where
    T: Send + 'static,
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<T> + Send + Sync + 'static,
{
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (slot_id, result) in wait_for_ready(io_uring)? {
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("map-file-blocks", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[slot].as_slice()[..actual_len];
                let mapped = mapper(ReaderBlock {
                    block_index,
                    offset: current_offset,
                    file_size,
                    data: buf,
                })?;
                let mut slot = results[block_index].lock().unwrap();
                *slot = Some(mapped);
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}

pub(super) fn thread_visit_blocks<F>(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    visitor: Arc<F>,
    use_direct: bool,
    timing_probe: Option<Arc<ReadPhaseTimingProbe>>,
) -> std::io::Result<()>
where
    F: for<'a> Fn(ReaderBlock<'a>) -> std::io::Result<()> + Send + Sync + 'static,
{
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;
    let mut pending = PendingReadSlots::new(qd);

    for slot in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        pending.reserve(slot, block_num as u64)?;
        submit_read_with_probe(
            io_uring,
            file,
            file_direct,
            &mut buffers[slot],
            current_offset,
            slot as u64,
            use_direct,
            file_size,
            timing_probe.as_deref(),
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        for (slot_id, result) in wait_for_ready(io_uring)? {
            if let Some(probe) = timing_probe.as_deref() {
                probe.note_first_completion();
            }
            let slot = usize::try_from(slot_id).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "read slot id overflowed")
            })?;
            let block_id = pending.complete(slot)?;
            let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
            let expected_len = expected_read_len(file_size, current_offset, block_size)?;
            let actual_len =
                validate_read_result("visit-file-blocks", current_offset, expected_len, result)?;
            if actual_len > 0 {
                let block_index = (current_offset / block_size) as usize;
                let buf = &buffers[slot].as_slice()[..actual_len];
                visitor(ReaderBlock {
                    block_index,
                    offset: current_offset,
                    file_size,
                    data: buf,
                })?;
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                pending.reserve(slot, block_num as u64)?;
                submit_read_with_probe(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[slot],
                    next_offset,
                    slot as u64,
                    use_direct,
                    file_size,
                    timing_probe.as_deref(),
                )?;
                block_num += 1;
                inflight += 1;
            }
        }
        if inflight == 0 {
            if let Some(probe) = timing_probe.as_deref() {
                probe.note_wrapup_start();
            }
        }
        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(());
        }
    }
}
