mod blocking;

use super::*;
pub(crate) use blocking::copy_file_range_blocking_with_progress;
use blocking::read_full_at;
use crate::io_util::checked_posix_fallocate;
#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordedCopyBackend {
    Threaded,
    Blocking,
    CopyFileRange,
    CopyFileRangeSingle,
    Reflink,
}

#[cfg(test)]
#[derive(Default)]
struct CopyBackendTrace {
    path_prefix: String,
    events: Vec<RecordedCopyBackend>,
}

#[cfg(test)]
static COPY_BACKEND_TRACE: Mutex<Option<CopyBackendTrace>> = Mutex::new(None);

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn begin_copy_backend_trace(path_prefix: impl Into<String>) {
    *COPY_BACKEND_TRACE.lock().unwrap() = Some(CopyBackendTrace {
        path_prefix: path_prefix.into(),
        events: Vec::new(),
    });
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn finish_copy_backend_trace() -> Vec<RecordedCopyBackend> {
    COPY_BACKEND_TRACE
        .lock()
        .unwrap()
        .take()
        .map(|trace| trace.events)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn record_copy_backend(source: &str, filename: &str, backend: RecordedCopyBackend) {
    let mut trace = COPY_BACKEND_TRACE.lock().unwrap();
    if let Some(trace) = trace.as_mut() {
        if source.starts_with(&trace.path_prefix) || filename.starts_with(&trace.path_prefix) {
            trace.events.push(backend);
        }
    }
}

pub(super) fn prepare_copy_destination(
    filename: &str,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
) -> io::Result<()> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let metadata = f.metadata()?;
    if metadata.file_type().is_file() {
        let required_size = dest_offset.saturating_add(copy_size);
        let current_len = metadata.len();
        let needs_resize = if truncate_target {
            current_len != required_size
        } else {
            current_len < required_size
        };
        if needs_resize {
            f.set_len(required_size)?;
            if current_len < required_size {
                let fadvise_offset = to_off_t(dest_offset, "destination offset")?;
                let fadvise_len = to_off_t(copy_size, "copy size")?;
                unsafe {
                    libc::posix_fadvise(
                        f.as_raw_fd(),
                        fadvise_offset,
                        fadvise_len,
                        libc::POSIX_FADV_NOREUSE,
                    );
                }
                checked_posix_fallocate(
                    &f,
                    0,
                    required_size,
                    "failed to preallocate copy destination",
                )?;
            }
        }
    }
    Ok(())
}

fn to_off_t(value: u64, field_name: &'static str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field_name} does not fit in off_t"),
        )
    })
}

pub fn copy_file_range_syscall(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    copy_file_range_syscall_with_progress(
        source,
        filename,
        source_offset,
        dest_offset,
        copy_size,
        truncate_target,
        io_mode_read,
        io_mode_write,
        None,
    )
}

pub(crate) fn copy_file_range_syscall_with_progress(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy does not support direct read/write modes",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file source",
        ));
    }
    let copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;
    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file destination",
        ));
    }

    let mut copied_total = 0_u64;
    let mut source_position: libc::loff_t = source_offset.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "source offset does not fit in loff_t",
        )
    })?;
    let mut target_position: libc::loff_t = dest_offset.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination offset does not fit in loff_t",
        )
    })?;

    while copied_total < copy_size {
        let remaining = copy_size - copied_total;
        let chunk = remaining.min(usize::MAX as u64) as usize;
        let copied = unsafe {
            libc::copy_file_range(
                source_file.as_raw_fd(),
                &mut source_position,
                target_file.as_raw_fd(),
                &mut target_position,
                chunk,
                0,
            )
        };
        if copied < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(io::Error::new(
                err.kind(),
                format!("copy_file_range syscall failed: {}", err),
            ));
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "copy_file_range stopped early after {} of {} bytes",
                    copied_total, copy_size
                ),
            ));
        }
        copied_total = copied_total.saturating_add(copied as u64);
        if let Some(progress_count) = progress_count.as_ref() {
            progress_count.fetch_add(copied as u64, Ordering::SeqCst);
        }
    }

    Ok(copied_total)
}

#[allow(dead_code)]
pub fn copy_file_range_chunked(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    copy_file_range_chunked_with_progress(
        source,
        filename,
        source_offset,
        dest_offset,
        copy_size,
        truncate_target,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        None,
    )
}

pub(crate) fn copy_file_range_chunked_with_progress(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy does not support direct read/write modes",
        ));
    }
    if copy_range_threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires at least one worker thread",
        ));
    }
    if copy_range_block_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a non-zero block size",
        ));
    }
    if copy_range_qd == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires qd greater than zero",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file source",
        ));
    }
    let copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;
    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy_file_range strategy requires a regular-file destination",
        ));
    }
    if copy_size == 0 {
        return Ok(0);
    }

    let chunk_size = copy_range_block_size
        .checked_mul(copy_range_qd as u64)
        .filter(|&size| size > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "copy_file_range chunk size overflowed",
            )
        })?;
    let next_offset = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();

    for _ in 0..copy_range_threads {
        let next_offset = next_offset.clone();
        let progress_count = progress_count.clone();
        let source = source.to_string();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<u64> {
            let source_file = File::open(&source)?;
            let target_file = OpenOptions::new().read(true).write(true).open(&filename)?;
            let mut copied_local = 0_u64;

            loop {
                let local_offset = next_offset.fetch_add(chunk_size, Ordering::SeqCst);
                if local_offset >= copy_size {
                    return Ok(copied_local);
                }
                let remaining = copy_size - local_offset;
                let extent_len = remaining.min(chunk_size);
                let mut src_pos: libc::loff_t = source_offset
                    .checked_add(local_offset)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "source offset overflowed")
                    })?
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "source offset does not fit in loff_t",
                        )
                    })?;
                let mut dst_pos: libc::loff_t = dest_offset
                    .checked_add(local_offset)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "destination offset overflowed",
                        )
                    })?
                    .try_into()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "destination offset does not fit in loff_t",
                        )
                    })?;
                let mut copied_extent = 0_u64;
                while copied_extent < extent_len {
                    let step = (extent_len - copied_extent).min(usize::MAX as u64) as usize;
                    let copied = unsafe {
                        libc::copy_file_range(
                            source_file.as_raw_fd(),
                            &mut src_pos,
                            target_file.as_raw_fd(),
                            &mut dst_pos,
                            step,
                            0,
                        )
                    };
                    if copied < 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(io::Error::new(
                            err.kind(),
                            format!("copy_file_range syscall failed: {}", err),
                        ));
                    }
                    if copied == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "copy_file_range stopped early after {} of {} bytes for chunk at {}",
                                copied_extent, extent_len, local_offset
                            ),
                        ));
                    }
                    copied_extent += copied as u64;
                    if let Some(progress_count) = progress_count.as_ref() {
                        progress_count.fetch_add(copied as u64, Ordering::SeqCst);
                    }
                }
                copied_local += copied_extent;
            }
        }));
    }

    let mut copied_total = 0_u64;
    for thread in threads {
        copied_total += thread
            .join()
            .map_err(|_| io::Error::other("copy_file_range worker thread panicked"))??;
    }
    if copied_total != copy_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "copy_file_range copied {} bytes but expected {}",
                copied_total, copy_size
            ),
        ));
    }
    Ok(copied_total)
}

pub fn copy_file_reflink(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    if io_mode_read == IOMode::Direct || io_mode_write == IOMode::Direct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy does not support direct read/write modes",
        ));
    }

    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy requires a regular-file source",
        ));
    }
    let effective_copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    if source_offset != 0
        || dest_offset != 0
        || !truncate_target
        || effective_copy_size != source_meta.len()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy currently supports only whole-file copies into a truncated destination",
        ));
    }

    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "reflink strategy requires a regular-file destination",
        ));
    }

    let rc = unsafe {
        libc::ioctl(
            target_file.as_raw_fd(),
            libc::FICLONE,
            source_file.as_raw_fd(),
        )
    };
    if rc != 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            format!("reflink clone failed: {}", err),
        ));
    }

    Ok(source_meta.len())
}

fn copy_file_range_threaded_impl(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    sync_after_write: bool,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let source_meta = File::open(source)?.metadata()?;
    let source_size = source_meta.len();
    let copy_size = copy_size.min(source_size.saturating_sub(source_offset));

    let direct_write = io_mode_write != IOMode::PageCache;
    let direct_read = io_mode_read == IOMode::Direct || io_mode_read == IOMode::Auto;

    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };

    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let progress_count = progress_count.clone();
        let filename = filename.to_string();
        let source = source.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let src_nodir = File::open(&source)?;
            let src_dir = open_direct_reader_or_fallback(&source, &src_nodir)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                Some((&src_dir, &src_nodir)),
                None,
                (&dest_file_dir, &dest_file_nodir),
                source_offset,
                dest_offset,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                progress_count,
                None,
                copy_size,
                direct_read,
                direct_write,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }

    if sync_after_write && io_mode_write != IOMode::PageCache {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(filename)?
            .sync_all()?;
    }

    Ok(write_count.load(Ordering::SeqCst))
}

pub fn copy_file_range_threaded(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    copy_file_range_threaded_impl(
        source,
        filename,
        source_offset,
        dest_offset,
        copy_size,
        truncate_target,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode_read,
        io_mode_write,
        false,
        progress_count,
    )
}

pub fn overwrite_changed_chunks_direct(
    source: &str,
    filename: &str,
    scan_threads: u64,
    scan_block_size: u64,
    scan_qd: usize,
    _num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
) -> io::Result<u64> {
    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk overwrite requires a regular-file source",
        ));
    }

    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk overwrite requires a regular-file destination",
        ));
    }

    let source_len = source_meta.len();
    let target_len = target_meta.len();
    prepare_copy_destination(filename, 0, source_len, false)?;

    let num_threads = usize::try_from(scan_threads.max(1)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk thread count does not fit in usize",
        )
    })?;
    let scan_block_size = usize::try_from(scan_block_size.max(4096)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "changed-chunk block size does not fit in usize",
        )
    })?;
    let write_block_size = usize::try_from(block_size_d.max(4096)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "direct write block size does not fit in usize",
        )
    })?;
    let stride = (num_threads as u64)
        .checked_mul(scan_block_size as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "changed-chunk stride overflowed",
            )
        })?;
    let common_len = source_len.min(target_len);
    let mut threads = Vec::with_capacity(num_threads);

    for thread_id in 0..num_threads {
        let source = source.to_string();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let source_file = File::open(&source)?;
            let target_file = File::open(&filename)?;
            let mut writer =
                OffsetWriter::with_truncate(&filename, source_len, qd_d, IOMode::Direct, false)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            let read_qd = scan_qd.max(1);
            let mut source_buffers = Vec::with_capacity(read_qd);
            let mut target_buffers = Vec::with_capacity(read_qd);
            for _ in 0..read_qd {
                source_buffers.push(AlignedBuffer::new(scan_block_size));
                target_buffers.push(AlignedBuffer::new(scan_block_size));
            }
            let mut ready_source = vec![false; read_qd];
            let mut ready_target = vec![false; read_qd];
            let mut buffer_offsets = vec![0u64; read_qd];
            let mut inflight = 0usize;
            let mut free_slots: Vec<usize> = (0..read_qd).collect();
            let mut next_offset = (thread_id as u64)
                .checked_mul(scan_block_size as u64)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk offset overflowed",
                    )
                })?;

            while next_offset < common_len && free_slots.last().is_some() {
                let slot = free_slots.pop().unwrap();
                let len = (common_len - next_offset).min(scan_block_size as u64) as usize;
                buffer_offsets[slot] = next_offset;
                unsafe {
                    let mut sqe_source = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe_source.prep_read(
                        source_file.as_raw_fd(),
                        &mut source_buffers[slot].as_mut_slice()[..len],
                        next_offset,
                    );
                    sqe_source.set_user_data(slot as u64);

                    let mut sqe_target = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe_target.prep_read(
                        target_file.as_raw_fd(),
                        &mut target_buffers[slot].as_mut_slice()[..len],
                        next_offset,
                    );
                    sqe_target.set_user_data((slot as u64) | (1u64 << 40));
                }
                next_offset = next_offset.checked_add(stride).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk offset overflowed",
                    )
                })?;
                inflight += 2;
            }
            if inflight > 0 {
                io_uring.submit_sqes().map_err(io::Error::other)?;
            }

            while inflight > 0 {
                let cq = io_uring.wait_for_cqe().map_err(io::Error::other)?;
                let ud = cq.user_data();
                let slot = (ud & 0xFFFF_FFFF) as usize;
                let target_side = (ud >> 40) != 0;
                cq.result()?;
                inflight -= 1;
                if target_side {
                    ready_target[slot] = true;
                } else {
                    ready_source[slot] = true;
                }

                let mut ready_slots = vec![slot];
                while io_uring.cq_ready() > 0 {
                    let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                        io::Error::other("completion queue reported ready but no CQE was available")
                    })?;
                    let ud = cq.user_data();
                    let slot = (ud & 0xFFFF_FFFF) as usize;
                    let target_side = (ud >> 40) != 0;
                    cq.result()?;
                    inflight -= 1;
                    if target_side {
                        ready_target[slot] = true;
                    } else {
                        ready_source[slot] = true;
                    }
                    if !ready_slots.contains(&slot) {
                        ready_slots.push(slot);
                    }
                }

                let mut submitted = false;
                for slot in ready_slots {
                    if !(ready_source[slot] && ready_target[slot]) {
                        continue;
                    }
                    ready_source[slot] = false;
                    ready_target[slot] = false;
                    let offset = buffer_offsets[slot];
                    let len = usize::try_from((common_len - offset).min(scan_block_size as u64))
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "changed-chunk length does not fit in usize",
                            )
                        })?;
                    if source_buffers[slot].as_slice()[..len]
                        != target_buffers[slot].as_slice()[..len]
                    {
                        let mut write_offset = 0usize;
                        while write_offset < len {
                            let write_end = (write_offset + write_block_size).min(len);
                            writer.write_at(
                                offset + write_offset as u64,
                                &source_buffers[slot].as_slice()[write_offset..write_end],
                            )?;
                            write_offset = write_end;
                        }
                    }
                    free_slots.push(slot);
                }

                while next_offset < common_len && !free_slots.is_empty() {
                    let slot = free_slots.pop().unwrap();
                    let len = (common_len - next_offset).min(scan_block_size as u64) as usize;
                    buffer_offsets[slot] = next_offset;
                    unsafe {
                        let mut sqe_source = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe_source.prep_read(
                            source_file.as_raw_fd(),
                            &mut source_buffers[slot].as_mut_slice()[..len],
                            next_offset,
                        );
                        sqe_source.set_user_data(slot as u64);

                        let mut sqe_target = io_uring
                            .prepare_sqe()
                            .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                        sqe_target.prep_read(
                            target_file.as_raw_fd(),
                            &mut target_buffers[slot].as_mut_slice()[..len],
                            next_offset,
                        );
                        sqe_target.set_user_data((slot as u64) | (1u64 << 40));
                    }
                    next_offset = next_offset.checked_add(stride).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "changed-chunk offset overflowed",
                        )
                    })?;
                    inflight += 2;
                    submitted = true;
                }
                if submitted {
                    io_uring.submit_sqes().map_err(io::Error::other)?;
                }
            }

            let mut tail_offset = common_len
                .checked_add((thread_id as u64) * (scan_block_size as u64))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "changed-chunk tail overflowed")
                })?;
            while tail_offset < source_len {
                let len = usize::try_from((source_len - tail_offset).min(scan_block_size as u64))
                    .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "changed-chunk tail length does not fit in usize",
                    )
                })?;
                let mut tail_buf = vec![0u8; len];
                read_full_at(&source_file, &mut tail_buf, tail_offset)?;
                let mut write_offset = 0usize;
                while write_offset < len {
                    let write_end = (write_offset + write_block_size).min(len);
                    writer.write_at(
                        tail_offset + write_offset as u64,
                        &tail_buf[write_offset..write_end],
                    )?;
                    write_offset = write_end;
                }
                tail_offset = tail_offset.checked_add(stride).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "changed-chunk tail overflowed")
                })?;
            }

            writer.flush()?;
            Ok(())
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("changed-chunk worker thread panicked"))??;
    }

    target_file.set_len(source_len)?;
    target_file.sync_all()?;
    Ok(source_len)
}

/*
write (direct)                      | 10.40        | 10.00        | PASS
write (auto, hot)                   | 10.50        | 10.00        | PASS

write (page cache, cold)            | 2.50         | 10.00        | REGRESSION
write (page cache, hot)             | 3.20         | 10.00        | REGRESSION
write (auto, cold)                  | 1.70         | 10.00        | REGRESSION
*/
