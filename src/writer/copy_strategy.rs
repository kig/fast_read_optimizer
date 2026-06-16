use super::*;
use crate::uring_util::io_uring_available;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub fn copy_file_range_with_strategy(
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
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
) -> io::Result<u64> {
    copy_file_range_with_strategy_and_progress(
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
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
        None,
    )
}

pub fn copy_file_range_with_strategy_and_progress(
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
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    // macOS whole-file fast-path: use platform copyfile if possible
    #[cfg(target_os = "macos")]
    {
        if source_offset == 0 && dest_offset == 0 && truncate_target {
            if let Ok(src_meta) = std::fs::metadata(source) {
                let effective_copy_size =
                    copy_size.min(src_meta.len().saturating_sub(source_offset));
                if effective_copy_size == src_meta.len() {
                    fro::os::copy_path(source, filename).map_err(|e| {
                        std::io::Error::new(e.kind(), format!("macOS fast copy failed: {}", e))
                    })?;
                    if let Some(progress) = progress_count.as_ref() {
                        progress
                            .fetch_add(effective_copy_size, std::sync::atomic::Ordering::SeqCst);
                    }
                    return Ok(effective_copy_size);
                }
            }
        }
    }

    match copy_strategy {
        CopyStrategy::Auto | CopyStrategy::Threaded => {
            if !io_uring_available(1024)? {
                #[cfg(test)]
                record_copy_backend(source, filename, RecordedCopyBackend::Blocking);
                return copy_file_range_blocking_with_progress(
                    source,
                    filename,
                    source_offset,
                    dest_offset,
                    copy_size,
                    truncate_target,
                    block_size_p,
                    block_size_d,
                    io_mode_read,
                    io_mode_write,
                    progress_count,
                );
            }
            #[cfg(test)]
            record_copy_backend(source, filename, RecordedCopyBackend::Threaded);
            copy_file_range_threaded(
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
                progress_count,
            )
        }
        CopyStrategy::CopyFileRange => {
            #[cfg(test)]
            record_copy_backend(source, filename, RecordedCopyBackend::CopyFileRange);
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
                progress_count,
            )
        }
        CopyStrategy::CopyFileRangeSingle => {
            #[cfg(test)]
            record_copy_backend(source, filename, RecordedCopyBackend::CopyFileRangeSingle);
            copy_file_range_syscall_with_progress(
                source,
                filename,
                source_offset,
                dest_offset,
                copy_size,
                truncate_target,
                io_mode_read,
                io_mode_write,
                progress_count,
            )
        }
        CopyStrategy::Reflink => {
            #[cfg(test)]
            record_copy_backend(source, filename, RecordedCopyBackend::Reflink);
            copy_file_reflink(
                source,
                filename,
                source_offset,
                dest_offset,
                copy_size,
                truncate_target,
                io_mode_read,
                io_mode_write,
            )
        }
    }
}
