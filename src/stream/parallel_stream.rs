use super::*;
use crate::io_util::checked_posix_fallocate;

impl ParallelStream {
    /// Simple fallback: read from a pipe/file and write processed blocks to a pipe (dest).
    /// This is a conservative single-threaded implementation used when the fast
    /// ParallelStream io_uring pipeline is not applicable (e.g., when dest is a pipe).
    pub fn map_pipe_fixed_size_to_pipe<F>(
        config: &LoadedConfig,
        src: &File,
        dest: &File,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8], &mut [u8]) -> std::io::Result<usize> + Send + Sync + 'static,
    {
        use std::io::{Read, Write};
        let mut reader = src.try_clone()?;
        let mut writer = dest.try_clone()?;
        let mut inbuf = vec![0u8; read_block_size as usize];
        let mut outbuf = vec![0u8; write_block_size];
        let mut total_written: u64 = 0;
        let mut block_ranges = Vec::new();
        loop {
            let n = reader.read(&mut inbuf)?;
            if n == 0 {
                break;
            }
            let produced = processor(&inbuf[..n], &mut outbuf)?;
            if produced > 0 {
                writer.write_all(&outbuf[..produced])?;
                block_ranges.push(BlockRange {
                    offset: total_written,
                    len: produced as u64,
                });
                total_written = total_written
                    .checked_add(produced as u64)
                    .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "overflow"))?;
            }
        }
        let write_params = resolve_writer_params_for_mode(config, "write", "/", IOMode::PageCache);
        Ok(ParallelWriteReport {
            bytes_written: total_written,
            block_ranges,
            write_params,
        })
    }

    /// Simple fallback: read from a regular file path and write processed blocks to a pipe (dest File).
    pub fn map_file_fixed_size_to_pipe<F>(
        config: &LoadedConfig,
        read_path: &str,
        dest: &File,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8], &mut [u8]) -> std::io::Result<usize> + Send + Sync + 'static,
    {
        let input_file = ParallelFile::open(config, "compute", read_path, IOMode::PageCache)?;
        let block_size = read_block_size;
        if block_size == 0 {
            return Err(std::io::Error::other("block size must be greater than zero").into());
        }
        let block_count = input_file.block_count(block_size)?;

        let _page_cache = config.get_params_for_path("compute", false, read_path);

        // Create the indexed pipe writer and hand it a Sender so it can return buffers
        let output_stream =
            ParallelWriter::indexed_pipe(dest.as_raw_fd(), block_count, write_block_size as u64)?;
        let output_stream_arc = Arc::new(output_stream);
        let output_stream_for_blocks = output_stream_arc.clone();

        let _read_report =
            input_file.foreach_block_parallel(block_size, move |chunk_index, raw_bytes| {
                let mut out = allocate_pipe_output_buffer(write_block_size);
                let produced = processor(raw_bytes, &mut out)?;
                out.truncate(produced);
                output_stream_for_blocks.write_at_index(chunk_index, out)?;
                Ok(())
            })?;

        // Unwrap the Arc back into ownership and finish
        let output_stream = Arc::try_unwrap(output_stream_arc)
            .map_err(|_| std::io::Error::other("failed to unwrap writer Arc"))?;
        output_stream.finish()
    }

    /// Map an input file to a pipe using processor-owned output buffers.
    /// This preserves any caller-provided alignment/allocation strategy so the
    /// pipe writer can stay on its vmsplice fast path.
    pub fn map_file_to_pipe_with_owned_buffers<F>(
        config: &LoadedConfig,
        read_path: &str,
        dest: &File,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8]) -> std::io::Result<Vec<u8>> + Send + Sync + 'static,
    {
        let input_file = ParallelFile::open(config, "compute", read_path, IOMode::PageCache)?;
        if read_block_size == 0 {
            return Err(std::io::Error::other("block size must be greater than zero").into());
        }
        let block_count = input_file.block_count(read_block_size)?;

        let output_stream =
            ParallelWriter::indexed_pipe(dest.as_raw_fd(), block_count, write_block_size as u64)?;
        let output_stream_arc = Arc::new(output_stream);
        let output_stream_for_blocks = output_stream_arc.clone();

        let _read_report =
            input_file.foreach_block_parallel(read_block_size, move |chunk_index, raw_bytes| {
                let produced = processor(raw_bytes)?;
                output_stream_for_blocks.write_at_index(chunk_index, produced)?;
                Ok(())
            })?;

        let output_stream = Arc::try_unwrap(output_stream_arc)
            .map_err(|_| std::io::Error::other("failed to unwrap writer Arc"))?;
        output_stream.finish()
    }

    /// Map an input file to an output file using fixed-size output blocks.
    /// The processor closure receives an input block slice and a mutable Vec<u8>
    /// to write output bytes into. The produced Vec is sent to the indexed
    /// ParallelWriter at the same block index. The writer is configured for
    /// `block_count` indexed writes where block_count is derived from the
    /// input file size and the provided read_block_size.
    pub fn map_file_fixed_size<F>(
        config: &LoadedConfig,
        read_path: &str,
        write_path: &str,
        read_block_size: u64,
        write_block_size: usize,
        processor: F,
    ) -> std::io::Result<ParallelWriteReport>
    where
        F: for<'a> Fn(&'a [u8], &mut [u8]) -> std::io::Result<usize> + Send + Sync + 'static,
    {
        if read_block_size == 0 || write_block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block sizes must be greater than zero",
            ));
        }

        // Open reader to query block count and to ensure the file exists.
        let reader = ParallelFile::open(config, "read", read_path, IOMode::Auto)?;
        let block_count = reader.block_count(read_block_size)?;

        // Prepare destination file for fixed-size output blocks.
        let writer_io_mode =
            effective_io_mode_for_block_size(IOMode::Auto, write_block_size as u64);
        let write_params =
            resolve_writer_params_for_mode(config, "write", write_path, writer_io_mode);

        let total_size = (block_count as u64)
            .checked_mul(write_block_size as u64)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "output size overflowed")
            })?;

        // Ensure destination file exists and is sized appropriately.
        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(write_path)?;
        if total_size > 0 {
            let metadata = dest_file.metadata()?;
            if metadata.file_type().is_file() {
                let current_len = metadata.len();
                if current_len != total_size {
                    dest_file.set_len(total_size)?;
                    checked_posix_fallocate(
                        &dest_file,
                        0,
                        total_size,
                        "failed to preallocate parallel stream output",
                    )?;
                }
            }
        }

        // Resolve reader params for the requested read_block_size and perform parallel reads.
        // Build IO params using the configured per-path thread and qd values but the
        // caller-provided block_size so reads happen at the requested granularity.
        let page_cache_cfg = config.get_params_for_path("read", false, read_path);
        let direct_cfg = config.get_params_for_path("read", true, read_path);
        let io_mode = effective_io_mode_for_block_size(IOMode::Auto, read_block_size);
        let params = resolve_reader_params(
            read_path,
            &crate::config::IOParams {
                num_threads: page_cache_cfg.num_threads,
                block_size: read_block_size,
                qd: page_cache_cfg.qd,
            },
            &crate::config::IOParams {
                num_threads: direct_cfg.num_threads,
                block_size: read_block_size,
                qd: direct_cfg.qd,
            },
            io_mode,
        )?;
        let processor = Arc::new(processor);
        let mut threads = Vec::new();
        // shared vector to record produced byte ranges per block
        let shared_block_ranges = Arc::new(Mutex::new(vec![None::<BlockRange>; block_count]));

        for thread_id in 0..params.num_threads {
            let read_path = read_path.to_string();
            let write_path = write_path.to_string();
            let processor = processor.clone();
            let params = params; // copy
            let write_block_size = write_block_size;
            let write_params = write_params; // copy
            let shared_block_ranges = shared_block_ranges.clone();

            threads.push(std::thread::spawn(move || -> std::io::Result<u64> {
                // Open per-thread reader files and a reader io_uring
                let (mut file, file_direct) = open_reader_files(&read_path, params.use_direct)?;
                let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
                let mut buffers = Vec::new();
                let mut write_buffers = Vec::new();
                for _ in 0..params.qd {
                    buffers.push(AlignedBuffer::new(read_block_size.try_into().unwrap()));
                    write_buffers.push(AlignedBuffer::new(write_block_size.try_into().unwrap()));
                }

                // Open per-thread destination file descriptors (page-cache and direct fallback)
                let dest_pagecache = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&write_path)?;
                let dest_direct =
                    crate::io_util::open_direct_writer_or_fallback(&write_path, &dest_pagecache)?;

                let file_size = file.seek(SeekFrom::End(0))?;
                let thread_base = thread_id * read_block_size;
                let mut block_num: u64 = 0;
                // inflight counts outstanding io (reads + writes). Start by submitting reads.
                let mut inflight: usize = 0;
                let mut pending = PendingReadSlots::new(params.qd);

                // pending write buffers to keep memory alive until write completion
                struct PendingWrite {
                    len: usize,
                }
                let mut pending_writes: Vec<Option<PendingWrite>> =
                    (0..params.qd).map(|_| None).collect();

                // initial submit of up to qd reads
                for slot in 0..params.qd {
                    let stride = block_num
                        .checked_mul(params.num_threads)
                        .and_then(|v| v.checked_mul(read_block_size))
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "read offset calculation overflowed",
                            )
                        })?;
                    let current_offset = thread_base.checked_add(stride).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "read offset calculation overflowed",
                        )
                    })?;
                    if current_offset >= file_size {
                        break;
                    }
                    pending.reserve(slot, block_num)?;
                    let is_aligned =
                        (current_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                    let fd = if params.use_direct && is_aligned {
                        file_direct.as_raw_fd()
                    } else {
                        file.as_raw_fd()
                    };
                    unsafe {
                        let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                            std::io::Error::other("io_uring submission queue is full")
                        })?;
                        // mark as read state (1)
                        let read_len =
                            expected_read_len(file_size, current_offset, read_block_size)?;
                        sqe.prep_read(
                            fd,
                            &mut buffers[slot].as_mut_slice()[..read_len],
                            current_offset,
                        );
                        sqe.set_user_data((slot as u64) | (1u64 << 40));
                    }
                    block_num += 1;
                    inflight += 1;
                }

                if inflight == 0 {
                    // nothing to do
                    return Ok(0);
                }
                io_uring.submit_sqes().map_err(std::io::Error::other)?;

                // total bytes this thread wrote
                let mut thread_bytes_written: u64 = 0;

                while inflight > 0 {
                    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
                    let user_data = cq.user_data();
                    let mut ready = vec![(user_data, cq.result()? as u32)];
                    while io_uring.cq_ready() > 0 {
                        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                            std::io::Error::other(
                                "completion queue reported ready but no CQE was available",
                            )
                        })?;
                        ready.push((cq.user_data(), cq.result()? as u32));
                    }

                    for (user_data, result) in ready {
                        let slot = (user_data & 0xFFFFFFFF) as usize;
                        let state = (user_data >> 40) as u8;

                        if state == 1 {
                            // Read finished for slot
                            // decrement inflight for the completed read
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on read completion",
                                )
                            })?;

                            // peek block id (do NOT free the slot yet; keep it reserved until write completes)
                            let block_id = pending.peek(slot)?;

                            let stride = block_id
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let current_offset =
                                thread_base.checked_add(stride).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;

                            let expected_len =
                                expected_read_len(file_size, current_offset, read_block_size)?;
                            let actual_len = validate_read_result(
                                "map-file-fixed",
                                current_offset,
                                expected_len,
                                result,
                            )?;

                            if actual_len > 0 {
                                let block_index = (current_offset / params.block_size) as usize;
                                // Reuse the slot buffer for the write: copy processed data into it.
                                let read_buf = &buffers[slot].as_slice()[..actual_len];
                                let out = &mut write_buffers[slot].as_mut_slice();
                                let produced = processor(read_buf, out)?;

                                // copy output into the slot's aligned buffer so the buffer can be
                                // reused for future reads only after the write completes
                                let produced_len = produced;
                                let dst_offset = (block_index as u64)
                                    .checked_mul(write_block_size as u64)
                                    .ok_or_else(|| {
                                        std::io::Error::new(
                                            std::io::ErrorKind::InvalidInput,
                                            "destination offset overflowed",
                                        )
                                    })?;

                                let is_aligned_write =
                                    (dst_offset % 4096 == 0) && (produced_len % 4096 == 0);
                                let fd = if write_params.use_direct && is_aligned_write {
                                    dest_direct.as_raw_fd()
                                } else {
                                    dest_pagecache.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    // mark as write state (2)
                                    sqe.prep_write(
                                        fd,
                                        &write_buffers[slot].as_slice()[..produced_len],
                                        dst_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (2u64 << 40));
                                }
                                /*
                                if std::env::var("FRO_PARALLEL_LOG").is_ok() {
                                    eprintln!(
                                        "[thread {}] submit write block_index={} len={}",
                                        thread_id, block_index, produced_len,
                                    );
                                }
                                */
                                // record pending write length; buffer itself is already in buffers[slot]
                                pending_writes[slot] = Some(PendingWrite { len: produced_len });
                                // a write was submitted; increment inflight to account for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                            }

                            // submit any sqes prepared above
                            io_uring.submit_sqes().map_err(std::io::Error::other)?;
                        } else if state == 2 {
                            // Write finished for slot
                            let written = result as usize;
                            let pending_write = pending_writes[slot].take().ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "missing pending write buffer",
                                )
                            })?;
                            if written != pending_write.len {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    format!(
                                        "short write: expected {} bytes, wrote {}",
                                        pending_write.len, written
                                    ),
                                ));
                            }
                            thread_bytes_written = thread_bytes_written
                                .checked_add(written as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "thread bytes_written overflowed",
                                    )
                                })?;

                            // a write completed, so one inflight op finished
                            inflight = inflight.checked_sub(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "inflight underflow on write completion",
                                )
                            })?;

                            // now free the slot's pending read record so the slot can be reused
                            let completed_block_id = pending.complete(slot)?;

                            // record the produced length for this block in the shared block ranges
                            let block_index = thread_id as usize
                                + (completed_block_id as usize) * (params.num_threads as usize);
                            if block_index >= block_count {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    format!(
                                        "computed block index {} out of range for {} blocks",
                                        block_index, block_count
                                    ),
                                ));
                            }
                            let dst_offset = (block_index as u64)
                                .checked_mul(write_block_size as u64)
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "destination offset overflowed",
                                    )
                                })?;
                            {
                                let mut ranges = shared_block_ranges.lock().unwrap();
                                ranges[block_index] = Some(BlockRange {
                                    offset: dst_offset,
                                    len: written as u64,
                                });
                            }

                            // attempt to submit next read into this slot
                            let stride = block_num
                                .checked_mul(params.num_threads)
                                .and_then(|v| v.checked_mul(read_block_size))
                                .ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "read offset calculation overflowed",
                                    )
                                })?;
                            let next_offset = thread_base.checked_add(stride).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "read offset calculation overflowed",
                                )
                            })?;
                            if next_offset < file_size {
                                pending.reserve(slot, block_num)?;
                                let is_aligned =
                                    (next_offset % 4096 == 0) && ((read_block_size % 4096) == 0);
                                let fd = if params.use_direct && is_aligned {
                                    file_direct.as_raw_fd()
                                } else {
                                    file.as_raw_fd()
                                };
                                unsafe {
                                    let mut sqe = io_uring.prepare_sqe().ok_or_else(|| {
                                        std::io::Error::other("io_uring submission queue is full")
                                    })?;
                                    let read_len =
                                        expected_read_len(file_size, next_offset, read_block_size)?;
                                    sqe.prep_read(
                                        fd,
                                        &mut buffers[slot].as_mut_slice()[..read_len],
                                        next_offset,
                                    );
                                    sqe.set_user_data((slot as u64) | (1u64 << 40));
                                }
                                block_num += 1;
                                // a read was submitted; increment inflight for it
                                inflight = inflight.checked_add(1).ok_or_else(|| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        "inflight overflowed",
                                    )
                                })?;
                                io_uring.submit_sqes().map_err(std::io::Error::other)?;
                            }
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "unknown io_uring state",
                            ));
                        }
                    }
                }

                Ok(thread_bytes_written)
            }));
        }

        // collect thread results and sum bytes
        let mut total_written: u64 = 0;
        for thread in threads {
            let written = thread
                .join()
                .map_err(|_| std::io::Error::other("worker thread panicked"))??;
            total_written = total_written.checked_add(written).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "total bytes_written overflowed",
                )
            })?;
        }

        // assemble block ranges from per-thread records
        let mut final_block_ranges = Vec::with_capacity(block_count);
        {
            let ranges = shared_block_ranges.lock().unwrap();
            for (i, entry) in ranges.iter().enumerate() {
                match entry {
                    Some(br) => final_block_ranges.push(*br),
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("missing block range for index {}", i),
                        ));
                    }
                }
            }
        }

        // truncate the destination file to the actual written size to remove unused padding
        if total_written != 0 {
            dest_file.set_len(total_written)?;
        } else if total_size == 0 {
            // nothing to do for empty files
        } else {
            // if no bytes were written but total_size was non-zero, truncate to zero
            dest_file.set_len(0)?;
        }

        Ok(ParallelWriteReport {
            bytes_written: total_written,
            block_ranges: final_block_ranges,
            write_params,
        })
    }
}
