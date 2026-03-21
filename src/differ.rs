use crate::common::{AlignedBuffer, IOMode};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use rand::RngExt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn thread_differ(
    thread_id: u64,
    file1: (&File, &File),
    file2: (&File, &File),
    num_threads: u64,
    block_size: u64,
    qd: usize,
    io_uring: &mut IoUring,
    total_size: u64,
    mismatch: Arc<AtomicU64>,
    bench_only: bool,
    report_mismatch: bool,
    use_direct: bool,
) -> io::Result<()> {
    let mut buffers1 = Vec::new();
    let mut buffers2 = Vec::new();
    for _ in 0..(qd * 2) {
        buffers1.push(AlignedBuffer::new(block_size as usize));
        buffers2.push(AlignedBuffer::new(block_size as usize));
    }
    let mut inflight = 0;
    let mut next_offset = thread_id * block_size;
    let mut buffer_offsets = vec![0u64; qd * 2];

    // Maintain a list of free buffer indices
    let mut free_buffers: Vec<usize> = (0..qd * 2).collect();

    // Initial fill up to qd pairs
    for _ in 0..qd {
        if next_offset >= total_size {
            break;
        }
        let idx = free_buffers.pop().unwrap();
        buffer_offsets[idx] = next_offset;
        let len = (total_size - next_offset).min(block_size);

        let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
        let f1_fd = if use_direct && is_aligned {
            file1.0.as_raw_fd()
        } else {
            file1.1.as_raw_fd()
        };
        let f2_fd = if use_direct && is_aligned {
            file2.0.as_raw_fd()
        } else {
            file2.1.as_raw_fd()
        };
        unsafe {
            let mut sqe1 = io_uring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            sqe1.prep_read(
                f1_fd,
                &mut buffers1[idx].as_mut_slice()[..len as usize],
                next_offset,
            );
            sqe1.set_user_data((idx as u64) | (1u64 << 40));

            let mut sqe2 = io_uring
                .prepare_sqe()
                .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
            sqe2.prep_read(
                f2_fd,
                &mut buffers2[idx].as_mut_slice()[..len as usize],
                next_offset,
            );
            sqe2.set_user_data((idx as u64) | (2u64 << 40));
        }
        next_offset += num_threads * block_size;
        inflight += 2;
    }

    if inflight == 0 {
        return Ok(());
    }
    io_uring.submit_sqes().map_err(io::Error::other)?;

    let mut ready1 = vec![false; qd * 2];
    let mut ready2 = vec![false; qd * 2];

    while inflight > 0 && mismatch.load(Ordering::Relaxed) == 0 {
        let cq = io_uring.wait_for_cqe().map_err(io::Error::other)?;
        let ud = cq.user_data();
        let idx = (ud & 0xFFFFFFFF) as usize;
        let file_num = (ud >> 40) as u8;
        cq.result()?;
        inflight -= 1;
        if file_num == 1 {
            ready1[idx] = true;
        } else {
            ready2[idx] = true;
        }

        let mut ready_indices = vec![idx];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().ok_or_else(|| {
                io::Error::other("completion queue reported ready but no CQE was available")
            })?;
            let ud = cq.user_data();
            let idx2 = (ud & 0xFFFFFFFF) as usize;
            let file_num = (ud >> 40) as u8;
            cq.result()?;
            inflight -= 1;
            if file_num == 1 {
                ready1[idx2] = true;
            } else {
                ready2[idx2] = true;
            }
            if !ready_indices.contains(&idx2) {
                ready_indices.push(idx2);
            }
        }

        for &idx in &ready_indices {
            if ready1[idx] && ready2[idx] {
                let off = buffer_offsets[idx];
                let len = (total_size - off).min(block_size) as usize;

                if !bench_only {
                    if buffers1[idx].as_slice()[..len] != buffers2[idx].as_slice()[..len] {
                        for j in 0..len {
                            if buffers1[idx].as_slice()[j] != buffers2[idx].as_slice()[j] {
                                let absolute_offset = off + j as u64;
                                if report_mismatch {
                                    println!(
                                        "Mismatch at offset {}: {:02x} != {:02x}",
                                        absolute_offset,
                                        buffers1[idx].as_slice()[j],
                                        buffers2[idx].as_slice()[j]
                                    );
                                }
                                mismatch
                                    .compare_exchange(
                                        0,
                                        absolute_offset + 1,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .ok();
                                break;
                            }
                        }
                    }
                }

                ready1[idx] = false;
                ready2[idx] = false;
                free_buffers.push(idx);
            }
        }

        let mut submitted = false;
        while next_offset < total_size
            && mismatch.load(Ordering::Relaxed) == 0
            && (inflight / 2) < qd
        {
            if let Some(next_idx) = free_buffers.pop() {
                buffer_offsets[next_idx] = next_offset;
                let next_len = (total_size - next_offset).min(block_size);

                let is_aligned = (next_offset % 4096 == 0) && (next_len == block_size);
                let f1_fd = if use_direct && is_aligned {
                    file1.0.as_raw_fd()
                } else {
                    file1.1.as_raw_fd()
                };
                let f2_fd = if use_direct && is_aligned {
                    file2.0.as_raw_fd()
                } else {
                    file2.1.as_raw_fd()
                };
                unsafe {
                    let mut sqe1 = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe1.prep_read(
                        f1_fd,
                        &mut buffers1[next_idx].as_mut_slice()[..next_len as usize],
                        next_offset,
                    );
                    sqe1.set_user_data((next_idx as u64) | (1u64 << 40));

                    let mut sqe2 = io_uring
                        .prepare_sqe()
                        .ok_or_else(|| io::Error::other("io_uring submission queue is full"))?;
                    sqe2.prep_read(
                        f2_fd,
                        &mut buffers2[next_idx].as_mut_slice()[..next_len as usize],
                        next_offset,
                    );
                    sqe2.set_user_data((next_idx as u64) | (2u64 << 40));
                }
                submitted = true;
                next_offset += num_threads * block_size;
                inflight += 2;
            } else {
                break;
            }
        }
        if submitted {
            io_uring.submit_sqes().map_err(io::Error::other)?;
        }
    }
    Ok(())
}

pub fn diff_files(
    file1: &str,
    file2: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    bench_only: bool,
    report_mismatch: bool,
) -> io::Result<u64> {
    let mismatch = Arc::new(AtomicU64::new(0));
    let mut threads = vec![];

    let file_cached =
        Ok(true) == is_first_page_resident(file1) && Ok(true) == is_first_page_resident(file2);
    let use_direct = ((!file_cached) && io_mode == IOMode::Auto) || io_mode == IOMode::Direct;

    let num_threads = if use_direct {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if use_direct {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if use_direct { qd_d } else { qd_p };

    let s1 = std::fs::metadata(file1)?.len();
    let s2 = std::fs::metadata(file2)?.len();
    let file_size = std::cmp::min(s1, s2);
    for thread_id in 0..num_threads {
        let mismatch = mismatch.clone();
        let f1_name = file1.to_string();
        let f2_name = file2.to_string();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let f1_pagecache = File::open(&f1_name)?;
            let f1_direct = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(&f1_name)?;
            let f2_pagecache = File::open(&f2_name)?;
            let f2_direct = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(&f2_name)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_differ(
                thread_id,
                (&f1_direct, &f1_pagecache),
                (&f2_direct, &f2_pagecache),
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                file_size,
                mismatch,
                bench_only,
                report_mismatch,
                use_direct,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("diff worker thread panicked"))??;
    }

    Ok(mismatch.load(Ordering::SeqCst))
}

pub fn bench_diff_memory(num_threads: usize, block_size: usize) {
    let total_size = 1024 * 1024 * 1024;
    let mut buf1 = vec![0u8; total_size];
    let mut buf2 = vec![0u8; total_size];
    rand::rng().fill(&mut buf1[..]);
    buf2.copy_from_slice(&buf1);
    let start = std::time::Instant::now();
    let mismatch = Arc::new(AtomicU64::new(0));
    let mut threads = vec![];
    let buf1 = Arc::new(buf1);
    let buf2 = Arc::new(buf2);
    for t in 0..num_threads {
        let b1 = buf1.clone();
        let b2 = buf2.clone();
        let mm = mismatch.clone();
        threads.push(std::thread::spawn(move || {
            let chunk_size = total_size / num_threads;
            let start_off = t * chunk_size;
            let end_off = if t == num_threads - 1 {
                total_size
            } else {
                start_off + chunk_size
            };
            let mut off = start_off;
            while off < end_off && mm.load(Ordering::Relaxed) == 0 {
                let next_off = (off + block_size).min(end_off);
                if b1[off..next_off] != b2[off..next_off] {
                    mm.compare_exchange(0, off as u64 + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .ok();
                    break;
                }
                off = next_off;
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let dur = start.elapsed().as_secs_f64();
    println!(
        "Memory diff {} bytes in {:.4} s, {:.1} GB/s",
        total_size,
        dur,
        (total_size * 2) as f64 / dur / 1e9
    );
}

pub fn bench_memcpy_memory(num_threads: usize, total_size: usize) {
    let start = std::time::Instant::now();
    let src = Arc::new(AlignedBuffer::new_uninit(total_size).unwrap());
    let mut dst = AlignedBuffer::new_uninit(total_size).unwrap();
    let src_ptr = src.as_ptr() as usize;
    let dst_ptr = dst.as_mut_slice().as_mut_ptr() as usize;
    let mut threads = vec![];

    for t in 0..num_threads {
        let chunk_size = total_size / num_threads;
        let start_off = t * chunk_size;
        let end_off = if t == num_threads - 1 {
            total_size
        } else {
            start_off + chunk_size
        };
        threads.push(std::thread::spawn(move || {
            let len = end_off - start_off;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (src_ptr as *const u8).add(start_off),
                    (dst_ptr as *mut u8).add(start_off),
                    len,
                );
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let dur = start.elapsed().as_secs_f64();
    std::hint::black_box(dst);
    println!(
        "Memory memcpy {} bytes in {:.4} s, {:.1} GB/s",
        total_size,
        dur,
        (total_size * 2) as f64 / dur / 1e9
    );
}
