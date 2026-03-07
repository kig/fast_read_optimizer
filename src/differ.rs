use iou::IoUring;
use rand::Rng;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::common::PageAligned;

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
    processed_count: Arc<AtomicU64>,
    bench_only: bool,
    force_direct: bool,
    no_direct: bool,
) {
    let mut buffers1 = Vec::new();
    let mut buffers2 = Vec::new();
    for _ in 0..(qd * 2) {
        let num_pages = (block_size as usize + 4095) / 4096;
        let mut a1 = vec![PageAligned([0; 4096]); num_pages].into_boxed_slice();
        let p1 = a1.as_mut_ptr() as *mut u8;
        buffers1.push((unsafe { std::slice::from_raw_parts_mut(p1, block_size as usize) }, a1));

        let mut a2 = vec![PageAligned([0; 4096]); num_pages].into_boxed_slice();
        let p2 = a2.as_mut_ptr() as *mut u8;
        buffers2.push((unsafe { std::slice::from_raw_parts_mut(p2, block_size as usize) }, a2));
    }

    let mut inflight = 0;
    let mut next_offset = thread_id * block_size;
    let mut buffer_offsets = vec![0u64; qd * 2];
    
    // Maintain a list of free buffer indices
    let mut free_buffers: Vec<usize> = (0..qd*2).collect();

    // Initial fill up to qd pairs
    for _ in 0..qd {
        if next_offset >= total_size { break; }
        let idx = free_buffers.pop().unwrap();
        buffer_offsets[idx] = next_offset;
        let len = (total_size - next_offset).min(block_size);
        
        let use_direct = if no_direct {
            false
        } else if force_direct {
            true
        } else {
            !crate::writer::is_range_in_page_cache(file1.1, next_offset, len as usize) || 
            !crate::writer::is_range_in_page_cache(file2.1, next_offset, len as usize)
        };

        let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
        let f1_fd = if use_direct && is_aligned { file1.0.as_raw_fd() } else { file1.1.as_raw_fd() };
        let f2_fd = if use_direct && is_aligned { file2.0.as_raw_fd() } else { file2.1.as_raw_fd() };
        unsafe {
            let mut sqe1 = io_uring.prepare_sqe().unwrap();
            sqe1.prep_read(f1_fd, &mut buffers1[idx].0[..len as usize], next_offset);
            sqe1.set_user_data((idx as u64) | ((if use_direct { 1u64 } else { 0u64 }) << 32) | (1u64 << 40));

            let mut sqe2 = io_uring.prepare_sqe().unwrap();
            sqe2.prep_read(f2_fd, &mut buffers2[idx].0[..len as usize], next_offset);
            sqe2.set_user_data((idx as u64) | ((if use_direct { 1u64 } else { 0u64 }) << 32) | (2u64 << 40));
        }
        next_offset += num_threads * block_size;
        inflight += 2;
    }
    
    if inflight == 0 { return; }
    io_uring.submit_sqes().unwrap();

    let mut ready1 = vec![false; qd * 2];
    let mut ready2 = vec![false; qd * 2];

    while inflight > 0 && mismatch.load(Ordering::Relaxed) == 0 {
        let cq = io_uring.wait_for_cqe().expect("wait_for_cqe failed");
        let ud = cq.user_data();
        let idx = (ud & 0xFFFFFFFF) as usize;
        let _use_direct_finished = ((ud >> 32) & 0xFF) != 0;
        let file_num = (ud >> 40) as u8;
        cq.result().expect("IO failed");
        inflight -= 1;
        if file_num == 1 { ready1[idx] = true; } else { ready2[idx] = true; }

        let mut ready_indices = vec![idx];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().unwrap();
            let ud = cq.user_data();
            let idx2 = (ud & 0xFFFFFFFF) as usize;
            let file_num = (ud >> 40) as u8;
            cq.result().expect("IO failed");
            inflight -= 1;
            if file_num == 1 { ready1[idx2] = true; } else { ready2[idx2] = true; }
            if !ready_indices.contains(&idx2) {
                ready_indices.push(idx2);
            }
        }

        for &idx in &ready_indices {
            if ready1[idx] && ready2[idx] {
                let off = buffer_offsets[idx];
                let len = (total_size - off).min(block_size) as usize;
                
                if !bench_only {
                    if buffers1[idx].0[..len] != buffers2[idx].0[..len] {
                        for j in 0..len {
                            if buffers1[idx].0[j] != buffers2[idx].0[j] {
                                let absolute_offset = off + j as u64;
                                println!("Mismatch at offset {}: {:02x} != {:02x}", absolute_offset, buffers1[idx].0[j], buffers2[idx].0[j]);
                                mismatch.compare_exchange(0, absolute_offset + 1, Ordering::SeqCst, Ordering::SeqCst).ok();
                                break;
                            }
                        }
                    }
                }
                
                processed_count.fetch_add(len as u64, Ordering::Relaxed);
                ready1[idx] = false;
                ready2[idx] = false;
                free_buffers.push(idx);
            }
        }

        let mut submitted = false;
        while next_offset < total_size && mismatch.load(Ordering::Relaxed) == 0 && (inflight / 2) < qd {
            if let Some(next_idx) = free_buffers.pop() {
                buffer_offsets[next_idx] = next_offset;
                let next_len = (total_size - next_offset).min(block_size);
                
                let use_direct = if no_direct {
                    false
                } else if force_direct {
                    true
                } else {
                    !crate::writer::is_range_in_page_cache(file1.1, next_offset, next_len as usize) || 
                    !crate::writer::is_range_in_page_cache(file2.1, next_offset, next_len as usize)
                };

                let is_aligned = (next_offset % 4096 == 0) && (next_len == block_size);
                let f1_fd = if use_direct && is_aligned { file1.0.as_raw_fd() } else { file1.1.as_raw_fd() };
                let f2_fd = if use_direct && is_aligned { file2.0.as_raw_fd() } else { file2.1.as_raw_fd() };
                unsafe {
                    let mut sqe1 = io_uring.prepare_sqe().unwrap();
                    sqe1.prep_read(f1_fd, &mut buffers1[next_idx].0[..next_len as usize], next_offset);
                    sqe1.set_user_data((next_idx as u64) | ((if use_direct { 1u64 } else { 0u64 }) << 32) | (1u64 << 40));

                    let mut sqe2 = io_uring.prepare_sqe().unwrap();
                    sqe2.prep_read(f2_fd, &mut buffers2[next_idx].0[..next_len as usize], next_offset);
                    sqe2.set_user_data((next_idx as u64) | ((if use_direct { 1u64 } else { 0u64 }) << 32) | (2u64 << 40));
                }
                submitted = true;
                next_offset += num_threads * block_size;
                inflight += 2;
            } else {
                break;
            }
        }
        if submitted {
            io_uring.submit_sqes().unwrap();
        }
    }
}

pub fn diff_files(file1: &str, file2: &str, num_threads: u64, block_size: u64, qd: usize, force_direct: bool, no_direct: bool, verbose: bool, bench_only: bool) -> u64 {
    let mut f1_check = File::open(file1).unwrap();
    let s1 = f1_check.seek(SeekFrom::End(0)).unwrap();
    let mut f2_check = File::open(file2).unwrap();
    let s2 = f2_check.seek(SeekFrom::End(0)).unwrap();
    if s1 != s2 {
        if verbose { eprintln!("Files have different sizes: {} != {}", s1, s2); }
        return 1;
    }

    let mismatch = Arc::new(AtomicU64::new(0));
    let processed_count = Arc::new(AtomicU64::new(0));
    let start = std::time::Instant::now();
    let mut threads = vec![];
    
    for thread_id in 0..num_threads {
        let mismatch = mismatch.clone();
        let processed_count = processed_count.clone();
        let f1_name = file1.to_string();
        let f2_name = file2.to_string();
        threads.push(std::thread::spawn(move || {
            let f1_nodir = File::open(&f1_name).unwrap();
            let f1_dir = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&f1_name).unwrap();
            let f2_nodir = File::open(&f2_name).unwrap();
            let f2_dir = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&f2_name).unwrap();
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_differ(thread_id, (&f1_dir, &f1_nodir), (&f2_dir, &f2_nodir), num_threads, block_size, qd, &mut io_uring, s1, mismatch, processed_count, bench_only, force_direct, no_direct);
        }));
    }
    
    for thread in threads { thread.join().unwrap(); }
    
    let cpu_time_used = start.elapsed().as_secs_f64();
    let count = processed_count.load(Ordering::SeqCst);
    if verbose {
        let total_bytes = count * 2;
        eprintln!("{} {} bytes in {:.4} s, {:.1} GB/s", if bench_only { "Dual-read" } else { "Diff" }, count, cpu_time_used, total_bytes as f64 / cpu_time_used / 1e9);
    }
    mismatch.load(Ordering::SeqCst)
}

pub fn bench_diff_memory(num_threads: usize, block_size: usize) {
    let total_size = 1024 * 1024 * 1024;
    let mut buf1 = vec![0u8; total_size];
    let mut buf2 = vec![0u8; total_size];
    rand::thread_rng().fill(&mut buf1[..]);
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
            let end_off = if t == num_threads - 1 { total_size } else { start_off + chunk_size };
            let mut off = start_off;
            while off < end_off && mm.load(Ordering::Relaxed) == 0 {
                let next_off = (off + block_size).min(end_off);
                if b1[off..next_off] != b2[off..next_off] {
                    mm.compare_exchange(0, off as u64 + 1, Ordering::SeqCst, Ordering::SeqCst).ok();
                    break;
                }
                off = next_off;
            }
        }));
    }
    for thread in threads { thread.join().unwrap(); }
    let dur = start.elapsed().as_secs_f64();
    println!("Memory diff {} bytes in {:.4} s, {:.1} GB/s", total_size, dur, (total_size * 2) as f64 / dur / 1e9);
}
