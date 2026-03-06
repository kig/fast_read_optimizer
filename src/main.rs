/*
    We're using io_uring for this.
*/
use iou::IoUring;
use rand::Rng;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;

// The thread reader function.
fn thread_reader(
    thread_id: u64,
    pattern: String,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
) -> Vec<u64> {
    let mut matches = Vec::new();
    let mut buffers = Vec::new();
    for _ in 0..qd {
        let mut allocation = vec![0u8; (block_size as usize) + 8192];
        let data_ptr = (allocation.as_mut_ptr() as usize + 4096) & !4095;
        let buffer = unsafe { std::slice::from_raw_parts_mut(data_ptr as *mut u8, block_size as usize) };
        buffers.push((buffer, allocation));
    }
    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = offset + (block_num as u64) * num_threads * block_size;
        if current_offset >= file_size { break; }
        unsafe {
            let mut sqe = io_uring.prepare_sqe().unwrap();
            sqe.prep_read(file.as_raw_fd(), &mut buffers[block_num % qd].0[..], current_offset);
            sqe.set_user_data(block_num as u64);
        }
        block_num += 1;
        inflight += 1;
    }
    
    if inflight == 0 { return matches; }
    io_uring.submit_sqes().unwrap();

    let finder = Finder::new(pattern.as_bytes());

    loop {
        let cq = io_uring.wait_for_cqe().unwrap();
        let block_id = cq.user_data() as u64;
        let result = cq.result().unwrap();
        if result > 0 {
            read_count.fetch_add(result as u64, Ordering::SeqCst);
            if !pattern.is_empty() {
                let buf = &buffers[block_id as usize % qd].0;
                for idx in finder.find_iter(&buf[..result as usize]) {
                    matches.push(offset + block_id * num_threads * block_size + idx as u64);
                }
            }
        }
        inflight -= 1;
        let next_offset = offset + (block_num as u64) * num_threads * block_size;
        if next_offset < file_size {
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_read(file.as_raw_fd(), &mut buffers[block_num % qd].0[..], next_offset);
                sqe.set_user_data(block_num as u64);
            }
            io_uring.submit_sqes().unwrap();
            block_num += 1;
            inflight += 1;
        }
        if inflight == 0 { return matches; }
    }
}

fn thread_writer(
    thread_id: u64,
    source_file: Option<(&File, &File)>,
    dest_file: (&File, &File),
    num_threads: u64,
    block_size: u64,
    qd: usize,
    io_uring: &mut IoUring,
    write_count: Arc<AtomicU64>,
    random_block: Option<&[u8]>,
    total_size: u64,
) {
    let mut buffers = Vec::new();
    for _ in 0..qd {
        let mut allocation = vec![0u8; (block_size as usize) + 8192];
        let data_ptr = (allocation.as_mut_ptr() as usize + 4096) & !4095;
        let buffer = unsafe { std::slice::from_raw_parts_mut(data_ptr as *mut u8, block_size as usize) };
        if let Some(rb) = random_block { buffer.copy_from_slice(rb); }
        buffers.push((buffer, allocation));
    }

    let mut inflight = 0;
    let mut next_offset = thread_id * block_size;
    let mut buffer_offsets = vec![0u64; qd];

    for i in 0..qd {
        if next_offset >= total_size { break; }
        buffer_offsets[i] = next_offset;
        let len = (total_size - next_offset).min(block_size);
        let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
        if let Some((src_dir, src_nodir)) = source_file.as_ref() {
            let fd = if is_aligned { src_dir.as_raw_fd() } else { src_nodir.as_raw_fd() };
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_read(fd, &mut buffers[i].0[..len as usize], next_offset);
                sqe.set_user_data((i as u64) | (1u64 << 32));
            }
        } else {
            let fd = if is_aligned { dest_file.0.as_raw_fd() } else { dest_file.1.as_raw_fd() };
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_write(fd, &buffers[i].0[..len as usize], next_offset);
                sqe.set_user_data((i as u64) | (2u64 << 32));
            }
        }
        next_offset += num_threads * block_size;
        inflight += 1;
    }
    if inflight == 0 { return; }
    io_uring.submit_sqes().unwrap();

    while inflight > 0 {
        let cq = io_uring.wait_for_cqe().expect("wait_for_cqe failed");
        let user_data = cq.user_data();
        let idx = (user_data & 0xFFFFFFFF) as usize;
        let state = (user_data >> 32) as u8;
        let result = cq.result().expect("IO failed");

        if state == 1 { 
            let len = result as u64;
            let is_aligned = (buffer_offsets[idx] % 4096 == 0) && (len % 4096 == 0);
            let fd = if is_aligned { dest_file.0.as_raw_fd() } else { dest_file.1.as_raw_fd() };
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_write(fd, &buffers[idx].0[..result as usize], buffer_offsets[idx]);
                sqe.set_user_data((idx as u64) | (2u64 << 32));
            }
            io_uring.submit_sqes().unwrap();
        } else {
            write_count.fetch_add(result as u64, Ordering::SeqCst);
            inflight -= 1;
            if next_offset < total_size {
                buffer_offsets[idx] = next_offset;
                let len = (total_size - next_offset).min(block_size);
                let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
                if let Some((src_dir, src_nodir)) = source_file.as_ref() {
                    let fd = if is_aligned { src_dir.as_raw_fd() } else { src_nodir.as_raw_fd() };
                    unsafe {
                        let mut sqe = io_uring.prepare_sqe().unwrap();
                        sqe.prep_read(fd, &mut buffers[idx].0[..len as usize], next_offset);
                        sqe.set_user_data((idx as u64) | (1u64 << 32));
                    }
                } else {
                    let fd = if is_aligned { dest_file.0.as_raw_fd() } else { dest_file.1.as_raw_fd() };
                    unsafe {
                        let mut sqe = io_uring.prepare_sqe().unwrap();
                        sqe.prep_write(fd, &buffers[idx].0[..len as usize], next_offset);
                        sqe.set_user_data((idx as u64) | (2u64 << 32));
                    }
                }
                io_uring.submit_sqes().unwrap();
                next_offset += num_threads * block_size;
                inflight += 1;
            }
        }
    }
}

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
) {
    let mut buffers1 = Vec::new();
    let mut buffers2 = Vec::new();
    for _ in 0..(qd * 2) {
        let mut a1 = vec![0u8; (block_size as usize) + 8192];
        let p1 = (a1.as_mut_ptr() as usize + 4096) & !4095;
        buffers1.push((unsafe { std::slice::from_raw_parts_mut(p1 as *mut u8, block_size as usize) }, a1));

        let mut a2 = vec![0u8; (block_size as usize) + 8192];
        let p2 = (a2.as_mut_ptr() as usize + 4096) & !4095;
        buffers2.push((unsafe { std::slice::from_raw_parts_mut(p2 as *mut u8, block_size as usize) }, a2));
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
        let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
        let f1_fd = if is_aligned { file1.0.as_raw_fd() } else { file1.1.as_raw_fd() };
        let f2_fd = if is_aligned { file2.0.as_raw_fd() } else { file2.1.as_raw_fd() };
        unsafe {
            let mut sqe1 = io_uring.prepare_sqe().unwrap();
            sqe1.prep_read(f1_fd, &mut buffers1[idx].0[..len as usize], next_offset);
            sqe1.set_user_data((idx as u64) | (1u64 << 32));

            let mut sqe2 = io_uring.prepare_sqe().unwrap();
            sqe2.prep_read(f2_fd, &mut buffers2[idx].0[..len as usize], next_offset);
            sqe2.set_user_data((idx as u64) | (2u64 << 32));
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
        let file_num = (ud >> 32) as u8;
        cq.result().expect("IO failed");
        inflight -= 1;
        if file_num == 1 { ready1[idx] = true; } else { ready2[idx] = true; }

        if ready1[idx] && ready2[idx] {
            let off = buffer_offsets[idx];
            let len = (total_size - off).min(block_size) as usize;
            
            // 1. Submit next read immediately using a DIFFERENT free buffer if available
            // Wait, we need a free buffer. We can just use one from the free list.
            if next_offset < total_size && mismatch.load(Ordering::Relaxed) == 0 {
                if let Some(next_idx) = free_buffers.pop() {
                    buffer_offsets[next_idx] = next_offset;
                    let next_len = (total_size - next_offset).min(block_size);
                    let is_aligned = (next_offset % 4096 == 0) && (next_len == block_size);
                    let f1_fd = if is_aligned { file1.0.as_raw_fd() } else { file1.1.as_raw_fd() };
                    let f2_fd = if is_aligned { file2.0.as_raw_fd() } else { file2.1.as_raw_fd() };
                    unsafe {
                        let mut sqe1 = io_uring.prepare_sqe().unwrap();
                        sqe1.prep_read(f1_fd, &mut buffers1[next_idx].0[..next_len as usize], next_offset);
                        sqe1.set_user_data((next_idx as u64) | (1u64 << 32));

                        let mut sqe2 = io_uring.prepare_sqe().unwrap();
                        sqe2.prep_read(f2_fd, &mut buffers2[next_idx].0[..next_len as usize], next_offset);
                        sqe2.set_user_data((next_idx as u64) | (2u64 << 32));
                    }
                    io_uring.submit_sqes().unwrap();
                    next_offset += num_threads * block_size;
                    inflight += 2;
                }
            }
            
            // 2. Perform comparison
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
            
            processed_count.fetch_add(len as u64, Ordering::SeqCst);
            ready1[idx] = false;
            ready2[idx] = false;
            free_buffers.push(idx); // Free the buffer after comparison

            // If we didn't submit a read earlier due to free_buffers being empty, we can submit one now
            if next_offset < total_size && mismatch.load(Ordering::Relaxed) == 0 && (inflight / 2) < qd {
                if let Some(next_idx) = free_buffers.pop() {
                    buffer_offsets[next_idx] = next_offset;
                    let next_len = (total_size - next_offset).min(block_size);
                    let is_aligned = (next_offset % 4096 == 0) && (next_len == block_size);
                    let f1_fd = if is_aligned { file1.0.as_raw_fd() } else { file1.1.as_raw_fd() };
                    let f2_fd = if is_aligned { file2.0.as_raw_fd() } else { file2.1.as_raw_fd() };
                    unsafe {
                        let mut sqe1 = io_uring.prepare_sqe().unwrap();
                        sqe1.prep_read(f1_fd, &mut buffers1[next_idx].0[..next_len as usize], next_offset);
                        sqe1.set_user_data((next_idx as u64) | (1u64 << 32));

                        let mut sqe2 = io_uring.prepare_sqe().unwrap();
                        sqe2.prep_read(f2_fd, &mut buffers2[next_idx].0[..next_len as usize], next_offset);
                        sqe2.set_user_data((next_idx as u64) | (2u64 << 32));
                    }
                    io_uring.submit_sqes().unwrap();
                    next_offset += num_threads * block_size;
                    inflight += 2;
                }
            }
        }
    }
}

fn diff_files(file1: &str, file2: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool, verbose: bool, bench_only: bool) -> u64 {
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
            let f1_dir = if direct_io { OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&f1_name).unwrap() } else { File::open(&f1_name).unwrap() };
            let f2_nodir = File::open(&f2_name).unwrap();
            let f2_dir = if direct_io { OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&f2_name).unwrap() } else { File::open(&f2_name).unwrap() };
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_differ(thread_id, (&f1_dir, &f1_nodir), (&f2_dir, &f2_nodir), num_threads, block_size, qd, &mut io_uring, s1, mismatch, processed_count, bench_only);
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

fn read_file(pattern: &str, filename: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool) -> u64 {
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let pattern = pattern.to_string();
        threads.push(std::thread::spawn(move || {
            let mut file = if direct_io { OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&filename).unwrap() }
            else { File::open(&filename).unwrap() };
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_reader(thread_id, pattern, num_threads, block_size, qd, &mut file, &mut io_uring, read_count)
        }));
    }
    let mut all_matches = Vec::new();
    for thread in threads { all_matches.extend(thread.join().unwrap()); }
    if !pattern.is_empty() {
        all_matches.sort();
        for m in all_matches { println!("{}:{}", m, pattern); }
    }
    read_count.load(Ordering::SeqCst)
}

fn write_file(source: Option<&str>, filename: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool) -> u64 {
    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let random_block = if source.is_none() {
        let mut block = vec![0u8; block_size as usize];
        rand::thread_rng().fill(&mut block[..]);
        Some(Arc::new(block))
    } else { None };
    let total_size = if let Some(s) = source {
        let mut f = File::open(s).unwrap();
        f.seek(SeekFrom::End(0)).unwrap()
    } else {
        let mut f = File::open(filename).unwrap();
        f.seek(SeekFrom::End(0)).unwrap()
    };
    {
        let f = OpenOptions::new().write(true).create(true).open(filename).unwrap();
        f.set_len(total_size).unwrap();
    }
    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source = source.map(|s| s.to_string());
        let random_block = random_block.clone();
        threads.push(std::thread::spawn(move || {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename).unwrap();
            let dest_file_dir = if direct_io { OpenOptions::new().write(true).custom_flags(libc::O_DIRECT).open(&filename).unwrap() } else { OpenOptions::new().write(true).open(&filename).unwrap() };
            let source_files = source.map(|s| {
                let s_nodir = File::open(&s).unwrap();
                let s_dir = if direct_io { OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&s).unwrap() } else { File::open(&s).unwrap() };
                (s_dir, s_nodir)
            });
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_writer(thread_id, source_files.as_ref().map(|(d, n)| (d, n)), (&dest_file_dir, &dest_file_nodir), num_threads, block_size, qd, &mut io_uring, write_count, random_block.as_ref().map(|b| &b[..]), total_size);
        }));
    }
    for thread in threads { thread.join().unwrap(); }
    write_count.load(Ordering::SeqCst)
}

fn run_optimizer<F>(
    name: &str,
    start_params: Vec<u64>,
    param_scaling_factors: Vec<u64>,
    num_iterations: usize,
    verbose: bool,
    mut op: F
) where F: FnMut(&[u64]) -> u64 {
    let mut rng = rand::thread_rng();
    let mut fastest_time = 1e9;
    let mut fastest_time_decayed = fastest_time;
    let mut optimize_params = start_params.clone();
    let mut best_params = optimize_params.clone();
    let mut iterations_since_last_fastest_found = 0;
    for _i in 0..num_iterations {
        let start = std::time::Instant::now();
        iterations_since_last_fastest_found += 1;
        fastest_time_decayed *= 1.0005;
        let mut scaled_params = vec![0u64; optimize_params.len()];
        for j in 0..optimize_params.len() {
            if num_iterations > 1 {
                let jump_multiplier = (rng.gen::<f64>().powf(2.0) * (iterations_since_last_fastest_found as f64 / 4.0).log2() + 1.0) as u64;
                let r = rng.gen::<u64>();
                if r < u64::MAX / 3 { optimize_params[j] += jump_multiplier; }
                else if r < u64::MAX / 3 * 2 { optimize_params[j] = optimize_params[j].saturating_sub(jump_multiplier).max(1); }
                else { optimize_params[j] = best_params[j]; }
            }
            scaled_params[j] = optimize_params[j] * param_scaling_factors[j];
        }
        let count = op(&scaled_params);
        let cpu_time_used = start.elapsed().as_secs_f64();
        if cpu_time_used < fastest_time_decayed {
            fastest_time_decayed = cpu_time_used;
            best_params = optimize_params.clone();
            iterations_since_last_fastest_found = 0;
        }
        if cpu_time_used < fastest_time || num_iterations == 1 {
            if cpu_time_used < fastest_time { fastest_time = cpu_time_used; }
            if verbose { eprintln!("{} {} bytes in {:.4} s, {:.1} GB/s, t={} bs={}, qd={}", name, count, cpu_time_used, count as f64 / cpu_time_used / 1e9, scaled_params[0], scaled_params[1] / 1024, scaled_params[2]); }
            else if num_iterations > 1 { println!("{} {} bytes in {:.4} s, {:.1} GB/s, t={} bs={}, qd={}", name, count, cpu_time_used, count as f64 / cpu_time_used / 1e9, scaled_params[0], scaled_params[1] / 1024, scaled_params[2]); }
        }
    }
}

fn bench_diff_memory(num_threads: usize, block_size: usize) {
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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args[1] == "--help" {
        println!("USAGE: {} grep [--direct] [-v] [-n iterations] <pattern> <filename>", args[0]);
        println!("USAGE: {} read [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} write [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} copy [--direct] [-v] [-n iterations] <source> <target>", args[0]);
        println!("USAGE: {} diff [--direct] [-v] <file1> <file2>", args[0]);
        println!("USAGE: {} dual-read-bench [--direct] [-v] <file1> <file2>", args[0]);
        println!("USAGE: {} bench-diff", args[0]);
        return;
    }
    let mode = args[1].as_str();
    let mut direct_io = false;
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut _filename2 = "";
    let mut iterations = if mode == "read" || mode == "grep" { 1000 } else { 1 };
    if mode == "grep" || mode == "copy" { iterations = 1; }
    let mut i = 2;
    while i < args.len() {
        if args[i] == "--direct" { direct_io = true; }
        else if args[i] == "-v" || args[i] == "--verbose" { verbose = true; }
        else if args[i] == "-n" {
            i += 1;
            if i < args.len() { iterations = args[i].parse().expect("Invalid number of iterations"); }
        } else if mode == "copy" || mode == "diff" || mode == "dual-read-bench" {
            if source.is_none() { source = Some(args[i].as_str()); }
            else if filename == "" { filename = args[i].as_str(); }
            else { _filename2 = args[i].as_str(); }
        } else if mode == "grep" {
            if pattern == "" { pattern = args[i].as_str(); }
            else { filename = args[i].as_str(); }
        } else { filename = args[i].as_str(); }
        i += 1;
    }
    if mode == "bench-diff" { bench_diff_memory(16, 1024 * 1024); return; }
    if filename == "" { println!("Filename missing"); return; }
    let mut num_threads = 32;
    let mut block_size = 384 * 1024;
    let mut qd: usize = 1;
    let mut bsf = 4;
    if direct_io {
        num_threads = 16;
        block_size = 3 * 1024 * 1024;
        qd = 2;
        bsf = 256;
    }
    if mode == "grep" || mode == "read" {
        if verbose || mode == "read" { eprintln!("Opening file {} for {}", filename, mode); }
        run_optimizer(if mode == "grep" { "Grep" } else { "Read" }, vec![num_threads, block_size / bsf / 1024, qd as u64], vec![1, bsf * 1024, 1], iterations, verbose || mode == "read", |p| {
            read_file(pattern, filename, p[0], p[1], p[2] as usize, direct_io)
        });
    } else if mode == "write" || mode == "copy" {
        if verbose || mode == "write" { eprintln!("Opening file {} for {}", filename, mode); }
        run_optimizer(if mode == "copy" { "Copy" } else { "Write" }, vec![num_threads, block_size / bsf / 1024, qd as u64], vec![1, bsf * 1024, 1], iterations, verbose || mode == "write", |p| {
            write_file(source, filename, p[0], p[1], p[2] as usize, direct_io)
        });
    } else if mode == "diff" || mode == "dual-read-bench" {
        if verbose { eprintln!("Opening files for {}", mode); }
        let res = diff_files(source.unwrap(), filename, num_threads, block_size, qd, direct_io, verbose, mode == "dual-read-bench");
        if res == 0 { if verbose { eprintln!("Files are identical"); } }
        else if mode == "diff" { std::process::exit(1); }
    }
}
