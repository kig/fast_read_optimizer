use iou::IoUring;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;
use crate::common::AlignedBuffer;

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
        buffers.push(AlignedBuffer::new(block_size as usize));
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
            sqe.prep_read(file.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), current_offset);
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
        let mut ready = vec![(cq.user_data() as u64, cq.result().unwrap())];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().unwrap();
            ready.push((cq.user_data() as u64, cq.result().unwrap()));
        }

        for (block_id, result) in ready {
            if result > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
                if !pattern.is_empty() {
                    let buf = buffers[block_id as usize % qd].as_slice();
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
                    sqe.prep_read(file.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), next_offset);
                    sqe.set_user_data(block_num as u64);
                }
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().unwrap();
        if inflight == 0 { return matches; }
    }
}

pub fn read_file(pattern: &str, filename: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool) -> u64 {
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
