use iou::IoUring;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;
use crate::common::{AlignedBuffer, IOMode};
use crate::mincore::{is_first_page_resident};

// The thread reader function.
fn thread_reader(
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
        let direct = use_direct && (current_offset % 4096 == 0) && ((current_offset + (buffers[0].as_slice() .len() as u64)) <= file_size);
        unsafe {
            let mut sqe = io_uring.prepare_sqe().unwrap();
            if direct {
                sqe.prep_read(file_direct.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), current_offset);
            } else {
                sqe.prep_read(file.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), current_offset);
            }
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
                    let buf = &buffers[block_id as usize % qd].as_slice()[..std::cmp::min(result as usize, (block_size as usize)+pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(offset + block_id * num_threads * block_size + idx as u64);
                    }
                }
            }
            inflight -= 1;
            
            let next_offset = offset + (block_num as u64) * num_threads * block_size;
            if next_offset < file_size {
                let direct = use_direct && (next_offset % 4096 == 0) && (next_offset + (buffers[0].as_slice().len() as u64) <= file_size);
                unsafe {
                    let mut sqe = io_uring.prepare_sqe().unwrap();
                    if direct {
                        sqe.prep_read(file_direct.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), next_offset);
                    } else {
                        sqe.prep_read(file.as_raw_fd(), buffers[block_num % qd].as_mut_slice(), next_offset);
                    }
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

pub fn read_file(
    pattern: &str, filename: &str,
    num_threads_p: u64, block_size_p: u64, qd_p: usize, 
    num_threads_d: u64, block_size_d: u64, qd_d: usize, 
    io_mode: IOMode
) -> u64 {
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));

    let file_cached = match is_first_page_resident(filename) { 
        Ok(true) => true && io_mode != IOMode::Direct, 
        _ => false || io_mode == IOMode::PageCache
    };
    let use_direct = (!file_cached) || io_mode == IOMode::Direct;

    let num_threads = if use_direct { num_threads_d } else { num_threads_p };
    let block_size = if use_direct { block_size_d } else { block_size_p };
    let qd = if use_direct { qd_d } else { qd_p };
    
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let pattern = pattern.to_string();
        threads.push(std::thread::spawn(move || {
            let mut file = File::open(&filename).unwrap();
            let mut file_direct = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&filename).unwrap();
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_reader(thread_id, pattern, num_threads, block_size, qd, &mut file, &mut file_direct, &mut io_uring, read_count, use_direct)
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
