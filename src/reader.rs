use iou::IoUring;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;
use crate::common::{AlignedBuffer, MmapCache, ACTIVE_PAGE_CACHE_READS};

// The thread reader function.
fn thread_reader(
    thread_id: u64,
    pattern: String,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: (&File, &File), // (dir, nodir)
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    force_direct: bool,
    no_direct: bool,
    mmap_cache: Arc<MmapCache>,
) -> Vec<u64> {
    let mut matches = Vec::new();
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.1.metadata().unwrap().len();
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = offset + (block_num as u64) * num_threads * block_size;
        if current_offset >= file_size { break; }
        
        let len = (file_size - current_offset).min(block_size);
        let is_hot = if no_direct {
            true
        } else if force_direct {
            false
        } else {
            mmap_cache.is_range_in_page_cache(current_offset, len as usize)
        };

        let use_direct = if no_direct {
            false
        } else if force_direct {
            true
        } else {
            !is_hot
        };

        if is_hot {
            // BLOCK until we have a page cache slot
            while ACTIVE_PAGE_CACHE_READS.fetch_add(1, Ordering::SeqCst) >= 4 {
                ACTIVE_PAGE_CACHE_READS.fetch_sub(1, Ordering::SeqCst);
                std::thread::yield_now();
            }
        }

        let is_aligned = (current_offset % 4096 == 0) && (len == block_size);
        let fd = if use_direct && is_aligned { file.0.as_raw_fd() } else { file.1.as_raw_fd() };

        unsafe {
            let mut sqe = io_uring.prepare_sqe().unwrap();
            sqe.prep_read(fd, buffers[block_num % qd].as_mut_slice(), current_offset);
            // User data: 32 bits index, 8 bits was_page_cache
            let was_page_cache = if is_hot { 1u64 } else { 0u64 };
            sqe.set_user_data((block_num as u64) | (was_page_cache << 40));
        }
        block_num += 1;
        inflight += 1;
    }
    
    if inflight == 0 { return matches; }
    io_uring.submit_sqes().unwrap();

    let finder = Finder::new(pattern.as_bytes());

    loop {
        let cq = io_uring.wait_for_cqe().unwrap();
        let mut ready = vec![(cq.user_data(), cq.result().unwrap())];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().unwrap();
            ready.push((cq.user_data(), cq.result().unwrap()));
        }

        for (ud, result) in ready {
            let block_id = ud & 0xFFFFFFFF;
            let was_page_cache = (ud >> 40) != 0;
            if was_page_cache {
                ACTIVE_PAGE_CACHE_READS.fetch_sub(1, Ordering::SeqCst);
            }

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
                let len = (file_size - next_offset).min(block_size);
                let is_hot = if no_direct {
                    true
                } else if force_direct {
                    false
                } else {
                    mmap_cache.is_range_in_page_cache(next_offset, len as usize)
                };

                let use_direct = if no_direct {
                    false
                } else if force_direct {
                    true
                } else {
                    !is_hot
                };

                if is_hot {
                    while ACTIVE_PAGE_CACHE_READS.fetch_add(1, Ordering::SeqCst) >= 4 {
                        ACTIVE_PAGE_CACHE_READS.fetch_sub(1, Ordering::SeqCst);
                        std::thread::yield_now();
                    }
                }

                let is_aligned = (next_offset % 4096 == 0) && (len == block_size);
                let fd = if use_direct && is_aligned { file.0.as_raw_fd() } else { file.1.as_raw_fd() };

                unsafe {
                    let mut sqe = io_uring.prepare_sqe().unwrap();
                    sqe.prep_read(fd, buffers[block_num % qd].as_mut_slice(), next_offset);
                    let was_page_cache = if is_hot { 1u64 } else { 0u64 };
                    sqe.set_user_data((block_num as u64) | (was_page_cache << 40));
                }
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().unwrap();
        if inflight == 0 { return matches; }
    }
}

pub fn read_file(pattern: &str, filename: &str, num_threads: u64, block_size: u64, qd: usize, force_direct: bool, no_direct: bool) -> u64 {
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));
    let file_for_mmap = File::open(filename).unwrap();
    let mmap_cache = Arc::new(MmapCache::new(&file_for_mmap));

    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let pattern = pattern.to_string();
        let mmap_cache = mmap_cache.clone();
        threads.push(std::thread::spawn(move || {
            let file_nodir = File::open(&filename).unwrap();
            let file_dir = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&filename).unwrap();
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_reader(thread_id, pattern, num_threads, block_size, qd, (&file_dir, &file_nodir), &mut io_uring, read_count, force_direct, no_direct, mmap_cache)
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
