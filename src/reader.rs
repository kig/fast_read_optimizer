use crate::common::{AlignedBuffer, IOMode};
use crate::mincore::is_first_page_resident;
use iou::IoUring;
use memchr::memmem::Finder;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn block_offset(thread_base: u64, block_id: u64, num_threads: u64, block_size: u64) -> u64 {
    thread_base + block_id * num_threads * block_size
}

fn should_use_direct_io(use_direct: bool, offset: u64, len: usize, file_size: u64) -> bool {
    debug_assert_eq!(
        len % 4096,
        0,
        "Direct I/O requires a 4096-byte aligned read length"
    );
    use_direct && (offset % 4096 == 0) && (len % 4096 == 0) && (offset + len as u64 <= file_size)
}

fn submit_read(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
) {
    let direct = should_use_direct_io(use_direct, offset, buffer.as_slice().len(), file_size);
    unsafe {
        let mut sqe = io_uring.prepare_sqe().unwrap();
        if direct {
            sqe.prep_read(file_direct.as_raw_fd(), buffer.as_mut_slice(), offset);
        } else {
            sqe.prep_read(file.as_raw_fd(), buffer.as_mut_slice(), offset);
        }
        sqe.set_user_data(block_id);
    }
}

fn open_reader_files(filename: &str, use_direct: bool) -> (File, File) {
    let file = File::open(filename).unwrap();
    let file_direct = if use_direct {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(filename)
            .unwrap()
    } else {
        File::open(filename).unwrap()
    };
    (file, file_direct)
}

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
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        );
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return matches;
    }
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
                    let buf = &buffers[block_id as usize % qd].as_slice()
                        [..std::cmp::min(result as usize, (block_size as usize) + pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(
                            block_offset(offset, block_id, num_threads, block_size) + idx as u64,
                        );
                    }
                }
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                );
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().unwrap();
        if inflight == 0 {
            return matches;
        }
    }
}

pub fn read_file(
    pattern: &str,
    filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> u64 {
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));

    let file_cached = match is_first_page_resident(filename) {
        Ok(true) => true && io_mode != IOMode::Direct,
        _ => false || io_mode == IOMode::PageCache,
    };
    let use_direct = (!file_cached) || io_mode == IOMode::Direct;

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

    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let pattern = pattern.to_string();
        threads.push(std::thread::spawn(move || {
            let (mut file, mut file_direct) = open_reader_files(&filename, use_direct);
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_reader(
                thread_id,
                pattern,
                num_threads,
                block_size,
                qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                use_direct,
            )
        }));
    }
    let mut all_matches = Vec::new();
    for thread in threads {
        all_matches.extend(thread.join().unwrap());
    }
    if !pattern.is_empty() {
        all_matches.sort();
        for m in all_matches {
            println!("{}:{}", m, pattern);
        }
    }
    read_count.load(Ordering::SeqCst)
}
