use iou::IoUring;
use rand::Rng;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

        if state == 1 { // Read finished
            let len = result as u64;
            let is_aligned = (buffer_offsets[idx] % 4096 == 0) && (len % 4096 == 0);
            let fd = if is_aligned { dest_file.0.as_raw_fd() } else { dest_file.1.as_raw_fd() };
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_write(fd, &buffers[idx].0[..result as usize], buffer_offsets[idx]);
                sqe.set_user_data((idx as u64) | (2u64 << 32));
            }
            io_uring.submit_sqes().unwrap();
        } else { // Write finished
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

pub fn write_file(source: Option<&str>, filename: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool) -> u64 {
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

pub fn bench_mmap_write(filename: &str) {
    let size = 1024 * 1024 * 1024; // 1 GB
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();
    f.set_len(size as u64).unwrap();
    let fd = f.as_raw_fd();

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        panic!("mmap failed");
    }

    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, size) };

    let start = std::time::Instant::now();
    
    // Fill with some data
    let mut rng = rand::thread_rng();
    let mut block = vec![0u8; 1024 * 1024];
    rng.fill(&mut block[..]);

    for i in 0..(size / block.len()) {
        slice[i * block.len()..(i + 1) * block.len()].copy_from_slice(&block);
    }

    // Ensure data is written to disk
    unsafe {
        libc::msync(ptr, size, libc::MS_SYNC);
    }

    let dur = start.elapsed().as_secs_f64();
    println!("Mmap write 1 GB in {:.4} s, {:.1} GB/s", dur, 1.0 / dur);

    unsafe {
        libc::munmap(ptr, size);
    }
}
