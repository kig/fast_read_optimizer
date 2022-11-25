/*
    We're using io_uring for this.
*/
use iou::{IoUring};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{Ordering, AtomicU64};
use std::sync::Arc;
use rand::Rng;
use std::str;
use memmem::{Searcher, TwoWaySearcher};

// The thread reader function.
// This is the function that is run by each thread.
// It reads the file in block_size chunks, starting at thread_id*block_size offset.
// The chunks are spaced num_threads*block_size apart.
fn thread_reader(thread_id: u64, pattern: String, num_threads: u64, block_size: u64, qd: usize, file: &mut File, io_uring: &mut IoUring, read_count: Arc<AtomicU64>) {
    // Allocate buffers for the data.
    let mut buffers = Vec::new();
    for _ in 0..qd {
        let mut allocation = vec![0u8; (block_size as usize) + 8192];
        let data_ptr = allocation.as_mut_ptr();
        let data_ptr = data_ptr as usize;
        let data_ptr = (data_ptr+4096) & !4095;
        let data_ptr = data_ptr as *mut u8;
        let buffer = unsafe { std::slice::from_raw_parts_mut(data_ptr, (block_size as usize) + if pattern.len() > 0 { 512 } else { 0 }) };
        buffers.push((buffer, allocation));
    }
    // Get the file size.
    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    // Reset the file position.
    file.seek(SeekFrom::Start(0)).unwrap();
    // The current offset.
    let offset = thread_id*block_size;
    // The current block number.
    let mut block_num = 0;

    let mut inflight: u64 = 0;

    // Queue the first qd reads.
    // After a read completes, replace it with the next read and update read_count.
    // When we reach EOF, stop queuing reads and wait for remaining reads to complete, and return.
    // This allows us to read the entire file in parallel.
    for _ in 0..qd {
        unsafe {
            let mut sqe = io_uring.prepare_sqe().unwrap();
            sqe.prep_read(file.as_raw_fd(), &mut buffers[block_num % qd].0[..], offset + (block_num as u64)*num_threads*block_size);
            sqe.set_user_data(block_num as u64);
            io_uring.submit_sqes().unwrap();
        }
        block_num += 1;
        inflight += 1;
    }

    let search = TwoWaySearcher::new(&pattern.as_bytes());

    loop {
        // Wait for a read to complete.
        let cq = io_uring.wait_for_cqe().unwrap();
        // Get the block number for the read that completed.
        let block_id = cq.user_data() as u64;
        // Update the read count.
        read_count.fetch_add(cq.result().unwrap() as u64, Ordering::SeqCst);
        if pattern.len() > 0 {
            let buf = &buffers[block_id as usize % qd].0;
            let mut search_idx = 0;
            while search_idx < (block_size as usize) + pattern.len() - 1 {
                match search.search_in(&buf[search_idx..]) {
                    Some(idx) => {
                        println!("Found pattern at {}", offset + (block_id as u64)*num_threads*block_size + search_idx as u64 + idx as u64);
                        search_idx += idx + pattern.len();
                    },
                    None => search_idx = (block_size as usize) + pattern.len(),
                }
            }
        }
            
        // If we're not at the end of the file, queue the next read.
        inflight -= 1;
        if offset + (block_num as u64)*num_threads*block_size < file_size {
            unsafe {
                let mut sqe = io_uring.prepare_sqe().unwrap();
                sqe.prep_read(file.as_raw_fd(), &mut buffers[block_num % qd].0[..], offset + (block_num as u64)*num_threads*block_size);
                sqe.set_user_data(block_num as u64);
                io_uring.submit_sqes().unwrap();
            }
            block_num += 1;
            inflight += 1;
        }
        // If we're at the end of the file, and all reads have completed, return.
        if offset + (block_num as u64)*num_threads*block_size >= file_size && inflight == 0 {
            return;
        }
    }
}

// Read through the entire file with num_threads threads, each 
// reading block_size bytes at a time in an interleaved fashion.
// E.g.
// thread 0 reads block_size bytes at locations i*num_threads*block_size + 0*block_size
// thread 1 reads block_size bytes at locations i*num_threads*block_size + 1*block_size
// thread 2 reads block_size bytes at locations i*num_threads*block_size + 2*block_size
// etc.
//
// The function returns the total number of bytes read by all the threads.
//
fn read_file(pattern: &str, filename: &str, num_threads: u64, block_size: u64, qd: usize, direct_io: bool) -> u64 {
    // Create num_threads threads and a shared read count.
    // Call thread_reader on each thread.
    // Join the threads after they're done and return the read count.
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let mut file = if direct_io {
            OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(filename).unwrap()
        } else {
            File::open(filename).unwrap()
        };
        let patt = format!("{}", &pattern);
        let mut io_uring = IoUring::new(1024).unwrap();
        threads.push(std::thread::spawn(move || {
            thread_reader(thread_id, patt, num_threads, block_size, qd, &mut file, &mut io_uring, read_count);
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    return read_count.load(Ordering::SeqCst);
}

// Use a stochastic hill climber to find the best-performing parameters for the read_file function.
// Takes in the filename and direct_io as constant arguments,
// and an array of params to optimize and an array of scaling factors to convert them to use with read_file.
// The optimizer nudges the parameters by a small integer amount (1, -1, 2, etc.), and the scaling factors are
// used to go from block_size = 3 -> block_size = 3 * 128 * 1024
fn run_optimizer(pattern: &str, filename: &str, direct_io: bool, start_params: Vec<u64>, param_scaling_factors: Vec<u64>) {
    let mut rng = rand::thread_rng();
    let mut fastest_time = 1e9;
    let mut fastest_time_decayed = fastest_time;
    let mut optimize_params = start_params.clone();
    let mut best_params = optimize_params.clone();
    let mut iterations_since_last_fastest_found = 0;

    for _ in 0..1000 {
        let start = std::time::Instant::now();

        iterations_since_last_fastest_found += 1;

        fastest_time_decayed *= 1.0005;

        let mut scaled_params = optimize_params.clone();

        for j in 0..optimize_params.len() {
            let jump_multiplier = (rng.gen::<f64>().powf(2.0) * (iterations_since_last_fastest_found as f64 / 4.0).log2() + 1.0) as u64;

            let r = rng.gen::<u64>();
            if r < u64::MAX / 3 {
                optimize_params[j] += jump_multiplier;
            } else if r < u64::MAX / 3 * 2 {
                if optimize_params[j] <= jump_multiplier {
                    optimize_params[j] = 1;
                } else {
                    optimize_params[j] -= jump_multiplier;
                }
            } else {
                optimize_params[j] = best_params[j];
            }
            scaled_params[j] = optimize_params[j] * param_scaling_factors[j];
        }

        let read_count = read_file(pattern, filename, scaled_params[0], scaled_params[1], scaled_params[2] as usize, direct_io);

        let cpu_time_used = start.elapsed().as_secs_f64();

        if cpu_time_used < fastest_time_decayed {
            fastest_time_decayed = cpu_time_used;
            best_params = optimize_params.clone();
            iterations_since_last_fastest_found = 0;
        }

        if cpu_time_used < fastest_time {
            fastest_time = cpu_time_used;
            println!("Read {} bytes in {} s, {:.1} GB/s, t={} bs={}, qd={}", 
                read_count, cpu_time_used, read_count as f64 / cpu_time_used / 1e9, 
                scaled_params[0],
                scaled_params[1] / 1024, // Nicer to report block size in kiB
                scaled_params[2]);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args[1] == "--help" {
        println!("USAGE: {} [--direct] PATTERN FILENAME", args[0]);
        return;
    }

    let mut num_threads = 32;
    let mut block_size = 384*1024;
    let mut qd = 1;
    let mut direct_io = false;
    let mut bsf = 4;

    if args[1] == "--direct" {
        direct_io = true;
        num_threads = 16;
        block_size = 3*1024*1024;
        qd = 2;
        bsf = 256;
    }

    let pattern = if args.len() < 3 { "" } else { &args[args.len()-2] };

    if pattern.len() > 512 {
        panic!("Patterns longer than 512 bytes are not supported.");
    }

    let filename = &args[args.len()-1];

    println!("Opening file {} for reading", filename);

    let mut fp = File::open(filename).unwrap();
    let fsize = fp.seek(SeekFrom::End(0)).unwrap();
    println!("Reading {} bytes", fsize);

    run_optimizer(pattern, filename, direct_io, vec![num_threads, block_size / bsf / 1024, qd], vec![1, bsf * 1024, 1]);
}

