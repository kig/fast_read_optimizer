use super::*;

pub fn write_file(
    filename: &str,
    create_size: Option<u64>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    return _write_file_internal(
        None,
        filename,
        create_size,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        IOMode::Direct,
        io_mode_write,
        GeneratedWritePattern::Random,
    );
}

pub fn write_generated_file(
    filename: &str,
    total_size: u64,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
    pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    _write_file_internal(
        None,
        filename,
        Some(total_size),
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        IOMode::Direct,
        io_mode_write,
        pattern,
    )
}

/*
copy (direct)                       | 6.50         | 6.00         | PASS
copy (auto, cold)                   | 6.40         | 6.00         | PASS

copy (page cache, cold)             | 1.00         | 0.50         | PASS

copy (hot cache R, direct W)        | 2.60         | 10.00        | REGRESSION
copy (auto, hot)                    | 1.40         | 10.00        | REGRESSION
*/
#[allow(dead_code)]
pub fn copy_file(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    return copy_file_with_strategy(
        source_filename,
        target_filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        CopyStrategy::Threaded,
    );
}

pub fn copy_file_with_strategy(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
) -> io::Result<u64> {
    copy_file_with_strategy_and_truncate(
        source_filename,
        target_filename,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
        true,
    )
}

pub fn copy_file_with_strategy_and_truncate(
    source_filename: &str,
    target_filename: &str,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    copy_range_threads: u64,
    copy_range_block_size: u64,
    copy_range_qd: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
    truncate_target: bool,
) -> io::Result<u64> {
    return copy_file_range_with_strategy(
        source_filename,
        target_filename,
        0,
        0,
        u64::MAX,
        truncate_target,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        copy_range_threads,
        copy_range_block_size,
        copy_range_qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
    );
}

pub fn write_buffer(
    filename: &str,
    data: &[u8],
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    write_buffer_range(
        filename,
        data,
        0,
        data.len(),
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode_write,
    )
}

pub fn write_buffer_range(
    filename: &str,
    data: &[u8],
    buffer_offset: usize,
    buffer_len: usize,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_write: IOMode,
) -> io::Result<u64> {
    let end = buffer_offset
        .checked_add(buffer_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer range overflows"))?;
    let slice = data.get(buffer_offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "buffer range {}..{} is out of bounds for {} bytes",
                buffer_offset,
                end,
                data.len()
            ),
        )
    })?;

    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let direct_write = io_mode_write != IOMode::PageCache;
    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };
    let total_size = slice.len() as u64;
    let source_buffer: Arc<[u8]> = Arc::from(slice);

    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            f.set_len(total_size)?;
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source_buffer = Arc::clone(&source_buffer);
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                None,
                Some(source_buffer.as_ref()),
                (&dest_file_dir, &dest_file_nodir),
                0,
                0,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                None,
                total_size,
                false,
                direct_write,
            )
        }));
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }

    Ok(write_count.load(Ordering::SeqCst))
}

fn _write_file_internal(
    source: Option<&str>,
    filename: &str,
    create_size: Option<u64>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    generated_pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    let mut threads = vec![];
    let write_count = Arc::new(AtomicU64::new(0));
    let (total_size, _file_cached) = if let Some(s) = source {
        let file_cached = Ok(true) == is_first_page_resident(s);
        let f = File::open(s)?;
        (f.metadata()?.len(), file_cached)
    } else if let Some(size) = create_size {
        (size, false)
    } else {
        let f = File::open(filename)?;
        (f.metadata()?.len(), false)
    };

    let direct_write = io_mode_write != IOMode::PageCache;
    let direct_read = io_mode_read == IOMode::Direct || io_mode_read == IOMode::Auto;

    let num_threads = if direct_write {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if direct_write {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if direct_write { qd_d } else { qd_p };

    // println!("direct-read: {} | direct-write: {} | t={} bs={} qd={}", direct_read, direct_write, num_threads, block_size / 1024, qd);

    let random_block = if source.is_none() {
        let mut block = vec![0u8; block_size as usize];
        if generated_pattern == GeneratedWritePattern::Random {
            rand::rng().fill(&mut block[..]);
        }
        Some(Arc::new(block))
    } else {
        None
    };

    // Ensure target file exists and has the correct size.
    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            f.set_len(total_size)?;
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source = source.map(|s| s.to_string());
        let random_block = random_block.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let source_files = source
                .map(|s| -> io::Result<(File, File)> {
                    let s_nodir = File::open(&s)?;
                    let s_dir = open_direct_reader_or_fallback(&s, &s_nodir)?;
                    Ok((s_dir, s_nodir))
                })
                .transpose()?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                source_files.as_ref().map(|(d, n)| (d, n)),
                None,
                (&dest_file_dir, &dest_file_nodir),
                0,
                0,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                random_block.as_ref().map(|b| &b[..]),
                total_size,
                direct_read,
                direct_write,
            )
        }));
    }
    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("write worker thread panicked"))??;
    }
    Ok(write_count.load(Ordering::SeqCst))
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
    unsafe {
        libc::posix_fallocate(f.as_raw_fd(), 0, size as i64);
    }
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

    let num_threads = 16;
    let start = std::time::Instant::now();

    let mut threads = vec![];
    let chunk_size = size / num_threads;

    for t in 0..num_threads {
        let thread_ptr_addr = unsafe { ptr.add(t * chunk_size) as usize };
        threads.push(std::thread::spawn(move || {
            let slice =
                unsafe { std::slice::from_raw_parts_mut(thread_ptr_addr as *mut u8, chunk_size) };
            let mut rng = rand::rng();
            let mut block = vec![0u8; 1024 * 1024];
            rng.fill(&mut block[..]);

            for i in 0..(chunk_size / block.len()) {
                slice[i * block.len()..(i + 1) * block.len()].copy_from_slice(&block);
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    // Ensure data is written to disk
    unsafe {
        libc::msync(ptr, size, libc::MS_SYNC);
    }

    let dur = start.elapsed().as_secs_f64();
    println!(
        "Parallel Mmap write 1 GB in {:.4} s, {:.1} GB/s",
        dur,
        1.0 / dur
    );

    unsafe {
        libc::munmap(ptr, size);
    }
}

pub fn bench_write(filename: &str) {
    let size = 1024 * 1024 * 1024; // 1 GB
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();
    unsafe {
        libc::posix_fallocate(f.as_raw_fd(), 0, size as i64);
    }

    let mut block = vec![0u8; 1024 * 1024];
    rand::rng().fill(&mut block[..]);

    let start = std::time::Instant::now();

    for _ in 0..(size / block.len()) {
        f.write_all(&block).unwrap();
    }

    f.sync_all().unwrap();

    let dur = start.elapsed().as_secs_f64();
    println!("Standard write 1 GB in {:.4} s, {:.1} GB/s", dur, 1.0 / dur);
}

