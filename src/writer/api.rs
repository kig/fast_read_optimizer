use super::*;
use crate::io_util::checked_posix_fallocate;
use crate::uring_util::io_uring_available;
use std::os::unix::fs::FileExt;

const SINGLE_DIRECT_WRITE_LIMIT: u64 = 512 * 1024;
const ZFS_SERIAL_DIRECT_WRITE_LIMIT: u64 = 16 << 20;
const EXT4_SERIAL_DIRECT_WRITE_LIMIT: u64 = 80 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GeneratedWriteStrategy {
    SingleDirect,
    SerialDirect,
}

#[derive(Clone, Copy)]
struct SharedWriteBuffer {
    ptr: *const u8,
    len: usize,
}

// SAFETY: `SharedWriteBuffer` only provides shared read-only access to caller-owned bytes that
// remain alive until all worker threads join.
unsafe impl Send for SharedWriteBuffer {}
// SAFETY: Sharing the descriptor across threads is sound because it never yields mutable access.
unsafe impl Sync for SharedWriteBuffer {}

impl SharedWriteBuffer {
    fn new(data: &[u8]) -> Self {
        Self {
            ptr: data.as_ptr(),
            len: data.len(),
        }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr`/`len` come from a live `&[u8]` in `new` and are only used while that
        // backing slice is kept alive by the caller.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn align_up_4096(value: u64) -> io::Result<u64> {
    value
        .checked_add(4095)
        .map(|v| v / 4096 * 4096)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "aligned write size overflowed"))
}

fn write_all_at(file: &File, offset: u64, data: &[u8]) -> io::Result<()> {
    let mut written = 0usize;
    while written < data.len() {
        let count = file.write_at(&data[written..], offset + written as u64)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short direct write",
            ));
        }
        written += count;
    }
    Ok(())
}

fn read_full_at(file: &File, offset: u64, data: &mut [u8]) -> io::Result<()> {
    let mut read_total = 0usize;
    while read_total < data.len() {
        let count = file.read_at(&mut data[read_total..], offset + read_total as u64)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short blocking copy read",
            ));
        }
        read_total += count;
    }
    Ok(())
}

fn write_buffer_range_blocking(
    filename: &str,
    data: &[u8],
    block_size: u64,
    direct_write: bool,
) -> io::Result<u64> {
    let total_size = data.len() as u64;
    let chunk_size = block_size.max(4096);
    let (page_cache_file, direct_file, _) = open_writer_files(filename, true, Some(total_size))?;
    let mut buffer = AlignedBuffer::new(chunk_size as usize);
    let mut written = 0u64;

    while written < total_size {
        let chunk_len = (total_size - written).min(chunk_size) as usize;
        buffer.as_mut_slice()[..chunk_len]
            .copy_from_slice(&data[written as usize..written as usize + chunk_len]);
        let aligned_write = (written % 4096 == 0) && (chunk_len as u64 == chunk_size);
        if direct_write && !aligned_write {
            note_direct_unaligned_fallback("write", written, chunk_len);
        }
        let file = if direct_write && aligned_write {
            &direct_file
        } else {
            &page_cache_file
        };
        write_all_at(file, written, &buffer.as_slice()[..chunk_len])?;
        written += chunk_len as u64;
    }

    Ok(total_size)
}

fn write_file_blocking(
    source: Option<&str>,
    filename: &str,
    total_size: u64,
    block_size: u64,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    generated_pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    let chunk_size = block_size.max(4096);
    let (dest_page_cache, dest_direct, _) = open_writer_files(filename, true, Some(total_size))?;
    let direct_write = io_mode_write != IOMode::PageCache;
    let direct_read = matches!(io_mode_read, IOMode::Direct | IOMode::Auto);
    let mut buffer = AlignedBuffer::new(chunk_size as usize);
    let source_files = source
        .map(|path| -> io::Result<(File, File)> {
            let page_cache = File::open(path)?;
            let direct = open_direct_reader_or_fallback(path, &page_cache)?;
            Ok((page_cache, direct))
        })
        .transpose()?;
    let mut written = 0u64;

    while written < total_size {
        let chunk_len = (total_size - written).min(chunk_size) as usize;
        if let Some((source_page_cache, source_direct)) = source_files.as_ref() {
            let aligned_read = (written % 4096 == 0) && (chunk_len as u64 == chunk_size);
            if direct_read && !aligned_read {
                note_direct_unaligned_fallback("copy-read", written, chunk_len);
            }
            let source_file = if direct_read && aligned_read {
                source_direct
            } else {
                source_page_cache
            };
            read_full_at(
                source_file,
                written,
                &mut buffer.as_mut_slice()[..chunk_len],
            )?;
        } else {
            fill_generated_pattern(&mut buffer.as_mut_slice()[..chunk_len], generated_pattern);
        }

        let aligned_write = (written % 4096 == 0) && (chunk_len as u64 == chunk_size);
        if direct_write && !aligned_write {
            note_direct_unaligned_fallback("write", written, chunk_len);
        }
        let dest_file = if direct_write && aligned_write {
            &dest_direct
        } else {
            &dest_page_cache
        };
        write_all_at(dest_file, written, &buffer.as_slice()[..chunk_len])?;
        written += chunk_len as u64;
    }

    Ok(total_size)
}

fn fill_generated_pattern(buffer: &mut [u8], pattern: GeneratedWritePattern) {
    match pattern {
        GeneratedWritePattern::Random => rand::rng().fill(buffer),
        GeneratedWritePattern::Zero => buffer.fill(0),
    }
}

pub(super) fn generated_write_strategy_for_fstype(
    total_size: u64,
    io_mode_write: IOMode,
    fstype: Option<&str>,
) -> Option<GeneratedWriteStrategy> {
    if io_mode_write == IOMode::PageCache || total_size == 0 {
        return None;
    }
    if total_size < SINGLE_DIRECT_WRITE_LIMIT {
        return Some(GeneratedWriteStrategy::SingleDirect);
    }
    let serial_limit = match fstype {
        Some("zfs") => ZFS_SERIAL_DIRECT_WRITE_LIMIT,
        Some("ext4") => EXT4_SERIAL_DIRECT_WRITE_LIMIT,
        _ => return None,
    };
    if total_size < serial_limit {
        Some(GeneratedWriteStrategy::SerialDirect)
    } else {
        None
    }
}

fn generated_write_strategy_for_path(
    filename: &str,
    total_size: u64,
    io_mode_write: IOMode,
) -> Option<GeneratedWriteStrategy> {
    let mount_info = crate::config::mount_info_for_path(filename);
    generated_write_strategy_for_fstype(
        total_size,
        io_mode_write,
        mount_info.as_ref().map(|info| info.fstype.as_str()),
    )
}

fn write_generated_single_direct(
    filename: &str,
    total_size: u64,
    pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    if total_size == 0 {
        let _ = open_writer_files(filename, true, Some(0))?;
        return Ok(0);
    }
    let padded_size = align_up_4096(total_size)?;
    let padded_len = usize::try_from(padded_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "single direct write size does not fit in usize",
        )
    })?;
    let (file_page_cache, file_direct, _) = open_writer_files(filename, true, Some(padded_size))?;
    let mut buffer = AlignedBuffer::new(padded_len);
    fill_generated_pattern(buffer.as_mut_slice(), pattern);
    write_all_at(&file_direct, 0, buffer.as_slice())?;
    if padded_size != total_size {
        file_page_cache.set_len(total_size)?;
    }
    Ok(total_size)
}

fn write_generated_serial_direct(
    filename: &str,
    total_size: u64,
    direct_block_size: u64,
    pattern: GeneratedWritePattern,
) -> io::Result<u64> {
    if total_size == 0 {
        let _ = open_writer_files(filename, true, Some(0))?;
        return Ok(0);
    }
    let padded_total = align_up_4096(total_size)?;
    let (file_page_cache, file_direct, _) = open_writer_files(filename, true, Some(padded_total))?;
    let chunk_size = aligned_block_size(direct_block_size as usize).max(4096) as u64;
    let mut written_data = 0u64;
    let mut file_offset = 0u64;
    while written_data < total_size {
        let data_len = (total_size - written_data).min(chunk_size);
        let write_len = align_up_4096(data_len)?;
        let write_len_usize = usize::try_from(write_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "serial direct write chunk does not fit in usize",
            )
        })?;
        let mut buffer = AlignedBuffer::new(write_len_usize);
        fill_generated_pattern(buffer.as_mut_slice(), pattern);
        write_all_at(&file_direct, file_offset, buffer.as_slice())?;
        written_data += data_len;
        file_offset += write_len;
    }
    if padded_total != total_size {
        file_page_cache.set_len(total_size)?;
    }
    Ok(total_size)
}

fn write_borrowed_buffer_single_direct(filename: &str, data: &[u8]) -> io::Result<u64> {
    if data.is_empty() {
        let _ = open_writer_files(filename, true, Some(0))?;
        return Ok(0);
    }
    let padded_size = align_up_4096(data.len() as u64)?;
    let padded_len = usize::try_from(padded_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "single direct buffered write size does not fit in usize",
        )
    })?;
    let (file_page_cache, file_direct, _) = open_writer_files(filename, true, Some(padded_size))?;
    let mut buffer = AlignedBuffer::new(padded_len);
    buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
    write_all_at(&file_direct, 0, buffer.as_slice())?;
    if padded_size != data.len() as u64 {
        file_page_cache.set_len(data.len() as u64)?;
    }
    Ok(data.len() as u64)
}

fn write_borrowed_buffer_serial_direct(
    filename: &str,
    data: &[u8],
    direct_block_size: u64,
) -> io::Result<u64> {
    if data.is_empty() {
        let _ = open_writer_files(filename, true, Some(0))?;
        return Ok(0);
    }
    let total_size = data.len() as u64;
    let padded_total = align_up_4096(total_size)?;
    let (file_page_cache, file_direct, _) = open_writer_files(filename, true, Some(padded_total))?;
    let chunk_size = aligned_block_size(direct_block_size as usize).max(4096);
    let mut offset = 0usize;
    while offset < data.len() {
        let data_len = (data.len() - offset).min(chunk_size);
        let write_len = align_up_4096(data_len as u64)?;
        let write_len_usize = usize::try_from(write_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "serial direct buffered write chunk does not fit in usize",
            )
        })?;
        let mut buffer = AlignedBuffer::new(write_len_usize);
        buffer.as_mut_slice()[..data_len].copy_from_slice(&data[offset..offset + data_len]);
        write_all_at(&file_direct, offset as u64, buffer.as_slice())?;
        offset += data_len;
    }
    if padded_total != total_size {
        file_page_cache.set_len(total_size)?;
    }
    Ok(total_size)
}

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
    let source_buffer = SharedWriteBuffer::new(slice);

    if let Some(strategy) = generated_write_strategy_for_path(filename, total_size, io_mode_write) {
        return match strategy {
            GeneratedWriteStrategy::SingleDirect => {
                write_borrowed_buffer_single_direct(filename, slice)
            }
            GeneratedWriteStrategy::SerialDirect => {
                write_borrowed_buffer_serial_direct(filename, slice, block_size_d)
            }
        };
    }

    if !io_uring_available(1024)? {
        return write_buffer_range_blocking(filename, slice, block_size, direct_write);
    }

    {
        let f = OpenOptions::new().write(true).create(true).open(filename)?;
        if f.metadata()?.file_type().is_file() {
            f.set_len(total_size)?;
            // SAFETY: `f` is a valid writable fd and `posix_fadvise` only consumes the fd/range
            // arguments to adjust kernel cache heuristics.
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
    }

    for thread_id in 0..num_threads {
        let write_count = write_count.clone();
        let filename = filename.to_string();
        let source_buffer = source_buffer;
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let dest_file_nodir = OpenOptions::new().write(true).open(&filename)?;
            let dest_file_dir = open_direct_writer_or_fallback(&filename, &dest_file_nodir)?;
            let mut io_uring = IoUring::new(1024).map_err(io::Error::other)?;
            thread_writer(
                thread_id,
                None,
                Some(source_buffer.as_slice()),
                (&dest_file_dir, &dest_file_nodir),
                0,
                0,
                num_threads,
                block_size,
                qd,
                &mut io_uring,
                write_count,
                None,
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
        let file_cached = Ok(true) == is_edge_pages_resident(s);
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

    if source.is_none() {
        if let Some(strategy) =
            generated_write_strategy_for_path(filename, total_size, io_mode_write)
        {
            return match strategy {
                GeneratedWriteStrategy::SingleDirect => {
                    write_generated_single_direct(filename, total_size, generated_pattern)
                }
                GeneratedWriteStrategy::SerialDirect => write_generated_serial_direct(
                    filename,
                    total_size,
                    block_size_d,
                    generated_pattern,
                ),
            };
        }
    }

    if !io_uring_available(1024)? {
        return write_file_blocking(
            source,
            filename,
            total_size,
            block_size,
            io_mode_read,
            io_mode_write,
            generated_pattern,
        );
    }

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
            // SAFETY: `f` is a valid writable fd and `posix_fadvise` only consumes the fd/range
            // arguments to adjust kernel cache heuristics.
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
                None,
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
    checked_posix_fallocate(
        &f,
        0,
        size as u64,
        "failed to preallocate mmap benchmark output",
    )
    .unwrap();
    let fd = f.as_raw_fd();

    // SAFETY: The file has been sized to `size`, offset 0 is page-aligned, and the returned
    // mapping stays live until the matching `munmap` at the end of the benchmark.
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
        // SAFETY: `t * chunk_size` stays within the `size`-byte mapping and each thread gets a
        // disjoint chunk because `chunk_size` evenly partitions the mapping here.
        let thread_ptr_addr = unsafe { ptr.add(t * chunk_size) as usize };
        threads.push(std::thread::spawn(move || {
            // SAFETY: `thread_ptr_addr..+chunk_size` is that thread's unique subrange of the live
            // mapping, so creating a mutable slice for it is sound.
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
    // SAFETY: `ptr..ptr+size` is the still-live mapping created above.
    unsafe {
        libc::msync(ptr, size, libc::MS_SYNC);
    }

    let dur = start.elapsed().as_secs_f64();
    println!(
        "Parallel Mmap write 1 GB in {:.4} s, {:.1} GB/s",
        dur,
        1.0 / dur
    );

    // SAFETY: `ptr..ptr+size` is the mapping created above and has not been unmapped yet.
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
    checked_posix_fallocate(
        &f,
        0,
        size as u64,
        "failed to preallocate write benchmark output",
    )
    .unwrap();

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
