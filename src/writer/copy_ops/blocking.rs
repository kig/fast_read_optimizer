use super::*;

pub(super) fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let read = file.read_at(&mut buf[filled..], offset + filled as u64)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "short read at offset {}: expected {} bytes, got {}",
                    offset,
                    buf.len(),
                    filled
                ),
            ));
        }
        filled += read;
    }
    Ok(())
}

fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0usize;
    while written < buf.len() {
        let count = file.write_at(&buf[written..], offset + written as u64)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "short write at offset {}: expected {} bytes, got {}",
                    offset,
                    buf.len(),
                    written
                ),
            ));
        }
        written += count;
    }
    Ok(())
}

pub(crate) fn copy_file_range_blocking_with_progress(
    source: &str,
    filename: &str,
    source_offset: u64,
    dest_offset: u64,
    copy_size: u64,
    truncate_target: bool,
    block_size_p: u64,
    block_size_d: u64,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    progress_count: Option<Arc<AtomicU64>>,
) -> io::Result<u64> {
    let source_file = File::open(source)?;
    let source_meta = source_file.metadata()?;
    if !source_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "blocking copy fallback requires a regular-file source",
        ));
    }

    let copy_size = copy_size.min(source_meta.len().saturating_sub(source_offset));
    prepare_copy_destination(filename, dest_offset, copy_size, truncate_target)?;
    let target_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;
    let target_meta = target_file.metadata()?;
    if !target_meta.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "blocking copy fallback requires a regular-file destination",
        ));
    }

    let source_direct = open_direct_reader_or_fallback(source, &source_file)?;
    let target_direct = open_direct_writer_or_fallback(filename, &target_file)?;
    let direct_read = matches!(io_mode_read, IOMode::Direct | IOMode::Auto);
    let direct_write = io_mode_write != IOMode::PageCache;
    let block_size = if direct_write {
        block_size_d.max(4096)
    } else {
        block_size_p.max(4096)
    };
    let mut buffer = AlignedBuffer::new(block_size as usize);
    let mut copied = 0u64;

    while copied < copy_size {
        let chunk_len = (copy_size - copied).min(block_size) as usize;
        let src_offset = source_offset + copied;
        let dst_offset = dest_offset + copied;
        let aligned_read = (src_offset % 4096 == 0) && (chunk_len as u64 == block_size);
        if direct_read && !aligned_read {
            note_direct_unaligned_fallback("copy-read", src_offset, chunk_len);
        }
        let read_file = if direct_read && aligned_read {
            &source_direct
        } else {
            &source_file
        };
        read_full_at(
            read_file,
            &mut buffer.as_mut_slice()[..chunk_len],
            src_offset,
        )?;

        let aligned_write = (dst_offset % 4096 == 0) && (chunk_len as u64 == block_size);
        if direct_write && !aligned_write {
            note_direct_unaligned_fallback("write", dst_offset, chunk_len);
        }
        let write_file = if direct_write && aligned_write {
            &target_direct
        } else {
            &target_file
        };
        write_all_at(write_file, &buffer.as_slice()[..chunk_len], dst_offset)?;

        copied += chunk_len as u64;
        if let Some(progress_count) = progress_count.as_ref() {
            progress_count.fetch_add(chunk_len as u64, Ordering::SeqCst);
        }
    }

    Ok(copied)
}

