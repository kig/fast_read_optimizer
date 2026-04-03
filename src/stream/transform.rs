use super::allocate_pipe_output_buffer;
use super::ParallelStream;
use crate::config::load_config;
use libc::{fcntl, F_SETPIPE_SZ};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::sync::mpsc;
use std::thread;

#[derive(Clone, Copy, Debug)]
pub struct ReaderTransformGeometry {
    pub read_block_size: u64,
    pub write_block_size: usize,
    pub input_chunk_multiple: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipeOutputPolicy {
    GiftedAlignedPages,
}

pub fn grow_pipe_capacity_best_effort(pipe_fd: i32, new_size: usize) {
    unsafe {
        let _res = fcntl(pipe_fd, F_SETPIPE_SZ, new_size);
    }
}

pub fn run_reader_transform_to_file<R: Read>(
    reader: &mut R,
    dest: &mut File,
    geometry: ReaderTransformGeometry,
    processor: fn(&[u8], &mut [u8]) -> io::Result<usize>,
) -> io::Result<()> {
    let mut read_buf = vec![0u8; geometry.read_block_size as usize];
    let mut write_buf = vec![0u8; geometry.write_block_size];
    let mut carry = Vec::new();
    let mut merged =
        Vec::with_capacity(geometry.read_block_size as usize + geometry.input_chunk_multiple);
    loop {
        let read = reader.read(&mut read_buf)?;
        if read == 0 {
            break;
        }
        let ready_len = if geometry.input_chunk_multiple <= 1 {
            read
        } else {
            let total = carry.len() + read;
            (total / geometry.input_chunk_multiple) * geometry.input_chunk_multiple
        };
        if ready_len == 0 {
            carry.extend_from_slice(&read_buf[..read]);
            continue;
        }
        let produced = if carry.is_empty() {
            let process_len = ready_len.min(read);
            let produced = processor(&read_buf[..process_len], &mut write_buf[..])?;
            if process_len < read {
                carry.extend_from_slice(&read_buf[process_len..read]);
            }
            produced
        } else {
            let take_from_read = ready_len - carry.len();
            merged.clear();
            merged.extend_from_slice(&carry);
            merged.extend_from_slice(&read_buf[..take_from_read]);
            carry.clear();
            if take_from_read < read {
                carry.extend_from_slice(&read_buf[take_from_read..read]);
            }
            processor(&merged, &mut write_buf[..])?
        };
        if produced != 0 {
            dest.write_all(&write_buf[..produced])?;
        }
    }
    if !carry.is_empty() {
        let produced = processor(&carry, &mut write_buf[..])?;
        if produced != 0 {
            dest.write_all(&write_buf[..produced])?;
        }
    }
    Ok(())
}

pub fn run_file_transform_to_file<F>(
    path: &str,
    dest: &mut File,
    geometry: ReaderTransformGeometry,
    processor: F,
) -> io::Result<()>
where
    F: for<'a> Fn(&'a [u8], &mut [u8]) -> io::Result<usize> + Send + Sync + 'static,
{
    let config = load_config(None);
    let _report = ParallelStream::map_file_fixed_size_to_file(
        &config,
        path,
        dest,
        geometry.read_block_size,
        geometry.write_block_size,
        processor,
    )?;
    Ok(())
}

pub fn run_reader_transform_to_pipe<R: Read>(
    dest: &mut File,
    reader: &mut R,
    geometry: ReaderTransformGeometry,
    output_policy: PipeOutputPolicy,
    processor: fn(&[u8], &mut [u8]) -> io::Result<usize>,
) -> io::Result<()> {
    use std::os::unix::io::RawFd;

    unsafe fn vmsplice_all(pipe_fd: RawFd, buf: &[u8]) -> io::Result<usize> {
        let mut written_total = 0usize;
        let mut ptr = buf.as_ptr();
        let mut remaining = buf.len();
        while remaining > 0 {
            let rc = if remaining % 4096 == 0 && ptr.align_offset(4096) == 0 {
                let iov = libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: remaining,
                };
                libc::vmsplice(pipe_fd, &iov as *const libc::iovec, 1, libc::SPLICE_F_GIFT)
            } else {
                libc::write(pipe_fd, ptr as *mut libc::c_void, remaining)
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EINTR | libc::EAGAIN) => continue,
                    _ => return Err(err),
                }
            }
            let n = rc as usize;
            written_total = written_total
                .checked_add(n)
                .ok_or_else(|| io::Error::other("vmsplice overflow"))?;
            ptr = ptr.add(n);
            remaining -= n;
        }
        Ok(written_total)
    }

    let _ = output_policy;
    grow_pipe_capacity_best_effort(dest.as_raw_fd(), geometry.write_block_size);
    let mut read_bufs = (0..3)
        .map(|_| vec![0u8; geometry.read_block_size as usize])
        .collect::<Vec<_>>();
    let mut carry = Vec::new();
    let mut merged =
        Vec::with_capacity(geometry.read_block_size as usize + geometry.input_chunk_multiple);
    let (free_tx, free_rx) = mpsc::sync_channel::<Vec<u8>>(3);
    for _ in 0..3 {
        free_tx
            .send(allocate_pipe_output_buffer(geometry.write_block_size))
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    let writer_pool_tx = free_tx.clone();
    drop(free_tx);

    let (tx, rx) = mpsc::sync_channel::<io::Result<(Vec<u8>, usize)>>(3);
    let writer = dest.try_clone()?;
    let writer_thread = thread::spawn(move || -> io::Result<()> {
        let pipe_fd = writer.as_raw_fd();
        for item in rx {
            let (buf, len) = item?;
            if len != 0 {
                let mut written = 0usize;
                while written < len {
                    written += unsafe { vmsplice_all(pipe_fd, &buf[written..len])? };
                }
                writer_pool_tx
                    .send(allocate_pipe_output_buffer(geometry.write_block_size))
                    .map_err(|err| io::Error::other(err.to_string()))?;
            } else {
                writer_pool_tx
                    .send(buf)
                    .map_err(|err| io::Error::other(err.to_string()))?;
            }
        }
        Ok(())
    });

    let mut read_slot = 0usize;
    loop {
        let read = reader.read(&mut read_bufs[read_slot])?;
        if read == 0 {
            break;
        }
        let ready_len = if geometry.input_chunk_multiple <= 1 {
            read
        } else {
            let total = carry.len() + read;
            (total / geometry.input_chunk_multiple) * geometry.input_chunk_multiple
        };
        if ready_len == 0 {
            carry.extend_from_slice(&read_bufs[read_slot][..read]);
            read_slot = (read_slot + 1) % read_bufs.len();
            continue;
        }
        let out = free_rx
            .recv()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let mut out = out;
        let produced = if carry.is_empty() {
            let process_len = ready_len.min(read);
            let produced = processor(&read_bufs[read_slot][..process_len], &mut out[..])?;
            if process_len < read {
                carry.extend_from_slice(&read_bufs[read_slot][process_len..read]);
            }
            produced
        } else {
            let take_from_read = ready_len - carry.len();
            merged.clear();
            merged.extend_from_slice(&carry);
            merged.extend_from_slice(&read_bufs[read_slot][..take_from_read]);
            carry.clear();
            if take_from_read < read {
                carry.extend_from_slice(&read_bufs[read_slot][take_from_read..read]);
            }
            processor(&merged, &mut out[..])?
        };
        tx.send(Ok((out, produced)))
            .map_err(|err| io::Error::other(err.to_string()))?;
        read_slot = (read_slot + 1) % read_bufs.len();
    }
    if !carry.is_empty() {
        let out = free_rx
            .recv()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let mut out = out;
        let produced = processor(&carry, &mut out[..])?;
        tx.send(Ok((out, produced)))
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    drop(tx);
    writer_thread
        .join()
        .map_err(|_| io::Error::other("transform pipe writer thread panicked"))??;
    Ok(())
}

pub fn run_file_transform_to_pipe_with_owned_output<F>(
    path: &str,
    dest: &mut File,
    geometry: ReaderTransformGeometry,
    output_policy: PipeOutputPolicy,
    processor: F,
) -> io::Result<()>
where
    F: for<'a> Fn(&'a [u8]) -> io::Result<Vec<u8>> + Send + Sync + 'static,
{
    let config = load_config(None);
    match output_policy {
        PipeOutputPolicy::GiftedAlignedPages => {
            grow_pipe_capacity_best_effort(dest.as_raw_fd(), geometry.write_block_size);
            let _report = ParallelStream::map_file_to_pipe_with_owned_buffers(
                &config,
                path,
                dest,
                geometry.read_block_size,
                geometry.write_block_size,
                processor,
            )?;
            Ok(())
        }
    }
}
