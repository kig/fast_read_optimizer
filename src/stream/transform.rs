use super::allocate_pipe_output_buffer;
use super::ParallelStream;
use crate::config::load_config;
use libc::{fcntl, F_SETPIPE_SZ};
use std::fs;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformInputSpec<'a> {
    Path(&'a str),
    Stdin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformOutputSpec<'a> {
    Path(&'a str),
    Stdout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformIoPairingKind {
    FileToFile,
    FileToStream,
    StreamToFile,
    StreamToStream,
}

pub enum TransformIoPairing {
    FileToFile { input_path: String, output: File },
    FileToStream { input_path: String, output: File },
    StreamToFile { input: File, output: File },
    StreamToStream { input: File, output: File },
}

impl TransformIoPairing {
    pub fn kind(&self) -> TransformIoPairingKind {
        match self {
            Self::FileToFile { .. } => TransformIoPairingKind::FileToFile,
            Self::FileToStream { .. } => TransformIoPairingKind::FileToStream,
            Self::StreamToFile { .. } => TransformIoPairingKind::StreamToFile,
            Self::StreamToStream { .. } => TransformIoPairingKind::StreamToStream,
        }
    }
}

pub fn classify_transform_io_pairing(
    regular_input: bool,
    regular_output: bool,
) -> TransformIoPairingKind {
    match (regular_input, regular_output) {
        (true, true) => TransformIoPairingKind::FileToFile,
        (true, false) => TransformIoPairingKind::FileToStream,
        (false, true) => TransformIoPairingKind::StreamToFile,
        (false, false) => TransformIoPairingKind::StreamToStream,
    }
}

/// Resolve a transform input into a reusable regular-file path when one exists.
///
/// Transform-style planners should use this helper instead of hand-rolling
/// `/proc/self/fd/*` probing so stdin redirection and ordinary file arguments share
/// the same regular-file detection rules as `auto_select_transform_io_pairing(...)`.
pub fn resolve_regular_transform_input_path(
    input: TransformInputSpec<'_>,
) -> io::Result<Option<String>> {
    match input {
        TransformInputSpec::Path(path) if is_regular_input_path(path)? => {
            Ok(Some(path.to_string()))
        }
        TransformInputSpec::Path(_) => Ok(None),
        TransformInputSpec::Stdin => regular_stdin_path(),
    }
}

/// Resolve stdin/stdout-or-path arguments into the most appropriate transform pairing.
///
/// Transform-style workloads that can specialize for regular-file and streaming paths
/// should use this helper so future callers inherit the same pairing behavior as
/// `base64` and `encrypt`/`decrypt`.
pub fn auto_select_transform_io_pairing(
    input: TransformInputSpec<'_>,
    output: TransformOutputSpec<'_>,
) -> io::Result<TransformIoPairing> {
    let regular_input_path = resolve_regular_transform_input_path(input)?;
    let regular_output = output_is_regular_like(output)?;
    match classify_transform_io_pairing(regular_input_path.is_some(), regular_output) {
        TransformIoPairingKind::FileToFile => Ok(TransformIoPairing::FileToFile {
            input_path: regular_input_path.expect("regular input path must exist"),
            output: open_transform_output(output)?,
        }),
        TransformIoPairingKind::FileToStream => Ok(TransformIoPairing::FileToStream {
            input_path: regular_input_path.expect("regular input path must exist"),
            output: open_transform_output(output)?,
        }),
        TransformIoPairingKind::StreamToFile => Ok(TransformIoPairing::StreamToFile {
            input: open_transform_input(input)?,
            output: open_transform_output(output)?,
        }),
        TransformIoPairingKind::StreamToStream => Ok(TransformIoPairing::StreamToStream {
            input: open_transform_input(input)?,
            output: open_transform_output(output)?,
        }),
    }
}

pub fn run_transform_io_pairing<T, FF2F, FF2S, SF2F, SF2S>(
    pairing: TransformIoPairing,
    file_to_file: FF2F,
    file_to_stream: FF2S,
    stream_to_file: SF2F,
    stream_to_stream: SF2S,
) -> io::Result<T>
where
    FF2F: FnOnce(String, File) -> io::Result<T>,
    FF2S: FnOnce(String, File) -> io::Result<T>,
    SF2F: FnOnce(File, File) -> io::Result<T>,
    SF2S: FnOnce(File, File) -> io::Result<T>,
{
    match pairing {
        TransformIoPairing::FileToFile { input_path, output } => file_to_file(input_path, output),
        TransformIoPairing::FileToStream { input_path, output } => {
            file_to_stream(input_path, output)
        }
        TransformIoPairing::StreamToFile { input, output } => stream_to_file(input, output),
        TransformIoPairing::StreamToStream { input, output } => stream_to_stream(input, output),
    }
}

pub fn run_transform_with_specs<T, FF2F, FF2S, SF2F, SF2S>(
    input: TransformInputSpec<'_>,
    output: TransformOutputSpec<'_>,
    file_to_file: FF2F,
    file_to_stream: FF2S,
    stream_to_file: SF2F,
    stream_to_stream: SF2S,
) -> io::Result<T>
where
    FF2F: FnOnce(String, File) -> io::Result<T>,
    FF2S: FnOnce(String, File) -> io::Result<T>,
    SF2F: FnOnce(File, File) -> io::Result<T>,
    SF2S: FnOnce(File, File) -> io::Result<T>,
{
    run_transform_io_pairing(
        auto_select_transform_io_pairing(input, output)?,
        file_to_file,
        file_to_stream,
        stream_to_file,
        stream_to_stream,
    )
}

fn regular_stdin_path() -> io::Result<Option<String>> {
    if !is_regular_fd(libc::STDIN_FILENO) {
        return Ok(None);
    }
    let read_path_buf = fs::read_link(format!("/proc/self/fd/{}", libc::STDIN_FILENO))?;
    let read_path = read_path_buf.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "stdin path is not valid UTF-8")
    })?;
    Ok(Some(read_path.to_string()))
}

fn output_is_regular_like(output: TransformOutputSpec<'_>) -> io::Result<bool> {
    Ok(match output {
        TransformOutputSpec::Path(path) => path != "-",
        TransformOutputSpec::Stdout => is_regular_fd(libc::STDOUT_FILENO) || is_stdout_dev_null()?,
    })
}

fn open_transform_input(input: TransformInputSpec<'_>) -> io::Result<File> {
    match input {
        TransformInputSpec::Path(path) => File::open(path),
        TransformInputSpec::Stdin => dup_fd_as_file(libc::STDIN_FILENO),
    }
}

fn open_transform_output(output: TransformOutputSpec<'_>) -> io::Result<File> {
    match output {
        TransformOutputSpec::Path(path) if path != "-" => File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path),
        TransformOutputSpec::Path(_) | TransformOutputSpec::Stdout => {
            dup_fd_as_file(libc::STDOUT_FILENO)
        }
    }
}

fn dup_fd_as_file(fd: RawFd) -> io::Result<File> {
    let dupfd = unsafe { libc::dup(fd) };
    if dupfd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(dupfd) })
}

fn is_regular_input_path(path: &str) -> io::Result<bool> {
    if path.starts_with("/dev/fd/") || path.starts_with("/proc/self/fd/") {
        return Ok(false);
    }
    Ok(fs::metadata(path)?.file_type().is_file())
}

fn is_regular_fd(fd: RawFd) -> bool {
    unsafe {
        let mut stat: libc::stat = std::mem::zeroed();
        libc::fstat(fd, &mut stat) == 0 && (stat.st_mode & libc::S_IFMT) == libc::S_IFREG
    }
}

fn is_stdout_dev_null() -> io::Result<bool> {
    unsafe {
        let mut stdout_stat: libc::stat = std::mem::zeroed();
        let mut dev_null_stat: libc::stat = std::mem::zeroed();
        if libc::fstat(libc::STDOUT_FILENO, &mut stdout_stat) != 0 {
            return Err(io::Error::last_os_error());
        }
        let path = b"/dev/null\0".as_ptr() as *const libc::c_char;
        if libc::stat(path, &mut dev_null_stat) != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(
            stdout_stat.st_dev == dev_null_stat.st_dev
                && stdout_stat.st_ino == dev_null_stat.st_ino,
        )
    }
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

#[cfg(test)]
mod tests {
    use super::{
        classify_transform_io_pairing, resolve_regular_transform_input_path,
        run_transform_io_pairing, TransformInputSpec, TransformIoPairing, TransformIoPairingKind,
    };
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).expect("create test temp base");
        let path = base.join(format!(
            "{}-{}-{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create test temp dir");
        path
    }

    #[test]
    fn classify_transform_io_pairing_covers_all_file_and_stream_combinations() {
        assert_eq!(
            classify_transform_io_pairing(true, true),
            TransformIoPairingKind::FileToFile
        );
        assert_eq!(
            classify_transform_io_pairing(true, false),
            TransformIoPairingKind::FileToStream
        );
        assert_eq!(
            classify_transform_io_pairing(false, true),
            TransformIoPairingKind::StreamToFile
        );
        assert_eq!(
            classify_transform_io_pairing(false, false),
            TransformIoPairingKind::StreamToStream
        );
    }

    #[test]
    fn run_transform_io_pairing_dispatches_each_variant() {
        let file = || File::open("/dev/null").expect("open /dev/null");

        let file_to_file = run_transform_io_pairing(
            TransformIoPairing::FileToFile {
                input_path: "in".to_string(),
                output: file(),
            },
            |input_path, _| Ok(format!("file-file:{input_path}")),
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
        )
        .expect("dispatch file to file");
        assert_eq!(file_to_file, "file-file:in");

        let file_to_stream = run_transform_io_pairing(
            TransformIoPairing::FileToStream {
                input_path: "in".to_string(),
                output: file(),
            },
            |_, _| unreachable!("wrong branch"),
            |input_path, _| Ok(format!("file-stream:{input_path}")),
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
        )
        .expect("dispatch file to stream");
        assert_eq!(file_to_stream, "file-stream:in");

        let stream_to_file = run_transform_io_pairing(
            TransformIoPairing::StreamToFile {
                input: file(),
                output: file(),
            },
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
            |_, _| Ok("stream-file".to_string()),
            |_, _| unreachable!("wrong branch"),
        )
        .expect("dispatch stream to file");
        assert_eq!(stream_to_file, "stream-file");

        let stream_to_stream = run_transform_io_pairing(
            TransformIoPairing::StreamToStream {
                input: file(),
                output: file(),
            },
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
            |_, _| unreachable!("wrong branch"),
            |_, _| Ok("stream-stream".to_string()),
        )
        .expect("dispatch stream to stream");
        assert_eq!(stream_to_stream, "stream-stream");
    }

    #[test]
    fn resolve_regular_transform_input_path_matches_regular_files_only() {
        let tmp = unique_temp_dir("fro-transform-regular-input");
        let regular = tmp.join("input.bin");
        fs::write(&regular, b"hello").expect("write regular input");

        assert_eq!(
            resolve_regular_transform_input_path(TransformInputSpec::Path(
                regular.to_str().expect("utf-8 test path")
            ))
            .expect("resolve regular path"),
            Some(regular.to_str().expect("utf-8 test path").to_string())
        );
        assert_eq!(
            resolve_regular_transform_input_path(TransformInputSpec::Path("/proc/self/fd/0"))
                .expect("resolve proc fd path"),
            None
        );
        assert_eq!(
            resolve_regular_transform_input_path(TransformInputSpec::Path("/dev/fd/0"))
                .expect("resolve dev fd path"),
            None
        );
    }
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
