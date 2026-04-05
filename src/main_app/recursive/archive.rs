use super::*;
use crate::common::{AlignedBuffer, IOMode};
use crate::config::{self, IOParams};
use crate::io_util::{sync_parent_directory, sync_path};
use crate::writer::{copy_file_range_threaded, OffsetWriter};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;

const TAR_BLOCK_SIZE: u64 = 512;
const TAR_EOF_BLOCKS: u64 = TAR_BLOCK_SIZE * 2;
const TAR_COPY_BUFFER_SIZE: usize = 1024 * 1024;
const TAR_FAST_COPY_SENDFILE_CHUNK_SIZE: usize = 0x7fff_f000usize;
const TAR_SMALL_SLAB_TARGET_BYTES: usize = 512 * 1024;
const TAR_SMALL_WRITE_WORKERS: usize = 4;
const TAR_SMALL_WRITE_QD: usize = 4;

mod bench;
pub(crate) use bench::bench_tar_archive_variant;

#[derive(Clone)]
enum TarEntryKind {
    RegularFile { size: u64, source_path: PathBuf },
    Directory,
    Symlink { target: Vec<u8> },
}

#[derive(Clone)]
struct TarEntry {
    archive_path: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
    kind: TarEntryKind,
    header_offset: u64,
    data_offset: u64,
}

#[derive(Clone)]
struct TarSlabTask {
    start_offset: u64,
    len: u64,
    entry_indices: Vec<usize>,
}

#[derive(Clone)]
struct TarLargeTask {
    entry_index: usize,
}

#[derive(Clone)]
enum TarPlannedTask {
    Slab(TarSlabTask),
    Large(TarLargeTask),
}

struct ReusableTarSlab {
    buffer: AlignedBuffer,
}

#[derive(Default)]
struct TarParallelCounters {
    small_slab_tasks_inflight: AtomicUsize,
    small_slab_entries_inflight: AtomicUsize,
    sendfile_streams_inflight: AtomicUsize,
    mt_copy_jobs_inflight: AtomicUsize,
    mt_copy_threads_inflight: AtomicUsize,
}

struct TarParallelSampler {
    done: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<io::Result<()>>>,
}

fn align_up(value: u64, align: u64) -> u64 {
    if align == 0 {
        return value;
    }
    let rem = value % align;
    if rem == 0 {
        value
    } else {
        value + (align - rem)
    }
}

fn file_name_bytes(path: &Path) -> io::Result<Vec<u8>> {
    if path == Path::new(".") {
        return Ok(vec![b'.']);
    }
    path.file_name()
        .map(|name| name.as_bytes().to_vec())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source path has no final component"))
}

fn join_tar_path(prefix: &Path, name: &std::ffi::OsStr, is_dir: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    if !prefix.as_os_str().is_empty() {
        bytes.extend_from_slice(prefix.as_os_str().as_bytes());
        bytes.push(b'/');
    }
    bytes.extend_from_slice(name.as_bytes());
    if is_dir {
        bytes.push(b'/');
    }
    bytes
}

fn split_tar_path(path: &[u8]) -> io::Result<([u8; 100], [u8; 155])> {
    if path.len() <= 100 {
        let mut name = [0u8; 100];
        name[..path.len()].copy_from_slice(path);
        return Ok((name, [0u8; 155]));
    }
    for idx in (0..path.len()).rev() {
        if path[idx] != b'/' {
            continue;
        }
        let prefix = &path[..idx];
        let name_part = &path[idx + 1..];
        if name_part.len() <= 100 && prefix.len() <= 155 {
            let mut name = [0u8; 100];
            let mut prefix_field = [0u8; 155];
            name[..name_part.len()].copy_from_slice(name_part);
            prefix_field[..prefix.len()].copy_from_slice(prefix);
            return Ok((name, prefix_field));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("tar path is too long for ustar: {}", String::from_utf8_lossy(path)),
    ))
}

fn encode_octal(value: u64, field_len: usize) -> io::Result<Vec<u8>> {
    if field_len < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar numeric field too small",
        ));
    }
    let digits = format!("{value:o}");
    if digits.len() + 1 > field_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar numeric field overflow for value {value}"),
        ));
    }
    let mut field = vec![b'0'; field_len];
    let start = field_len - digits.len() - 1;
    field[start..start + digits.len()].copy_from_slice(digits.as_bytes());
    field[field_len - 1] = 0;
    Ok(field)
}

fn write_field<const N: usize>(header: &mut [u8; 512], offset: usize, data: &[u8]) -> io::Result<()> {
    if data.len() > N {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar field overflowed",
        ));
    }
    header[offset..offset + data.len()].copy_from_slice(data);
    Ok(())
}

fn tar_header_bytes(entry: &TarEntry) -> io::Result<[u8; 512]> {
    let mut header = [0u8; 512];
    let (name, prefix) = split_tar_path(&entry.archive_path)?;
    header[0..100].copy_from_slice(&name);
    write_field::<8>(&mut header, 100, &encode_octal(u64::from(entry.mode & 0o7777), 8)?)?;
    write_field::<8>(&mut header, 108, &encode_octal(u64::from(entry.uid), 8)?)?;
    write_field::<8>(&mut header, 116, &encode_octal(u64::from(entry.gid), 8)?)?;
    let size = match &entry.kind {
        TarEntryKind::RegularFile { size, .. } => *size,
        TarEntryKind::Directory | TarEntryKind::Symlink { .. } => 0,
    };
    write_field::<12>(&mut header, 124, &encode_octal(size, 12)?)?;
    write_field::<12>(&mut header, 136, &encode_octal(entry.mtime, 12)?)?;
    header[148..156].fill(b' ');
    header[156] = match &entry.kind {
        TarEntryKind::RegularFile { .. } => b'0',
        TarEntryKind::Directory => b'5',
        TarEntryKind::Symlink { .. } => b'2',
    };
    if let TarEntryKind::Symlink { target } = &entry.kind {
        if target.len() > 100 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "symlink target too long for ustar: {}",
                    String::from_utf8_lossy(target)
                ),
            ));
        }
        header[157..157 + target.len()].copy_from_slice(target);
    }
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    header[345..500].copy_from_slice(&prefix);
    let checksum = header.iter().map(|byte| u32::from(*byte)).sum::<u32>() as u64;
    let digits = format!("{checksum:o}");
    if digits.len() > 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar header checksum overflow for {checksum}"),
        ));
    }
    let mut checksum_field = [b'0'; 8];
    let start = 6 - digits.len();
    checksum_field[start..start + digits.len()].copy_from_slice(digits.as_bytes());
    checksum_field[6] = 0;
    checksum_field[7] = b' ';
    header[148..156].copy_from_slice(&checksum_field);
    Ok(header)
}

fn madvise_best_effort(ptr: *mut libc::c_void, len: usize, advice: libc::c_int) -> io::Result<()> {
    if unsafe { libc::madvise(ptr, len, advice) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINVAL | libc::ENOSYS) => Ok(()),
        _ => Err(err),
    }
}

impl TarParallelSampler {
    fn start(label: &'static str, counters: Arc<TarParallelCounters>) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let done_flag = done.clone();
        let handle = std::thread::spawn(move || -> io::Result<()> {
            while !done_flag.load(Ordering::Relaxed) {
                let small_tasks = counters.small_slab_tasks_inflight.load(Ordering::Relaxed);
                let small_entries = counters.small_slab_entries_inflight.load(Ordering::Relaxed);
                let sendfile = counters.sendfile_streams_inflight.load(Ordering::Relaxed);
                let mt_jobs = counters.mt_copy_jobs_inflight.load(Ordering::Relaxed);
                let mt_threads = counters.mt_copy_threads_inflight.load(Ordering::Relaxed);
                eprintln!(
                    "{label} parallel: small_slab_tasks={small_tasks}, small_slab_entries={small_entries}, sendfile_streams={sendfile}, mt_copy_jobs={mt_jobs}, mt_copy_threads={mt_threads}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Ok(())
        });
        Self {
            done,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> io::Result<()> {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| io::Error::other("tar parallel sampler panicked"))??;
        }
        Ok(())
    }
}

fn sendfile_to_sink(source: &fs::File, target: &fs::File, source_len: u64) -> io::Result<Option<u64>> {
    let mut copied_total = 0_u64;
    let mut source_pos: libc::off_t = 0;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(TAR_FAST_COPY_SENDFILE_CHUNK_SIZE as u64) as usize;
        let copied = unsafe {
            libc::sendfile(
                target.as_raw_fd(),
                source.as_raw_fd(),
                &mut source_pos,
                chunk,
            )
        };
        if copied > 0 {
            copied_total = copied_total.saturating_add(copied as u64);
            continue;
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("sendfile stopped early after {copied_total} of {source_len} bytes"),
            ));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EXDEV) => {
                return Ok(None)
            }
            _ => return Err(err),
        }
    }
    Ok(Some(copied_total))
}

fn write_all(file: &mut fs::File, data: &[u8]) -> io::Result<()> {
    let mut written = 0usize;
    while written < data.len() {
        let count = file.write(&data[written..])?;
        if count == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short tar stream write"));
        }
        written += count;
    }
    Ok(())
}

fn buffered_stream_copy(source: &fs::File, target: &mut fs::File, source_len: u64) -> io::Result<u64> {
    let mut buffer = vec![0u8; TAR_COPY_BUFFER_SIZE.min(source_len.max(1) as usize)];
    let mut copied_total = 0_u64;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(buffer.len() as u64) as usize;
        let read = source.read_at(&mut buffer[..chunk], copied_total)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short buffered tar read after {copied_total} of {source_len} bytes"),
            ));
        }
        write_all(target, &buffer[..read])?;
        copied_total = copied_total.saturating_add(read as u64);
    }
    Ok(copied_total)
}

fn tar_entry_total_len(entry: &TarEntry) -> u64 {
    TAR_BLOCK_SIZE
        + match &entry.kind {
            TarEntryKind::RegularFile { size, .. } => align_up(*size, TAR_BLOCK_SIZE),
            TarEntryKind::Directory | TarEntryKind::Symlink { .. } => 0,
        }
}

fn tar_entry_padding_len(entry: &TarEntry) -> u64 {
    match &entry.kind {
        TarEntryKind::RegularFile { size, .. } => align_up(*size, TAR_BLOCK_SIZE).saturating_sub(*size),
        TarEntryKind::Directory | TarEntryKind::Symlink { .. } => 0,
    }
}

fn tar_entry_is_small(entry: &TarEntry) -> bool {
    match &entry.kind {
        TarEntryKind::RegularFile { .. } => tar_entry_total_len(entry) <= TAR_SMALL_SLAB_TARGET_BYTES as u64,
        TarEntryKind::Directory | TarEntryKind::Symlink { .. } => true,
    }
}

fn push_current_slab(
    planned: &mut Vec<TarPlannedTask>,
    current_start: &mut Option<u64>,
    current_end: &mut u64,
    current_entries: &mut Vec<usize>,
) {
    if let Some(start_offset) = current_start.take() {
        planned.push(TarPlannedTask::Slab(TarSlabTask {
            start_offset,
            len: current_end.saturating_sub(start_offset),
            entry_indices: std::mem::take(current_entries),
        }));
        *current_end = 0;
    }
}

fn plan_tar_tasks(entries: &[TarEntry]) -> Vec<TarPlannedTask> {
    let mut planned = Vec::new();
    let mut current_start = None;
    let mut current_end = 0_u64;
    let mut current_entries = Vec::new();

    for (entry_index, entry) in entries.iter().enumerate() {
        if tar_entry_is_small(entry) {
            let entry_start = entry.header_offset;
            let entry_end = entry_start.saturating_add(tar_entry_total_len(entry));
            if let Some(start_offset) = current_start {
                let contiguous = entry_start == current_end;
                let proposed_len = entry_end.saturating_sub(start_offset);
                if contiguous && proposed_len <= TAR_SMALL_SLAB_TARGET_BYTES as u64 {
                    current_entries.push(entry_index);
                    current_end = entry_end;
                    continue;
                }
                push_current_slab(
                    &mut planned,
                    &mut current_start,
                    &mut current_end,
                    &mut current_entries,
                );
            }
            current_start = Some(entry_start);
            current_end = entry_end;
            current_entries.push(entry_index);
        } else {
            push_current_slab(
                &mut planned,
                &mut current_start,
                &mut current_end,
                &mut current_entries,
            );
            planned.push(TarPlannedTask::Large(TarLargeTask { entry_index }));
        }
    }

    push_current_slab(
        &mut planned,
        &mut current_start,
        &mut current_end,
        &mut current_entries,
    );
    planned
}

fn usize_from_u64(value: u64, label: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} does not fit in usize"),
        )
    })
}

fn read_small_file_into(source_path: &Path, destination: &mut [u8]) -> io::Result<()> {
    let mut source = fs::File::open(source_path)?;
    source.read_exact(destination)
}

impl ReusableTarSlab {
    fn new() -> io::Result<Self> {
        let buffer = AlignedBuffer::new_uninit(TAR_SMALL_SLAB_TARGET_BYTES)?;
        madvise_best_effort(
            buffer.as_slice().as_ptr() as *mut libc::c_void,
            buffer.len(),
            libc::MADV_HUGEPAGE,
        )?;
        Ok(Self { buffer })
    }

    fn fill<'a>(&'a mut self, entries: &[TarEntry], task: &TarSlabTask) -> io::Result<&'a [u8]> {
        let slab_len = usize_from_u64(task.len, "tar slab length")?;
        let slab = &mut self.buffer.as_mut_slice()[..slab_len];
        slab.fill(0);
        for &entry_index in &task.entry_indices {
            let entry = &entries[entry_index];
            let header_offset = usize_from_u64(
                entry.header_offset.saturating_sub(task.start_offset),
                "tar slab header offset",
            )?;
            let header_end = header_offset
                .checked_add(TAR_BLOCK_SIZE as usize)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "tar slab header overflowed"))?;
            slab[header_offset..header_end].copy_from_slice(&tar_header_bytes(entry)?);
            if let TarEntryKind::RegularFile { size, source_path } = &entry.kind {
                let data_offset = usize_from_u64(
                    entry.data_offset.saturating_sub(task.start_offset),
                    "tar slab data offset",
                )?;
                let data_len = usize_from_u64(*size, "tar small file size")?;
                let data_end = data_offset.checked_add(data_len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "tar slab data overflowed")
                })?;
                read_small_file_into(source_path, &mut slab[data_offset..data_end])?;
            }
        }
        Ok(&slab[..])
    }
}

fn write_tar_large_entry(
    entry: &TarEntry,
    output: &str,
    page_cache_params: &IOParams,
    direct_params: &IOParams,
    counters: Option<&TarParallelCounters>,
) -> io::Result<u64> {
    if let Some(counters) = counters {
        counters.mt_copy_jobs_inflight.fetch_add(1, Ordering::Relaxed);
        counters
            .mt_copy_threads_inflight
            .fetch_add(direct_params.num_threads as usize, Ordering::Relaxed);
    }
    let output_file = OpenOptions::new().write(true).open(output)?;
    let header = tar_header_bytes(entry)?;
    write_all_at(&output_file, entry.header_offset, &header)?;
    let result = if let TarEntryKind::RegularFile { size, source_path } = &entry.kind {
        let source = source_path.to_string_lossy();
        copy_file_range_threaded(
            source.as_ref(),
            output,
            0,
            entry.data_offset,
            *size,
            false,
            page_cache_params.num_threads,
            page_cache_params.block_size,
            page_cache_params.qd,
            direct_params.num_threads,
            direct_params.block_size,
            direct_params.qd,
            IOMode::Auto,
            IOMode::Direct,
        )?;
        Ok(tar_entry_total_len(entry))
    } else {
        Ok(tar_entry_total_len(entry))
    };
    if let Some(counters) = counters {
        counters.mt_copy_jobs_inflight.fetch_sub(1, Ordering::Relaxed);
        counters
            .mt_copy_threads_inflight
            .fetch_sub(direct_params.num_threads as usize, Ordering::Relaxed);
    }
    result
}

fn stream_tar_to_dev_null(entries: &[TarEntry], tasks: &[TarPlannedTask], verbose: bool) -> io::Result<u64> {
    let mut dev_null = OpenOptions::new().write(true).open("/dev/null")?;
    let mut total_bytes = 0_u64;
    let mut slab = ReusableTarSlab::new()?;
    let parallel_counters = Arc::new(TarParallelCounters::default());
    let parallel_sampler = if verbose {
        Some(TarParallelSampler::start(
            "tar-devnull",
            parallel_counters.clone(),
        ))
    } else {
        None
    };
    for task in tasks {
        match task {
            TarPlannedTask::Slab(task) => {
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_add(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_add(task.entry_indices.len(), Ordering::Relaxed);
                let slab_bytes = slab.fill(entries, task)?;
                write_all(&mut dev_null, slab_bytes)?;
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_sub(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_sub(task.entry_indices.len(), Ordering::Relaxed);
                total_bytes = total_bytes.saturating_add(task.len);
            }
            TarPlannedTask::Large(task) => {
                let entry = &entries[task.entry_index];
                let header = tar_header_bytes(entry)?;
                write_all(&mut dev_null, &header)?;
                total_bytes = total_bytes.saturating_add(TAR_BLOCK_SIZE);
                if let TarEntryKind::RegularFile { size, source_path } = &entry.kind {
                    let source_file = fs::File::open(source_path)?;
                    parallel_counters
                        .sendfile_streams_inflight
                        .fetch_add(1, Ordering::Relaxed);
                    let copied = match sendfile_to_sink(&source_file, &dev_null, *size)? {
                        Some(copied) => copied,
                        None => buffered_stream_copy(&source_file, &mut dev_null, *size)?,
                    };
                    parallel_counters
                        .sendfile_streams_inflight
                        .fetch_sub(1, Ordering::Relaxed);
                    total_bytes = total_bytes.saturating_add(copied);
                    let padding = tar_entry_padding_len(entry);
                    if padding > 0 {
                        let zero_pad = [0u8; TAR_BLOCK_SIZE as usize];
                        write_all(&mut dev_null, &zero_pad[..padding as usize])?;
                        total_bytes = total_bytes.saturating_add(padding);
                    }
                }
            }
        }
    }
    let zero_blocks = [0u8; TAR_EOF_BLOCKS as usize];
    write_all(&mut dev_null, &zero_blocks)?;
    total_bytes = total_bytes.saturating_add(TAR_EOF_BLOCKS);
    if verbose {
        eprintln!("tar create: streamed {} bytes to /dev/null", total_bytes);
    }
    if let Some(sampler) = parallel_sampler {
        sampler.finish()?;
    }
    Ok(total_bytes)
}

fn write_all_at(file: &fs::File, offset: u64, data: &[u8]) -> io::Result<()> {
    let mut written = 0usize;
    while written < data.len() {
        let count = file.write_at(&data[written..], offset + written as u64)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short tar header write",
            ));
        }
        written += count;
    }
    Ok(())
}

fn append_tar_entry(
    entries: &mut Vec<TarEntry>,
    archive_path: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
    kind: TarEntryKind,
    next_offset: &mut u64,
) {
    let header_offset = *next_offset;
    *next_offset = next_offset.saturating_add(TAR_BLOCK_SIZE);
    let data_offset = *next_offset;
    if let TarEntryKind::RegularFile { size, .. } = &kind {
        *next_offset = next_offset.saturating_add(align_up(*size, TAR_BLOCK_SIZE));
    }
    entries.push(TarEntry {
        archive_path,
        mode,
        uid,
        gid,
        mtime,
        kind,
        header_offset,
        data_offset,
    });
}

fn collect_tar_manifest(source: &Path, output: &Path) -> io::Result<(Vec<TarEntry>, u64)> {
    let source_meta = fs::symlink_metadata(source)?;
    let source_abs = if source_meta.file_type().is_dir() {
        source.canonicalize()?
    } else {
        paths::prospective_absolute_path(source)?
    };
    let output_abs = paths::prospective_absolute_path(output)?;
    if source_meta.file_type().is_dir() && output_abs.starts_with(&source_abs) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to archive directory {} into itself via {}",
                source.display(),
                output.display()
            ),
        ));
    }
    if !source_meta.file_type().is_dir() && output_abs == source_abs {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "refusing to overwrite the archived source path",
        ));
    }

    let mut entries = Vec::new();
    let mut next_offset = 0_u64;
    let root_name = file_name_bytes(source)?;
    let root_name_path = PathBuf::from(std::ffi::OsString::from_vec(root_name.clone()));
    let root_mtime = source_meta.mtime().max(0) as u64;

    if source_meta.file_type().is_dir() {
        append_tar_entry(
            &mut entries,
            {
                let mut path = root_name.clone();
                path.push(b'/');
                path
            },
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::Directory,
            &mut next_offset,
        );
        let mut stack = vec![(source.to_path_buf(), root_name_path)];
        while let Some((dir_path, archive_prefix)) = stack.pop() {
            let mut entries_in_dir = fs::read_dir(&dir_path)?
                .collect::<Result<Vec<_>, io::Error>>()?;
            entries_in_dir.sort_by_key(|entry| entry.file_name());
            let mut child_dirs = Vec::new();
            for entry in entries_in_dir {
                let file_type = entry.file_type()?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)?;
                let archive_path = join_tar_path(&archive_prefix, &entry.file_name(), file_type.is_dir());
                let mtime = metadata.mtime().max(0) as u64;
                if file_type.is_dir() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::Directory,
                        &mut next_offset,
                    );
                    child_dirs.push((path, archive_prefix.join(entry.file_name())));
                } else if file_type.is_symlink() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::Symlink {
                            target: fs::read_link(&path)?.as_os_str().as_bytes().to_vec(),
                        },
                        &mut next_offset,
                    );
                } else if file_type.is_file() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::RegularFile {
                            size: metadata.len(),
                            source_path: path,
                        },
                        &mut next_offset,
                    );
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "tar only supports regular files, directories, and symlinks (saw {})",
                            path.display()
                        ),
                    ));
                }
            }
            child_dirs.reverse();
            stack.extend(child_dirs);
        }
    } else if source_meta.file_type().is_symlink() {
        append_tar_entry(
            &mut entries,
            root_name,
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::Symlink {
                target: fs::read_link(source)?.as_os_str().as_bytes().to_vec(),
            },
            &mut next_offset,
        );
    } else if source_meta.file_type().is_file() {
        append_tar_entry(
            &mut entries,
            root_name,
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::RegularFile {
                size: source_meta.len(),
                source_path: source.to_path_buf(),
            },
            &mut next_offset,
        );
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar create currently supports only regular files, directories, and symlinks",
        ));
    }

    Ok((entries, next_offset.saturating_add(TAR_EOF_BLOCKS)))
}

fn prepare_tar_output(path: &Path, total_size: u64) -> io::Result<fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(total_size)?;
    let allocate_len = i64::try_from(total_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar size does not fit in off_t",
        )
    })?;
    unsafe {
        libc::posix_fallocate(file.as_raw_fd(), 0, allocate_len);
    }
    Ok(file)
}

pub(crate) fn create_uncompressed_tar(source: &Path, output: &Path, verbose: bool) -> io::Result<u64> {
    let (entries, total_size) = collect_tar_manifest(source, output)?;
    let planned_tasks = plan_tar_tasks(&entries);
    if output == Path::new("/dev/null") {
        return stream_tar_to_dev_null(&entries, &planned_tasks, verbose);
    }
    let config = config::load_config(None);
    let output_str = output.to_string_lossy().into_owned();
    let copy_page_cache = config.get_params_for_path("copy", false, &output_str);
    let copy_direct = config.get_params_for_path("copy", true, &output_str);
    let output_file = prepare_tar_output(output, total_size)?;
    let output_path = output.to_path_buf();
    let entries = Arc::new(entries);
    let slab_tasks = Arc::new(
        planned_tasks
            .iter()
            .filter_map(|task| match task {
                TarPlannedTask::Slab(task) => Some(task.clone()),
                TarPlannedTask::Large(_) => None,
            })
            .collect::<Vec<_>>(),
    );
    let large_tasks = Arc::new(
        planned_tasks
            .iter()
            .filter_map(|task| match task {
                TarPlannedTask::Slab(_) => None,
                TarPlannedTask::Large(task) => Some(task.clone()),
            })
            .collect::<Vec<_>>(),
    );
    let next_slab = Arc::new(AtomicUsize::new(0));
    let next_large = Arc::new(AtomicUsize::new(0));
    let sample_counters = Arc::new(ThroughputSampleCounters::default());
    let parallel_counters = Arc::new(TarParallelCounters::default());
    let sampler = if verbose {
        Some(ThroughputSampler::start("tar-create", "items", sample_counters.clone()))
    } else {
        None
    };
    let parallel_sampler = if verbose {
        Some(TarParallelSampler::start(
            "tar-create",
            parallel_counters.clone(),
        ))
    } else {
        None
    };
    let slab_worker_count = TAR_SMALL_WRITE_WORKERS.min(slab_tasks.len().max(1));
    let large_worker_count = recursive_copy_large_worker_count().min(large_tasks.len().max(1));
    let mut threads = Vec::with_capacity(slab_worker_count.saturating_add(large_worker_count));
    for _ in 0..slab_worker_count {
        let entries = entries.clone();
        let slab_tasks = slab_tasks.clone();
        let next_slab = next_slab.clone();
        let output_path = output_path.clone();
        let sample_counters = sample_counters.clone();
        let parallel_counters = parallel_counters.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let output_path = output_path.to_string_lossy().into_owned();
            let mut writer =
                OffsetWriter::with_truncate(&output_path, total_size, TAR_SMALL_WRITE_QD, IOMode::Direct, false)?;
            let mut slab = ReusableTarSlab::new()?;
            while let Some(task) = slab_tasks.get(next_slab.fetch_add(1, Ordering::SeqCst)) {
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_add(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_add(task.entry_indices.len(), Ordering::Relaxed);
                let slab_bytes = slab.fill(&entries, task)?;
                writer.write_at(task.start_offset, slab_bytes)?;
                parallel_counters
                    .small_slab_tasks_inflight
                    .fetch_sub(1, Ordering::Relaxed);
                parallel_counters
                    .small_slab_entries_inflight
                    .fetch_sub(task.entry_indices.len(), Ordering::Relaxed);
                sample_counters.bytes.fetch_add(task.len, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
            }
            writer.flush()?;
            OpenOptions::new().read(true).write(true).open(&output_path)?.sync_all()?;
            Ok(())
        }));
    }
    for _ in 0..large_worker_count {
        let entries = entries.clone();
        let large_tasks = large_tasks.clone();
        let next_large = next_large.clone();
        let output_path = output_path.clone();
        let sample_counters = sample_counters.clone();
        let copy_page_cache = copy_page_cache.clone();
        let copy_direct = copy_direct.clone();
        let parallel_counters = parallel_counters.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let output_path = output_path.to_string_lossy().into_owned();
            while let Some(task) = large_tasks.get(next_large.fetch_add(1, Ordering::SeqCst)) {
                let written = write_tar_large_entry(
                    &entries[task.entry_index],
                    &output_path,
                    &copy_page_cache,
                    &copy_direct,
                    Some(parallel_counters.as_ref()),
                )?;
                sample_counters.bytes.fetch_add(written, Ordering::Relaxed);
                sample_counters.units.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("tar worker panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(sampler) = sampler {
        sampler.finish()?;
    }
    if let Some(sampler) = parallel_sampler {
        sampler.finish()?;
    }
    if let Some(err) = first_error {
        return Err(err);
    }

    let zero_blocks = [0u8; TAR_EOF_BLOCKS as usize];
    write_all_at(&output_file, total_size - TAR_EOF_BLOCKS, &zero_blocks)?;
    output_file.sync_all()?;
    sync_path(output)?;
    sync_parent_directory(output)?;
    if verbose {
        eprintln!(
            "tar create: entries={}, total_size={}",
            entries.len(),
            total_size
        );
    }
    Ok(total_size)
}
