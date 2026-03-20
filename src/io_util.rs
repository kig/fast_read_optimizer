use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;

pub fn direct_open_should_fallback(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOTTY | libc::ESPIPE)
    ) || err.kind() == io::ErrorKind::InvalidInput
}

pub fn open_direct_reader_or_fallback(path: &str, fallback: &File) -> io::Result<File> {
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(err) if direct_open_should_fallback(&err) => fallback.try_clone(),
        Err(err) => Err(err),
    }
}

pub fn open_direct_writer_or_fallback(path: &str, fallback: &File) -> io::Result<File> {
    match OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(err) if direct_open_should_fallback(&err) => fallback.try_clone(),
        Err(err) => Err(err),
    }
}

pub fn open_reader_files(path: &str, use_direct: bool) -> io::Result<(File, File)> {
    let file = File::open(path)?;
    let file_direct = if use_direct {
        open_direct_reader_or_fallback(path, &file)?
    } else {
        file.try_clone()?
    };
    Ok((file, file_direct))
}

pub fn expected_read_len(file_size: u64, offset: u64, block_size: u64) -> io::Result<usize> {
    let remaining = file_size.checked_sub(offset).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "read offset exceeded the known file size",
        )
    })?;
    usize::try_from(remaining.min(block_size)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected read length does not fit in usize",
        )
    })
}

pub fn validate_read_result(
    operation: &str,
    offset: u64,
    expected_len: usize,
    result: u32,
) -> io::Result<usize> {
    interpret_read_result(operation, offset, expected_len, i64::from(result))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileMetadataSnapshot {
    len: u64,
    mtime_sec: i64,
    mtime_nsec: i64,
    ctime_sec: i64,
    ctime_nsec: i64,
    dev: u64,
    ino: u64,
}

fn snapshot_regular_metadata(metadata: &fs::Metadata) -> Option<FileMetadataSnapshot> {
    if !metadata.file_type().is_file() {
        return None;
    }
    Some(FileMetadataSnapshot {
        len: metadata.len(),
        mtime_sec: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime_sec: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

fn describe_snapshot(snapshot: Option<FileMetadataSnapshot>) -> String {
    match snapshot {
        Some(snapshot) => format!(
            "len={}, mtime={}.{}, ctime={}.{}, dev={}, ino={}",
            snapshot.len,
            snapshot.mtime_sec,
            snapshot.mtime_nsec,
            snapshot.ctime_sec,
            snapshot.ctime_nsec,
            snapshot.dev,
            snapshot.ino
        ),
        None => "non-regular-file-or-missing".to_string(),
    }
}

#[derive(Debug)]
struct AdvisoryFileLock {
    file: File,
}

impl AdvisoryFileLock {
    fn lock(path: &str, exclusive: bool, create: bool) -> io::Result<Self> {
        let mut options = OpenOptions::new();
        if exclusive {
            options.read(true).write(true);
        } else {
            options.read(true);
        }
        if create {
            options.create(true);
        }
        let file = options.open(path)?;
        let operation = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        let rc = unsafe { libc::flock(file.as_raw_fd(), operation) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to lock {} for copy: {}", path, err),
            ));
        }
        Ok(Self { file })
    }
}

impl Drop for AdvisoryFileLock {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub struct CopyOperationGuard {
    source_path: String,
    source_snapshot: Option<FileMetadataSnapshot>,
    _locks: Vec<AdvisoryFileLock>,
}

impl CopyOperationGuard {
    pub fn new(source: &str, target: &str, use_lock: bool) -> io::Result<Self> {
        let mut locks = Vec::new();
        if use_lock {
            let mut entries = if source == target {
                vec![(source, true, false)]
            } else {
                vec![(source, false, false), (target, true, true)]
            };
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (path, exclusive, create) in entries {
                locks.push(AdvisoryFileLock::lock(path, exclusive, create)?);
            }
        }

        Ok(Self {
            source_path: source.to_string(),
            source_snapshot: snapshot_regular_metadata(&fs::metadata(source)?),
            _locks: locks,
        })
    }

    pub fn ensure_source_unchanged(&self) -> io::Result<()> {
        let current = match fs::metadata(&self.source_path) {
            Ok(metadata) => snapshot_regular_metadata(&metadata),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };
        if current != self.source_snapshot {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source changed during copy: before [{}], after [{}]",
                    describe_snapshot(self.source_snapshot),
                    describe_snapshot(current)
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PendingReadSlots {
    pending_block_ids: Vec<Option<u64>>,
}

impl PendingReadSlots {
    pub fn new(qd: usize) -> Self {
        Self {
            pending_block_ids: vec![None; qd],
        }
    }

    pub fn reserve(&mut self, slot: usize, block_id: u64) -> io::Result<()> {
        let entry = self.pending_block_ids.get_mut(slot).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read slot {slot} is out of range"),
            )
        })?;
        if entry.replace(block_id).is_some() {
            return Err(io::Error::other(format!(
                "read slot {slot} was reused before its prior completion was observed"
            )));
        }
        Ok(())
    }

    pub fn complete(&mut self, slot: usize) -> io::Result<u64> {
        self.pending_block_ids
            .get_mut(slot)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("read slot {slot} is out of range"),
                )
            })?
            .take()
            .ok_or_else(|| {
                io::Error::other(format!("read slot {slot} completed with no pending block"))
            })
    }
}

fn interpret_read_result(
    operation: &str,
    offset: u64,
    expected_len: usize,
    actual_len: i64,
) -> io::Result<usize> {
    if actual_len < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{operation} reported negative completion at offset {offset}: {actual_len}"),
        ));
    }
    let actual_len = usize::try_from(actual_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{operation} completion does not fit in usize at offset {offset}"),
        )
    })?;
    if actual_len == expected_len {
        return Ok(actual_len);
    }
    let kind = if actual_len < expected_len {
        io::ErrorKind::UnexpectedEof
    } else {
        io::ErrorKind::InvalidData
    };
    let detail = if actual_len < expected_len {
        "short read"
    } else {
        "oversized read completion"
    };
    Err(io::Error::new(
        kind,
        format!(
            "{operation} {detail} at offset {offset}: expected {expected_len} bytes, got {actual_len}"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "{}-{}-{}",
            prefix,
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn expected_read_len_uses_tail_for_final_block() {
        assert_eq!(expected_read_len(10_000, 9_500, 1_024).unwrap(), 500);
    }

    #[test]
    fn expected_read_len_rejects_offset_past_file_size() {
        let err = expected_read_len(4_096, 4_097, 4_096).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn validate_read_result_accepts_exact_completion() {
        assert_eq!(validate_read_result("read", 4_096, 512, 512).unwrap(), 512);
    }

    #[test]
    fn validate_read_result_rejects_short_completion() {
        let err = validate_read_result("read", 8_192, 4_096, 2_048).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("short read"));
    }

    #[test]
    fn validate_read_result_rejects_early_eof() {
        let err = validate_read_result("read", 12_288, 4_096, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("expected 4096 bytes, got 0"));
    }

    #[test]
    fn injected_short_read_lengths_follow_documented_contract() {
        const BLOCK_SIZE: usize = 1024;
        const OFFSET: u64 = 16 * 1024;

        let mut lengths = Vec::new();
        lengths.extend((0_i64..=255).collect::<Vec<_>>());
        lengths.extend((0_i64..).map(|n| n * 17).take_while(|len| *len <= 1190));
        lengths.extend([-255, -34, -17, -1, 1023, 1024, 1025]);
        lengths.sort_unstable();
        lengths.dedup();

        for actual_len in lengths {
            let result = interpret_read_result("read", OFFSET, BLOCK_SIZE, actual_len);
            match actual_len.cmp(&(BLOCK_SIZE as i64)) {
                std::cmp::Ordering::Less if actual_len >= 0 => {
                    let err = result.unwrap_err();
                    assert_eq!(
                        err.kind(),
                        io::ErrorKind::UnexpectedEof,
                        "length {actual_len} should be rejected as a short read"
                    );
                }
                std::cmp::Ordering::Equal => {
                    assert_eq!(
                        result.unwrap(),
                        BLOCK_SIZE,
                        "length {actual_len} should be accepted as an exact completion"
                    );
                }
                std::cmp::Ordering::Greater => {
                    let err = result.unwrap_err();
                    assert_eq!(
                        err.kind(),
                        io::ErrorKind::InvalidData,
                        "length {actual_len} should be rejected as oversized"
                    );
                }
                std::cmp::Ordering::Less => {
                    let err = result.unwrap_err();
                    assert_eq!(
                        err.kind(),
                        io::ErrorKind::InvalidData,
                        "negative completion {actual_len} should be rejected"
                    );
                }
            }
        }
    }

    #[test]
    fn pending_read_slots_allow_out_of_order_reuse_only_after_completion() {
        let mut slots = PendingReadSlots::new(3);
        slots.reserve(0, 10).unwrap();
        slots.reserve(1, 11).unwrap();
        slots.reserve(2, 12).unwrap();

        assert_eq!(slots.complete(1).unwrap(), 11);
        slots.reserve(1, 13).unwrap();
        assert_eq!(slots.complete(0).unwrap(), 10);
        assert_eq!(slots.complete(2).unwrap(), 12);
        assert_eq!(slots.complete(1).unwrap(), 13);
    }

    #[test]
    fn pending_read_slots_reject_reusing_live_slot() {
        let mut slots = PendingReadSlots::new(2);
        slots.reserve(0, 7).unwrap();
        let err = slots.reserve(0, 8).unwrap_err();
        assert!(err.to_string().contains("reused"));
    }

    #[test]
    fn copy_operation_guard_reports_source_metadata_drift() {
        let tmp = unique_temp_dir("fro-copy-guard-drift");
        fs::create_dir_all(&tmp).unwrap();
        let source = tmp.join("source.bin");
        let target = tmp.join("target.bin");
        fs::write(&source, b"abcdef").unwrap();
        fs::write(&target, b"uvwxyz").unwrap();

        let guard =
            CopyOperationGuard::new(source.to_str().unwrap(), target.to_str().unwrap(), false)
                .unwrap();
        let mut source_file = OpenOptions::new().append(true).open(&source).unwrap();
        source_file.write_all(b"!").unwrap();
        source_file.sync_all().unwrap();

        let err = guard.ensure_source_unchanged().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("source changed during copy"));
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::expected_read_len;
    use std::io;

    #[kani::proof]
    fn expected_read_len_matches_min_formula() {
        let file_size: u64 = kani::any();
        let offset: u64 = kani::any();
        let block_size: u64 = kani::any();

        kani::assume(offset <= file_size);
        let expected_u64 = (file_size - offset).min(block_size);
        kani::assume(expected_u64 <= usize::MAX as u64);

        let expected = usize::try_from(expected_u64).unwrap();
        assert_eq!(
            expected_read_len(file_size, offset, block_size).unwrap(),
            expected
        );
    }

    #[kani::proof]
    fn expected_read_len_rejects_offsets_past_end() {
        let file_size: u64 = kani::any();
        let offset: u64 = kani::any();
        let block_size: u64 = kani::any();

        kani::assume(offset > file_size);

        let err = expected_read_len(file_size, offset, block_size).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[kani::proof]
    fn expected_read_len_is_monotonic_in_offset() {
        let file_size: u64 = kani::any();
        let offset_a: u64 = kani::any();
        let offset_b: u64 = kani::any();
        let block_size: u64 = kani::any();

        kani::assume(offset_a <= offset_b);
        kani::assume(offset_b <= file_size);

        let expected_a = (file_size - offset_a).min(block_size);
        let expected_b = (file_size - offset_b).min(block_size);
        kani::assume(expected_a <= usize::MAX as u64);
        kani::assume(expected_b <= usize::MAX as u64);

        let len_a = expected_read_len(file_size, offset_a, block_size).unwrap();
        let len_b = expected_read_len(file_size, offset_b, block_size).unwrap();
        assert!(len_a >= len_b);
    }
}
