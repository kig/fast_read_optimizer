use crate::block_hash::{
    default_hash_base, hash_file_blocks, load_manifest_replicas, save_manifest_replicas_durable,
    BlockHashAlgorithm, BlockHashManifest,
};
use crate::common::{CopyStrategy, IOMode};
use crate::config::{load_config, IOParams};
use crate::io_util::{sync_parent_directory, sync_path, CopyOperationGuard};
use crate::reader::load_file_to_memory_for_mode;
use crate::writer::{copy_file_with_strategy as copy_file_inner, write_buffer};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCopyReport {
    pub bytes_copied: u64,
    pub source_bytes_hashed: u64,
    pub verified_blocks: usize,
    pub repaired_blocks: usize,
    pub used_recovery: bool,
    pub hash_type: BlockHashAlgorithm,
    pub hashes_persisted: bool,
}

fn path_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

struct PendingVerifiedTarget {
    final_path: PathBuf,
    temp_path: PathBuf,
    remove_final_on_drop: bool,
    committed: bool,
}

impl PendingVerifiedTarget {
    fn new(final_path: &Path, remove_final_on_drop: bool) -> io::Result<Self> {
        Ok(Self {
            final_path: final_path.to_path_buf(),
            temp_path: verified_copy_temp_path(final_path)?,
            remove_final_on_drop,
            committed: false,
        })
    }

    fn working_path(&self) -> &Path {
        &self.temp_path
    }

    fn commit(mut self) -> io::Result<()> {
        std::fs::rename(&self.temp_path, &self.final_path)?;
        self.committed = true;
        sync_parent_directory(&self.final_path)
    }
}

impl Drop for PendingVerifiedTarget {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.temp_path);
            if self.remove_final_on_drop {
                let _ = std::fs::remove_file(&self.final_path);
            }
        }
    }
}

fn verified_copy_temp_path(target: &Path) -> io::Result<PathBuf> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let file_name = target.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("target path must include a file name: {}", target.display()),
        )
    })?;
    let file_name = file_name.to_string_lossy();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for attempt in 0..1024_u32 {
        let candidate = parent.join(format!(
            ".{file_name}.fro-verified-copy-tmp-{}-{}-{}",
            process::id(),
            nanos,
            attempt
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "could not allocate a unique temporary target next to {}",
            target.display()
        ),
    ))
}

fn ensure_copied_bytes_match_source(
    source_manifest: &BlockHashManifest,
    observed_source_size: u64,
    bytes_copied: u64,
) -> io::Result<()> {
    let expected_size = source_manifest.file_size;
    if bytes_copied != expected_size {
        return Err(invalid_data(format!(
            "verified copy expected to copy {} bytes but wrote {}",
            expected_size, bytes_copied
        )));
    }
    let _ = observed_source_size;
    Ok(())
}

fn hash_with_params(
    path: &str,
    hash_type: BlockHashAlgorithm,
    page_cache: &IOParams,
    direct: &IOParams,
    io_mode: IOMode,
) -> io::Result<BlockHashManifest> {
    hash_file_blocks(
        path,
        hash_type,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
    )
}

fn mismatched_blocks(
    expected: &BlockHashManifest,
    current: &BlockHashManifest,
) -> io::Result<Vec<usize>> {
    if expected.hash_type != current.hash_type {
        return Err(invalid_data(format!(
            "verification hash type mismatch: expected {:?}, got {:?}",
            expected.hash_type, current.hash_type
        )));
    }
    if expected.file_size != current.file_size {
        return Err(invalid_data(format!(
            "verification file size mismatch: expected {}, got {}",
            expected.file_size, current.file_size
        )));
    }
    if expected.block_size != current.block_size {
        return Err(invalid_data(format!(
            "verification block size mismatch: expected {}, got {}",
            expected.block_size, current.block_size
        )));
    }
    if expected.block_hashes.len() != current.block_hashes.len() {
        return Err(invalid_data(format!(
            "verification block count mismatch: expected {}, got {}",
            expected.block_hashes.len(),
            current.block_hashes.len()
        )));
    }

    Ok(expected
        .block_hashes
        .iter()
        .zip(current.block_hashes.iter())
        .enumerate()
        .filter_map(|(index, (expected_hash, current_hash))| {
            if expected_hash != current_hash {
                Some(index)
            } else {
                None
            }
        })
        .collect())
}

fn read_block_at(
    file: &mut File,
    block_index: usize,
    file_size: u64,
    block_size: u64,
) -> io::Result<Vec<u8>> {
    let offset = block_index as u64 * block_size;
    let len = std::cmp::min(block_size, file_size.saturating_sub(offset)) as usize;
    let mut data = vec![0u8; len];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut data)?;
    Ok(data)
}

fn write_block_at(
    file: &mut File,
    block_index: usize,
    block_size: u64,
    data: &[u8],
) -> io::Result<()> {
    let offset = block_index as u64 * block_size;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)
}

fn repair_target_from_source_manifest(
    source: &str,
    target: &str,
    source_manifest: &BlockHashManifest,
    source_page_cache: &IOParams,
    source_direct: &IOParams,
    target_page_cache: &IOParams,
    target_direct: &IOParams,
    io_mode_read: IOMode,
) -> io::Result<usize> {
    let current_source = hash_with_params(
        source,
        source_manifest.hash_type,
        source_page_cache,
        source_direct,
        io_mode_read,
    )?;
    let source_drift = mismatched_blocks(source_manifest, &current_source)?;
    if !source_drift.is_empty() {
        return Err(invalid_data(format!(
            "source no longer matches the initial manifest ({} mismatched block(s))",
            source_drift.len()
        )));
    }

    let current_target = hash_with_params(
        target,
        source_manifest.hash_type,
        target_page_cache,
        target_direct,
        io_mode_read,
    )?;
    let mismatches = mismatched_blocks(source_manifest, &current_target)?;
    if mismatches.is_empty() {
        return Ok(0);
    }

    let mut source_file = File::open(source)?;
    let mut target_file = OpenOptions::new().write(true).open(target)?;
    for block_index in &mismatches {
        let block = read_block_at(
            &mut source_file,
            *block_index,
            source_manifest.file_size,
            source_manifest.block_size,
        )?;
        write_block_at(
            &mut target_file,
            *block_index,
            source_manifest.block_size,
            &block,
        )?;
    }
    target_file.sync_all()?;
    Ok(mismatches.len())
}

fn persist_hashes_if_requested(
    source: &str,
    source_manifest: &BlockHashManifest,
    target_hash_base: Option<&str>,
) -> io::Result<bool> {
    let Some(target_hash_base) = target_hash_base else {
        return Ok(false);
    };

    let source_hash_base = default_hash_base(source);
    if load_manifest_replicas(&source_hash_base)
        .iter()
        .all(|replica| replica.is_none())
    {
        save_manifest_replicas_durable(&source_hash_base, source_manifest)?;
    }

    save_manifest_replicas_durable(target_hash_base, source_manifest)?;
    Ok(true)
}

/// Copy one file to a temporary sibling, `fsync` and verify that temporary file,
/// then rename it into place and sync the parent directory.
///
/// If `hash_base` is `Some(...)`, successful verification also leaves durable
/// sidecars at the destination and at the source if the source did not already
/// have sidecars.
///
/// On success, the destination matched the source-derived block-hash manifest at
/// the end of this run. This is stronger than hashing the destination after the
/// copy, because the oracle comes from the source rather than the post-copy
/// target state.
///
/// Current limits:
///
/// - default locking is advisory only; non-cooperating writers can still mutate the source or target
#[allow(dead_code)]
pub fn copy_file_verified<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
) -> io::Result<VerifiedCopyReport> {
    copy_file_verified_with_options_and_lock(
        source,
        target,
        IOMode::Auto,
        IOMode::Auto,
        BlockHashAlgorithm::Xxh3,
        false,
        None,
        CopyStrategy::Threaded,
        true,
    )
}

#[allow(dead_code)]
pub fn copy_file_verified_with_options<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    hash_type: BlockHashAlgorithm,
    via_memory: bool,
    hash_base: Option<&str>,
) -> io::Result<VerifiedCopyReport> {
    copy_file_verified_with_options_and_lock(
        source,
        target,
        io_mode_read,
        io_mode_write,
        hash_type,
        via_memory,
        hash_base,
        CopyStrategy::Threaded,
        true,
    )
}

pub(crate) fn copy_file_verified_with_options_and_lock<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    hash_type: BlockHashAlgorithm,
    via_memory: bool,
    hash_base: Option<&str>,
    copy_strategy: CopyStrategy,
    use_lock: bool,
) -> io::Result<VerifiedCopyReport> {
    let source = path_str(source.as_ref())?;
    let target_path = target.as_ref();
    let target = path_str(target_path)?;
    let target_missing_before_guard = !target_path.exists();
    let _guard = CopyOperationGuard::new(source, target, use_lock)?;
    let pending_target = PendingVerifiedTarget::new(target_path, target_missing_before_guard)?;
    let working_target = path_str(pending_target.working_path())?;
    let config = load_config(None);

    let source_page_cache = config.get_params_for_path("verify", false, source);
    let source_direct = config.get_params_for_path("verify", true, source);
    let source_manifest = hash_with_params(
        source,
        hash_type,
        &source_page_cache,
        &source_direct,
        io_mode_read,
    )?;

    let bytes_copied = if via_memory {
        let loaded = load_file_to_memory_for_mode(&config, "read", source, io_mode_read)?;
        let write_page_cache = config.get_params_for_path("write", false, target);
        let write_direct = config.get_params_for_path("write", true, target);
        write_buffer(
            working_target,
            &loaded.data,
            write_page_cache.num_threads,
            write_page_cache.block_size,
            write_page_cache.qd,
            write_direct.num_threads,
            write_direct.block_size,
            write_direct.qd,
            io_mode_write,
        )?
    } else {
        let copy_page_cache = config.get_params_for_path("copy", false, target);
        let copy_direct = config.get_params_for_path("copy", true, target);
        let copy_range = config.get_copy_range_params_for_path(target);
        copy_file_inner(
            source,
            working_target,
            copy_page_cache.num_threads,
            copy_page_cache.block_size,
            copy_page_cache.qd,
            copy_direct.num_threads,
            copy_direct.block_size,
            copy_direct.qd,
            copy_range.num_threads,
            copy_range.block_size,
            copy_range.qd,
            io_mode_read,
            io_mode_write,
            copy_strategy,
        )?
    };

    ensure_copied_bytes_match_source(&source_manifest, source_manifest.file_size, bytes_copied)?;

    sync_path(pending_target.working_path())?;

    let target_page_cache = config.get_params_for_path("verify", false, target);
    let target_direct = config.get_params_for_path("verify", true, target);
    let initial_target = hash_with_params(
        working_target,
        source_manifest.hash_type,
        &target_page_cache,
        &target_direct,
        io_mode_read,
    )?;
    let initial_mismatches = mismatched_blocks(&source_manifest, &initial_target)?;

    let repaired_blocks = if initial_mismatches.is_empty() {
        0
    } else {
        repair_target_from_source_manifest(
            source,
            working_target,
            &source_manifest,
            &source_page_cache,
            &source_direct,
            &target_page_cache,
            &target_direct,
            io_mode_read,
        )?
    };

    let final_target = hash_with_params(
        working_target,
        source_manifest.hash_type,
        &target_page_cache,
        &target_direct,
        io_mode_read,
    )?;
    let final_mismatches = mismatched_blocks(&source_manifest, &final_target)?;
    if !final_mismatches.is_empty() {
        return Err(invalid_data(format!(
            "verified copy still has {} mismatched block(s) after repair",
            final_mismatches.len()
        )));
    }

    pending_target.commit()?;
    let hashes_persisted = persist_hashes_if_requested(source, &source_manifest, hash_base)?;

    Ok(VerifiedCopyReport {
        bytes_copied,
        source_bytes_hashed: source_manifest.bytes_hashed,
        verified_blocks: source_manifest.block_hashes.len(),
        repaired_blocks,
        used_recovery: repaired_blocks > 0,
        hash_type: source_manifest.hash_type,
        hashes_persisted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::fs;
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

    fn manifest_for_size(file_size: u64) -> BlockHashManifest {
        BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size,
            block_size: 1024 * 1024,
            bytes_hashed: file_size,
            block_hashes: Vec::new(),
            hash_of_hashes: serde_json::from_str("\"0000000000000000\"").unwrap(),
        }
    }

    #[test]
    fn pending_verified_target_promotes_temp_file_over_final_path() {
        let tmp = unique_temp_dir("fro-verified-copy-pending-commit");
        fs::create_dir_all(&tmp).unwrap();
        let final_path = tmp.join("target.bin");
        fs::write(&final_path, b"old target").unwrap();

        let pending = PendingVerifiedTarget::new(&final_path, false).unwrap();
        fs::write(pending.working_path(), b"new target").unwrap();
        pending.commit().unwrap();

        assert_eq!(fs::read(&final_path).unwrap(), b"new target");
        let leftovers = fs::read_dir(&tmp)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".fro-verified-copy-tmp-"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[test]
    fn pending_verified_target_cleans_up_temp_and_placeholder_on_drop() {
        let tmp = unique_temp_dir("fro-verified-copy-pending-drop");
        fs::create_dir_all(&tmp).unwrap();
        let final_path = tmp.join("target.bin");

        {
            let pending = PendingVerifiedTarget::new(&final_path, true).unwrap();
            fs::write(pending.working_path(), b"partial").unwrap();
        }

        assert!(!final_path.exists());
        let leftovers = fs::read_dir(&tmp)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".fro-verified-copy-tmp-"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    struct ChangingLenMock {
        next_len: Cell<u64>,
        step: u64,
    }

    impl ChangingLenMock {
        fn new(initial_len: u64, step: u64) -> Self {
            Self {
                next_len: Cell::new(initial_len),
                step,
            }
        }

        fn len(&self) -> u64 {
            let current = self.next_len.get();
            self.next_len.set(current + self.step);
            current
        }
    }

    #[test]
    fn copied_byte_count_tracks_hashed_manifest_size_not_later_metadata() {
        let source = ChangingLenMock::new(4096, 4096);
        let source_manifest = manifest_for_size(source.len());
        let later_metadata_len = source.len();

        ensure_copied_bytes_match_source(&source_manifest, later_metadata_len, 4096).unwrap();
    }

    #[test]
    fn repair_target_from_source_manifest_can_fix_mismatched_blocks() {
        let tmp = unique_temp_dir("fro-verified-copy-repair");
        fs::create_dir_all(&tmp).unwrap();
        let source = tmp.join("source.bin");
        let target = tmp.join("target.bin");
        let bytes = (0..(2 * 1024 * 1024 + 333))
            .map(|i| ((i * 37) % 251) as u8)
            .collect::<Vec<_>>();
        let mut corrupted = bytes.clone();
        corrupted[(1024 * 1024 + 9)..(1024 * 1024 + 13)].copy_from_slice(&[1, 2, 3, 4]);

        fs::write(&source, &bytes).unwrap();
        fs::write(&target, &corrupted).unwrap();

        let source_str = source.to_str().unwrap();
        let target_str = target.to_str().unwrap();
        let config = load_config(None);
        let source_page_cache = config.get_params_for_path("verify", false, source_str);
        let source_direct = config.get_params_for_path("verify", true, source_str);
        let source_manifest = hash_with_params(
            source_str,
            BlockHashAlgorithm::Xxh3,
            &source_page_cache,
            &source_direct,
            IOMode::PageCache,
        )
        .unwrap();

        let target_page_cache = config.get_params_for_path("verify", false, target_str);
        let target_direct = config.get_params_for_path("verify", true, target_str);
        let repaired = repair_target_from_source_manifest(
            source_str,
            target_str,
            &source_manifest,
            &source_page_cache,
            &source_direct,
            &target_page_cache,
            &target_direct,
            IOMode::PageCache,
        )
        .unwrap();

        assert!(repaired > 0);
        let final_target = hash_with_params(
            target_str,
            BlockHashAlgorithm::Xxh3,
            &target_page_cache,
            &target_direct,
            IOMode::PageCache,
        )
        .unwrap();
        assert!(mismatched_blocks(&source_manifest, &final_target)
            .unwrap()
            .is_empty());
        assert_eq!(fs::read(&target).unwrap(), bytes);
    }

    #[test]
    fn repair_target_from_source_manifest_fails_if_source_drifted() {
        let tmp = unique_temp_dir("fro-verified-copy-source-drift");
        fs::create_dir_all(&tmp).unwrap();
        let source = tmp.join("source.bin");
        let target = tmp.join("target.bin");
        let bytes = (0..(2 * 1024 * 1024 + 111))
            .map(|i| ((i * 41) % 251) as u8)
            .collect::<Vec<_>>();
        let mut corrupted_target = bytes.clone();
        corrupted_target[(1024 * 1024 + 5)..(1024 * 1024 + 9)].copy_from_slice(&[9, 8, 7, 6]);
        let mut drifted_source = bytes.clone();
        drifted_source[(1024 * 1024 + 5)..(1024 * 1024 + 9)].copy_from_slice(&[1, 3, 5, 7]);

        fs::write(&source, &bytes).unwrap();
        fs::write(&target, &corrupted_target).unwrap();

        let source_str = source.to_str().unwrap();
        let target_str = target.to_str().unwrap();
        let config = load_config(None);
        let source_page_cache = config.get_params_for_path("verify", false, source_str);
        let source_direct = config.get_params_for_path("verify", true, source_str);
        let source_manifest = hash_with_params(
            source_str,
            BlockHashAlgorithm::Xxh3,
            &source_page_cache,
            &source_direct,
            IOMode::PageCache,
        )
        .unwrap();

        fs::write(&source, &drifted_source).unwrap();

        let target_page_cache = config.get_params_for_path("verify", false, target_str);
        let target_direct = config.get_params_for_path("verify", true, target_str);
        let err = repair_target_from_source_manifest(
            source_str,
            target_str,
            &source_manifest,
            &source_page_cache,
            &source_direct,
            &target_page_cache,
            &target_direct,
            IOMode::PageCache,
        )
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("source no longer matches"));
    }
}
