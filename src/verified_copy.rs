use crate::block_hash::{
    default_hash_base, hash_file_blocks, recover_file_with_copies, save_manifest_replicas_durable,
    verify_file_with_replicas, BlockHashAlgorithm, RecoverMode, VerifyReport,
};
use crate::config::{load_config, IOParams};
use crate::common::IOMode;
use crate::reader::load_file_to_memory_for_mode;
use crate::writer::{copy_file as copy_file_inner, write_buffer};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCopyReport {
    pub bytes_copied: u64,
    pub source_bytes_hashed: u64,
    pub verified_blocks: usize,
    pub repaired_blocks: usize,
    pub used_recovery: bool,
    pub hash_type: BlockHashAlgorithm,
}

fn path_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path is not valid UTF-8: {}", path.display()),
        )
    })
}

fn sync_file(path: &str) -> io::Result<()> {
    OpenOptions::new().read(true).write(true).open(path)?.sync_all()
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn verify_and_repair_target(
    source: &str,
    target: &str,
    hash_base: &str,
    verify_page_cache: &IOParams,
    verify_direct: &IOParams,
    io_mode_read: IOMode,
) -> io::Result<(VerifyReport, usize, bool)> {
    let verify_report = verify_file_with_replicas(
        target,
        Some(hash_base),
        verify_page_cache.num_threads,
        verify_page_cache.block_size,
        verify_page_cache.qd,
        verify_direct.num_threads,
        verify_direct.block_size,
        verify_direct.qd,
        io_mode_read,
    )?;
    if verify_report.bad_blocks.is_empty() {
        return Ok((verify_report, 0, false));
    }

    let recover_report = recover_file_with_copies(
        target,
        &[source.to_string()],
        Some(hash_base),
        verify_page_cache.num_threads,
        verify_page_cache.block_size,
        verify_page_cache.qd,
        verify_direct.num_threads,
        verify_direct.block_size,
        verify_direct.qd,
        io_mode_read,
        RecoverMode::Standard,
    )?;
    if !recover_report.failed_blocks.is_empty() {
        return Err(invalid_data(format!(
            "verified copy could not repair {} block(s)",
            recover_report.failed_blocks.len()
        )));
    }

    sync_file(target)?;

    let verify_report = verify_file_with_replicas(
        target,
        Some(hash_base),
        verify_page_cache.num_threads,
        verify_page_cache.block_size,
        verify_page_cache.qd,
        verify_direct.num_threads,
        verify_direct.block_size,
        verify_direct.qd,
        io_mode_read,
    )?;
    if !verify_report.bad_blocks.is_empty() {
        return Err(invalid_data(format!(
            "verified copy still has {} bad block(s) after repair",
            verify_report.bad_blocks.len()
        )));
    }

    Ok((verify_report, recover_report.repaired_blocks, true))
}

/// Copy one file to another, write source-derived block-hash sidecars for the
/// destination, `fsync` the destination file and sidecar files, then verify the
/// destination against that source-derived manifest.
///
/// On success, the destination matched the source-derived block-hash manifest at
/// the end of this run. This is stronger than hashing the destination after the
/// copy, because the oracle comes from the source rather than the post-copy
/// target state.
///
/// Current limits:
///
/// - this syncs the destination file and sidecar files, but not the parent directory
/// - this does not defend against concurrent mutation of the source while the operation runs
#[allow(dead_code)]
pub fn copy_file_verified<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
) -> io::Result<VerifiedCopyReport> {
    copy_file_verified_with_options(
        source,
        target,
        IOMode::Auto,
        IOMode::Auto,
        BlockHashAlgorithm::Xxh3,
        false,
        None,
    )
}

pub fn copy_file_verified_with_options<S: AsRef<Path>, D: AsRef<Path>>(
    source: S,
    target: D,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    hash_type: BlockHashAlgorithm,
    via_memory: bool,
    hash_base: Option<&str>,
) -> io::Result<VerifiedCopyReport> {
    let source = path_str(source.as_ref())?;
    let target = path_str(target.as_ref())?;
    let config = load_config(None);

    let source_page_cache = config.get_params_for_path("verify", false, source);
    let source_direct = config.get_params_for_path("verify", true, source);
    let source_manifest = hash_file_blocks(
        source,
        hash_type,
        source_page_cache.num_threads,
        source_page_cache.block_size,
        source_page_cache.qd,
        source_direct.num_threads,
        source_direct.block_size,
        source_direct.qd,
        io_mode_read,
    )?;

    let source_size = fs::metadata(source)?.len();
    let bytes_copied = if via_memory {
        let loaded = load_file_to_memory_for_mode(&config, "read", source, io_mode_read)?;
        let write_page_cache = config.get_params_for_path("write", false, target);
        let write_direct = config.get_params_for_path("write", true, target);
        write_buffer(
            target,
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
        copy_file_inner(
            source,
            target,
            copy_page_cache.num_threads,
            copy_page_cache.block_size,
            copy_page_cache.qd,
            copy_direct.num_threads,
            copy_direct.block_size,
            copy_direct.qd,
            io_mode_read,
            io_mode_write,
        )?
    };

    if bytes_copied != source_size {
        return Err(invalid_data(format!(
            "verified copy expected to copy {} bytes but wrote {}",
            source_size, bytes_copied
        )));
    }

    sync_file(target)?;

    let hash_base_owned = hash_base
        .map(str::to_string)
        .unwrap_or_else(|| default_hash_base(target));
    save_manifest_replicas_durable(&hash_base_owned, &source_manifest)?;

    let target_page_cache = config.get_params_for_path("verify", false, target);
    let target_direct = config.get_params_for_path("verify", true, target);
    let (verify_report, repaired_blocks, used_recovery) = verify_and_repair_target(
        source,
        target,
        &hash_base_owned,
        &target_page_cache,
        &target_direct,
        io_mode_read,
    )?;

    Ok(VerifiedCopyReport {
        bytes_copied,
        source_bytes_hashed: source_manifest.bytes_hashed,
        verified_blocks: verify_report.total_blocks,
        repaired_blocks,
        used_recovery,
        hash_type: source_manifest.hash_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn verify_and_repair_target_can_recover_from_source_copy() {
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
        let manifest = hash_file_blocks(
            source_str,
            BlockHashAlgorithm::Xxh3,
            source_page_cache.num_threads,
            source_page_cache.block_size,
            source_page_cache.qd,
            source_direct.num_threads,
            source_direct.block_size,
            source_direct.qd,
            IOMode::PageCache,
        )
        .unwrap();
        let hash_base = default_hash_base(target_str);
        save_manifest_replicas_durable(&hash_base, &manifest).unwrap();

        let target_page_cache = config.get_params_for_path("verify", false, target_str);
        let target_direct = config.get_params_for_path("verify", true, target_str);
        let (verify_report, repaired_blocks, used_recovery) = verify_and_repair_target(
            source_str,
            target_str,
            &hash_base,
            &target_page_cache,
            &target_direct,
            IOMode::PageCache,
        )
        .unwrap();

        assert!(used_recovery);
        assert!(repaired_blocks > 0);
        assert!(verify_report.bad_blocks.is_empty());
        assert_eq!(fs::read(&target).unwrap(), bytes);
    }

    #[test]
    fn verify_and_repair_target_fails_if_source_no_longer_matches_manifest() {
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
        let manifest = hash_file_blocks(
            source_str,
            BlockHashAlgorithm::Xxh3,
            source_page_cache.num_threads,
            source_page_cache.block_size,
            source_page_cache.qd,
            source_direct.num_threads,
            source_direct.block_size,
            source_direct.qd,
            IOMode::PageCache,
        )
        .unwrap();
        let hash_base = default_hash_base(target_str);
        save_manifest_replicas_durable(&hash_base, &manifest).unwrap();

        fs::write(&source, &drifted_source).unwrap();

        let target_page_cache = config.get_params_for_path("verify", false, target_str);
        let target_direct = config.get_params_for_path("verify", true, target_str);
        let err = verify_and_repair_target(
            source_str,
            target_str,
            &hash_base,
            &target_page_cache,
            &target_direct,
            IOMode::PageCache,
        )
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("could not repair"));
    }
}
