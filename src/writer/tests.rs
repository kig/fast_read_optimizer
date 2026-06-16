use super::copy_ops::prepare_copy_destination;
use super::*;

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_file(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{}-{}-{}", prefix, std::process::id(), nanos));
    path
}

#[test]
fn sequential_writer_appends_in_order() {
    let path = unique_temp_file("fro-sequential-writer");
    let mut writer =
        SequentialWriter::create(path.to_str().unwrap(), 3, 4096, IOMode::Auto).unwrap();

    let a = vec![0x11; 4096];
    let b = vec![0x22; 123];
    let c = vec![0x33; 8192];

    assert_eq!(writer.append(&a).unwrap(), 0);
    assert_eq!(writer.append(&b).unwrap(), a.len() as u64);
    assert_eq!(writer.append(&c).unwrap(), (a.len() + b.len()) as u64);
    writer.flush().unwrap();

    let data = fs::read(&path).unwrap();
    assert_eq!(writer.bytes_written(), (a.len() + b.len() + c.len()) as u64);
    assert_eq!(data.len(), a.len() + b.len() + c.len());
    assert_eq!(&data[..a.len()], &a);
    assert_eq!(&data[a.len()..a.len() + b.len()], &b);
    assert_eq!(&data[a.len() + b.len()..], &c);

    let _ = fs::remove_file(path);
}

#[test]
fn buf_writer_batches_chunks_into_output_file() {
    let path = unique_temp_file("fro-buf-writer");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut writer = BufWriter::with_capacity(2, file, 2, 4096).unwrap();

    writer.write_all(&vec![b'a'; 3000]).unwrap();
    writer.write_all(&vec![b'b'; 5000]).unwrap();
    writer.write_all(&vec![b'c'; 17]).unwrap();
    writer.into_inner().unwrap();

    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), 8017);
    assert!(data[..3000].iter().all(|&byte| byte == b'a'));
    assert!(data[3000..8000].iter().all(|&byte| byte == b'b'));
    assert!(data[8000..].iter().all(|&byte| byte == b'c'));

    let _ = fs::remove_file(path);
}

#[test]
fn offset_writer_tracks_written_extent_for_sparse_offsets() {
    let path = unique_temp_file("fro-offset-writer-extent");
    let mut writer =
        OffsetWriter::create(path.to_str().unwrap(), 16, 2, IOMode::PageCache).unwrap();

    writer.write_at(12, b"xy").unwrap();
    writer.write_at(0, b"abcd").unwrap();
    writer.flush().unwrap();

    assert_eq!(writer.bytes_written(), 6);
    assert_eq!(writer.written_extent(), 14);

    let _ = fs::remove_file(path);
}

#[test]
fn offset_writer_rejects_end_offset_overflow() {
    let path = unique_temp_file("fro-offset-writer-overflow");
    let mut writer =
        OffsetWriter::create(path.to_str().unwrap(), 16, 2, IOMode::PageCache).unwrap();

    let err = writer.write_at(u64::MAX - 1, b"abcd").unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let _ = fs::remove_file(path);
}

#[test]
fn prepare_copy_destination_keeps_exact_sized_target_intact() {
    let path = unique_temp_file("fro-prepare-copy-destination");
    fs::write(&path, b"abcdefgh").unwrap();

    prepare_copy_destination(path.to_str().unwrap(), 0, 8, true).unwrap();

    assert_eq!(fs::metadata(&path).unwrap().len(), 8);
    assert_eq!(fs::read(&path).unwrap(), b"abcdefgh");

    let _ = fs::remove_file(path);
}

#[test]
fn prepare_copy_destination_rejects_offsets_that_do_not_fit_off_t() {
    let path = unique_temp_file("fro-prepare-copy-destination-large-offset");
    fs::write(&path, b"").unwrap();

    let result = std::panic::catch_unwind(|| {
        prepare_copy_destination(path.to_str().unwrap(), (i64::MAX as u64) + 1, 4096, false)
    });
    assert!(result.is_ok(), "prepare_copy_destination should not panic");
    let err = result.unwrap().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let _ = fs::remove_file(path);
}

#[test]
fn overwrite_changed_chunks_direct_updates_only_different_chunks() {
    let source = unique_temp_file("fro-overwrite-changed-source");
    let target = unique_temp_file("fro-overwrite-changed-target");
    let mut source_bytes = vec![0x11; 8192];
    source_bytes[4096..8192].fill(0x22);
    let mut target_bytes = source_bytes.clone();
    target_bytes[4096..8192].fill(0x33);
    fs::write(&source, &source_bytes).unwrap();
    fs::write(&target, &target_bytes).unwrap();

    overwrite_changed_chunks_direct(
        source.to_str().unwrap(),
        target.to_str().unwrap(),
        1,
        4096,
        2,
        2,
        4096,
        2,
    )
    .unwrap();

    assert_eq!(fs::read(&target).unwrap(), source_bytes);

    let _ = fs::remove_file(source);
    let _ = fs::remove_file(target);
}

#[test]
fn generated_write_strategy_respects_direct_thresholds() {
    assert_eq!(
        generated_write_strategy_for_fstype(128 * 1024, IOMode::Direct, Some("ext4")),
        Some(GeneratedWriteStrategy::SingleDirect)
    );
    assert_eq!(
        generated_write_strategy_for_fstype(8 << 20, IOMode::Direct, Some("zfs")),
        Some(GeneratedWriteStrategy::SerialDirect)
    );
    assert_eq!(
        generated_write_strategy_for_fstype(64 << 20, IOMode::Direct, Some("ext4")),
        Some(GeneratedWriteStrategy::SerialDirect)
    );
}

#[test]
fn generated_write_strategy_skips_page_cache_and_large_unknown_mounts() {
    assert_eq!(
        generated_write_strategy_for_fstype(128 * 1024, IOMode::PageCache, Some("ext4")),
        None
    );
    assert_eq!(
        generated_write_strategy_for_fstype(8 << 20, IOMode::Direct, Some("xfs")),
        None
    );
    assert_eq!(
        generated_write_strategy_for_fstype(96 << 20, IOMode::Direct, Some("ext4")),
        None
    );
}
