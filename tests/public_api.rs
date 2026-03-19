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

#[test]
fn read_write_and_copy_file_use_high_level_api() {
    let tmp = unique_temp_dir("fro-public-api");
    fs::create_dir_all(&tmp).unwrap();
    let source = tmp.join("source.bin");
    let copy = tmp.join("copy.bin");
    let bytes = (0..(1024 * 1024 + 137))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();

    let written = fro::write_file(&source, &bytes).unwrap();
    assert_eq!(written, bytes.len() as u64);
    assert_eq!(fro::read_file(&source).unwrap(), bytes);

    let copied = fro::copy_file(&source, &copy).unwrap();
    assert_eq!(copied, bytes.len() as u64);
    assert_eq!(fs::read(copy).unwrap(), bytes);
}

#[test]
fn write_file_range_and_memory_copy_use_high_level_api() {
    let tmp = unique_temp_dir("fro-public-api-memory");
    fs::create_dir_all(&tmp).unwrap();
    let source = tmp.join("source.bin");
    let sliced = tmp.join("slice.bin");
    let copied = tmp.join("copied.bin");
    let bytes = (0..(1024 * 1024 + 137))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();

    fs::write(&source, &bytes).unwrap();

    let range_written = fro::write_file_range(&sliced, &bytes, 11, 4099).unwrap();
    assert_eq!(range_written, 4099);
    assert_eq!(fs::read(&sliced).unwrap(), bytes[11..(11 + 4099)].to_vec());

    let copied_bytes = fro::copy_file_via_memory(&source, &copied).unwrap();
    assert_eq!(copied_bytes, bytes.len() as u64);
    assert_eq!(fs::read(&copied).unwrap(), bytes);
}

#[test]
fn offset_writer_can_preserve_existing_prefix_and_suffix() {
    let tmp = unique_temp_dir("fro-public-api-offset");
    fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("target.bin");
    fs::write(&path, b"abcdefghij").unwrap();

    let writer = fro::offset_writer_with_options(&path, 10, fro::IOMode::PageCache, false).unwrap();
    writer.write_at_offset(3, b"XYZ".to_vec()).unwrap();
    let report = writer.finish().unwrap();

    assert_eq!(report.bytes_written, 3);
    assert_eq!(fs::read(path).unwrap(), b"abcXYZghij".to_vec());
}

#[test]
fn offset_writer_zero_fills_unwritten_gaps_by_default() {
    let tmp = unique_temp_dir("fro-public-api-offset-zero-fill");
    fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("target.bin");

    let writer = fro::offset_writer(&path, 16).unwrap();
    writer.write_at_offset(0, b"abcd".to_vec()).unwrap();
    writer.write_at_offset(12, b"xy".to_vec()).unwrap();
    let report = writer.finish().unwrap();

    assert_eq!(report.bytes_written, 6);
    assert_eq!(
        fs::read(path).unwrap(),
        b"abcd\0\0\0\0\0\0\0\0xy\0\0".to_vec()
    );
}

#[test]
fn direct_mode_keeps_high_level_block_iteration_stable() {
    let tmp = unique_temp_dir("fro-public-api-direct");
    fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("source.bin");
    let bytes = (0..(1024 * 1024 + 137))
        .map(|i| ((i * 11) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    let page_cache_block_size =
        fro::optimal_block_size_with_mode(&path, fro::IOMode::PageCache).unwrap();
    let direct_block_size = fro::optimal_block_size_with_mode(&path, fro::IOMode::Direct).unwrap();
    assert_eq!(page_cache_block_size, direct_block_size);

    let page_cache_reader = fro::open_with_mode(&path, fro::IOMode::PageCache).unwrap();
    let direct_reader = fro::open_with_mode(&path, fro::IOMode::Direct).unwrap();
    let page_cache_chunks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let direct_chunks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let page_cache_chunks_for_visit = page_cache_chunks.clone();
    page_cache_reader
        .foreach_block(move |index, data| {
            page_cache_chunks_for_visit
                .lock()
                .unwrap()
                .push((index, data.len()));
            Ok(())
        })
        .unwrap();

    let direct_chunks_for_visit = direct_chunks.clone();
    direct_reader
        .foreach_block(move |index, data| {
            direct_chunks_for_visit
                .lock()
                .unwrap()
                .push((index, data.len()));
            Ok(())
        })
        .unwrap();

    assert_eq!(
        *page_cache_chunks.lock().unwrap(),
        *direct_chunks.lock().unwrap()
    );
}
