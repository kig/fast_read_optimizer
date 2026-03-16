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
fn offset_writer_can_preserve_existing_prefix_and_suffix() {
    let tmp = unique_temp_dir("fro-public-api-offset");
    fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("target.bin");
    fs::write(&path, b"abcdefghij").unwrap();

    let writer = fro::offset_writer_with_options(&path, 10, fro::IOMode::PageCache, false).unwrap();
    writer.write_at_offset(3, b"XYZ".to_vec()).unwrap();
    let report = writer.finish().unwrap();

    assert_eq!(report.bytes_written, 6);
    assert_eq!(fs::read(path).unwrap(), b"abcXYZghij".to_vec());
}
