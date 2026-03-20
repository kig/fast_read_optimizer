use proptest::prelude::*;
use std::fs;
use std::process;
use std::sync::{Arc, Mutex};
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

fn model_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 37 + 11) % 251) as u8).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn property_visit_file_blocks_reconstructs_small_files(
        file_size in 0usize..65536,
        num_threads in 1u64..=4,
        block_size_shift in 0u32..=5,
        qd in 1usize..=3,
    ) {
        let tmp = unique_temp_dir("fro-proptest-read-partition");
        fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("source.bin");
        let block_size = 1024u64 << block_size_shift;
        let data = model_bytes(file_size);
        fs::write(&path, &data).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<(usize, u64, Vec<u8>)>::new()));
        let seen_for_visit = Arc::clone(&seen);
        let (bytes_read, file_len, _) = fro::reader::visit_file_blocks(
            path.to_str().unwrap(),
            num_threads,
            block_size,
            qd,
            num_threads,
            block_size,
            qd,
            fro::IOMode::PageCache,
            move |block| {
                seen_for_visit.lock().unwrap().push((
                    block.block_index,
                    block.offset,
                    block.data.to_vec(),
                ));
                Ok::<_, std::io::Error>(())
            },
        )
        .unwrap();

        let mut seen = seen.lock().unwrap().clone();
        seen.sort_by_key(|entry| entry.0);
        let mut reconstructed = Vec::new();
        let mut next_offset = 0u64;
        for (expected_index, (block_index, offset, bytes)) in seen.iter().enumerate() {
            prop_assert_eq!(*block_index, expected_index);
            prop_assert_eq!(*offset, next_offset);
            prop_assert!(bytes.len() as u64 <= block_size);
            next_offset += bytes.len() as u64;
            reconstructed.extend_from_slice(bytes);
        }

        prop_assert_eq!(bytes_read, data.len() as u64);
        prop_assert_eq!(file_len, data.len() as u64);
        prop_assert_eq!(reconstructed, data);
    }

    #[test]
    fn property_offset_writer_matches_reference_model(
        total_size in 0usize..2048,
        writes in prop::collection::vec((0usize..4096, 1usize..65, any::<u8>()), 0..16),
    ) {
        let tmp = unique_temp_dir("fro-proptest-offset-writer");
        fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("target.bin");
        let writer = fro::offset_writer_with_mode(&path, total_size as u64, fro::IOMode::PageCache).unwrap();
        let mut model = vec![0u8; total_size];
        let mut expected_bytes_written = 0u64;

        for (raw_offset, raw_len, fill) in writes {
            if total_size == 0 {
                break;
            }
            let offset = raw_offset % total_size;
            let remaining = total_size - offset;
            let len = 1 + ((raw_len - 1) % remaining);
            let bytes = vec![fill; len];
            writer.write_at_offset(offset as u64, bytes.clone()).unwrap();
            model[offset..offset + len].copy_from_slice(&bytes);
            expected_bytes_written += len as u64;
        }

        let report = writer.finish().unwrap();
        let on_disk = fs::read(&path).unwrap();
        prop_assert_eq!(report.bytes_written, expected_bytes_written);
        prop_assert_eq!(on_disk, model);
    }
}
