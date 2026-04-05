use super::*;
use super::execution::{benchmark_block_size, benchmark_uring_qd};

    use crate::config::AppConfig;
    use std::fs;
    use std::path::PathBuf;

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
    }

    #[test]
    fn load_file_to_memory_round_trips_bytes() {
        let path = unique_temp_file("fro-load");
        let data = (0..(512 * 1024 + 1234))
            .map(|i| ((i * 17) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let loaded = load_file_to_memory(
            path.to_str().unwrap(),
            4,
            128 * 1024,
            2,
            2,
            512 * 1024,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(loaded.bytes_read, data.len() as u64);
        assert_eq!(loaded.data.as_slice(), data.as_slice());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn measure_file_load_to_memory_mmap_reports_full_length() {
        let path = unique_temp_file("fro-load-mmap");
        let data = (0..(256 * 1024 + 321))
            .map(|i| ((i * 13) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let bytes = measure_file_load_to_memory(
            path.to_str().unwrap(),
            2,
            128 * 1024,
            2,
            2,
            256 * 1024,
            2,
            IOMode::PageCache,
            ReadToMemoryMode::Mmap,
            ReadToMemoryOptions::default(),
        )
        .unwrap();

        assert_eq!(bytes, data.len() as u64);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn measure_file_load_to_memory_multiple_targets_reports_full_length() {
        let path = unique_temp_file("fro-load-multi-target");
        let data = (0..(512 * 1024 + 777))
            .map(|i| ((i * 29) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let bytes = measure_file_load_to_memory(
            path.to_str().unwrap(),
            4,
            128 * 1024,
            2,
            2,
            256 * 1024,
            2,
            IOMode::PageCache,
            ReadToMemoryMode::MultipleTargetBuffers,
            ReadToMemoryOptions::default(),
        )
        .unwrap();

        assert_eq!(bytes, data.len() as u64);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn mapped_read_buffer_unmaps_prefix_in_page_chunks() {
        let path = unique_temp_file("fro-load-mmap-unmap");
        let data = vec![0x5A; 8192];
        fs::write(&path, &data).unwrap();

        let file = File::open(&path).unwrap();
        let mut mapped =
            MappedReadBuffer::map(&file, data.len(), ReadToMemoryOptions::default()).unwrap();
        assert_eq!(mapped.as_slice().len(), 8192);

        let unmapped = mapped.unmap_prefix(5000).unwrap();
        assert_eq!(unmapped, 4096);
        assert_eq!(mapped.as_slice().len(), 4096);
        assert!(mapped.as_slice().iter().all(|&b| b == 0x5A));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_to_memory_defaults_use_auto_hugepages_policy() {
        let options = ReadToMemoryOptions::default();
        assert_eq!(options.hugepages, HugepageAdvice::Auto);
        assert!(!options.measure_unmap_time);
    }

    #[test]
    fn auto_hugepages_disable_small_files() {
        let options = ReadToMemoryOptions::default();
        assert!(!options.use_hugepages_for_len(HUGEPAGE_MIN_FILE_LEN - 1));
        assert!(options.use_hugepages_for_len(HUGEPAGE_MIN_FILE_LEN));
    }

    #[test]
    fn auto_to_memory_mode_uses_direct_loader_when_direct_is_forced() {
        assert_eq!(
            resolve_to_memory_mode("/dev/null", IOMode::Direct, ReadToMemoryMode::Auto),
            ReadToMemoryMode::PagedSharedBuffer
        );
    }

    #[test]
    fn auto_to_memory_mode_uses_mmap_when_page_cache_is_forced() {
        assert_eq!(
            resolve_to_memory_mode("/dev/null", IOMode::PageCache, ReadToMemoryMode::Auto),
            ReadToMemoryMode::Mmap
        );
    }

    #[test]
    fn auto_to_memory_mode_treats_empty_files_as_cached() {
        let path = unique_temp_file("fro-load-auto-empty");
        fs::write(&path, b"").unwrap();
        assert_eq!(
            resolve_to_memory_mode(path.to_str().unwrap(), IOMode::Auto, ReadToMemoryMode::Auto),
            ReadToMemoryMode::Mmap
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn auto_lift_mode_prefers_page_cache_for_resident_file() {
        assert!(auto_lift_mode_for_residency(false) == IOMode::Direct);
        assert!(auto_lift_mode_for_residency(true) == IOMode::PageCache);
    }

    #[test]
    fn benchmark_block_size_caps_at_one_megabyte() {
        assert_eq!(benchmark_block_size(4096), 4096);
        assert_eq!(benchmark_block_size(80 * 1024 * 1024), 1024 * 1024);
    }

    #[test]
    fn benchmark_uring_qd_scales_up_to_four() {
        assert_eq!(benchmark_uring_qd(4 * 1024), 1);
        assert_eq!(benchmark_uring_qd(1024 * 1024), 1);
        assert_eq!(benchmark_uring_qd(3 * 1024 * 1024), 3);
        assert_eq!(benchmark_uring_qd(80 * 1024 * 1024), 4);
    }

    #[test]
    fn resolve_reader_params_for_mode_uses_config_mode() {
        let path = unique_temp_file("fro-load-config");
        let data = vec![0x5A; 300 * 1024 * 1024];
        fs::write(&path, &data).unwrap();

        let mut cfg = AppConfig::default();
        cfg.hash.page_cache = IOParams {
            num_threads: 9,
            block_size: 2 * 1024 * 1024,
            qd: 3,
        };

        let loaded = LoadedConfig::Legacy {
            path: PathBuf::from("fro.json"),
            config: cfg,
        };

        let params = resolve_reader_params_for_mode(
            &loaded,
            "hash",
            path.to_str().unwrap(),
            IOMode::PageCache,
        )
        .unwrap();
        assert_eq!(params.num_threads, 9);
        assert_eq!(params.block_size, 2 * 1024 * 1024);
        assert_eq!(params.qd, 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn map_file_blocks_runs_callback_in_file_order() {
        let path = unique_temp_file("fro-map-blocks");
        let block_size = 128 * 1024;
        let data = (0..(block_size * 3 + 77))
            .map(|i| ((i * 19) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let mapped = map_file_blocks(
            path.to_str().unwrap(),
            3,
            block_size as u64,
            2,
            2,
            block_size as u64,
            2,
            IOMode::PageCache,
            |block| Ok::<_, std::io::Error>((block.block_index, block.data.len(), block.offset)),
        )
        .unwrap();

        assert_eq!(mapped.bytes_read, data.len() as u64);
        assert_eq!(mapped.blocks.len(), 4);
        assert_eq!(mapped.blocks[0], (0, block_size, 0));
        assert_eq!(mapped.blocks[1], (1, block_size, block_size as u64));
        assert_eq!(mapped.blocks[2], (2, block_size, (block_size * 2) as u64));
        assert_eq!(mapped.blocks[3], (3, 77, (block_size * 3) as u64));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn visit_file_blocks_visits_every_block() {
        let path = unique_temp_file("fro-visit-blocks");
        let block_size = 64 * 1024;
        let data = (0..(block_size * 2 + 55))
            .map(|i| ((i * 23) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &data).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<(usize, usize)>::new()));
        let seen_for_visit = seen.clone();
        let (bytes_read, file_size, params) = visit_file_blocks(
            path.to_str().unwrap(),
            2,
            block_size as u64,
            2,
            2,
            block_size as u64,
            2,
            IOMode::PageCache,
            move |block| {
                seen_for_visit
                    .lock()
                    .unwrap()
                    .push((block.block_index, block.data.len()));
                Ok::<_, std::io::Error>(())
            },
        )
        .unwrap();

        let mut seen = seen.lock().unwrap().clone();
        seen.sort_unstable();
        assert_eq!(bytes_read, data.len() as u64);
        assert_eq!(file_size, data.len() as u64);
        assert_eq!(params.block_size, block_size as u64);
        assert_eq!(seen, vec![(0, block_size), (1, block_size), (2, 55)]);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn boundary_match_finds_pattern_across_blocks() {
        let prev = GrepScanBlock {
            offset: 0,
            len: 4,
            prefix: b"ab".to_vec(),
            suffix: b"cd".to_vec(),
            matches: Vec::new(),
        };
        let next = GrepScanBlock {
            offset: 4,
            len: 4,
            prefix: b"ef".to_vec(),
            suffix: b"gh".to_vec(),
            matches: Vec::new(),
        };

        assert_eq!(find_boundary_matches(&prev, &next, b"cdef"), vec![2]);
    }

    #[test]
    fn block_offset_rejects_overflow() {
        let err = block_offset(u64::MAX - 7, 2, 8, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_output_offset_rejects_non_usize_offset() {
        let err = checked_output_offset(u64::MAX, 16, 32).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_output_offset_rejects_out_of_bounds_range() {
        let err = checked_output_offset(24, 16, 32).unwrap_err();
        assert!(err.to_string().contains("destination"));
    }

    #[test]
    fn output_slice_mut_writes_only_checked_window() {
        let mut backing = vec![0u8; 32];
        let shared = SharedOutput {
            ptr: backing.as_mut_ptr(),
            len: backing.len(),
        };
        let start = checked_output_offset(8, 12, backing.len()).unwrap();

        unsafe {
            output_slice_mut(&shared, start, 12).fill(0xAB);
        }

        assert!(backing[..8].iter().all(|byte| *byte == 0));
        assert!(backing[8..20].iter().all(|byte| *byte == 0xAB));
        assert!(backing[20..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn output_slice_mut_preserves_non_overlapping_regions() {
        let mut backing = vec![0u8; 24];
        let shared = SharedOutput {
            ptr: backing.as_mut_ptr(),
            len: backing.len(),
        };
        let left = checked_output_offset(0, 8, backing.len()).unwrap();
        let right = checked_output_offset(16, 8, backing.len()).unwrap();

        unsafe {
            output_slice_mut(&shared, left, 8).fill(0x11);
            output_slice_mut(&shared, right, 8).fill(0x22);
        }

        assert_eq!(&backing[..8], &[0x11; 8]);
        assert_eq!(&backing[8..16], &[0; 8]);
        assert_eq!(&backing[16..], &[0x22; 8]);
}
