    use super::*;
    use crate::config::{AppConfig, LoadedConfig};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("{}-{}-{}", prefix, std::process::id(), nanos))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn indexed_writer_reorders_output_by_index() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-index");
        let writer = ParallelWriter::indexed(&cfg, "write", &path, IOMode::PageCache, 3).unwrap();
        writer.write_at_index(2, b"ccc".to_vec()).unwrap();
        writer.write_at_index(0, b"a".to_vec()).unwrap();
        writer.write_at_index(1, b"bb".to_vec()).unwrap();
        let report = writer.finish().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"abbccc".to_vec());
        assert_eq!(
            report.block_ranges,
            vec![
                BlockRange { offset: 0, len: 1 },
                BlockRange { offset: 1, len: 2 },
                BlockRange { offset: 3, len: 3 },
            ]
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_writes_exact_offsets() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 8).unwrap();
        writer.write_at_offset(4, b"efgh".to_vec()).unwrap();
        writer.write_at_offset(0, b"abcd".to_vec()).unwrap();
        let report = writer.finish().unwrap();
        assert_eq!(report.bytes_written, 8);
        assert_eq!(fs::read(&path).unwrap(), b"abcdefgh".to_vec());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn offset_writer_rejects_end_offset_overflow() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset-overflow");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 8).unwrap();
        writer
            .write_at_offset(u64::MAX - 1, b"abcd".to_vec())
            .unwrap();

        let err = writer.finish().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn fixed_size_offset_writer_zero_fills_unwritten_gaps() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-offset-gaps");
        let writer =
            ParallelWriter::fixed_size(&cfg, "write", &path, IOMode::PageCache, 16).unwrap();
        writer.write_at_offset(0, b"abcd".to_vec()).unwrap();
        writer.write_at_offset(12, b"xy".to_vec()).unwrap();

        let report = writer.finish().unwrap();
        assert_eq!(report.bytes_written, 6);
        assert_eq!(
            fs::read(&path).unwrap(),
            b"abcd\0\0\0\0\0\0\0\0xy\0\0".to_vec()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn indexed_writer_rejects_duplicate_indexes() {
        let cfg = crate::config::load_config(None);
        let path = unique_temp_file("fro-parallel-writer-dup");
        let writer = ParallelWriter::indexed(&cfg, "write", &path, IOMode::PageCache, 1).unwrap();
        writer.write_at_index(0, b"a".to_vec()).unwrap();
        writer.write_at_index(0, b"b".to_vec()).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(err.to_string().contains("duplicate write"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn foreach_block_uses_stable_logical_block_size_across_io_modes() {
        let path = unique_temp_file("fro-parallel-file-block-size");
        fs::write(&path, (0..200).map(|i| i as u8).collect::<Vec<_>>()).unwrap();

        let mut app = AppConfig::default();
        app.read.page_cache.block_size = 64 * 1024;
        app.read.direct.block_size = 1024 * 1024;
        let cfg = LoadedConfig::Legacy {
            path: PathBuf::from("unused.json"),
            config: app,
        };

        let page_cache = ParallelFile::open(&cfg, "read", &path, IOMode::PageCache).unwrap();
        let direct = ParallelFile::open(&cfg, "read", &path, IOMode::Direct).unwrap();

        assert_eq!(page_cache.block_size().unwrap(), 1024 * 1024);
        assert_eq!(direct.block_size().unwrap(), 1024 * 1024);

        let page_cache_chunks = Arc::new(Mutex::new(Vec::new()));
        let direct_chunks = Arc::new(Mutex::new(Vec::new()));

        let page_cache_chunks_for_visit = page_cache_chunks.clone();
        page_cache
            .foreach_block(move |index, data| {
                page_cache_chunks_for_visit
                    .lock()
                    .unwrap()
                    .push((index, data.len()));
                Ok(())
            })
            .unwrap();

        let direct_chunks_for_visit = direct_chunks.clone();
        direct
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
        let _ = fs::remove_file(path);
    }

    #[test]
    fn map_reduce_blocks_preserves_block_order_for_reducer() {
        let path = unique_temp_file("fro-parallel-file-map-reduce");
        fs::write(&path, b"abcdefghijkl").unwrap();

        let cfg = crate::config::load_config(None);
        let file = ParallelFile::open(&cfg, "read", &path, IOMode::PageCache).unwrap();
        let parts = file
            .map_reduce_blocks_with_params(
                ResolvedReadParams {
                    use_direct: false,
                    num_threads: 2,
                    block_size: 4,
                    qd: 1,
                },
                |block_index, data| Ok((block_index, data.to_vec())),
                |parts, report| {
                    assert_eq!(report.file_size, 12);
                    Ok(parts)
                },
            )
            .unwrap();

        assert_eq!(
            parts,
            vec![
                (0, b"abcd".to_vec()),
                (1, b"efgh".to_vec()),
                (2, b"ijkl".to_vec()),
            ]
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn parallel_stream_identity_smoke() {
        let cfg = crate::config::load_config(None);
        let input_path = unique_temp_file("fro-parallel-stream-input");
        let output_path = unique_temp_file("fro-parallel-stream-output");
        // create input with 15 bytes
        let data: Vec<u8> = (0..15).collect();
        fs::write(&input_path, &data).unwrap();

        // processor: identity copy
        let processor = |input: &[u8], out: &mut [u8]| -> std::io::Result<usize> {
            out[..input.len()].clone_from_slice(input);
            Ok(input.len())
        };

        // read block size 4, write block size 4
        let _report =
            ParallelStream::map_file_fixed_size(&cfg, &input_path, &output_path, 4, 4, processor)
                .unwrap();
        // read produced bytes prefix (should equal input length)
        let produced = fs::read(&output_path).unwrap();
        assert_eq!(&produced[..15], &data[..]);

        let _ = fs::remove_file(input_path);
        let _ = fs::remove_file(output_path);
    }

    // simple base64 encoder used for tests (scalar)
    fn encode_base64_simple(input: &[u8]) -> Vec<u8> {
        const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut i = 0;
        while i + 3 <= input.len() {
            let a = input[i] as u32;
            let b = input[i + 1] as u32;
            let c = input[i + 2] as u32;
            let n = (a << 16) | (b << 8) | c;
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(T[((n >> 6) & 0x3F) as usize]);
            out.push(T[(n & 0x3F) as usize]);
            i += 3;
        }
        let rem = input.len() - i;
        if rem == 1 {
            let a = input[i] as u32;
            let n = a << 16;
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(b'=');
            out.push(b'=');
        } else if rem == 2 {
            let a = input[i] as u32;
            let b = input[i + 1] as u32;
            let n = (a << 16) | (b << 8);
            out.push(T[((n >> 18) & 0x3F) as usize]);
            out.push(T[((n >> 12) & 0x3F) as usize]);
            out.push(T[((n >> 6) & 0x3F) as usize]);
            out.push(b'=');
        }
        out
    }

    #[test]
    fn parallel_stream_base64_partial_last_block() {
        let cfg = crate::config::load_config(None);
        let input_path = unique_temp_file("fro-parallel-stream-input-b64");
        let output_path = unique_temp_file("fro-parallel-stream-output-b64");
        // create input with 7 bytes so last block is 3 bytes when block_size=4
        let data: Vec<u8> = (0..7).collect();
        fs::write(&input_path, &data).unwrap();

        // read block 4, write block size (max encoded length for a full block == 8)
        let read_block = 4u64;
        let write_block = 8usize;

        let processor = |input: &[u8], out: &mut [u8]| -> std::io::Result<usize> {
            let enc = encode_base64_simple(input);
            out[..enc.len()].clone_from_slice(&enc);
            Ok(enc.len())
        };

        let _report = ParallelStream::map_file_fixed_size(
            &cfg,
            &input_path,
            &output_path,
            read_block,
            write_block,
            processor,
        )
        .unwrap();

        // compute expected concatenated encoding
        let mut expected = Vec::new();
        let mut pos = 0usize;
        while pos < data.len() {
            let end = (pos + read_block as usize).min(data.len());
            expected.extend_from_slice(&encode_base64_simple(&data[pos..end]));
            pos = end;
        }

        let produced = fs::read(&output_path).unwrap();
        // compare only the prefix equal to expected length
        assert_eq!(&produced[..expected.len()], &expected[..]);

        let _ = fs::remove_file(input_path);
        let _ = fs::remove_file(output_path);
    }
