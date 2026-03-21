use crate::common::IOMode;
use crate::config::LoadedConfig;
use crate::reader::{
    resolve_reader_params, resolve_reader_params_for_mode, visit_file_blocks,
    visit_file_blocks_with_resolved_params, ResolvedReadParams,
};
use crate::writer::{
    resolve_writer_params_for_mode, OffsetWriter, ResolvedWriteParams, SequentialWriter,
};
use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::{mpsc, Arc, Mutex};

fn default_logical_block_size(config: &LoadedConfig, mode: &str, path: &str) -> u64 {
    let page_cache = config.get_params_for_path(mode, false, path);
    let direct = config.get_params_for_path(mode, true, path);
    page_cache.block_size.max(direct.block_size)
}

fn effective_io_mode_for_block_size(io_mode: IOMode, block_size: u64) -> IOMode {
    if block_size % 4096 == 0 {
        io_mode
    } else {
        IOMode::PageCache
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelReadReport {
    pub bytes_read: u64,
    pub file_size: u64,
    pub params: crate::reader::ResolvedReadParams,
}

#[derive(Debug)]
pub struct ParallelWriteReport {
    pub bytes_written: u64,
    pub block_ranges: Vec<BlockRange>,
    pub write_params: ResolvedWriteParams,
}

#[derive(Clone)]
pub struct ParallelFile {
    config: LoadedConfig,
    mode: String,
    path: String,
    io_mode: IOMode,
    file: Arc<File>,
}

#[derive(Clone)]
pub struct ParallelWriter {
    tx: mpsc::Sender<WriteRequest>,
    finish_handle:
        Arc<Mutex<Option<std::thread::JoinHandle<std::io::Result<ParallelWriteReport>>>>>,
}

enum WriteRequest {
    ByIndex { index: usize, data: Vec<u8> },
    ByOffset { offset: u64, data: Vec<u8> },
}

enum WriterMode {
    ByIndex { block_count: usize },
    ByOffset { total_size: u64, truncate: bool },
}

impl ParallelFile {
    pub fn open(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
    ) -> std::io::Result<Self> {
        Ok(Self {
            config: config.clone(),
            mode: mode.to_string(),
            path: path.to_string(),
            io_mode,
            file: Arc::new(File::open(path)?),
        })
    }

    pub fn len(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn block_count(&self, block_size: u64) -> std::io::Result<usize> {
        if block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block_size must be greater than zero",
            ));
        }
        let file_size = self.len()?;
        Ok(if file_size == 0 {
            0
        } else {
            file_size.div_ceil(block_size) as usize
        })
    }

    pub fn foreach_block_parallel<F>(
        &self,
        block_size: u64,
        visit: F,
    ) -> std::io::Result<ParallelReadReport>
    where
        F: Fn(usize, &[u8]) -> std::io::Result<()> + Send + Sync + 'static,
    {
        let path = self.path.clone();
        let config = self.config.clone();
        let io_mode = effective_io_mode_for_block_size(self.io_mode, block_size);
        let visit = Arc::new(visit);
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        let (bytes_read, file_size, params) = visit_file_blocks(
            &path,
            page_cache.num_threads,
            block_size,
            page_cache.qd,
            direct.num_threads,
            block_size,
            direct.qd,
            io_mode,
            move |block| visit(block.block_index, block.data),
        )?;
        Ok(ParallelReadReport {
            bytes_read,
            file_size,
            params,
        })
    }

    pub fn block_size(&self) -> std::io::Result<u64> {
        Ok(default_logical_block_size(
            &self.config,
            &self.mode,
            &self.path,
        ))
    }

    pub fn foreach_block<F>(&self, visit: F) -> std::io::Result<ParallelReadReport>
    where
        F: Fn(usize, &[u8]) -> std::io::Result<()> + Send + Sync + 'static,
    {
        self.foreach_block_parallel(self.block_size()?, visit)
    }

    pub fn map_reduce_blocks<T, M, R, U>(
        &self,
        block_size: u64,
        map: M,
        reduce: R,
    ) -> std::io::Result<U>
    where
        T: Send + 'static,
        M: Fn(usize, &[u8]) -> std::io::Result<T> + Send + Sync + 'static,
        R: FnOnce(Vec<T>, ParallelReadReport) -> std::io::Result<U>,
    {
        let path = self.path.clone();
        let config = self.config.clone();
        let io_mode = effective_io_mode_for_block_size(self.io_mode, block_size);
        let page_cache = config.get_params_for_path(&self.mode, false, &path);
        let direct = config.get_params_for_path(&self.mode, true, &path);
        let params = resolve_reader_params(
            &path,
            &crate::config::IOParams {
                num_threads: page_cache.num_threads,
                block_size,
                qd: page_cache.qd,
            },
            &crate::config::IOParams {
                num_threads: direct.num_threads,
                block_size,
                qd: direct.qd,
            },
            io_mode,
        )?;
        self.map_reduce_blocks_with_params(params, map, reduce)
    }

    pub fn map_reduce_blocks_with_params<T, M, R, U>(
        &self,
        params: ResolvedReadParams,
        map: M,
        reduce: R,
    ) -> std::io::Result<U>
    where
        T: Send + 'static,
        M: Fn(usize, &[u8]) -> std::io::Result<T> + Send + Sync + 'static,
        R: FnOnce(Vec<T>, ParallelReadReport) -> std::io::Result<U>,
    {
        if params.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "block_size must be greater than zero",
            ));
        }

        let block_count = self.block_count(params.block_size)?;
        let mapped = Arc::new(Mutex::new(
            std::iter::repeat_with(|| None)
                .take(block_count)
                .collect::<Vec<Option<T>>>(),
        ));
        let mapped_slots = Arc::clone(&mapped);
        let path = self.path.clone();

        let (bytes_read, file_size) =
            visit_file_blocks_with_resolved_params(&path, params, move |block| {
                let value = map(block.block_index, block.data)?;
                mapped_slots.lock().unwrap()[block.block_index] = Some(value);
                Ok(())
            })?;

        let mapped = Arc::into_inner(mapped)
            .ok_or_else(|| std::io::Error::other("mapped block storage still shared"))?
            .into_inner()
            .map_err(|_| std::io::Error::other("mapped block storage poisoned"))?;
        let values = mapped
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| std::io::Error::other(format!("missing mapped block {index}")))
            })
            .collect::<std::io::Result<Vec<_>>>()?;

        reduce(
            values,
            ParallelReadReport {
                bytes_read,
                file_size,
                params,
            },
        )
    }

    pub fn foreach_index_parallel<F>(&self, job_count: usize, visit: F) -> std::io::Result<()>
    where
        F: Fn(usize) -> std::io::Result<()> + Send + Sync + 'static,
    {
        let params =
            resolve_reader_params_for_mode(&self.config, &self.mode, &self.path, self.io_mode)?;
        let visit = Arc::new(visit);
        let mut threads = Vec::new();
        for thread_id in 0..params.num_threads {
            let visit = visit.clone();
            threads.push(std::thread::spawn(move || -> std::io::Result<()> {
                let mut job_index = thread_id as usize;
                while job_index < job_count {
                    visit(job_index)?;
                    job_index += params.num_threads as usize;
                }
                Ok(())
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|_| std::io::Error::other("indexed worker thread panicked"))??;
        }
        Ok(())
    }

    pub fn read_range(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        let mut filled = 0usize;
        while filled < len {
            let read = self
                .file
                .read_at(&mut bytes[filled..], offset + filled as u64)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "short read at offset {}: expected {} bytes, got {}",
                        offset, len, filled
                    ),
                ));
            }
            filled += read;
        }
        Ok(bytes)
    }
}

impl ParallelWriter {
    pub fn indexed(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        block_count: usize,
    ) -> std::io::Result<Self> {
        Self::spawn(
            path,
            resolve_writer_params_for_mode(config, mode, path, io_mode),
            io_mode,
            WriterMode::ByIndex { block_count },
        )
    }

    pub fn fixed_size(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        total_size: u64,
    ) -> std::io::Result<Self> {
        Self::fixed_size_with_truncate(config, mode, path, io_mode, total_size, true)
    }

    pub fn fixed_size_with_truncate(
        config: &LoadedConfig,
        mode: &str,
        path: &str,
        io_mode: IOMode,
        total_size: u64,
        truncate: bool,
    ) -> std::io::Result<Self> {
        Self::spawn(
            path,
            resolve_writer_params_for_mode(config, mode, path, io_mode),
            io_mode,
            WriterMode::ByOffset {
                total_size,
                truncate,
            },
        )
    }

    pub fn write_at_index(&self, index: usize, data: Vec<u8>) -> std::io::Result<()> {
        self.tx
            .send(WriteRequest::ByIndex { index, data })
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    pub fn write_at_offset(&self, offset: u64, data: Vec<u8>) -> std::io::Result<()> {
        self.tx
            .send(WriteRequest::ByOffset { offset, data })
            .map_err(|err| std::io::Error::other(err.to_string()))
    }

    pub fn finish(self) -> std::io::Result<ParallelWriteReport> {
        let ParallelWriter { tx, finish_handle } = self;
        drop(tx);
        let handle = finish_handle
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| std::io::Error::other("parallel writer already finished"))?;
        handle
            .join()
            .map_err(|_| std::io::Error::other("parallel writer thread panicked"))?
    }

    fn spawn(
        path: &str,
        write_params: ResolvedWriteParams,
        io_mode: IOMode,
        mode: WriterMode,
    ) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel::<WriteRequest>();
        let output_path = path.to_string();
        let join_handle = std::thread::spawn(move || -> std::io::Result<ParallelWriteReport> {
            match mode {
                WriterMode::ByIndex { block_count } => {
                    let mut out = SequentialWriter::create(
                        &output_path,
                        write_params.qd,
                        write_params.block_size,
                        io_mode,
                    )?;
                    let mut block_ranges = vec![BlockRange { offset: 0, len: 0 }; block_count];
                    let mut pending = BTreeMap::<usize, Vec<u8>>::new();
                    let mut seen = vec![false; block_count];
                    let mut next_index = 0usize;

                    for request in rx {
                        let (index, data) = match request {
                            WriteRequest::ByIndex { index, data } => (index, data),
                            WriteRequest::ByOffset { .. } => {
                                return Err(std::io::Error::other(
                                    "parallel writer is configured for indexed writes",
                                ))
                            }
                        };
                        if index >= block_count {
                            return Err(std::io::Error::other(format!(
                                "block index {} is out of range for {} blocks",
                                index, block_count
                            )));
                        }
                        if seen[index] || pending.contains_key(&index) {
                            return Err(std::io::Error::other(format!(
                                "duplicate write for block index {}",
                                index
                            )));
                        }
                        seen[index] = true;
                        pending.insert(index, data);
                        while let Some(data) = pending.remove(&next_index) {
                            let offset = out.append(&data)?;
                            block_ranges[next_index] = BlockRange {
                                offset,
                                len: data.len() as u64,
                            };
                            next_index += 1;
                        }
                    }

                    if next_index != block_count {
                        return Err(std::io::Error::other(format!(
                            "missing output blocks: wrote {}, expected {}",
                            next_index, block_count
                        )));
                    }
                    out.flush()?;
                    Ok(ParallelWriteReport {
                        bytes_written: out.bytes_written(),
                        block_ranges,
                        write_params,
                    })
                }
                WriterMode::ByOffset {
                    total_size,
                    truncate,
                } => {
                    let mut out = OffsetWriter::with_truncate(
                        &output_path,
                        total_size,
                        write_params.qd,
                        io_mode,
                        truncate,
                    )?;
                    for request in rx {
                        let (offset, data) = match request {
                            WriteRequest::ByOffset { offset, data } => (offset, data),
                            WriteRequest::ByIndex { .. } => {
                                return Err(std::io::Error::other(
                                    "parallel writer is configured for offset writes",
                                ))
                            }
                        };
                        let end_offset =
                            offset.checked_add(data.len() as u64).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "write end offset overflowed",
                                )
                            })?;
                        if end_offset > total_size {
                            return Err(std::io::Error::other(format!(
                                "write past end of file: offset {} len {} total {}",
                                offset,
                                data.len(),
                                total_size
                            )));
                        }
                        out.write_at(offset, &data)?;
                    }
                    out.flush()?;
                    Ok(ParallelWriteReport {
                        bytes_written: out.bytes_written(),
                        block_ranges: Vec::new(),
                        write_params,
                    })
                }
            }
        });

        Ok(Self {
            tx,
            finish_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
    }
}

#[cfg(test)]
mod tests {
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
}
