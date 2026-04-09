use super::*;
use memchr::memchr_iter;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufReader as StdBufReader, BufWriter as StdBufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SORT_MEM_LIMIT_ENV: &str = "FRO_SORT_MAX_IN_MEMORY_BYTES";
const SORT_MIN_SPILL_CHUNK_BYTES: u64 = 8 << 20;
const SORT_STREAM_BLOCK_SIZE: usize = 2 << 20;

pub(super) struct SortCheckFailure {
    pub(super) line_number: u64,
    pub(super) line: Vec<u8>,
}

pub(super) struct SortCheckResult {
    pub(super) total_bytes: u64,
    pub(super) disorder: Option<SortCheckFailure>,
}

pub(super) fn sort_inputs(
    inputs: &[StreamInput],
    io_mode: IOMode,
    mode: SortMode,
    unique: bool,
    reverse: bool,
    output_path: Option<&str>,
) -> io::Result<u64> {
    let memory_budget = sort_memory_budget_bytes()?;
    if let Some(total_bytes) = total_regular_input_bytes(inputs)? {
        if total_bytes <= memory_budget {
            return sort_inputs_in_memory(inputs, io_mode, mode, unique, reverse, output_path);
        }
    }
    sort_inputs_streamed(
        inputs,
        io_mode,
        mode,
        unique,
        reverse,
        output_path,
        memory_budget,
    )
}

fn sort_inputs_in_memory(
    inputs: &[StreamInput],
    io_mode: IOMode,
    mode: SortMode,
    unique: bool,
    reverse: bool,
    output_path: Option<&str>,
) -> io::Result<u64> {
    let mut total_bytes = 0u64;
    let mut storage = Vec::new();
    let mut lines = Vec::new();
    let mut next_sequence = 0u64;
    for input in inputs {
        let bytes = loaded_or_stream_bytes(input, io_mode).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("cannot read '{}': {err}", sort_input_label(input)),
            )
        })?;
        total_bytes = total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("sort input byte count overflow"))?;
        append_input_lines(&mut storage, &mut lines, &bytes, &mut next_sequence)?;
    }
    finalize_sorted_lines(&mut lines, &storage, mode, unique, reverse)?;
    with_output_writer(output_path, io_mode, |out| {
        write_sorted_lines(out, &lines, &storage)
    })?;
    Ok(total_bytes)
}

fn sort_inputs_streamed(
    inputs: &[StreamInput],
    io_mode: IOMode,
    mode: SortMode,
    unique: bool,
    reverse: bool,
    output_path: Option<&str>,
    memory_budget: u64,
) -> io::Result<u64> {
    let chunk_target = spill_chunk_target_bytes(memory_budget);
    let mut spill = SpillSorter::new(mode, unique, reverse, chunk_target);
    let mut total_bytes = 0u64;
    for input in inputs {
        let bytes = visit_ordered_input_counted(input, io_mode, |block| spill.push_block(block))
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("cannot read '{}': {err}", sort_input_label(input)),
                )
            })?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("sort input byte count overflow"))?;
    }
    spill.finish(output_path, io_mode)?;
    Ok(total_bytes)
}

pub(super) fn merge_presorted_inputs(
    inputs: &[StreamInput],
    io_mode: IOMode,
    mode: SortMode,
    unique: bool,
    reverse: bool,
    output_path: Option<&str>,
) -> io::Result<u64> {
    let mut temp_files = SpillTempFiles::new();
    let mut next_sequence = 0u64;
    let mut total_bytes = 0u64;
    for input in inputs {
        let path = temp_files.next_chunk_path();
        total_bytes = total_bytes
            .checked_add(
                write_presorted_input_chunk(&path, input, io_mode, &mut next_sequence).map_err(
                    |err| {
                        io::Error::new(
                            err.kind(),
                            format!("cannot read '{}': {err}", sort_input_label(input)),
                        )
                    },
                )?,
            )
            .ok_or_else(|| io::Error::other("sort merge input byte count overflow"))?;
        temp_files.paths.push(path);
    }
    with_output_writer(output_path, io_mode, |out| {
        merge_sorted_chunks(out, &temp_files.paths, mode, unique, reverse)
    })?;
    Ok(total_bytes)
}

pub(super) fn check_input_sorted(
    input: &StreamInput,
    io_mode: IOMode,
    mode: SortMode,
    unique: bool,
    reverse: bool,
) -> io::Result<SortCheckResult> {
    let mut checker = SortCheckState::new(mode, unique, reverse);
    match visit_ordered_input_counted(input, io_mode, |block| checker.push_block(block)) {
        Ok(total_bytes) => {
            checker.finish()?;
            Ok(SortCheckResult {
                total_bytes,
                disorder: None,
            })
        }
        Err(err) => {
            if let Some(disorder) = err
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<SortCheckDisorder>())
            {
                return Ok(SortCheckResult {
                    total_bytes: 0,
                    disorder: Some(SortCheckFailure {
                        line_number: disorder.line_number,
                        line: disorder.line.clone(),
                    }),
                });
            }
            Err(io::Error::new(
                err.kind(),
                format!("cannot read '{}': {err}", sort_input_label(input)),
            ))
        }
    }
}

fn sort_memory_budget_bytes() -> io::Result<u64> {
    if let Ok(value) = std::env::var(SORT_MEM_LIMIT_ENV) {
        return value.parse::<u64>().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{SORT_MEM_LIMIT_ENV} must be an integer byte count: {err}"),
            )
        });
    }
    Ok(mem_available_bytes().unwrap_or(u64::MAX))
}

fn mem_available_bytes() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        let value = line.strip_prefix("MemAvailable:")?;
        let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
        return kib.checked_mul(1024);
    }
    None
}

fn spill_chunk_target_bytes(memory_budget: u64) -> usize {
    let chunk_target = if memory_budget == u64::MAX {
        u64::MAX
    } else {
        memory_budget / 2
    };
    chunk_target
        .max(SORT_MIN_SPILL_CHUNK_BYTES)
        .min(usize::MAX as u64) as usize
}

struct SortCheckState {
    mode: SortMode,
    unique: bool,
    reverse: bool,
    previous: Option<Vec<u8>>,
    carry: Vec<u8>,
    line_number: u64,
}

impl SortCheckState {
    fn new(mode: SortMode, unique: bool, reverse: bool) -> Self {
        Self {
            mode,
            unique,
            reverse,
            previous: None,
            carry: Vec::new(),
            line_number: 0,
        }
    }

    fn push_block(&mut self, block: &[u8]) -> io::Result<()> {
        let mut consumed = 0usize;
        for newline in memchr_iter(b'\n', block) {
            self.carry.extend_from_slice(&block[consumed..newline]);
            self.finish_line()?;
            consumed = newline + 1;
        }
        self.carry.extend_from_slice(&block[consumed..]);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.carry.is_empty() {
            self.finish_line()?;
        }
        Ok(())
    }

    fn finish_line(&mut self) -> io::Result<()> {
        self.line_number = self
            .line_number
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort check line count overflow"))?;
        let current = std::mem::take(&mut self.carry);
        if let Some(previous) = self.previous.as_deref() {
            let compare = compare_line_bytes(previous, &current, self.mode, self.reverse);
            let strict_duplicate = self.unique && same_sort_key(previous, &current, self.mode);
            if compare == Ordering::Greater || strict_duplicate {
                return Err(io::Error::other(SortCheckDisorder {
                    line_number: self.line_number,
                    line: current,
                }));
            }
        }
        self.previous = Some(current);
        Ok(())
    }
}

#[derive(Debug)]
struct SortCheckDisorder {
    line_number: u64,
    line: Vec<u8>,
}

impl std::fmt::Display for SortCheckDisorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sort disorder at line {}", self.line_number)
    }
}

impl std::error::Error for SortCheckDisorder {}

fn total_regular_input_bytes(inputs: &[StreamInput]) -> io::Result<Option<u64>> {
    let mut total = 0u64;
    for input in inputs {
        match input {
            StreamInput::File(path) if is_regular_input_path(path)? => {
                total = total
                    .checked_add(fs::metadata(path)?.len())
                    .ok_or_else(|| io::Error::other("sort regular input byte count overflow"))?;
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(total))
}

fn with_output_writer<F>(
    output_path: Option<&str>,
    io_mode: IOMode,
    mut write_fn: F,
) -> io::Result<()>
where
    F: FnMut(&mut dyn Write) -> io::Result<()>,
{
    if let Some(path) = output_path {
        let mut out = fro::create_with_mode(path, io_mode)?;
        write_fn(&mut out)
            .map_err(|err| io::Error::new(err.kind(), format!("cannot write '{path}': {err}")))
    } else {
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::with_capacity(SORT_STREAM_BLOCK_SIZE, stdout.lock());
        write_fn(&mut out).map_err(|err| io::Error::new(err.kind(), format!("write failed: {err}")))
    }
}

struct SpillSorter {
    mode: SortMode,
    unique: bool,
    reverse: bool,
    chunk_target: usize,
    storage: Vec<u8>,
    lines: Vec<SortLineRef>,
    carry: Vec<u8>,
    next_sequence: u64,
    temp_files: Option<SpillTempFiles>,
}

impl SpillSorter {
    fn new(mode: SortMode, unique: bool, reverse: bool, chunk_target: usize) -> Self {
        Self {
            mode,
            unique,
            reverse,
            chunk_target,
            storage: Vec::new(),
            lines: Vec::new(),
            carry: Vec::new(),
            next_sequence: 0,
            temp_files: None,
        }
    }

    fn push_block(&mut self, block: &[u8]) -> io::Result<()> {
        let mut consumed = 0usize;
        for newline in memchr_iter(b'\n', block) {
            self.carry.extend_from_slice(&block[consumed..newline]);
            self.push_complete_line()?;
            consumed = newline + 1;
        }
        self.carry.extend_from_slice(&block[consumed..]);
        Ok(())
    }

    fn push_complete_line(&mut self) -> io::Result<()> {
        let start = self.storage.len();
        self.storage.extend_from_slice(&self.carry);
        self.lines.push(SortLineRef {
            start,
            len: self.carry.len(),
            sequence: self.next_sequence,
        });
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
        self.carry.clear();
        if self.storage.len() >= self.chunk_target {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.lines.is_empty() {
            return Ok(());
        }
        finalize_sorted_lines(
            &mut self.lines,
            &self.storage,
            self.mode,
            self.unique,
            self.reverse,
        )?;
        let temp_files = self.temp_files.get_or_insert_with(SpillTempFiles::new);
        let path = temp_files.next_chunk_path();
        write_chunk_file(&path, &self.lines, &self.storage)?;
        temp_files.paths.push(path);
        self.storage.clear();
        self.lines.clear();
        Ok(())
    }

    fn finish(mut self, output_path: Option<&str>, io_mode: IOMode) -> io::Result<()> {
        if !self.carry.is_empty() {
            self.push_complete_line()?;
        }
        if self.temp_files.is_none() {
            finalize_sorted_lines(
                &mut self.lines,
                &self.storage,
                self.mode,
                self.unique,
                self.reverse,
            )?;
            return with_output_writer(output_path, io_mode, |out| {
                write_sorted_lines(out, &self.lines, &self.storage)
            });
        }
        self.flush_chunk()?;
        let temp_files = self
            .temp_files
            .take()
            .ok_or_else(|| io::Error::other("missing sort spill temp files"))?;
        with_output_writer(output_path, io_mode, |out| {
            merge_sorted_chunks(out, &temp_files.paths, self.mode, self.unique, self.reverse)
        })
    }
}

struct SpillTempFiles {
    dir: PathBuf,
    paths: Vec<PathBuf>,
    next_index: usize,
}

impl SpillTempFiles {
    fn new() -> Self {
        let mut dir = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        dir.push(format!("fro-sort-{}-{timestamp}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        Self {
            dir,
            paths: Vec::new(),
            next_index: 0,
        }
    }

    fn next_chunk_path(&mut self) -> PathBuf {
        let path = self.dir.join(format!("chunk-{:06}.bin", self.next_index));
        self.next_index += 1;
        path
    }
}

impl Drop for SpillTempFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir(&self.dir);
    }
}

fn write_chunk_record<W: Write>(writer: &mut W, sequence: u64, line: &[u8]) -> io::Result<()> {
    writer.write_all(&sequence.to_le_bytes())?;
    writer.write_all(&(line.len() as u64).to_le_bytes())?;
    writer.write_all(line)
}

fn write_presorted_input_chunk(
    path: &Path,
    input: &StreamInput,
    io_mode: IOMode,
    next_sequence: &mut u64,
) -> io::Result<u64> {
    let file = File::create(path)?;
    let mut writer = StdBufWriter::new(file);
    let mut carry = Vec::new();
    let total_bytes = visit_ordered_input_counted(input, io_mode, |block| {
        let mut consumed = 0usize;
        for newline in memchr_iter(b'\n', block) {
            carry.extend_from_slice(&block[consumed..newline]);
            write_chunk_record(&mut writer, *next_sequence, &carry)?;
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
            carry.clear();
            consumed = newline + 1;
        }
        carry.extend_from_slice(&block[consumed..]);
        Ok(())
    })?;
    if !carry.is_empty() {
        write_chunk_record(&mut writer, *next_sequence, &carry)?;
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
    }
    writer.flush()?;
    Ok(total_bytes)
}

fn write_chunk_file(path: &Path, lines: &[SortLineRef], storage: &[u8]) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = StdBufWriter::new(file);
    for line in lines {
        write_chunk_record(&mut writer, line.sequence, line.bytes(storage))?;
    }
    writer.flush()
}

struct ChunkReader {
    reader: StdBufReader<File>,
}

impl ChunkReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: StdBufReader::with_capacity(SORT_STREAM_BLOCK_SIZE, File::open(path)?),
        })
    }

    fn next_record(&mut self) -> io::Result<Option<ChunkRecord>> {
        let mut header = [0u8; 16];
        let mut filled = 0usize;
        while filled < header.len() {
            let read = self.reader.read(&mut header[filled..])?;
            if read == 0 {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated sort spill header",
                ));
            }
            filled += read;
        }
        let sequence = u64::from_le_bytes(header[..8].try_into().unwrap());
        let len = u64::from_le_bytes(header[8..].try_into().unwrap());
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "sort spill line too large"))?;
        let mut line = vec![0u8; len];
        self.reader.read_exact(&mut line)?;
        Ok(Some(ChunkRecord { line, sequence }))
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ChunkRecord {
    line: Vec<u8>,
    sequence: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct HeapItem {
    record: ChunkRecord,
    chunk_index: usize,
    mode: SortMode,
    unique: bool,
    reverse: bool,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_output_lines(
            &self.record.line,
            self.record.sequence,
            &other.record.line,
            other.record.sequence,
            self.mode,
            self.unique,
            self.reverse,
        )
        .reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_sorted_chunks(
    out: &mut dyn Write,
    paths: &[PathBuf],
    mode: SortMode,
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    let mut readers = paths
        .iter()
        .map(|path| ChunkReader::open(path))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (chunk_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next_record()? {
            heap.push(HeapItem {
                record,
                chunk_index,
                mode,
                unique,
                reverse,
            });
        }
    }

    let mut output_buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    let mut last_written: Option<Vec<u8>> = None;
    while let Some(item) = heap.pop() {
        let should_write = match &last_written {
            Some(previous) if unique => !same_sort_key(previous, &item.record.line, mode),
            _ => true,
        };
        if should_write {
            buffered_write_sort_line(out, &mut output_buffer, &item.record.line)?;
            if unique {
                last_written = Some(item.record.line.clone());
            }
        }
        if let Some(record) = readers[item.chunk_index].next_record()? {
            heap.push(HeapItem {
                record,
                chunk_index: item.chunk_index,
                mode,
                unique,
                reverse,
            });
        }
    }
    flush_sort_output_buffer(out, &mut output_buffer)?;
    out.flush()
}
