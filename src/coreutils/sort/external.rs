mod packed;

use super::compare::{compare_line_bytes, compare_output_lines};
use super::*;
use memchr::memchr_iter;
pub(crate) use packed::{sort_inputs, SortCheckFailure, SortCheckResult};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufReader as StdBufReader, BufWriter as StdBufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const SORT_MEM_LIMIT_ENV: &str = "FRO_SORT_MAX_IN_MEMORY_BYTES";
const SORT_MIN_SPILL_CHUNK_BYTES: u64 = 8 << 20;
const SORT_TINY_REGULAR_FAST_PATH_BYTES: u64 = 64 << 10;
const SORT_DEFAULT_MAX_IN_MEMORY_BYTES: u64 = 64 << 20;
const SORT_DEFAULT_MAX_STREAMING_MEMORY_BYTES: u64 = 128 << 20;
const SORT_BYTEWISE_PACKED_FAST_MAX_BYTES: u64 = u32::MAX as u64;
const SORT_PARALLEL_LINE_BUILD_THRESHOLD_BYTES: u64 = 64 << 20;
const SORT_PARALLEL_OUTPUT_THRESHOLD_BYTES: u64 = 64 << 20;
const SORT_PARALLEL_OUTPUT_CHUNK_BYTES: usize = 1 << 20;
const SORT_PARALLEL_MAX_THREADS: usize = 8;
const SORT_STREAM_BLOCK_SIZE: usize = 2 << 20;

pub(super) fn parse_sort_buffer_size(value: &str, option_name: &str) -> io::Result<u64> {
    let trimmed = value.trim();
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {option_name} argument '{value}'"),
        )
    };
    if trimmed.is_empty() {
        return Err(invalid());
    }

    if let Some(percent) = trimmed.strip_suffix('%') {
        let amount = percent.parse::<u64>().map_err(|_| invalid())?;
        let base = mem_available_bytes().unwrap_or(SORT_DEFAULT_MAX_STREAMING_MEMORY_BYTES);
        return base
            .checked_mul(amount)
            .and_then(|scaled| scaled.checked_div(100))
            .ok_or_else(invalid);
    }

    let lower = trimmed.to_ascii_lowercase();
    let split = lower
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(lower.len());
    if split == 0 {
        return Err(invalid());
    }
    let amount = lower[..split].parse::<u64>().map_err(|_| invalid())?;
    let multiplier = match lower[split..].trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        "p" | "pb" | "pib" => 1024_u64.pow(5),
        "e" | "eb" | "eib" => 1024_u64.pow(6),
        _ => return Err(invalid()),
    };
    amount.checked_mul(multiplier).ok_or_else(invalid)
}

fn tiny_regular_sort_fast_path_enabled(
    total_bytes: u64,
    io_mode: IOMode,
    output_path: Option<&str>,
) -> bool {
    total_bytes <= SORT_TINY_REGULAR_FAST_PATH_BYTES
        && output_path.is_none()
        && io_mode != IOMode::Direct
}

fn sort_inputs_tiny_regular_fast(
    inputs: &[StreamInput],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
) -> io::Result<u64> {
    let mut total_bytes = 0u64;
    let mut storage = Vec::new();
    let mut lines = Vec::new();
    let mut next_sequence = 0u64;
    for input in inputs {
        let StreamInput::File(path) = input else {
            return Err(io::Error::other(
                "tiny sort fast path requires regular files",
            ));
        };
        let bytes = fs::read(path).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("cannot read '{}': {err}", sort_input_label(input)),
            )
        })?;
        total_bytes = total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("sort input byte count overflow"))?;
        append_input_lines(
            &mut storage,
            &mut lines,
            &bytes,
            &mut next_sequence,
            terminator,
        )?;
    }
    finalize_sorted_lines(&mut lines, &storage, comparator, unique, reverse)?;
    if debug {
        emit_sort_debug_preamble(comparator)?;
    }
    let mut out =
        StdBufWriter::with_capacity(SORT_STREAM_BLOCK_SIZE, fro::command_io::stdout_file()?);
    if debug {
        write_sorted_lines_with_debug(
            &mut out,
            &lines,
            &storage,
            comparator,
            unique,
            RecordTerminator::Newline,
        )
    } else {
        write_sorted_lines(&mut out, &lines, &storage, terminator)
    }
    .map_err(|err| io::Error::new(err.kind(), format!("write failed: {err}")))?;
    Ok(total_bytes)
}

fn sort_inputs_in_memory(
    inputs: &[StreamInput],
    io_mode: IOMode,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
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
        append_input_lines(
            &mut storage,
            &mut lines,
            &bytes,
            &mut next_sequence,
            terminator,
        )?;
    }
    finalize_sorted_lines(&mut lines, &storage, comparator, unique, reverse)?;
    if debug {
        emit_sort_debug_preamble(comparator)?;
    }
    with_output_writer(output_path, io_mode, |out| {
        if debug {
            write_sorted_lines_with_debug(
                out,
                &lines,
                &storage,
                comparator,
                unique,
                RecordTerminator::Newline,
            )
        } else {
            write_sorted_lines(out, &lines, &storage, terminator)
        }
    })?;
    Ok(total_bytes)
}

pub(super) fn check_input_sorted(
    input: &StreamInput,
    io_mode: IOMode,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    terminator: RecordTerminator,
) -> io::Result<SortCheckResult> {
    let mut checker = SortCheckState::new(comparator, unique, reverse, terminator);
    let disorder_result = |total_bytes, err: io::Error| {
        if let Some(disorder) = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<SortCheckDisorder>())
        {
            Ok(SortCheckResult {
                total_bytes,
                disorder: Some(SortCheckFailure {
                    line_number: disorder.line_number,
                    line: disorder.line.clone(),
                }),
            })
        } else {
            Err(io::Error::new(
                err.kind(),
                format!("cannot read '{}': {err}", sort_input_label(input)),
            ))
        }
    };
    match visit_ordered_input_counted(input, io_mode, |block| checker.push_block(block)) {
        Ok(total_bytes) => match checker.finish() {
            Ok(()) => Ok(SortCheckResult {
                total_bytes,
                disorder: None,
            }),
            Err(err) => disorder_result(total_bytes, err),
        },
        Err(err) => disorder_result(0, err),
    }
}

pub(super) fn sort_memory_budget_bytes(buffer_size_override: Option<u64>) -> io::Result<u64> {
    if let Some(value) = buffer_size_override {
        return Ok(value);
    }
    if let Ok(value) = std::env::var(SORT_MEM_LIMIT_ENV) {
        return value.parse::<u64>().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{SORT_MEM_LIMIT_ENV} must be an integer byte count: {err}"),
            )
        });
    }
    Ok(mem_available_bytes()
        .unwrap_or(u64::MAX)
        .min(SORT_DEFAULT_MAX_STREAMING_MEMORY_BYTES))
}

fn sort_in_memory_limit_bytes(memory_budget: u64) -> u64 {
    memory_budget.min(SORT_DEFAULT_MAX_IN_MEMORY_BYTES)
}

pub(super) fn mem_available_bytes() -> Option<u64> {
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

fn sort_parallel_threads(parallel_override: Option<usize>) -> usize {
    parallel_override
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)
        })
        .max(1)
        .min(SORT_PARALLEL_MAX_THREADS)
}

struct SortCheckState {
    comparator: SortComparator,
    unique: bool,
    reverse: bool,
    terminator: RecordTerminator,
    previous: Option<Vec<u8>>,
    carry: Vec<u8>,
    line_number: u64,
}

impl SortCheckState {
    fn new(
        comparator: &SortComparator,
        unique: bool,
        reverse: bool,
        terminator: RecordTerminator,
    ) -> Self {
        Self {
            comparator: comparator.clone(),
            unique,
            reverse,
            terminator,
            previous: None,
            carry: Vec::new(),
            line_number: 0,
        }
    }

    fn push_block(&mut self, block: &[u8]) -> io::Result<()> {
        let mut consumed = 0usize;
        for separator in memchr_iter(self.terminator.byte(), block) {
            self.carry.extend_from_slice(&block[consumed..separator]);
            self.finish_line()?;
            consumed = separator + 1;
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
            let compare = compare_line_bytes(
                previous,
                &current,
                &self.comparator,
                self.unique,
                self.reverse,
            );
            let strict_duplicate =
                self.unique && same_sort_key(previous, &current, &self.comparator);
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
        let mut out = std::io::BufWriter::with_capacity(
            SORT_STREAM_BLOCK_SIZE,
            fro::command_io::stdout_file()?,
        );
        write_fn(&mut out).map_err(|err| io::Error::new(err.kind(), format!("write failed: {err}")))
    }
}

include!("external/spill.rs");
