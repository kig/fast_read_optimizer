use super::*;
use memchr::memchr_iter;
use std::io::{self, Write};
use std::thread;
use stringzilla::stringzilla as sz;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PackedSortLineRef {
    start: u32,
    len: u32,
}

impl PackedSortLineRef {
    fn bytes<'a>(self, storage: &'a [u8]) -> &'a [u8] {
        &storage[self.start as usize..self.start as usize + self.len as usize]
    }
}

pub(crate) struct SortCheckFailure {
    pub(crate) line_number: u64,
    pub(crate) line: Vec<u8>,
}

pub(crate) struct SortCheckResult {
    pub(crate) total_bytes: u64,
    pub(crate) disorder: Option<SortCheckFailure>,
}

pub(crate) fn sort_inputs(
    inputs: &[StreamInput],
    io_mode: IOMode,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    output_path: Option<&str>,
    temporary_directory: Option<&Path>,
    compress_program: Option<&str>,
    buffer_size_override: Option<u64>,
    batch_size: Option<usize>,
    parallel_override: Option<usize>,
) -> io::Result<u64> {
    let configured_budget = sort_memory_budget_bytes(buffer_size_override)?;
    let in_memory_limit = sort_in_memory_limit_bytes(configured_budget);
    if let Some(total_bytes) = total_regular_input_bytes(inputs)? {
        if tiny_regular_sort_fast_path_enabled(total_bytes, io_mode, output_path) {
            return sort_inputs_tiny_regular_fast(
                inputs, comparator, unique, reverse, debug, terminator,
            );
        }
        if !debug
            && bytewise_packed_fast_path_enabled(
                inputs,
                total_bytes,
                comparator,
                temporary_directory,
            )?
        {
            return sort_inputs_bytewise_packed_fast(
                inputs,
                io_mode,
                unique,
                reverse,
                terminator,
                output_path,
                parallel_override,
            );
        }
        if total_bytes <= in_memory_limit {
            return sort_inputs_in_memory(
                inputs,
                io_mode,
                comparator,
                unique,
                reverse,
                debug,
                terminator,
                output_path,
            );
        }
        return sort_inputs_streamed(
            inputs,
            io_mode,
            comparator,
            unique,
            reverse,
            debug,
            terminator,
            output_path,
            configured_budget,
            temporary_directory,
            compress_program,
            batch_size,
        );
    }
    sort_inputs_streamed(
        inputs,
        io_mode,
        comparator,
        unique,
        reverse,
        debug,
        terminator,
        output_path,
        configured_budget,
        temporary_directory,
        compress_program,
        batch_size,
    )
}

pub(super) fn bytewise_packed_fast_path_enabled(
    inputs: &[StreamInput],
    total_bytes: u64,
    comparator: &SortComparator,
    temporary_directory: Option<&Path>,
) -> io::Result<bool> {
    if comparator.mode != SortMode::Bytewise
        || comparator.has_key_selection()
        || comparator.dictionary_order
        || comparator.ignore_case
        || comparator.ignore_leading_blanks
        || comparator.ignore_nonprinting
        || total_bytes > SORT_BYTEWISE_PACKED_FAST_MAX_BYTES
        || inputs.len() != 1
        || temporary_directory.is_some()
    {
        return Ok(false);
    }
    match &inputs[0] {
        StreamInput::File(path) => is_regular_input_path(path),
        StreamInput::Stdin { .. } => Ok(false),
    }
}

pub(super) fn sort_inputs_bytewise_packed_fast(
    inputs: &[StreamInput],
    io_mode: IOMode,
    unique: bool,
    reverse: bool,
    terminator: RecordTerminator,
    output_path: Option<&str>,
    parallel_override: Option<usize>,
) -> io::Result<u64> {
    let input = inputs
        .first()
        .ok_or_else(|| io::Error::other("missing sort input"))?;
    let storage = loaded_or_stream_bytes(input, io_mode).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("cannot read '{}': {err}", sort_input_label(input)),
        )
    })?;
    let total_bytes = storage.len() as u64;
    let mut lines = build_packed_line_refs(&storage, terminator, parallel_override)?;
    finalize_packed_sorted_lines(&mut lines, &storage, unique, reverse)?;
    with_output_writer(output_path, io_mode, |out| {
        write_sorted_packed_lines(out, &lines, &storage, terminator, parallel_override)
    })?;
    Ok(total_bytes)
}

fn build_packed_line_refs(
    storage: &[u8],
    terminator: RecordTerminator,
    parallel_override: Option<usize>,
) -> io::Result<Vec<PackedSortLineRef>> {
    if storage.len() > SORT_BYTEWISE_PACKED_FAST_MAX_BYTES as usize {
        return Err(io::Error::other(
            "sort packed fast path input exceeds 4 GiB",
        ));
    }
    let threads = super::sort_parallel_threads(parallel_override);
    let terminator = terminator.byte();
    if threads <= 1 || storage.len() < SORT_PARALLEL_LINE_BUILD_THRESHOLD_BYTES as usize {
        return build_packed_line_refs_range(storage, 0, storage, terminator);
    }
    let mut ranges = Vec::new();
    let chunk = storage.len().div_ceil(threads);
    let mut start = 0usize;
    while start < storage.len() {
        let mut end = (start + chunk).min(storage.len());
        if end < storage.len() {
            while end < storage.len() && storage[end - 1] != terminator {
                end += 1;
            }
        }
        ranges.push((start, end.min(storage.len())));
        start = end.min(storage.len());
    }
    let results = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ranges.len());
        for (start, end) in ranges.iter().copied() {
            let chunk = &storage[start..end];
            handles
                .push(scope.spawn(move || {
                    build_packed_line_refs_range(chunk, start, storage, terminator)
                }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<io::Result<Vec<_>>>()
    })?;
    let total = results.iter().map(Vec::len).sum();
    let mut merged = Vec::with_capacity(total);
    for refs in results {
        merged.extend(refs);
    }
    Ok(merged)
}

fn build_packed_line_refs_range(
    chunk: &[u8],
    global_start: usize,
    storage: &[u8],
    terminator: u8,
) -> io::Result<Vec<PackedSortLineRef>> {
    let mut refs = Vec::with_capacity(chunk.len() / 24);
    let mut line_start = 0usize;
    for separator in memchr_iter(terminator, chunk) {
        refs.push(PackedSortLineRef {
            start: u32::try_from(global_start + line_start)
                .map_err(|_| io::Error::other("sort packed line start overflow"))?,
            len: u32::try_from(separator - line_start)
                .map_err(|_| io::Error::other("sort packed line length overflow"))?,
        });
        line_start = separator + 1;
    }
    if global_start + chunk.len() == storage.len() && line_start < chunk.len() {
        refs.push(PackedSortLineRef {
            start: u32::try_from(global_start + line_start)
                .map_err(|_| io::Error::other("sort packed tail start overflow"))?,
            len: u32::try_from(chunk.len() - line_start)
                .map_err(|_| io::Error::other("sort packed tail length overflow"))?,
        });
    }
    Ok(refs)
}

fn finalize_packed_sorted_lines(
    lines: &mut Vec<PackedSortLineRef>,
    storage: &[u8],
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    sort_packed_line_refs(lines, storage)?;
    if unique {
        lines.dedup_by(|left, right| left.bytes(storage) == right.bytes(storage));
    }
    if reverse {
        lines.reverse();
    }
    Ok(())
}

fn sort_packed_line_refs(lines: &mut [PackedSortLineRef], storage: &[u8]) -> io::Result<()> {
    if lines.len() < 2 {
        return Ok(());
    }
    let snapshot = lines.to_vec();
    let mut order = vec![0 as sz::SortedIdx; snapshot.len()];
    sz::argsort_permutation_by(|idx| snapshot[idx].bytes(storage), &mut order)
        .map_err(|status| io::Error::other(format!("StringZilla sort failed: {status:?}")))?;
    for (dst, sorted_idx) in lines.iter_mut().zip(order.into_iter()) {
        *dst = snapshot[sorted_idx];
    }
    Ok(())
}

fn write_sorted_packed_lines(
    out: &mut dyn Write,
    lines: &[PackedSortLineRef],
    storage: &[u8],
    terminator: RecordTerminator,
    parallel_override: Option<usize>,
) -> io::Result<()> {
    if lines.is_empty() {
        return out.flush();
    }
    let threads = super::sort_parallel_threads(parallel_override);
    if storage.len() < SORT_PARALLEL_OUTPUT_THRESHOLD_BYTES as usize || threads <= 1 {
        let mut buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
        for line in lines {
            buffered_write_sort_line(out, &mut buffer, line.bytes(storage), terminator)?;
        }
        flush_sort_output_buffer(out, &mut buffer)?;
        return out.flush();
    }

    let chunk_ranges = build_output_chunk_ranges(lines);
    let threads = threads.min(chunk_ranges.len().max(1));
    let term = terminator.byte();
    let mut next_chunk = 0usize;
    while next_chunk < chunk_ranges.len() {
        let window_end = (next_chunk + threads).min(chunk_ranges.len());
        let buffers = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(window_end - next_chunk);
            for &(start, end) in &chunk_ranges[next_chunk..window_end] {
                let lines = &lines[start..end];
                handles.push(scope.spawn(move || build_sorted_output_chunk(lines, storage, term)));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        for buffer in buffers {
            out.write_all(&buffer)?;
        }
        next_chunk = window_end;
    }
    out.flush()
}

fn build_output_chunk_ranges(lines: &[PackedSortLineRef]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < lines.len() {
        let mut used = 0usize;
        let mut end = start;
        while end < lines.len() {
            let next = used.saturating_add(lines[end].len as usize + 1);
            if next > SORT_PARALLEL_OUTPUT_CHUNK_BYTES && end > start {
                break;
            }
            used = next;
            end += 1;
        }
        ranges.push((start, end));
        start = end;
    }
    ranges
}

fn build_sorted_output_chunk(
    lines: &[PackedSortLineRef],
    storage: &[u8],
    terminator: u8,
) -> Vec<u8> {
    let chunk_len = lines
        .iter()
        .map(|line| line.len as usize + 1)
        .sum::<usize>();
    let mut buffer = Vec::with_capacity(chunk_len);
    for line in lines {
        buffer.extend_from_slice(line.bytes(storage));
        buffer.push(terminator);
    }
    buffer
}
