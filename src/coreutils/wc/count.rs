use super::*;
use std::os::unix::io::AsRawFd;
use unicode_width::UnicodeWidthChar;

fn fast_count_fd<R: AsRawFd>(reader: &mut R) -> Option<u64> {
    let fd = reader.as_raw_fd();
    unsafe {
        let mut s = std::mem::zeroed::<stat>();
        if fstat(fd, &mut s) != 0 {
            return None;
        }

        return match s.st_mode & S_IFMT {
            // Path 1: Regular File (Instant)
            S_IFREG => Some(s.st_size as u64),

            // Path 2: Pipe (Zero-Copy)
            S_IFIFO => {
                let dev_null = std::fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/null")
                    .ok()?;
                let mut noop = |_bytes: u64| Ok(());
                copy_fd_to_fd_splice_counted(fd, dev_null.as_raw_fd(), &mut noop).ok()?
            }

            // Path 3: Fallback (Standard Read)
            _ => None,
        };
    }
}

fn get_regular_file_path<R: AsRawFd>(reader: &mut R) -> Option<String> {
    let fd = reader.as_raw_fd();
    unsafe {
        let mut s = std::mem::zeroed::<stat>();
        if fstat(fd, &mut s) != 0 {
            return None;
        }

        return match s.st_mode & S_IFMT {
            S_IFREG => Some(format!("/proc/self/fd/{}", fd)),

            _ => None,
        };
    }
}

const WC_STREAM_BLOCK_SIZE: usize = 2 << 20;
use libc::{madvise, MADV_HUGEPAGE};

fn get_aligned_wc_block() -> Vec<u8> {
    let huge_page_size = 2 * 1024 * 1024;
    let capacity = huge_page_size;
    let layout = std::alloc::Layout::from_size_align(capacity, huge_page_size)
        .expect("Invalid layout for huge pages");

    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        return vec![0u8; capacity];
    }

    unsafe {
        let _ret = madvise(ptr as *mut libc::c_void, capacity, MADV_HUGEPAGE);
    }
    let out_vec = unsafe { Vec::from_raw_parts(ptr, WC_STREAM_BLOCK_SIZE, capacity) };

    out_vec
}

#[derive(Debug, Clone, Copy)]
pub(super) struct WcCountOptions {
    pub(super) lines: bool,
    pub(super) words: bool,
    pub(super) chars: bool,
    pub(super) bytes: bool,
    pub(super) max_line_length: bool,
}

pub(super) fn count_wc_block(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if options.words && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: guarded by runtime AVX2 detection.
        return unsafe { count_wc_block_avx2(block, options) };
    }
    count_wc_block_scalar(block, options)
}

pub(super) fn count_wc_block_scalar(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    let bytes = if options.bytes { block.len() as u64 } else { 0 };

    if !options.words {
        let lines = if options.lines {
            memchr_iter(b'\n', block).count() as u64
        } else {
            0
        };
        return WcBlockCounts {
            lines,
            words: 0,
            chars: 0,
            bytes,
            max_line_length: 0,
            starts_in_word: false,
            ends_in_word: false,
        };
    }

    let mut words = 0_u64;
    let mut lines = 0_u64;
    let mut prev_is_whitespace = true;
    for &byte in block {
        let is_whitespace = WC_WHITESPACE_TABLE[byte as usize] != 0;
        words += u64::from(!is_whitespace && prev_is_whitespace);
        lines += u64::from(options.lines && byte == b'\n');
        prev_is_whitespace = is_whitespace;
    }

    WcBlockCounts {
        lines,
        words,
        chars: 0,
        bytes,
        max_line_length: 0,
        starts_in_word: block.first().is_some_and(|byte| !is_wc_whitespace(*byte)),
        ends_in_word: block.last().is_some_and(|byte| !is_wc_whitespace(*byte)),
    }
}
use libc::{fstat, stat, S_IFIFO, S_IFMT, S_IFREG};

fn utf8_sequence_len(first: u8) -> Option<usize> {
    match first {
        0x00..=0x7f => Some(1),
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

fn is_utf8_continuation(byte: u8) -> bool {
    (0x80..=0xbf).contains(&byte)
}

fn utf8_sequence_is_valid(bytes: &[u8]) -> bool {
    match bytes {
        [0x00..=0x7f] => true,
        [0xc2..=0xdf, second] => is_utf8_continuation(*second),
        [0xe0, second, third] => (0xa0..=0xbf).contains(second) && is_utf8_continuation(*third),
        [0xe1..=0xec | 0xee..=0xef, second, third] => {
            is_utf8_continuation(*second) && is_utf8_continuation(*third)
        }
        [0xed, second, third] => (0x80..=0x9f).contains(second) && is_utf8_continuation(*third),
        [0xf0, second, third, fourth] => {
            (0x90..=0xbf).contains(second)
                && is_utf8_continuation(*third)
                && is_utf8_continuation(*fourth)
        }
        [0xf1..=0xf3, second, third, fourth] => {
            is_utf8_continuation(*second)
                && is_utf8_continuation(*third)
                && is_utf8_continuation(*fourth)
        }
        [0xf4, second, third, fourth] => {
            (0x80..=0x8f).contains(second)
                && is_utf8_continuation(*third)
                && is_utf8_continuation(*fourth)
        }
        _ => false,
    }
}

fn utf8_prefix_can_complete(bytes: &[u8], expected_len: usize) -> bool {
    match expected_len {
        1 => bytes.len() == 1,
        2 => match bytes {
            [0xc2..=0xdf] => true,
            [0xc2..=0xdf, second] => is_utf8_continuation(*second),
            _ => false,
        },
        3 => match bytes {
            [0xe0..=0xef] => true,
            [0xe0, second] => (0xa0..=0xbf).contains(second),
            [0xed, second] => (0x80..=0x9f).contains(second),
            [0xe1..=0xec | 0xee..=0xef, second] => is_utf8_continuation(*second),
            [first, second, third] if utf8_sequence_len(*first) == Some(3) => {
                utf8_sequence_is_valid(&[*first, *second, *third])
            }
            _ => false,
        },
        4 => match bytes {
            [0xf0..=0xf4] => true,
            [0xf0, second] => (0x90..=0xbf).contains(second),
            [0xf4, second] => (0x80..=0x8f).contains(second),
            [0xf1..=0xf3, second] => is_utf8_continuation(*second),
            [first, second, third] if utf8_sequence_len(*first) == Some(4) => match *first {
                0xf0 => (0x90..=0xbf).contains(second) && is_utf8_continuation(*third),
                0xf4 => (0x80..=0x8f).contains(second) && is_utf8_continuation(*third),
                0xf1..=0xf3 => is_utf8_continuation(*second) && is_utf8_continuation(*third),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
}

fn utf8_incomplete_suffix_len(bytes: &[u8]) -> usize {
    let max_suffix = bytes.len().min(3);
    for suffix_len in (1..=max_suffix).rev() {
        let start = bytes.len() - suffix_len;
        let candidate = &bytes[start..];
        let Some(expected_len) = utf8_sequence_len(candidate[0]) else {
            continue;
        };
        if expected_len <= suffix_len {
            continue;
        }
        if utf8_prefix_can_complete(candidate, expected_len) {
            return suffix_len;
        }
    }
    0
}

fn count_complete_utf8_chars(bytes: &[u8]) -> u64 {
    let mut chars = 0_u64;
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let first = bytes[offset];
        match utf8_sequence_len(first) {
            Some(1) => {
                chars += 1;
                offset += 1;
            }
            Some(expected_len) if offset + expected_len <= bytes.len() => {
                let candidate = &bytes[offset..offset + expected_len];
                if utf8_sequence_is_valid(candidate) {
                    chars += 1;
                    offset += expected_len;
                } else {
                    offset += 1;
                }
            }
            Some(_) => break,
            None => offset += 1,
        }
    }
    chars
}

fn count_utf8_chars_with_carry(carry: &mut Vec<u8>, scratch: &mut Vec<u8>, block: &[u8]) -> u64 {
    if carry.is_empty() {
        let suffix_len = utf8_incomplete_suffix_len(block);
        let complete_len = block.len() - suffix_len;
        carry.extend_from_slice(&block[complete_len..]);
        return count_complete_utf8_chars(&block[..complete_len]);
    }

    scratch.clear();
    scratch.extend_from_slice(carry);
    scratch.extend_from_slice(block);
    let suffix_len = utf8_incomplete_suffix_len(scratch);
    let complete_len = scratch.len() - suffix_len;
    let chars = count_complete_utf8_chars(&scratch[..complete_len]);
    carry.clear();
    carry.extend_from_slice(&scratch[complete_len..]);
    chars
}

fn update_max_line_length_from_complete_bytes(
    bytes: &[u8],
    current_line_length: &mut u64,
    max_line_length: &mut u64,
) {
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let first = bytes[offset];
        match utf8_sequence_len(first) {
            Some(1) => {
                match first {
                    b'\n' => {
                        *max_line_length = (*max_line_length).max(*current_line_length);
                        *current_line_length = 0;
                    }
                    b'\t' => *current_line_length += 8 - (*current_line_length % 8),
                    b'\r' => {}
                    byte => {
                        if let Some(display_width) = UnicodeWidthChar::width(byte as char) {
                            *current_line_length += display_width as u64;
                        }
                    }
                }
                offset += 1;
            }
            Some(expected_len) if offset + expected_len <= bytes.len() => {
                let candidate = &bytes[offset..offset + expected_len];
                if utf8_sequence_is_valid(candidate) {
                    let ch = std::str::from_utf8(candidate)
                        .ok()
                        .and_then(|s| s.chars().next())
                        .expect("valid UTF-8 candidate should decode");
                    match ch {
                        '\n' => {
                            *max_line_length = (*max_line_length).max(*current_line_length);
                            *current_line_length = 0;
                        }
                        '\t' => *current_line_length += 8 - (*current_line_length % 8),
                        '\r' => {}
                        _ => {
                            *current_line_length += UnicodeWidthChar::width(ch).unwrap_or(0) as u64
                        }
                    }
                    offset += expected_len;
                } else {
                    offset += 1;
                }
            }
            Some(_) => break,
            None => offset += 1,
        }
    }
}

#[derive(Debug)]
pub(super) struct WcBlockCounts {
    pub(super) lines: u64,
    pub(super) words: u64,
    pub(super) chars: u64,
    pub(super) bytes: u64,
    pub(super) max_line_length: u64,
    pub(super) starts_in_word: bool,
    pub(super) ends_in_word: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WcTotals {
    pub(super) lines: u64,
    pub(super) words: u64,
    pub(super) chars: u64,
    pub(super) bytes: u64,
    pub(super) max_line_length: u64,
}

const fn wc_whitespace_table() -> [u8; 256] {
    let mut table = [0u8; 256];
    table[b' ' as usize] = 1;
    table[b'\t' as usize] = 1;
    table[b'\n' as usize] = 1;
    table[0x0b] = 1;
    table[0x0c] = 1;
    table[b'\r' as usize] = 1;
    table
}

static WC_WHITESPACE_TABLE: [u8; 256] = wc_whitespace_table();

pub(super) fn is_wc_whitespace(byte: u8) -> bool {
    WC_WHITESPACE_TABLE[byte as usize] != 0
}

fn merge_wc_counts(
    totals: &mut WcTotals,
    previous_ended_in_word: &mut bool,
    counts: WcBlockCounts,
    track_words: bool,
) {
    totals.lines += counts.lines;
    totals.words += counts.words;
    totals.chars += counts.chars;
    totals.bytes += counts.bytes;
    totals.max_line_length = totals.max_line_length.max(counts.max_line_length);
    if track_words && *previous_ended_in_word && counts.starts_in_word {
        totals.words = totals.words.saturating_sub(1);
    }
    *previous_ended_in_word = track_words && counts.ends_in_word;
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn count_wc_block_avx2(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_or_si256,
        _mm256_set1_epi8,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_or_si256,
        _mm256_set1_epi8,
    };

    let bytes = if options.bytes { block.len() as u64 } else { 0 };
    let mut lines = 0_u64;
    let mut words = 0_u64;
    let mut prev_is_whitespace = true;
    let mut offset = 0_usize;

    let space = _mm256_set1_epi8(b' ' as i8);
    let tab = _mm256_set1_epi8(b'\t' as i8);
    let newline = _mm256_set1_epi8(b'\n' as i8);
    let vtab = _mm256_set1_epi8(0x0b_i8);
    let formfeed = _mm256_set1_epi8(0x0c_i8);
    let carriage_return = _mm256_set1_epi8(b'\r' as i8);

    while offset + 32 <= block.len() {
        let ptr = block.as_ptr().add(offset) as *const __m256i;
        let chunk = _mm256_loadu_si256(ptr);
        let whitespace = _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_cmpeq_epi8(chunk, space),
                    _mm256_cmpeq_epi8(chunk, tab),
                ),
                _mm256_or_si256(
                    _mm256_cmpeq_epi8(chunk, newline),
                    _mm256_cmpeq_epi8(chunk, vtab),
                ),
            ),
            _mm256_or_si256(
                _mm256_cmpeq_epi8(chunk, formfeed),
                _mm256_cmpeq_epi8(chunk, carriage_return),
            ),
        );
        let whitespace_mask = _mm256_movemask_epi8(whitespace) as u32;
        let previous_mask = (whitespace_mask << 1) | u32::from(prev_is_whitespace);
        words += ((!whitespace_mask) & previous_mask).count_ones() as u64;
        prev_is_whitespace = ((whitespace_mask >> 31) & 1) != 0;

        if options.lines {
            lines += (_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, newline)) as u32).count_ones()
                as u64;
        }
        offset += 32;
    }

    for &byte in &block[offset..] {
        let is_whitespace = is_wc_whitespace(byte);
        words += u64::from(!is_whitespace && prev_is_whitespace);
        if options.lines && byte == b'\n' {
            lines += 1;
        }
        prev_is_whitespace = is_whitespace;
    }

    WcBlockCounts {
        lines,
        words,
        chars: 0,
        bytes,
        max_line_length: 0,
        starts_in_word: block.first().is_some_and(|byte| !is_wc_whitespace(*byte)),
        ends_in_word: block.last().is_some_and(|byte| !is_wc_whitespace(*byte)),
    }
}

pub(super) fn reduce_wc_counts(blocks: &[WcBlockCounts]) -> WcTotals {
    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        chars: 0,
        bytes: 0,
        max_line_length: 0,
    };
    let mut previous_ended_in_word = false;
    for block in blocks {
        merge_wc_counts(
            &mut totals,
            &mut previous_ended_in_word,
            WcBlockCounts {
                lines: block.lines,
                words: block.words,
                chars: block.chars,
                bytes: block.bytes,
                max_line_length: block.max_line_length,
                starts_in_word: block.starts_in_word,
                ends_in_word: block.ends_in_word,
            },
            true,
        );
    }
    totals
}

// Used for testing
#[allow(unused)]
pub(super) fn wc_totals_from_reader<R: Read>(
    reader: &mut R,
    options: WcCountOptions,
) -> io::Result<WcTotals> {
    if options.bytes
        && !options.lines
        && !options.words
        && !options.chars
        && !options.max_line_length
    {
        let mut totals = WcTotals {
            lines: 0,
            words: 0,
            chars: 0,
            bytes: 0,
            max_line_length: 0,
        };
        let mut buffer = get_aligned_wc_block();
        let mut bytes = 0u64;
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
        }
        totals.bytes = bytes;
        return Ok(totals);
    }

    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        chars: 0,
        bytes: 0,
        max_line_length: 0,
    };
    let mut previous_ended_in_word = false;
    let mut buffer = get_aligned_wc_block();
    let mut utf8_carry = Vec::with_capacity(3);
    let mut utf8_scratch = Vec::new();
    let mut max_line_length_carry = Vec::with_capacity(3);
    let mut max_line_length_scratch = Vec::new();
    let mut current_line_length = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            if options.max_line_length && !max_line_length_carry.is_empty() {
                update_max_line_length_from_complete_bytes(
                    &max_line_length_carry,
                    &mut current_line_length,
                    &mut totals.max_line_length,
                );
            }
            if options.max_line_length {
                totals.max_line_length = totals.max_line_length.max(current_line_length);
            }
            return Ok(totals);
        }
        let block = &buffer[..read];
        let counts = count_wc_block(block, options);
        merge_wc_counts(
            &mut totals,
            &mut previous_ended_in_word,
            counts,
            options.words,
        );
        if options.chars {
            totals.chars += count_utf8_chars_with_carry(&mut utf8_carry, &mut utf8_scratch, block);
        }
        if options.max_line_length {
            max_line_length_scratch.clear();
            max_line_length_scratch.extend_from_slice(&max_line_length_carry);
            max_line_length_scratch.extend_from_slice(block);
            let suffix_len = utf8_incomplete_suffix_len(&max_line_length_scratch);
            let complete_len = max_line_length_scratch.len() - suffix_len;
            update_max_line_length_from_complete_bytes(
                &max_line_length_scratch[..complete_len],
                &mut current_line_length,
                &mut totals.max_line_length,
            );
            max_line_length_carry.clear();
            max_line_length_carry.extend_from_slice(&max_line_length_scratch[complete_len..]);
        }
    }
}

// Used for testing
#[allow(unused)]
pub(super) fn wc_totals_from_reader_parallel<R: Read>(
    reader: &mut R,
    options: WcCountOptions,
) -> io::Result<WcTotals> {
    if options.bytes
        && !options.lines
        && !options.words
        && !options.chars
        && !options.max_line_length
    {
        return wc_totals_from_reader(reader, options);
    }

    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        chars: 0,
        bytes: 0,
        max_line_length: 0,
    };
    let mut previous_ended_in_word = false;
    let mut buffer = get_aligned_wc_block();
    let mut utf8_carry = Vec::with_capacity(3);
    let mut utf8_scratch = Vec::new();
    let mut max_line_length_carry = Vec::with_capacity(3);
    let mut max_line_length_scratch = Vec::new();
    let mut current_line_length = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            if options.max_line_length && !max_line_length_carry.is_empty() {
                update_max_line_length_from_complete_bytes(
                    &max_line_length_carry,
                    &mut current_line_length,
                    &mut totals.max_line_length,
                );
            }
            if options.max_line_length {
                totals.max_line_length = totals.max_line_length.max(current_line_length);
            }
            return Ok(totals);
        }
        let block = &buffer[..read];
        let counts = count_wc_block(block, options);
        merge_wc_counts(
            &mut totals,
            &mut previous_ended_in_word,
            counts,
            options.words,
        );
        if options.chars {
            totals.chars += count_utf8_chars_with_carry(&mut utf8_carry, &mut utf8_scratch, block);
        }
        if options.max_line_length {
            max_line_length_scratch.clear();
            max_line_length_scratch.extend_from_slice(&max_line_length_carry);
            max_line_length_scratch.extend_from_slice(block);
            let suffix_len = utf8_incomplete_suffix_len(&max_line_length_scratch);
            let complete_len = max_line_length_scratch.len() - suffix_len;
            update_max_line_length_from_complete_bytes(
                &max_line_length_scratch[..complete_len],
                &mut current_line_length,
                &mut totals.max_line_length,
            );
            max_line_length_carry.clear();
            max_line_length_carry.extend_from_slice(&max_line_length_scratch[complete_len..]);
        }
    }
}

pub(super) fn wc_totals_from_fd_parallel<R: AsRawFd + Read>(
    reader: &mut R,
    options: WcCountOptions,
    config: &crate::config::LoadedConfig,
    io_mode: IOMode,
) -> io::Result<WcTotals> {
    if options.chars || options.max_line_length {
        return wc_totals_from_reader(reader, options);
    }

    if options.bytes && !options.lines && !options.words {
        let mut totals = WcTotals {
            lines: 0,
            words: 0,
            chars: 0,
            bytes: 0,
            max_line_length: 0,
        };
        if let Some(bytes) = fast_count_fd(reader) {
            totals.bytes = bytes;
            return Ok(totals);
        } else {
            return wc_totals_from_reader(reader, options);
        }
    }

    if let Some(file) = get_regular_file_path(reader) {
        let blocks = map_file_blocks_for_mode(
            &config,
            "read",
            &file,
            internal_io_mode(io_mode),
            move |block| Ok::<_, io::Error>(count_wc_block(block.data, options)),
        )?;
        return Ok(reduce_wc_counts(&blocks.blocks));
    }

    let fd = reader.as_raw_fd();
    grow_pipe_best_effort(fd)?;
    let mut pipe_reader = crate::reader::BufReader::with_capacity(WC_STREAM_BLOCK_SIZE, reader);
    wc_totals_from_reader(&mut pipe_reader, options)
}

pub(super) fn wc_metadata_totals(
    input: &StreamInput,
    options: WcCountOptions,
) -> io::Result<Option<WcTotals>> {
    if !options.bytes || options.lines || options.words || options.chars || options.max_line_length
    {
        return Ok(None);
    }
    let StreamInput::File(path) = input else {
        return Ok(None);
    };
    if !is_regular_input_path(path)? {
        return Ok(None);
    }
    let file = std::fs::File::open(path)?;
    Ok(Some(WcTotals {
        lines: 0,
        words: 0,
        chars: 0,
        bytes: file.metadata()?.len(),
        max_line_length: 0,
    }))
}

pub(super) fn write_wc_result<W: Write>(
    out: &mut W,
    totals: WcTotals,
    label: Option<&str>,
    print_lines: bool,
    print_words: bool,
    print_chars: bool,
    print_bytes: bool,
    print_max_line_length: bool,
) -> io::Result<()> {
    let mut first = true;
    for (enabled, value) in [
        (print_lines, totals.lines),
        (print_words, totals.words),
        (print_chars, totals.chars),
        (print_bytes, totals.bytes),
        (print_max_line_length, totals.max_line_length),
    ] {
        if enabled {
            if !first {
                write!(out, " ")?;
            }
            write!(out, "{value}")?;
            first = false;
        }
    }
    if let Some(label) = label {
        writeln!(out, " {label}")
    } else {
        writeln!(out)
    }
}
