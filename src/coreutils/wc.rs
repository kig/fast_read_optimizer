use super::*;

const WC_STREAM_BLOCK_SIZE: usize = 8 << 20;

#[derive(Debug, Clone, Copy)]
struct WcCountOptions {
    lines: bool,
    words: bool,
    bytes: bool,
}

fn count_wc_block(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if options.words && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: guarded by runtime AVX2 detection.
        return unsafe { count_wc_block_avx2(block, options) };
    }
    count_wc_block_scalar(block, options)
}

fn count_wc_block_scalar(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
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
            bytes,
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
        bytes,
        starts_in_word: block.first().is_some_and(|byte| !is_wc_whitespace(*byte)),
        ends_in_word: block.last().is_some_and(|byte| !is_wc_whitespace(*byte)),
    }
}

fn wc_totals_from_reader<R: Read>(reader: &mut R, options: WcCountOptions) -> io::Result<WcTotals> {
    if options.bytes && !options.lines && !options.words {
        let mut totals = WcTotals {
            lines: 0,
            words: 0,
            bytes: 0,
        };
        let mut buffer = vec![0_u8; WC_STREAM_BLOCK_SIZE];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                return Ok(totals);
            }
            totals.bytes += read as u64;
        }
    }

    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    let mut buffer = vec![0_u8; WC_STREAM_BLOCK_SIZE];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(totals);
        }
        let block = &buffer[..read];
        let counts = count_wc_block(block, options);
        merge_wc_counts(&mut totals, &mut previous_ended_in_word, counts, options.words);
    }
}

fn wc_totals_from_reader_parallel<R: Read>(
    reader: &mut R,
    options: WcCountOptions,
) -> io::Result<WcTotals> {
    wc_totals_from_reader(reader, options)
}

fn wc_metadata_totals(input: &StreamInput, options: WcCountOptions) -> io::Result<Option<WcTotals>> {
    if !options.bytes || options.lines || options.words {
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
        bytes: file.metadata()?.len(),
    }))
}

fn write_wc_result<W: Write>(
    out: &mut W,
    totals: WcTotals,
    label: Option<&str>,
    print_lines: bool,
    print_words: bool,
    print_bytes: bool,
) -> io::Result<()> {
    let mut first = true;
    for (enabled, value) in [
        (print_lines, totals.lines),
        (print_words, totals.words),
        (print_bytes, totals.bytes),
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

#[derive(Debug)]
struct WcBlockCounts {
    lines: u64,
    words: u64,
    bytes: u64,
    starts_in_word: bool,
    ends_in_word: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WcTotals {
    lines: u64,
    words: u64,
    bytes: u64,
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

fn is_wc_whitespace(byte: u8) -> bool {
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
    totals.bytes += counts.bytes;
    if track_words && *previous_ended_in_word && counts.starts_in_word {
        totals.words = totals.words.saturating_sub(1);
    }
    *previous_ended_in_word = track_words && counts.ends_in_word;
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn count_wc_block_avx2(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
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
                _mm256_or_si256(_mm256_cmpeq_epi8(chunk, space), _mm256_cmpeq_epi8(chunk, tab)),
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
        bytes,
        starts_in_word: block.first().is_some_and(|byte| !is_wc_whitespace(*byte)),
        ends_in_word: block.last().is_some_and(|byte| !is_wc_whitespace(*byte)),
    }
}

fn reduce_wc_counts(blocks: &[WcBlockCounts]) -> WcTotals {
    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    for block in blocks {
        merge_wc_counts(&mut totals, &mut previous_ended_in_word, WcBlockCounts {
            lines: block.lines,
            words: block.words,
            bytes: block.bytes,
            starts_in_word: block.starts_in_word,
            ends_in_word: block.ends_in_word,
        }, true);
    }
    totals
}

pub(super) fn run_wc(args: &[String]) -> io::Result<()> {
    let mut print_lines = false;
    let mut print_words = false;
    let mut print_bytes = false;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-l" => print_lines = true,
            "-w" => print_words = true,
            "-c" => print_bytes = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
    }
    if !print_lines && !print_words && !print_bytes {
        print_lines = true;
        print_words = true;
        print_bytes = true;
    }
    let options = WcCountOptions {
        lines: print_lines,
        words: print_words,
        bytes: print_bytes,
    };

    let inputs = parse_stream_inputs(files);
    let config = load_config(None);
    let mut out = stdout_buf_writer()?;
    for input in inputs {
        let label = match &input {
            StreamInput::File(file) => Some(file.as_str()),
            StreamInput::Stdin { label } => label.as_deref(),
        };
        let totals = if let Some(totals) = wc_metadata_totals(&input, options)? {
            totals
        } else {
            match &input {
                StreamInput::File(file) if is_regular_input_path(file)? => {
                    let blocks = map_file_blocks_for_mode(
                        &config,
                        "read",
                        file,
                        internal_io_mode(io_mode),
                        move |block| Ok::<_, io::Error>(count_wc_block(block.data, options)),
                    )?;
                    reduce_wc_counts(&blocks.blocks)
                }
                StreamInput::File(file) => {
                    let mut reader = BufReader::new(std::fs::File::open(file)?);
                    wc_totals_from_reader_parallel(&mut reader, options)?
                }
                StreamInput::Stdin { .. } => {
                    let mut reader = stdin_buf_reader()?;
                    wc_totals_from_reader_parallel(&mut reader, options)?
                }
            }
        };
        write_wc_result(
            &mut out,
            totals,
            label,
            print_lines,
            print_words,
            print_bytes,
        )?;
    }
    out.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduce_wc_counts_merges_cross_block_words() {
        let blocks = [
            WcBlockCounts {
                lines: 0,
                words: 1,
                bytes: 3,
                starts_in_word: true,
                ends_in_word: true,
            },
            WcBlockCounts {
                lines: 1,
                words: 1,
                bytes: 4,
                starts_in_word: true,
                ends_in_word: false,
            },
        ];

        assert_eq!(
            reduce_wc_counts(&blocks),
            WcTotals {
                lines: 1,
                words: 1,
                bytes: 7,
            }
        );
    }

    #[test]
    fn wc_whitespace_matches_posix_ascii_set() {
        for byte in [b' ', b'\t', b'\n', 0x0b, 0x0c, b'\r'] {
            assert!(is_wc_whitespace(byte), "byte {byte:#x} should split words");
        }
        for byte in [0_u8, b'a', 0x1c, 0x7f, 0x80, 0xff] {
            assert!(
                !is_wc_whitespace(byte),
                "byte {byte:#x} should not split words"
            );
        }
    }

    #[test]
    fn count_wc_block_counts_lines_words_and_bytes() {
        let counts = count_wc_block(
            b"one two\nthree\x0bfour\r\nfive",
            WcCountOptions {
                lines: true,
                words: true,
                bytes: true,
            },
        );
        assert_eq!(counts.lines, 2);
        assert_eq!(counts.words, 5);
        assert_eq!(counts.bytes, 24);
        assert!(counts.starts_in_word);
        assert!(counts.ends_in_word);
    }

    #[test]
    fn wc_parallel_totals_match_sequential_totals() {
        let bytes = b"alpha beta\ngamma\r\ndelta\x0bepsilon zeta".repeat(1024);
        let options = WcCountOptions {
            lines: true,
            words: true,
            bytes: true,
        };
        let sequential = wc_totals_from_reader(&mut std::io::Cursor::new(bytes.as_slice()), options)
            .unwrap();
        let parallel =
            wc_totals_from_reader_parallel(&mut std::io::Cursor::new(bytes.as_slice()), options)
                .unwrap();
        assert_eq!(parallel, sequential);
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[test]
    fn wc_avx2_block_counts_match_scalar_counts() {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }
        let block = b"alpha beta\ngamma\r\ndelta\x0bepsilon zeta\x80\xfftail".repeat(257);
        let options = WcCountOptions {
            lines: true,
            words: true,
            bytes: true,
        };
        let scalar = count_wc_block_scalar(&block, options);
        let avx2 = unsafe { count_wc_block_avx2(&block, options) };
        assert_eq!(avx2.lines, scalar.lines);
        assert_eq!(avx2.words, scalar.words);
        assert_eq!(avx2.bytes, scalar.bytes);
        assert_eq!(avx2.starts_in_word, scalar.starts_in_word);
        assert_eq!(avx2.ends_in_word, scalar.ends_in_word);
    }

    #[test]
    fn wc_metadata_totals_only_applies_to_byte_only_regular_files() {
        let tmp = std::env::temp_dir().join(format!(
            "fro-wc-metadata-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&tmp, b"abcdef").unwrap();

        let byte_only = WcCountOptions {
            lines: false,
            words: false,
            bytes: true,
        };
        assert_eq!(
            wc_metadata_totals(&StreamInput::File(tmp.display().to_string()), byte_only).unwrap(),
            Some(WcTotals {
                lines: 0,
                words: 0,
                bytes: 6,
            })
        );

        let combined = WcCountOptions {
            lines: true,
            words: false,
            bytes: true,
        };
        assert_eq!(
            wc_metadata_totals(&StreamInput::File(tmp.display().to_string()), combined).unwrap(),
            None
        );
        assert_eq!(
            wc_metadata_totals(&StreamInput::Stdin { label: None }, byte_only).unwrap(),
            None
        );

        let _ = std::fs::remove_file(tmp);
    }
}
