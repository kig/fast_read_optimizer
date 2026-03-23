use super::*;

#[derive(Debug, Clone, Copy)]
struct WcCountOptions {
    lines: bool,
    words: bool,
    bytes: bool,
}

fn count_wc_block(block: &[u8], options: WcCountOptions) -> WcBlockCounts {
    let lines = if options.lines {
        memchr_iter(b'\n', block).count() as u64
    } else {
        0
    };
    let bytes = if options.bytes { block.len() as u64 } else { 0 };

    if !options.words {
        return WcBlockCounts {
            lines,
            words: 0,
            bytes,
            starts_in_word: false,
            ends_in_word: false,
        };
    }

    let mut words = 0_u64;
    let mut prev_is_whitespace = true;
    for &byte in block {
        let is_whitespace = WC_WHITESPACE_TABLE[byte as usize] != 0;
        words += u64::from(!is_whitespace && prev_is_whitespace);
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
        let mut buffer = vec![0_u8; 8 << 20];
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
    let mut buffer = vec![0_u8; 8 << 20];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(totals);
        }
        let block = &buffer[..read];
        let counts = count_wc_block(block, options);
        totals.lines += counts.lines;
        totals.words += counts.words;
        totals.bytes += counts.bytes;
        if options.words && previous_ended_in_word && counts.starts_in_word {
            totals.words = totals.words.saturating_sub(1);
        }
        previous_ended_in_word = options.words && counts.ends_in_word;
    }
}

fn wc_totals_from_reader_parallel<R: Read>(
    reader: &mut R,
    options: WcCountOptions,
) -> io::Result<WcTotals> {
    wc_totals_from_reader(reader, options)
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

fn reduce_wc_counts(blocks: &[WcBlockCounts]) -> WcTotals {
    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    for block in blocks {
        totals.lines += block.lines;
        totals.words += block.words;
        totals.bytes += block.bytes;
        if previous_ended_in_word && block.starts_in_word {
            totals.words -= 1;
        }
        previous_ended_in_word = block.ends_in_word;
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
        let (totals, label) = match input {
            StreamInput::File(file) if is_regular_input_path(&file)? => {
                let blocks = map_file_blocks_for_mode(
                    &config,
                    "read",
                    &file,
                    internal_io_mode(io_mode),
                    move |block| Ok::<_, io::Error>(count_wc_block(block.data, options)),
                )?;
                (reduce_wc_counts(&blocks.blocks), Some(file))
            }
            StreamInput::File(file) => {
                let mut reader = BufReader::new(std::fs::File::open(&file)?);
                (
                    wc_totals_from_reader_parallel(&mut reader, options)?,
                    Some(file),
                )
            }
            StreamInput::Stdin { label } => {
                let mut reader = stdin_buf_reader()?;
                (wc_totals_from_reader_parallel(&mut reader, options)?, label)
            }
        };
        write_wc_result(
            &mut out,
            totals,
            label.as_deref(),
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
}
