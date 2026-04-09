use super::*;

const RADIX_SORT_MAX_LINE_LEN: usize = 256;
const RADIX_SORT_MAX_WORK: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SortLineRef {
    start: usize,
    len: usize,
}

impl SortLineRef {
    fn bytes<'a>(self, storage: &'a [u8]) -> &'a [u8] {
        &storage[self.start..self.start + self.len]
    }

    fn radix_key_at(self, storage: &[u8], depth: usize) -> usize {
        if depth < self.len {
            usize::from(storage[self.start + depth]) + 1
        } else {
            0
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortMode {
    Bytewise,
    Numeric,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NumericPrefix<'a> {
    negative: bool,
    int_digits: &'a [u8],
    frac_digits: &'a [u8],
}

impl<'a> NumericPrefix<'a> {
    fn is_zero(self) -> bool {
        self.int_digits.is_empty() && self.frac_digits.is_empty()
    }
}

fn trim_leading_zeros(bytes: &[u8]) -> &[u8] {
    let first_non_zero = bytes
        .iter()
        .position(|&byte| byte != b'0')
        .unwrap_or(bytes.len());
    &bytes[first_non_zero..]
}

fn trim_trailing_zeros(bytes: &[u8]) -> &[u8] {
    let last_non_zero = bytes
        .iter()
        .rposition(|&byte| byte != b'0')
        .map(|idx| idx + 1)
        .unwrap_or(0);
    &bytes[..last_non_zero]
}

fn parse_numeric_prefix(line: &[u8]) -> NumericPrefix<'_> {
    let mut idx = 0usize;
    while idx < line.len() && matches!(line[idx], b' ' | b'\t') {
        idx += 1;
    }

    let negative = idx < line.len() && line[idx] == b'-';
    if negative {
        idx += 1;
    }

    let int_start = idx;
    while idx < line.len() && line[idx].is_ascii_digit() {
        idx += 1;
    }
    let int_digits = &line[int_start..idx];

    let frac_digits = if idx < line.len() && line[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < line.len() && line[idx].is_ascii_digit() {
            idx += 1;
        }
        &line[frac_start..idx]
    } else {
        &[]
    };

    let has_digits = !int_digits.is_empty() || !frac_digits.is_empty();
    if !has_digits {
        return NumericPrefix {
            negative: false,
            int_digits: &[],
            frac_digits: &[],
        };
    }

    NumericPrefix {
        negative,
        int_digits: trim_leading_zeros(int_digits),
        frac_digits: trim_trailing_zeros(frac_digits),
    }
}

fn compare_numeric_magnitude(
    left: NumericPrefix<'_>,
    right: NumericPrefix<'_>,
) -> std::cmp::Ordering {
    left.int_digits
        .len()
        .cmp(&right.int_digits.len())
        .then_with(|| left.int_digits.cmp(right.int_digits))
        .then_with(|| {
            let max_frac_len = left.frac_digits.len().max(right.frac_digits.len());
            for idx in 0..max_frac_len {
                let left_digit = left.frac_digits.get(idx).copied().unwrap_or(b'0');
                let right_digit = right.frac_digits.get(idx).copied().unwrap_or(b'0');
                match left_digit.cmp(&right_digit) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        })
}

fn compare_numeric_prefixes(
    left: NumericPrefix<'_>,
    right: NumericPrefix<'_>,
) -> std::cmp::Ordering {
    if left.is_zero() && right.is_zero() {
        return std::cmp::Ordering::Equal;
    }
    match (left.negative, right.negative) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => compare_numeric_magnitude(left, right),
        (true, true) => compare_numeric_magnitude(right, left),
    }
}

fn compare_numeric_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    compare_numeric_prefixes(parse_numeric_prefix(left), parse_numeric_prefix(right))
}

fn print_sort_help(program: &str) {
    println!("sort - Sort newline-delimited records");
    println!();
    println!("Usage: {program} [OPTION]... [FILE]...");
    println!();
    println!(
        "This bounded slice sorts newline-delimited records in locale-independent byte or numeric order."
    );
    println!("It currently supports the default case plus numeric/reverse/unique and -o output.");
    println!();
    println!("Supported options:");
    println!("  -n, --numeric-sort   compare leading numeric prefixes in C-locale style");
    println!("  -r, --reverse        reverse the result of comparisons");
    println!("  -u, --unique         output only the first of an equal run");
    println!("  -o FILE              write result to FILE after reading all input");
    println!("      --output=FILE    same as -o FILE");
    println!("      --auto           choose direct IO automatically for regular files");
    println!("      --direct         force direct IO for regular files when possible");
    println!("      --no-direct      force page-cache IO for regular files");
    println!("      --report-gbps    print aggregate input throughput to stderr");
    println!("  -h, --help           display this help and exit");
    println!();
    println!("Notes:");
    println!("  - Use '-' once to read stdin.");
    println!("  - Use '--' before file names that start with '-'.");
    println!("  - Unsupported GNU sort features currently return an error:");
    println!("    general keys, month/version/human modes, merge/check modes,");
    println!("    zero-terminated records, temp-file controls, and locale collation.");
}

fn sort_input_label(input: &StreamInput) -> &str {
    match input {
        StreamInput::File(path) => path.as_str(),
        StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
    }
}

fn append_input_lines(
    storage: &mut Vec<u8>,
    lines: &mut Vec<SortLineRef>,
    input: &[u8],
) -> io::Result<()> {
    let base = storage.len();
    storage.extend_from_slice(input);

    let mut line_start = 0usize;
    for (idx, &byte) in input.iter().enumerate() {
        if byte == b'\n' {
            lines.push(SortLineRef {
                start: base + line_start,
                len: idx - line_start,
            });
            line_start = idx + 1;
        }
    }
    if line_start < input.len() {
        lines.push(SortLineRef {
            start: base + line_start,
            len: input.len() - line_start,
        });
    }

    Ok(())
}

fn sort_line_refs(lines: &mut [SortLineRef], storage: &[u8], mode: SortMode, unique: bool) {
    if mode == SortMode::Numeric {
        if unique {
            lines.sort_by(|left, right| {
                compare_numeric_lines(left.bytes(storage), right.bytes(storage))
            });
        } else {
            lines.sort_unstable_by(|left, right| {
                compare_numeric_lines(left.bytes(storage), right.bytes(storage))
                    .then_with(|| left.bytes(storage).cmp(right.bytes(storage)))
            });
        }
        return;
    }

    if lines.len() < 2 {
        return;
    }

    let max_len = lines.iter().map(|line| line.len).max().unwrap_or(0);
    let work = max_len.saturating_mul(lines.len());
    if max_len == 0 || max_len > RADIX_SORT_MAX_LINE_LEN || work > RADIX_SORT_MAX_WORK {
        lines.sort_unstable_by(|left, right| left.bytes(storage).cmp(right.bytes(storage)));
        return;
    }

    let mut from = lines.to_vec();
    let mut to = vec![SortLineRef::default(); lines.len()];
    for depth in (0..max_len).rev() {
        let mut counts = [0usize; 257];
        for line in &from {
            counts[line.radix_key_at(storage, depth)] += 1;
        }

        let mut offsets = [0usize; 257];
        let mut total = 0usize;
        for (idx, count) in counts.into_iter().enumerate() {
            offsets[idx] = total;
            total += count;
        }

        for line in &from {
            let key = line.radix_key_at(storage, depth);
            to[offsets[key]] = *line;
            offsets[key] += 1;
        }
        std::mem::swap(&mut from, &mut to);
    }
    lines.copy_from_slice(&from);
}

fn finalize_sorted_lines(
    lines: &mut Vec<SortLineRef>,
    storage: &[u8],
    mode: SortMode,
    unique: bool,
    reverse: bool,
) {
    sort_line_refs(lines, storage, mode, unique);
    if unique {
        match mode {
            SortMode::Bytewise => {
                lines.dedup_by(|left, right| left.bytes(storage) == right.bytes(storage))
            }
            SortMode::Numeric => lines.dedup_by(|left, right| {
                compare_numeric_lines(left.bytes(storage), right.bytes(storage))
                    == std::cmp::Ordering::Equal
            }),
        }
    }
    if reverse {
        lines.reverse();
    }
}

fn apply_short_sort_flags(
    arg: &str,
    mode: &mut SortMode,
    reverse: &mut bool,
    unique: &mut bool,
) -> bool {
    if !arg.starts_with('-') || arg.starts_with("--") || arg == "-" {
        return false;
    }
    for flag in arg[1..].bytes() {
        match flag {
            b'n' => *mode = SortMode::Numeric,
            b'r' => *reverse = true,
            b'u' => *unique = true,
            _ => return false,
        }
    }
    true
}

fn write_sorted_lines<W: Write>(
    mut out: W,
    lines: &[SortLineRef],
    storage: &[u8],
) -> io::Result<()> {
    for line in lines {
        out.write_all(line.bytes(storage))?;
        out.write_all(b"\n")?;
    }
    out.flush()
}

pub(super) fn run_sort(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut mode = SortMode::Bytewise;
    let mut reverse = false;
    let mut unique = false;
    let mut output_path = None;
    let mut files = Vec::new();
    let mut end_flags = false;
    let mut idx = 1usize;

    while idx < args.len() {
        let arg = &args[idx];
        match arg.as_str() {
            "--" if !end_flags => end_flags = true,
            "-h" | "--help" if !end_flags => {
                print_sort_help(args[0].as_str());
                return Ok(0);
            }
            "-n" | "--numeric-sort" if !end_flags => mode = SortMode::Numeric,
            "-r" | "--reverse" if !end_flags => reverse = true,
            "-u" | "--unique" if !end_flags => unique = true,
            "-o" | "--output" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    eprintln!("sort: option requires an argument -- 'o'");
                    eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                output_path = Some(path.clone());
                idx += 1;
            }
            "--auto" if !end_flags => io_mode = IOMode::Auto,
            "--direct" if !end_flags => io_mode = IOMode::Direct,
            "--no-direct" if !end_flags => io_mode = IOMode::PageCache,
            "--report-gbps" if !end_flags => report_throughput = true,
            other if !end_flags && other.starts_with("--output=") => {
                output_path = Some(other["--output=".len()..].to_string());
            }
            other
                if !end_flags
                    && apply_short_sort_flags(other, &mut mode, &mut reverse, &mut unique) => {}
            other
                if !end_flags
                    && other.starts_with('-')
                    && !other.starts_with("--")
                    && other != "-" =>
            {
                let mut handled = true;
                let mut consumed_next = false;
                for (pos, flag) in other[1..].char_indices() {
                    match flag {
                        'n' => mode = SortMode::Numeric,
                        'r' => reverse = true,
                        'u' => unique = true,
                        'o' => {
                            let value_start = 2 + pos;
                            if value_start < other.len() {
                                output_path = Some(other[value_start..].to_string());
                            } else {
                                let Some(path) = args.get(idx + 1) else {
                                    eprintln!("sort: option requires an argument -- 'o'");
                                    eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                output_path = Some(path.clone());
                                consumed_next = true;
                            }
                            break;
                        }
                        _ => {
                            handled = false;
                            break;
                        }
                    }
                }
                if !handled {
                    eprintln!("sort: unsupported option '{other}'");
                    eprintln!(
                        "sort: fro sort currently supports bytewise or numeric line sorting plus optional reverse/unique output and -o."
                    );
                    eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                }
                if consumed_next {
                    idx += 1;
                }
            }
            other if !end_flags && other.starts_with('-') && other != "-" => {
                eprintln!("sort: unsupported option '{other}'");
                eprintln!(
                    "sort: fro sort currently supports bytewise or numeric line sorting plus optional reverse/unique output and -o."
                );
                eprintln!("Try 'sort --help' for more information.");
                return Ok(2);
            }
            other => files.push(other.to_string()),
        }
        idx += 1;
    }

    let inputs = parse_stream_inputs(files);
    if inputs
        .iter()
        .filter(|input| matches!(input, StreamInput::Stdin { .. }))
        .count()
        > 1
    {
        eprintln!("sort: repeated '-' operands are not supported");
        eprintln!("Try 'sort --help' for more information.");
        return Ok(2);
    }

    let started_at = std::time::Instant::now();
    let mut total_bytes = 0u64;
    let mut storage = Vec::new();
    let mut lines = Vec::new();
    for input in &inputs {
        let bytes = match loaded_or_stream_bytes(input, io_mode) {
            Ok(bytes) => bytes,
            Err(err) => {
                eprintln!("sort: cannot read '{}': {err}", sort_input_label(input));
                return Ok(2);
            }
        };
        total_bytes = total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("sort input byte count overflow"))?;
        append_input_lines(&mut storage, &mut lines, &bytes)?;
    }

    finalize_sorted_lines(&mut lines, &storage, mode, unique, reverse);

    let write_result = if let Some(path) = output_path.as_deref() {
        let mut out = fro::create_with_mode(path, io_mode)?;
        write_sorted_lines(&mut out, &lines, &storage)
    } else {
        let out = stdout_buf_writer()?;
        write_sorted_lines(out, &lines, &storage)
    };
    if let Err(err) = write_result {
        if let Some(path) = output_path.as_deref() {
            eprintln!("sort: cannot write '{path}': {err}");
        } else {
            eprintln!("sort: write failed: {err}");
        }
        return Ok(2);
    }

    if report_throughput {
        report_gbps("sort", total_bytes, started_at);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs_for_lines(lines: &[&[u8]]) -> (Vec<u8>, Vec<SortLineRef>) {
        let mut storage = Vec::new();
        let mut refs = Vec::new();
        for line in lines {
            let start = storage.len();
            storage.extend_from_slice(line);
            refs.push(SortLineRef {
                start,
                len: line.len(),
            });
        }
        (storage, refs)
    }

    #[test]
    fn radix_sort_matches_bytewise_order_for_prefixes_and_empty_lines() {
        let input = [
            b"beta".as_slice(),
            b"".as_slice(),
            b"alpha".as_slice(),
            b"alph".as_slice(),
            b"alpha".as_slice(),
            b"z".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&input);
        sort_line_refs(&mut refs, &storage, SortMode::Bytewise, false);
        let sorted = refs
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            sorted,
            vec![
                b"".to_vec(),
                b"alph".to_vec(),
                b"alpha".to_vec(),
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"z".to_vec(),
            ]
        );
    }

    #[test]
    fn append_input_lines_keeps_missing_final_newline_as_a_record() {
        let mut storage = Vec::new();
        let mut refs = Vec::new();
        append_input_lines(&mut storage, &mut refs, b"beta\nalpha").unwrap();
        let lines = refs
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(lines, vec![b"beta".to_vec(), b"alpha".to_vec()]);
    }

    #[test]
    fn finalize_sorted_lines_applies_unique_and_reverse_after_sorting() {
        let input = [
            b"beta".as_slice(),
            b"alpha".as_slice(),
            b"beta".as_slice(),
            b"alpha".as_slice(),
            b"".as_slice(),
        ];
        let (storage, refs) = refs_for_lines(&input);

        let mut sorted = refs.clone();
        finalize_sorted_lines(&mut sorted, &storage, SortMode::Bytewise, false, false);
        assert_eq!(
            sorted
                .iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"".to_vec(),
                b"alpha".to_vec(),
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"beta".to_vec(),
            ]
        );

        let mut unique_only = refs.clone();
        finalize_sorted_lines(&mut unique_only, &storage, SortMode::Bytewise, true, false);
        assert_eq!(
            unique_only
                .iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![b"".to_vec(), b"alpha".to_vec(), b"beta".to_vec()]
        );

        let mut unique_reverse = refs;
        finalize_sorted_lines(
            &mut unique_reverse,
            &storage,
            SortMode::Bytewise,
            true,
            true,
        );
        assert_eq!(
            unique_reverse
                .iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![b"beta".to_vec(), b"alpha".to_vec(), b"".to_vec()]
        );
    }

    #[test]
    fn numeric_compare_matches_expected_prefix_ordering() {
        let lines = [
            b"x".as_slice(),
            b"10".as_slice(),
            b"2".as_slice(),
            b"-3".as_slice(),
            b".5".as_slice(),
            b"02".as_slice(),
            b"2a".as_slice(),
            b"+2".as_slice(),
            b"  10".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&lines);
        sort_line_refs(&mut refs, &storage, SortMode::Numeric, false);
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"-3".to_vec(),
                b"+2".to_vec(),
                b"x".to_vec(),
                b".5".to_vec(),
                b"02".to_vec(),
                b"2".to_vec(),
                b"2a".to_vec(),
                b"  10".to_vec(),
                b"10".to_vec(),
            ]
        );
    }

    #[test]
    fn numeric_unique_keeps_first_line_for_equal_numeric_keys() {
        let input = [
            b"1.0".as_slice(),
            b"1".as_slice(),
            b"1.00".as_slice(),
            b"2".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&input);
        finalize_sorted_lines(&mut refs, &storage, SortMode::Numeric, true, false);
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![b"1.0".to_vec(), b"2".to_vec()]
        );
    }
}
