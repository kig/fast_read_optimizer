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

fn print_sort_help(program: &str) {
    println!("sort - Sort newline-delimited records bytewise");
    println!();
    println!("Usage: {program} [OPTION]... [FILE]...");
    println!();
    println!("This bounded first slice sorts newline-delimited records in ascending byte order.");
    println!("It is locale-independent and currently implements only the default bytewise case.");
    println!();
    println!("Supported options:");
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
    println!("    reverse, unique, numeric/month/version modes, keys, merge/check modes,");
    println!("    zero-terminated records, output/temp-file controls, and locale collation.");
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

fn sort_line_refs(lines: &mut [SortLineRef], storage: &[u8]) {
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

pub(super) fn run_sort(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut files = Vec::new();
    let mut end_flags = false;

    for arg in &args[1..] {
        match arg.as_str() {
            "--" if !end_flags => end_flags = true,
            "-h" | "--help" if !end_flags => {
                print_sort_help(args[0].as_str());
                return Ok(0);
            }
            "--auto" if !end_flags => io_mode = IOMode::Auto,
            "--direct" if !end_flags => io_mode = IOMode::Direct,
            "--no-direct" if !end_flags => io_mode = IOMode::PageCache,
            "--report-gbps" if !end_flags => report_throughput = true,
            other if !end_flags && other.starts_with('-') && other != "-" => {
                eprintln!("sort: unsupported option '{other}'");
                eprintln!(
                    "sort: fro sort currently supports only bytewise ascending line sorting."
                );
                eprintln!("Try 'sort --help' for more information.");
                return Ok(2);
            }
            other => files.push(other.to_string()),
        }
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

    sort_line_refs(&mut lines, &storage);

    let mut out = stdout_buf_writer()?;
    for line in &lines {
        out.write_all(line.bytes(&storage))?;
        out.write_all(b"\n")?;
    }
    out.flush()?;

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
        sort_line_refs(&mut refs, &storage);
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
}
