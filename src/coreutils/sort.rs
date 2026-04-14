use super::*;
use std::io::Write;
use std::path::Path;
use stringzilla::stringzilla as sz;

mod compare;
mod external;

use compare::{compare_line_bytes, compare_line_refs, same_sort_key};

const SORT_WRITE_BUFFER_SIZE: usize = 2 << 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SortLineRef {
    start: usize,
    len: usize,
    sequence: u64,
}

impl SortLineRef {
    fn bytes<'a>(self, storage: &'a [u8]) -> &'a [u8] {
        &storage[self.start..self.start + self.len]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortMode {
    Bytewise,
    Numeric,
    GeneralNumeric,
    HumanNumeric,
    Month,
    Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SortKeyPosition {
    field: usize,
    char_offset: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortKeyEnd {
    EndOfLine,
    FieldEnd { field: usize },
    Char { field: usize, char_end: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SortKeySpec {
    start: SortKeyPosition,
    end: SortKeyEnd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SortComparator {
    mode: SortMode,
    keys: Vec<SortKeySpec>,
}

impl SortComparator {
    fn new(mode: SortMode, keys: Vec<SortKeySpec>) -> Self {
        Self { mode, keys }
    }

    fn has_key_selection(&self) -> bool {
        !self.keys.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RecordTerminator {
    #[default]
    Newline,
    Nul,
}

impl RecordTerminator {
    fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SortCheckMode {
    #[default]
    None,
    DiagnoseFirst,
    Silent,
}

impl SortCheckMode {
    fn is_enabled(self) -> bool {
        self != Self::None
    }

    fn emits_diagnostics(self) -> bool {
        self == Self::DiagnoseFirst
    }

    fn short_flag(self) -> &'static str {
        match self {
            Self::None => "",
            Self::DiagnoseFirst => "-c",
            Self::Silent => "-C",
        }
    }
}

fn set_sort_check_mode(current: &mut SortCheckMode, next: SortCheckMode) -> io::Result<()> {
    if *current != SortCheckMode::None && *current != next {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "options '-cC' are incompatible",
        ));
    }
    *current = next;
    Ok(())
}

fn print_sort_help(program: &str) {
    fro::cio_println!("sort - Sort newline-delimited records (or NUL-delimited with -z)");
    fro::cio_println!();
    fro::cio_println!("Usage: {program} [OPTION]... [FILE]...");
    fro::cio_println!();
    fro::cio_println!(
        "This bounded slice sorts locale-independent byte, numeric, general-numeric, human-numeric, month, or version records, with bounded GNU-style -k/--key selection, using newlines by default and NULs with -z."
    );
    fro::cio_println!(
        "It currently supports the default case plus bounded -k/--key, -g/-h/-M/-n/-V, -z, reverse/unique, merge/check, and -o output."
    );
    fro::cio_println!();
    fro::cio_println!("Supported options:");
    fro::cio_println!("  -c, --check, --check=diagnose-first");
    fro::cio_println!("                       check for sorted input; diagnose first bad line");
    fro::cio_println!("  -C, --check=quiet, --check=silent");
    fro::cio_println!("                       like -c, but do not report the first bad line");
    fro::cio_println!("  -g, --general-numeric-sort");
    fro::cio_println!("                       compare leading C-locale floating-point prefixes");
    fro::cio_println!("  -h, --human-numeric-sort");
    fro::cio_println!(
        "                       compare leading numbers grouped by human suffix family"
    );
    fro::cio_println!("  -m, --merge          merge already sorted inputs without resorting");
    fro::cio_println!(
        "  -M, --month-sort     compare leading month abbreviations like GNU sort -M"
    );
    fro::cio_println!("  -n, --numeric-sort   compare leading numeric prefixes in C-locale style");
    fro::cio_println!("  -k KEY, --key=KEY    sort via bounded GNU-style field ranges");
    fro::cio_println!("  -r, --reverse        reverse the result of comparisons");
    fro::cio_println!("  -u, --unique         output only the first of an equal run");
    fro::cio_println!("  -V, --version-sort   compare digit runs with GNU version-order semantics");
    fro::cio_println!("  -z, --zero-terminated  use NUL as the input and output record terminator");
    fro::cio_println!("  -o FILE              write result to FILE after reading all input");
    fro::cio_println!("      --output=FILE    same as -o FILE");
    fro::cio_println!(
        "  -T DIR               write spill files under DIR when out-of-core merge is needed"
    );
    fro::cio_println!("      --temporary-directory=DIR");
    fro::cio_println!("                       same as -T DIR");
    fro::cio_println!("      --auto           choose direct IO automatically for regular files");
    fro::cio_println!("      --direct         force direct IO for regular files when possible");
    fro::cio_println!("      --no-direct      force page-cache IO for regular files");
    fro::cio_println!("      --report-gbps    print aggregate input throughput to stderr");
    fro::cio_println!("      --help           shows this message and exits.");
    fro::cio_println!("      --version        prints the fro sort version string and exits.");
    fro::cio_println!();
    fro::cio_println!("Notes:");
    fro::cio_println!("  - Use '-' once to read stdin.");
    fro::cio_println!("  - Use '--' before file names that start with '-'.");
    fro::cio_println!("  - Bytewise in-memory sorting uses a StringZilla argsort fast path.");
    fro::cio_println!("  - Inputs larger than available memory spill sorted runs and merge them.");
    fro::cio_println!("  - -T only matters when spill temp files are actually created.");
    fro::cio_println!("  - -m reuses the spill/merge backend on already sorted inputs.");
    fro::cio_println!("  - -c/--check=diagnose-first validates one input stream and exits 1 on the first disorder.");
    fro::cio_println!("  - -C/--check=quiet/--check=silent reuses the same check path but suppresses disorder diagnostics.");
    fro::cio_println!("  - -g uses C-locale strtod-style prefixes; NaNs sort after non-numbers and before infinities.");
    fro::cio_println!("  - -h compares the leading numeric prefix plus an optional K/M/G/T/P/E/Z/Y suffix family.");
    fro::cio_println!("  - -M looks at the first nonblank three-letter month abbreviation and treats other lines as invalid month keys.");
    fro::cio_println!("  - -k/--key accepts one or more blank-separated field ranges in the form F[.C][,F[.C]], without per-key modifiers or locale collation.");
    fro::cio_println!("  - -V uses GNU/libc version-order comparisons while preserving the existing spill, merge, and check backend.");
    fro::cio_println!("  - Unsupported GNU sort features currently return an error:");
    fro::cio_println!("    locale collation and per-key modifiers.");
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
    next_sequence: &mut u64,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let base = storage.len();
    storage.extend_from_slice(input);

    let mut line_start = 0usize;
    let terminator = terminator.byte();
    for (idx, &byte) in input.iter().enumerate() {
        if byte == terminator {
            lines.push(SortLineRef {
                start: base + line_start,
                len: idx - line_start,
                sequence: *next_sequence,
            });
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
            line_start = idx + 1;
        }
    }
    if line_start < input.len() {
        lines.push(SortLineRef {
            start: base + line_start,
            len: input.len() - line_start,
            sequence: *next_sequence,
        });
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
    }

    Ok(())
}

fn sort_line_refs(
    lines: &mut [SortLineRef],
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
) -> io::Result<()> {
    if lines.len() < 2 {
        return Ok(());
    }
    match comparator.mode {
        SortMode::Bytewise if !comparator.has_key_selection() => {
            let snapshot = lines.to_vec();
            let mut order = vec![0 as sz::SortedIdx; snapshot.len()];
            sz::argsort_permutation_by(|idx| snapshot[idx].bytes(storage), &mut order).map_err(
                |status| io::Error::other(format!("StringZilla sort failed: {status:?}")),
            )?;
            for (dst, sorted_idx) in lines.iter_mut().zip(order.into_iter()) {
                *dst = snapshot[sorted_idx];
            }
        }
        SortMode::Numeric
        | SortMode::GeneralNumeric
        | SortMode::HumanNumeric
        | SortMode::Month
        | SortMode::Version
        | SortMode::Bytewise => {
            lines.sort_unstable_by(|left, right| {
                compare_line_refs(*left, *right, storage, comparator, unique)
            });
        }
    }
    Ok(())
}

fn finalize_sorted_lines(
    lines: &mut Vec<SortLineRef>,
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    sort_line_refs(lines, storage, comparator, unique)?;
    if unique {
        lines.dedup_by(|left, right| {
            same_sort_key(left.bytes(storage), right.bytes(storage), comparator)
        });
    }
    if reverse {
        lines.reverse();
    }
    Ok(())
}

fn apply_short_sort_flags(
    arg: &str,
    check_mode: &mut SortCheckMode,
    merge: &mut bool,
    mode: &mut SortMode,
    reverse: &mut bool,
    unique: &mut bool,
    terminator: &mut RecordTerminator,
) -> io::Result<bool> {
    if !arg.starts_with('-') || arg.starts_with("--") || arg == "-" {
        return Ok(false);
    }
    for flag in arg[1..].bytes() {
        match flag {
            b'c' => set_sort_check_mode(check_mode, SortCheckMode::DiagnoseFirst)?,
            b'C' => set_sort_check_mode(check_mode, SortCheckMode::Silent)?,
            b'g' => *mode = SortMode::GeneralNumeric,
            b'h' => *mode = SortMode::HumanNumeric,
            b'M' => *mode = SortMode::Month,
            b'm' => *merge = true,
            b'n' => *mode = SortMode::Numeric,
            b'r' => *reverse = true,
            b'u' => *unique = true,
            b'V' => *mode = SortMode::Version,
            b'z' => *terminator = RecordTerminator::Nul,
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn parse_sort_key_position(raw: &str, is_end: bool) -> io::Result<(usize, Option<usize>)> {
    if raw.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty sort key position",
        ));
    }
    let bytes = raw.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid sort key position '{raw}'"),
        ));
    }

    let field = raw[..idx].parse::<usize>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid sort key field in '{raw}': {err}"),
        )
    })?;
    if field == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("sort key fields are 1-based in '{raw}'"),
        ));
    }

    let mut char_pos = None;
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let char_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if char_start == idx {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid sort key character position in '{raw}'"),
            ));
        }
        let parsed = raw[char_start..idx].parse::<usize>().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid sort key character position in '{raw}': {err}"),
            )
        })?;
        if !is_end && parsed == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("sort key start character positions are 1-based in '{raw}'"),
            ));
        }
        char_pos = Some(parsed);
    }

    if idx != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported sort key modifiers in '{raw}'"),
        ));
    }

    Ok((field, char_pos))
}

fn parse_sort_key_spec(raw: &str) -> io::Result<SortKeySpec> {
    let (start_raw, end_raw) = raw
        .split_once(',')
        .map_or((raw, None), |(start, end)| (start, Some(end)));
    let (start_field, start_char) = parse_sort_key_position(start_raw, false)?;
    let start = SortKeyPosition {
        field: start_field,
        char_offset: start_char.unwrap_or(1) - 1,
    };
    let end = match end_raw {
        None => SortKeyEnd::EndOfLine,
        Some(raw_end) => {
            let (end_field, end_char) = parse_sort_key_position(raw_end, true)?;
            match end_char {
                None | Some(0) => SortKeyEnd::FieldEnd { field: end_field },
                Some(char_end) => SortKeyEnd::Char {
                    field: end_field,
                    char_end,
                },
            }
        }
    };
    Ok(SortKeySpec { start, end })
}

fn write_sorted_lines<W: Write>(
    mut out: W,
    lines: &[SortLineRef],
    storage: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    for line in lines {
        buffered_write_sort_line(&mut out, &mut buffer, line.bytes(storage), terminator)?;
    }
    flush_sort_output_buffer(&mut out, &mut buffer)?;
    out.flush()
}

fn buffered_write_sort_line<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
    line: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let terminator = terminator.byte();
    if buffer.len() + line.len() + 1 > SORT_WRITE_BUFFER_SIZE && !buffer.is_empty() {
        flush_sort_output_buffer(out, buffer)?;
    }
    if line.len() + 1 >= SORT_WRITE_BUFFER_SIZE {
        out.write_all(line)?;
        out.write_all(&[terminator])?;
        return Ok(());
    }
    buffer.extend_from_slice(line);
    buffer.push(terminator);
    Ok(())
}

fn report_sort_disorder(
    label: &str,
    disorder: &external::SortCheckFailure,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut stderr = fro::command_io::stderr_buf_writer(4096)?;
    write!(stderr, "sort: {label}:{}: disorder: ", disorder.line_number)?;
    stderr.write_all(String::from_utf8_lossy(&disorder.line).as_bytes())?;
    if terminator == RecordTerminator::Nul {
        stderr.write_all(b"\0")
    } else {
        stderr.write_all(b"\n")
    }
}

fn flush_sort_output_buffer<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    if !buffer.is_empty() {
        out.write_all(buffer)?;
        buffer.clear();
    }
    Ok(())
}

pub(super) fn run_sort(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut check_mode = SortCheckMode::None;
    let mut merge = false;
    let mut mode = SortMode::Bytewise;
    let mut reverse = false;
    let mut unique = false;
    let mut terminator = RecordTerminator::Newline;
    let mut output_path = None;
    let mut temporary_directory = None;
    let mut key_specs = Vec::new();
    let mut files = Vec::new();
    let mut end_flags = false;
    let mut idx = 1usize;

    while idx < args.len() {
        let arg = &args[idx];
        match arg.as_str() {
            "--" if !end_flags => end_flags = true,
            "--help" if !end_flags => {
                print_sort_help(args[0].as_str());
                return Ok(0);
            }
            "-c" | "--check" if !end_flags => {
                if let Err(err) = set_sort_check_mode(&mut check_mode, SortCheckMode::DiagnoseFirst)
                {
                    fro::cio_eprintln!("sort: {err}");
                    return Ok(2);
                }
            }
            "-C" | "--check=quiet" | "--check=silent" if !end_flags => {
                if let Err(err) = set_sort_check_mode(&mut check_mode, SortCheckMode::Silent) {
                    fro::cio_eprintln!("sort: {err}");
                    return Ok(2);
                }
            }
            "--check=diagnose-first" if !end_flags => {
                if let Err(err) = set_sort_check_mode(&mut check_mode, SortCheckMode::DiagnoseFirst)
                {
                    fro::cio_eprintln!("sort: {err}");
                    return Ok(2);
                }
            }
            "-g" | "--general-numeric-sort" if !end_flags => mode = SortMode::GeneralNumeric,
            "-h" | "--human-numeric-sort" if !end_flags => mode = SortMode::HumanNumeric,
            "-M" | "--month-sort" if !end_flags => mode = SortMode::Month,
            "-m" | "--merge" if !end_flags => merge = true,
            "-n" | "--numeric-sort" if !end_flags => mode = SortMode::Numeric,
            "-k" | "--key" if !end_flags => {
                let Some(spec) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'k'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match parse_sort_key_spec(spec) {
                    Ok(parsed) => key_specs.push(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
            "-r" | "--reverse" if !end_flags => reverse = true,
            "-u" | "--unique" if !end_flags => unique = true,
            "-V" | "--version-sort" if !end_flags => mode = SortMode::Version,
            "-z" | "--zero-terminated" if !end_flags => terminator = RecordTerminator::Nul,
            "-o" | "--output" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'o'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                output_path = Some(path.clone());
                idx += 1;
            }
            "-T" | "--temporary-directory" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'T'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                temporary_directory = Some(path.clone());
                idx += 1;
            }
            "--auto" if !end_flags => io_mode = IOMode::Auto,
            "--direct" if !end_flags => io_mode = IOMode::Direct,
            "--no-direct" if !end_flags => io_mode = IOMode::PageCache,
            "--report-gbps" if !end_flags => report_throughput = true,
            other if !end_flags && other.starts_with("--output=") => {
                output_path = Some(other["--output=".len()..].to_string());
            }
            other if !end_flags && other.starts_with("--key=") => {
                match parse_sort_key_spec(&other["--key=".len()..]) {
                    Ok(parsed) => key_specs.push(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
            }
            other if !end_flags && other.starts_with("--temporary-directory=") => {
                temporary_directory = Some(other["--temporary-directory=".len()..].to_string());
            }
            other
                if !end_flags
                    && match apply_short_sort_flags(
                        other,
                        &mut check_mode,
                        &mut merge,
                        &mut mode,
                        &mut reverse,
                        &mut unique,
                        &mut terminator,
                    ) {
                        Ok(true) => true,
                        Ok(false) => false,
                        Err(err) => {
                            fro::cio_eprintln!("sort: {err}");
                            return Ok(2);
                        }
                    } => {}
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
                        'c' => {
                            if let Err(err) =
                                set_sort_check_mode(&mut check_mode, SortCheckMode::DiagnoseFirst)
                            {
                                fro::cio_eprintln!("sort: {err}");
                                return Ok(2);
                            }
                        }
                        'C' => {
                            if let Err(err) =
                                set_sort_check_mode(&mut check_mode, SortCheckMode::Silent)
                            {
                                fro::cio_eprintln!("sort: {err}");
                                return Ok(2);
                            }
                        }
                        'g' => mode = SortMode::GeneralNumeric,
                        'h' => mode = SortMode::HumanNumeric,
                        'M' => mode = SortMode::Month,
                        'm' => merge = true,
                        'n' => mode = SortMode::Numeric,
                        'k' => {
                            let value_start = 2 + pos;
                            let raw_spec = if value_start < other.len() {
                                &other[value_start..]
                            } else {
                                let Some(spec) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'k'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                consumed_next = true;
                                spec.as_str()
                            };
                            match parse_sort_key_spec(raw_spec) {
                                Ok(parsed) => key_specs.push(parsed),
                                Err(err) => {
                                    fro::cio_eprintln!("sort: {err}");
                                    return Ok(2);
                                }
                            }
                            break;
                        }
                        'r' => reverse = true,
                        'u' => unique = true,
                        'V' => mode = SortMode::Version,
                        'z' => terminator = RecordTerminator::Nul,
                        'o' => {
                            let value_start = 2 + pos;
                            if value_start < other.len() {
                                output_path = Some(other[value_start..].to_string());
                            } else {
                                let Some(path) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'o'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                output_path = Some(path.clone());
                                consumed_next = true;
                            }
                            break;
                        }
                        'T' => {
                            let value_start = 2 + pos;
                            if value_start < other.len() {
                                temporary_directory = Some(other[value_start..].to_string());
                            } else {
                                let Some(path) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'T'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                temporary_directory = Some(path.clone());
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
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported sort flag: {other}"),
                    ));
                }
                if consumed_next {
                    idx += 1;
                }
            }
            other if !end_flags && other.starts_with('-') && other != "-" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported sort flag: {other}"),
                ));
            }
            other => files.push(other.to_string()),
        }
        idx += 1;
    }

    if check_mode.is_enabled() && files.len() > 1 {
        fro::cio_eprintln!(
            "sort: extra operand '{}' not allowed with {}",
            files[1],
            check_mode.short_flag()
        );
        return Ok(2);
    }
    if check_mode.is_enabled() && output_path.is_some() {
        fro::cio_eprintln!(
            "sort: options '{}o' are incompatible",
            check_mode.short_flag()
        );
        return Ok(2);
    }

    let inputs = parse_stream_inputs(files);
    if inputs
        .iter()
        .filter(|input| matches!(input, StreamInput::Stdin { .. }))
        .count()
        > 1
    {
        fro::cio_eprintln!("sort: repeated '-' operands are not supported");
        fro::cio_eprintln!("Try 'sort --help' for more information.");
        return Ok(2);
    }

    let started_at = std::time::Instant::now();
    let comparator = SortComparator::new(mode, key_specs);
    let total_bytes = if check_mode.is_enabled() {
        match external::check_input_sorted(
            &inputs[0],
            io_mode,
            &comparator,
            unique,
            reverse,
            terminator,
        ) {
            Ok(result) => {
                if let Some(disorder) = result.disorder {
                    if check_mode.emits_diagnostics() {
                        report_sort_disorder(sort_input_label(&inputs[0]), &disorder, terminator)?;
                    }
                    return Ok(1);
                }
                result.total_bytes
            }
            Err(err) => {
                fro::cio_eprintln!("sort: {err}");
                return Ok(2);
            }
        }
    } else {
        match if merge {
            external::merge_presorted_inputs(
                &inputs,
                io_mode,
                &comparator,
                unique,
                reverse,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
            )
        } else {
            external::sort_inputs(
                &inputs,
                io_mode,
                &comparator,
                unique,
                reverse,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
            )
        } {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(path) = output_path.as_deref() {
                    if err.kind() == io::ErrorKind::PermissionDenied {
                        fro::cio_eprintln!("sort: cannot write '{path}': {err}");
                    } else {
                        fro::cio_eprintln!("sort: {err}");
                    }
                } else {
                    fro::cio_eprintln!("sort: {err}");
                }
                return Ok(2);
            }
        }
    };

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
        for (sequence, line) in lines.iter().enumerate() {
            let start = storage.len();
            storage.extend_from_slice(line);
            refs.push(SortLineRef {
                start,
                len: line.len(),
                sequence: sequence as u64,
            });
        }
        (storage, refs)
    }

    fn comparator(mode: SortMode) -> SortComparator {
        SortComparator::new(mode, Vec::new())
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
        sort_line_refs(&mut refs, &storage, &comparator(SortMode::Bytewise), false).unwrap();
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
        let mut next_sequence = 0;
        append_input_lines(
            &mut storage,
            &mut refs,
            b"beta\nalpha",
            &mut next_sequence,
            RecordTerminator::Newline,
        )
        .unwrap();
        let lines = refs
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(lines, vec![b"beta".to_vec(), b"alpha".to_vec()]);
    }

    #[test]
    fn append_input_lines_splits_nul_terminated_records() {
        let mut storage = Vec::new();
        let mut refs = Vec::new();
        let mut next_sequence = 0;
        append_input_lines(
            &mut storage,
            &mut refs,
            b"beta\0alpha",
            &mut next_sequence,
            RecordTerminator::Nul,
        )
        .unwrap();
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
        finalize_sorted_lines(
            &mut sorted,
            &storage,
            &comparator(SortMode::Bytewise),
            false,
            false,
        )
        .unwrap();
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
        finalize_sorted_lines(
            &mut unique_only,
            &storage,
            &comparator(SortMode::Bytewise),
            true,
            false,
        )
        .unwrap();
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
            &comparator(SortMode::Bytewise),
            true,
            true,
        )
        .unwrap();
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
        sort_line_refs(&mut refs, &storage, &comparator(SortMode::Numeric), false).unwrap();
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
        finalize_sorted_lines(
            &mut refs,
            &storage,
            &comparator(SortMode::Numeric),
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![b"1.0".to_vec(), b"2".to_vec()]
        );
    }

    #[test]
    fn general_numeric_compare_orders_invalid_nan_and_numbers() {
        let lines = [
            b"x".as_slice(),
            b"NaN".as_slice(),
            b"-inf".as_slice(),
            b"-3".as_slice(),
            b".5".as_slice(),
            b"0x10".as_slice(),
            b"1e2".as_slice(),
            b"+inf".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&lines);
        sort_line_refs(
            &mut refs,
            &storage,
            &comparator(SortMode::GeneralNumeric),
            false,
        )
        .unwrap();
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"x".to_vec(),
                b"NaN".to_vec(),
                b"-inf".to_vec(),
                b"-3".to_vec(),
                b".5".to_vec(),
                b"0x10".to_vec(),
                b"1e2".to_vec(),
                b"+inf".to_vec(),
            ]
        );
    }

    #[test]
    fn human_numeric_unique_compares_suffix_families() {
        let input = [
            b"1KiB".as_slice(),
            b"1K".as_slice(),
            b"1024".as_slice(),
            b"1000".as_slice(),
            b"1024K".as_slice(),
            b"1M".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&input);
        finalize_sorted_lines(
            &mut refs,
            &storage,
            &comparator(SortMode::HumanNumeric),
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"1000".to_vec(),
                b"1024".to_vec(),
                b"1KiB".to_vec(),
                b"1024K".to_vec(),
                b"1M".to_vec(),
            ]
        );
    }

    #[test]
    fn month_sort_groups_by_month_prefix_and_treats_invalid_as_equal_keys() {
        let input = [
            b"foo".as_slice(),
            b"Jan".as_slice(),
            b"January".as_slice(),
            b"  feb".as_slice(),
            b"Feb".as_slice(),
            b"Dec".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&input);
        finalize_sorted_lines(
            &mut refs,
            &storage,
            &comparator(SortMode::Month),
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"foo".to_vec(),
                b"Jan".to_vec(),
                b"  feb".to_vec(),
                b"Dec".to_vec()
            ]
        );
    }

    #[test]
    fn version_sort_matches_strverscmp_style_digit_ordering() {
        let input = [
            b"v1".as_slice(),
            b"v01".as_slice(),
            b"v1.0".as_slice(),
            b"v1.0.02".as_slice(),
            b"v1.0.2".as_slice(),
            b"v1.0.10".as_slice(),
            b"v1~".as_slice(),
        ];
        let (storage, mut refs) = refs_for_lines(&input);
        sort_line_refs(&mut refs, &storage, &comparator(SortMode::Version), false).unwrap();
        assert_eq!(
            refs.iter()
                .map(|line| line.bytes(&storage).to_vec())
                .collect::<Vec<_>>(),
            vec![
                b"v1~".to_vec(),
                b"v01".to_vec(),
                b"v1".to_vec(),
                b"v1.0".to_vec(),
                b"v1.0.02".to_vec(),
                b"v1.0.2".to_vec(),
                b"v1.0.10".to_vec(),
            ]
        );
    }

    #[test]
    fn compare_line_bytes_matches_numeric_last_resort_ordering() {
        assert_eq!(
            compare_line_bytes(b"1", b"1.0", &comparator(SortMode::Numeric), false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"x", b"NaN", &comparator(SortMode::GeneralNumeric), false,),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(
                b"1KiB",
                b"1024K",
                &comparator(SortMode::HumanNumeric),
                false,
            ),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"JAN", b"Jan", &comparator(SortMode::Month), false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"v01", b"v1", &comparator(SortMode::Version), false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"beta", b"alpha", &comparator(SortMode::Bytewise), true),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn parse_sort_key_spec_supports_bounded_field_ranges() {
        assert_eq!(
            parse_sort_key_spec("2.3,4.5").unwrap(),
            SortKeySpec {
                start: SortKeyPosition {
                    field: 2,
                    char_offset: 2,
                },
                end: SortKeyEnd::Char {
                    field: 4,
                    char_end: 5,
                },
            }
        );
        assert_eq!(
            parse_sort_key_spec("3,3.0").unwrap(),
            SortKeySpec {
                start: SortKeyPosition {
                    field: 3,
                    char_offset: 0,
                },
                end: SortKeyEnd::FieldEnd { field: 3 },
            }
        );
        assert!(parse_sort_key_spec("0,1").is_err());
        assert!(parse_sort_key_spec("1.0,1").is_err());
        assert!(parse_sort_key_spec("1b,1").is_err());
    }
}
