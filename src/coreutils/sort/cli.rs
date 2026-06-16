use std::io;

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

fn parse_sort_word(word: &str) -> Option<SortMode> {
    match word {
        "general-numeric" => Some(SortMode::GeneralNumeric),
        "human-numeric" => Some(SortMode::HumanNumeric),
        "month" => Some(SortMode::Month),
        "numeric" => Some(SortMode::Numeric),
        "random" => Some(SortMode::Random),
        "version" => Some(SortMode::Version),
        _ => None,
    }
}

fn report_invalid_sort_word(word: &str) {
    fro::cio_eprintln!("sort: invalid argument '{word}' for '--sort'");
    fro::cio_eprintln!("Valid arguments in this bounded slice are:");
    for valid in [
        "general-numeric",
        "human-numeric",
        "month",
        "numeric",
        "random",
        "version",
    ] {
        fro::cio_eprintln!("  - '{valid}'");
    }
    fro::cio_eprintln!("Try 'sort --help' for more information.");
}

fn parse_sort_parallel(raw: &str) -> io::Result<usize> {
    let trimmed = raw.trim();
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid --parallel argument '{raw}'"),
        )
    };
    if trimmed.is_empty() {
        return Err(invalid());
    }
    let parsed = trimmed.parse::<u64>().map_err(|_| invalid())?;
    if parsed == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "number in parallel must be nonzero",
        ));
    }
    Ok(parsed.min(usize::MAX as u64) as usize)
}

#[derive(Debug, PartialEq, Eq)]
enum SortBatchSizeParseError {
    InvalidArgument(String),
    TooSmall(String),
}

fn parse_sort_batch_size(raw: &str) -> Result<usize, SortBatchSizeParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(SortBatchSizeParseError::InvalidArgument(raw.to_string()));
    }
    let parsed = trimmed
        .parse::<u64>()
        .map_err(|_| SortBatchSizeParseError::InvalidArgument(raw.to_string()))?;
    if parsed < 2 {
        return Err(SortBatchSizeParseError::TooSmall(trimmed.to_string()));
    }
    Ok(parsed.min(usize::MAX as u64) as usize)
}

fn report_sort_batch_size_parse_error(err: SortBatchSizeParseError) {
    match err {
        SortBatchSizeParseError::InvalidArgument(raw) => {
            fro::cio_eprintln!("sort: invalid --batch-size argument '{raw}'");
        }
        SortBatchSizeParseError::TooSmall(raw) => {
            fro::cio_eprintln!("sort: invalid --batch-size argument '{raw}'");
            fro::cio_eprintln!("sort: minimum --batch-size argument is '2'");
        }
    }
}

fn print_sort_help(program: &str) {
    fro::cio_println!("sort - Sort newline-delimited records (or NUL-delimited with -z)");
    fro::cio_println!();
    fro::cio_println!("Usage: {program} [OPTION]... [FILE]...");
    fro::cio_println!("  or:  {program} [OPTION]... --files0-from=F");
    fro::cio_println!();
    fro::cio_println!(
        "This bounded slice sorts locale-independent byte or version records plus the existing numeric/general-numeric/human-numeric/month modes, with bounded GNU-style -b/-d/-f/-i/-k/-t/--sort=WORD selection, using newlines by default and NULs with -z."
    );
    fro::cio_println!(
        "It currently supports the default case plus bounded -b/-d/-f/-i/-k/-t/--sort=WORD/--dictionary-order/--ignore-leading-blanks/--ignore-case/--ignore-nonprinting/--key/--field-separator, -g/-h/-M/-n/-R/-V, --random-source, --files0-from, --parallel, --compress-program, -z, reverse/stable/unique, merge/check, -S buffer sizing, and -o output."
    );
    fro::cio_println!();
    fro::cio_println!("Supported options:");
    fro::cio_println!("  -c, --check, --check=diagnose-first");
    fro::cio_println!("                       check for sorted input; diagnose first bad line");
    fro::cio_println!("  -C, --check=quiet, --check=silent");
    fro::cio_println!("                       like -c, but do not report the first bad line");
    fro::cio_println!("  -b, --ignore-leading-blanks  ignore leading blanks in sort keys");
    fro::cio_println!(
        "  -d, --dictionary-order  compare only blanks and ASCII letters/digits in sort keys"
    );
    fro::cio_println!(
        "  -f, --ignore-case   fold ASCII lowercase bytes to uppercase for key comparison"
    );
    fro::cio_println!(
        "  -i, --ignore-nonprinting  compare only printable ASCII bytes in sort keys"
    );
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
    fro::cio_println!(
        "  -R, --random-sort    shuffle records while grouping identical selected keys"
    );
    fro::cio_println!("  -k KEY, --key=KEY    sort via bounded GNU-style field ranges");
    fro::cio_println!("  -t SEP, --field-separator=SEP");
    fro::cio_println!(
        "                       use byte SEP instead of the default blank-run field split"
    );
    fro::cio_println!("  -r, --reverse        reverse the result of comparisons");
    fro::cio_println!("      --random-source=FILE");
    fro::cio_println!(
        "                       seed -R/--random-sort from the first 16 bytes of FILE"
    );
    fro::cio_println!("      --files0-from=F  read NUL-terminated input file names from F");
    fro::cio_println!("      --batch-size=NMERGE  merge at most NMERGE temp runs at once");
    fro::cio_println!("      --parallel=N     cap packed sort worker threads at N");
    fro::cio_println!("      --compress-program=PROG");
    fro::cio_println!(
        "                       shell PROG for spill-run compression and PROG -d for merge reads"
    );
    fro::cio_println!("      --debug          annotate the part of each line used to sort");
    fro::cio_println!("      --sort=WORD      choose general-numeric, human-numeric, month, numeric, random, or version ordering");
    fro::cio_println!("  -s, --stable         preserve input order for records with equal keys");
    fro::cio_println!("  -S, --buffer-size=SIZE");
    fro::cio_println!("                       cap the in-memory sort buffer before spilling runs");
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
    fro::cio_println!("  - --files0-from=F reads NUL-delimited path names from F (or stdin when F is -) and rejects '-' entries because stdin is already consumed by the list.");
    fro::cio_println!("  - Bytewise in-memory sorting uses a StringZilla argsort fast path.");
    fro::cio_println!("  - Inputs larger than available memory spill sorted runs and merge them.");
    fro::cio_println!("  - -T only matters when spill temp files are actually created.");
    fro::cio_println!("  - --compress-program=PROG only applies when the spill/merge backend materializes temp runs; fro shells `PROG` to write them and `PROG -d` to read them back.");
    fro::cio_println!("  - --batch-size=NMERGE caps the existing out-of-core merge fan-in for spill runs and -m presorted-input merges; in-memory sorts ignore it.");
    fro::cio_println!("  - -m reuses the spill/merge backend on already sorted inputs.");
    fro::cio_println!("  - -c/--check=diagnose-first validates one input stream and exits 1 on the first disorder.");
    fro::cio_println!("  - -C/--check=quiet/--check=silent reuses the same check path but suppresses disorder diagnostics.");
    fro::cio_println!("  - --debug reuses the existing sort/merge pipeline, prints per-record key annotations to stdout, and emits bounded questionable-usage warnings to stderr.");
    fro::cio_println!("  - -g uses C-locale strtod-style prefixes; NaNs sort after non-numbers and before infinities.");
    fro::cio_println!("  - -h compares the leading numeric prefix plus an optional K/M/G/T/P/E/Z/Y suffix family.");
    fro::cio_println!("  - -d/--dictionary-order filters each selected key down to spaces, tabs, and ASCII alphanumerics before the existing compare/merge/check/unique/output pipeline runs.");
    fro::cio_println!("  - -b/--ignore-leading-blanks trims leading spaces and tabs from the selected sort key before the existing compare, merge, check, unique, and output pipeline runs.");
    fro::cio_println!("  - -f/--ignore-case folds ASCII lowercase to uppercase for the selected sort keys while reusing the existing comparator/output backend.");
    fro::cio_println!("  - -i/--ignore-nonprinting filters each selected key down to printable ASCII bytes before the existing compare, merge, check, unique, and output pipeline runs.");
    fro::cio_println!("  - -M looks at the first nonblank three-letter month abbreviation and treats other lines as invalid month keys.");
    fro::cio_println!("  - -R/--random-sort hashes the selected sort key with a seeded GNU-style MD5 ordering so identical keys stay adjacent while the existing merge, spill, check, unique, and output pipeline stays intact.");
    fro::cio_println!("  - --random-source=FILE seeds -R/--random-sort from FILE without changing non-random sorts; FILE must provide at least 16 bytes.");
    fro::cio_println!("  - --sort=WORD reuses the existing -g/-h/-M/-n/-R/-V ordering backends for general-numeric, human-numeric, month, numeric, random, and version words only.");
    fro::cio_println!("  - -s/--stable disables GNU sort's last-resort whole-line comparison and keeps equal-key records in input order.");
    fro::cio_println!("  - -k/--key accepts one or more blank-separated field ranges in the form F[.C][,F[.C]], without per-key modifiers or locale collation.");
    fro::cio_println!("  - -t/--field-separator switches -k/--key field discovery from blank runs to one exact byte separator while reusing the existing compare, merge, check, unique, spill, and output pipeline.");
    fro::cio_println!("  - -S/--buffer-size=SIZE overrides the spill threshold that decides when the existing out-of-core sort backend takes over; GNU-style percentages use available memory when known.");
    fro::cio_println!("  - --parallel=N accepts GNU's positive thread-count syntax and currently bounds only the existing packed regular-file line-build/output helpers.");
    fro::cio_println!("  - -V uses GNU/libc version-order comparisons while preserving the existing spill, merge, and check backend.");
    fro::cio_println!("  - -d is GNU-compatible with bytewise and version ordering, but remains incompatible with -n/-g/-h/-M.");
    fro::cio_println!("  - -i is GNU-compatible with bytewise and version ordering, but remains incompatible with -n/-g/-h/-M.");
    fro::cio_println!("  - Unsupported GNU sort features currently return an error:");
    fro::cio_println!("    locale collation and per-key modifiers.");
}

fn apply_short_sort_flags(
    arg: &str,
    check_mode: &mut SortCheckMode,
    merge: &mut bool,
    mode: &mut SortMode,
    reverse: &mut bool,
    stable: &mut bool,
    dictionary_order: &mut bool,
    ignore_case: &mut bool,
    ignore_leading_blanks: &mut bool,
    ignore_nonprinting: &mut bool,
    unique: &mut bool,
    terminator: &mut RecordTerminator,
) -> io::Result<bool> {
    if !arg.starts_with('-') || arg.starts_with("--") || arg == "-" {
        return Ok(false);
    }
    for flag in arg[1..].bytes() {
        match flag {
            b'b' => *ignore_leading_blanks = true,
            b'c' => set_sort_check_mode(check_mode, SortCheckMode::DiagnoseFirst)?,
            b'C' => set_sort_check_mode(check_mode, SortCheckMode::Silent)?,
            b'd' => *dictionary_order = true,
            b'f' => *ignore_case = true,
            b'i' => *ignore_nonprinting = true,
            b'g' => *mode = SortMode::GeneralNumeric,
            b'h' => *mode = SortMode::HumanNumeric,
            b'M' => *mode = SortMode::Month,
            b'm' => *merge = true,
            b'n' => *mode = SortMode::Numeric,
            b'R' => *mode = SortMode::Random,
            b'r' => *reverse = true,
            b's' => *stable = true,
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

fn parse_field_separator(raw: &str) -> io::Result<u8> {
    match raw.as_bytes() {
        [] => Err(io::Error::new(io::ErrorKind::InvalidInput, "empty tab")),
        [separator] => Ok(*separator),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("multi-character tab '{raw}'"),
        )),
    }
}
fn set_random_source(current: &mut Option<String>, next: &str) -> io::Result<()> {
    if current.as_deref().is_some_and(|existing| existing != next) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "multiple random sources specified",
        ));
    }
    *current = Some(next.to_string());
    Ok(())
}
