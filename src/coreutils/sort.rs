use super::*;
use std::path::Path;

mod compare;
mod external;
#[cfg(test)]
mod tests;

include!("sort/common.rs");
include!("sort/cli.rs");

pub(super) fn run_sort(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut debug = false;
    let mut check_mode = SortCheckMode::None;
    let mut merge = false;
    let mut mode = SortMode::Bytewise;
    let mut reverse = false;
    let mut stable = false;
    let mut dictionary_order = false;
    let mut ignore_case = false;
    let mut ignore_leading_blanks = false;
    let mut ignore_nonprinting = false;
    let mut unique = false;
    let mut terminator = RecordTerminator::Newline;
    let mut output_path = None;
    let mut buffer_size_override = None;
    let mut field_separator = None;
    let mut temporary_directory = None;
    let mut random_source = None;
    let mut files0_from = None;
    let mut batch_size = None;
    let mut parallel_override = None;
    let mut compress_program = None;
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
            "--debug" if !end_flags => debug = true,
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
            "-b" | "--ignore-leading-blanks" if !end_flags => ignore_leading_blanks = true,
            "-d" | "--dictionary-order" if !end_flags => dictionary_order = true,
            "-f" | "--ignore-case" if !end_flags => ignore_case = true,
            "-i" | "--ignore-nonprinting" if !end_flags => ignore_nonprinting = true,
            "-g" | "--general-numeric-sort" if !end_flags => mode = SortMode::GeneralNumeric,
            "-h" | "--human-numeric-sort" if !end_flags => mode = SortMode::HumanNumeric,
            "-M" | "--month-sort" if !end_flags => mode = SortMode::Month,
            "-m" | "--merge" if !end_flags => merge = true,
            "-n" | "--numeric-sort" if !end_flags => mode = SortMode::Numeric,
            "-R" | "--random-sort" if !end_flags => mode = SortMode::Random,
            "--sort" if !end_flags => {
                let Some(word) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--sort' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                let Some(parsed) = parse_sort_word(word) else {
                    report_invalid_sort_word(word);
                    return Ok(2);
                };
                mode = parsed;
                idx += 1;
            }
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
            "-t" if !end_flags => {
                let Some(raw) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 't'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match parse_field_separator(raw) {
                    Ok(separator) => field_separator = Some(separator),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
            "--field-separator" if !end_flags => {
                let Some(raw) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--field-separator' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match parse_field_separator(raw) {
                    Ok(separator) => field_separator = Some(separator),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
            "-r" | "--reverse" if !end_flags => reverse = true,
            "-s" | "--stable" if !end_flags => stable = true,
            "-S" if !end_flags => {
                let Some(value) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'S'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match external::parse_sort_buffer_size(value, "-S") {
                    Ok(parsed) => buffer_size_override = Some(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
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
            "--random-source" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--random-source' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                if let Err(err) = set_random_source(&mut random_source, path) {
                    fro::cio_eprintln!("sort: {err}");
                    return Ok(2);
                }
                idx += 1;
            }
            "--files0-from" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--files0-from' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                files0_from = Some(path.clone());
                idx += 1;
            }
            "--batch-size" if !end_flags => {
                let Some(value) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--batch-size' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match parse_sort_batch_size(value) {
                    Ok(parsed) => batch_size = Some(parsed),
                    Err(err) => {
                        report_sort_batch_size_parse_error(err);
                        return Ok(2);
                    }
                }
                idx += 1;
            }
            "--parallel" if !end_flags => {
                let Some(value) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--parallel' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match parse_sort_parallel(value) {
                    Ok(parsed) => parallel_override = Some(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
            "--compress-program" if !end_flags => {
                let Some(value) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--compress-program' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                compress_program = Some(value.clone());
                idx += 1;
            }
            "--buffer-size" if !end_flags => {
                let Some(value) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option '--buffer-size' requires an argument");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                match external::parse_sort_buffer_size(value, "--buffer-size") {
                    Ok(parsed) => buffer_size_override = Some(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
                idx += 1;
            }
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
            other if !end_flags && other.starts_with("--sort=") => {
                let word = &other["--sort=".len()..];
                let Some(parsed) = parse_sort_word(word) else {
                    report_invalid_sort_word(word);
                    return Ok(2);
                };
                mode = parsed;
            }
            other if !end_flags && other.starts_with("--field-separator=") => {
                match parse_field_separator(&other["--field-separator=".len()..]) {
                    Ok(separator) => field_separator = Some(separator),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
            }
            other if !end_flags && other.starts_with("--random-source=") => {
                if let Err(err) =
                    set_random_source(&mut random_source, &other["--random-source=".len()..])
                {
                    fro::cio_eprintln!("sort: {err}");
                    return Ok(2);
                }
            }
            other if !end_flags && other.starts_with("--files0-from=") => {
                files0_from = Some(other["--files0-from=".len()..].to_string());
            }
            other if !end_flags && other.starts_with("--batch-size=") => {
                match parse_sort_batch_size(&other["--batch-size=".len()..]) {
                    Ok(parsed) => batch_size = Some(parsed),
                    Err(err) => {
                        report_sort_batch_size_parse_error(err);
                        return Ok(2);
                    }
                }
            }
            other if !end_flags && other.starts_with("--parallel=") => {
                match parse_sort_parallel(&other["--parallel=".len()..]) {
                    Ok(parsed) => parallel_override = Some(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
            }
            other if !end_flags && other.starts_with("--compress-program=") => {
                compress_program = Some(other["--compress-program=".len()..].to_string());
            }
            other if !end_flags && other.starts_with("--temporary-directory=") => {
                temporary_directory = Some(other["--temporary-directory=".len()..].to_string());
            }
            other if !end_flags && other.starts_with("--buffer-size=") => {
                match external::parse_sort_buffer_size(
                    &other["--buffer-size=".len()..],
                    "--buffer-size",
                ) {
                    Ok(parsed) => buffer_size_override = Some(parsed),
                    Err(err) => {
                        fro::cio_eprintln!("sort: {err}");
                        return Ok(2);
                    }
                }
            }
            other
                if !end_flags
                    && match apply_short_sort_flags(
                        other,
                        &mut check_mode,
                        &mut merge,
                        &mut mode,
                        &mut reverse,
                        &mut stable,
                        &mut dictionary_order,
                        &mut ignore_case,
                        &mut ignore_leading_blanks,
                        &mut ignore_nonprinting,
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
                        'b' => ignore_leading_blanks = true,
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
                        'd' => dictionary_order = true,
                        'f' => ignore_case = true,
                        'i' => ignore_nonprinting = true,
                        'g' => mode = SortMode::GeneralNumeric,
                        'h' => mode = SortMode::HumanNumeric,
                        'M' => mode = SortMode::Month,
                        'm' => merge = true,
                        'n' => mode = SortMode::Numeric,
                        'R' => mode = SortMode::Random,
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
                        't' => {
                            let value_start = 2 + pos;
                            let raw_separator = if value_start < other.len() {
                                &other[value_start..]
                            } else {
                                let Some(raw) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 't'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                consumed_next = true;
                                raw.as_str()
                            };
                            match parse_field_separator(raw_separator) {
                                Ok(separator) => field_separator = Some(separator),
                                Err(err) => {
                                    fro::cio_eprintln!("sort: {err}");
                                    return Ok(2);
                                }
                            }
                            break;
                        }
                        'r' => reverse = true,
                        's' => stable = true,
                        'S' => {
                            let value_start = 2 + pos;
                            let raw_size = if value_start < other.len() {
                                &other[value_start..]
                            } else {
                                let Some(value) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'S'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                consumed_next = true;
                                value.as_str()
                            };
                            match external::parse_sort_buffer_size(raw_size, "-S") {
                                Ok(parsed) => buffer_size_override = Some(parsed),
                                Err(err) => {
                                    fro::cio_eprintln!("sort: {err}");
                                    return Ok(2);
                                }
                            }
                            break;
                        }
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
    if debug && check_mode.is_enabled() {
        fro::cio_eprintln!(
            "sort: options '{} --debug' are incompatible",
            check_mode.short_flag()
        );
        return Ok(2);
    }
    if debug && output_path.is_some() {
        fro::cio_eprintln!("sort: options '-o --debug' are incompatible");
        return Ok(2);
    }
    if dictionary_order {
        let incompatible = match mode {
            SortMode::Numeric => Some('n'),
            SortMode::GeneralNumeric => Some('g'),
            SortMode::HumanNumeric => Some('h'),
            SortMode::Month => Some('M'),
            SortMode::Bytewise | SortMode::Random | SortMode::Version => None,
        };
        if let Some(flag) = incompatible {
            fro::cio_eprintln!("sort: options '-d{flag}' are incompatible");
            return Ok(2);
        }
    }
    if ignore_nonprinting {
        let incompatible = match mode {
            SortMode::Numeric => Some("-in"),
            SortMode::GeneralNumeric => Some("-gi"),
            SortMode::HumanNumeric => Some("-hi"),
            SortMode::Month => Some("-iM"),
            SortMode::Bytewise | SortMode::Random | SortMode::Version => None,
        };
        if let Some(flags) = incompatible {
            fro::cio_eprintln!("sort: options '{flags}' are incompatible");
            return Ok(2);
        }
    }

    let inputs = if let Some(files0_from) = files0_from.as_deref() {
        if !files.is_empty() {
            fro::cio_eprintln!("sort: extra operand '{}'", files[0]);
            fro::cio_eprintln!("file operands cannot be combined with --files0-from");
            fro::cio_eprintln!("Try 'sort --help' for more information.");
            return Ok(2);
        }
        match sort_inputs_from_files0(files0_from) {
            Ok(inputs) => inputs,
            Err(err) => {
                fro::cio_eprintln!("sort: {err}");
                return Ok(2);
            }
        }
    } else {
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
        inputs
    };

    let started_at = std::time::Instant::now();
    let mut comparator = SortComparator::new(
        mode,
        key_specs,
        field_separator,
        stable,
        dictionary_order,
        ignore_case,
        ignore_leading_blanks,
        ignore_nonprinting,
    );
    if mode == SortMode::Random {
        let random_seed = match random_seed_from_source(random_source.as_deref()) {
            Ok(seed) => seed,
            Err(err) => {
                fro::cio_eprintln!("sort: {err}");
                return Ok(2);
            }
        };
        comparator = comparator.with_random_seed(random_seed);
    }
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
                debug,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
                compress_program.as_deref(),
                batch_size,
                parallel_override,
            )
        } else {
            external::sort_inputs(
                &inputs,
                io_mode,
                &comparator,
                unique,
                reverse,
                debug,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
                compress_program.as_deref(),
                buffer_size_override,
                batch_size,
                parallel_override,
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
