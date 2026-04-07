use super::*;
use std::os::unix::fs::FileExt;

fn count_newlines_in_range(
    path: &str,
    io_mode: IOMode,
    mode: &str,
    start_offset: u64,
    end_offset: u64,
) -> io::Result<u64> {
    let data = load_file_bytes(path, io_mode, mode)?;
    let bytes = data.data.as_slice();
    let start = usize::try_from(start_offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?
        .min(bytes.len());
    let end = usize::try_from(end_offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?
        .min(bytes.len());
    Ok(memchr_iter(b'\n', &bytes[start..end]).count() as u64)
}

pub(super) fn cmp_effective_compare_len(
    first_len: u64,
    second_len: u64,
    limit: Option<u64>,
) -> u64 {
    first_len.min(second_len).min(limit.unwrap_or(u64::MAX))
}

pub(super) fn cmp_remaining_len_after_skip(len: u64, skip: u64) -> u64 {
    len.saturating_sub(skip)
}

pub(super) fn cmp_effective_compare_len_with_skips(
    first_len: u64,
    second_len: u64,
    first_skip: u64,
    second_skip: u64,
    limit: Option<u64>,
) -> u64 {
    cmp_effective_compare_len(
        cmp_remaining_len_after_skip(first_len, first_skip),
        cmp_remaining_len_after_skip(second_len, second_skip),
        limit,
    )
}

pub(super) fn cmp_flags_are_compatible(quiet: bool, verbose: bool) -> bool {
    !(quiet && verbose)
}

fn cmp_decimal_width(value: u64) -> usize {
    value.max(1).to_string().len()
}

fn cmp_byte_display_parts(byte: u8) -> (bool, u8) {
    if byte >= 128 {
        (true, byte - 128)
    } else {
        (false, byte)
    }
}

fn cmp_render_core_byte(core: u8) -> String {
    match core {
        0..=31 => format!("^{}", char::from(core + 64)),
        127 => "^?".to_string(),
        _ => char::from(core).to_string(),
    }
}

fn cmp_render_byte_char(byte: u8) -> String {
    let (meta, core) = cmp_byte_display_parts(byte);
    let rendered = cmp_render_core_byte(core);
    if meta {
        format!("M-{rendered}")
    } else {
        rendered
    }
}

fn cmp_read_byte_at(path: &str, offset: u64) -> io::Result<u8> {
    let file = fs::File::open(path)?;
    let mut byte = [0u8; 1];
    file.read_exact_at(&mut byte, offset)?;
    Ok(byte[0])
}

fn cmp_eof_line(newlines_before_eof: u64, ends_with_newline: bool) -> (u64, &'static str) {
    if ends_with_newline {
        (newlines_before_eof, "line")
    } else {
        (newlines_before_eof + 1, "in line")
    }
}

fn parse_cmp_limit(value: &str) -> io::Result<u64> {
    parse_cmp_count(value, "--bytes")
}

fn parse_cmp_skip_spec(value: &str) -> io::Result<(u64, u64)> {
    if let Some((left, right)) = value.split_once(':') {
        let first_skip =
            parse_cmp_count(left, "--ignore-initial").map_err(|_| invalid_cmp_skip_spec(value))?;
        let second_skip =
            parse_cmp_count(right, "--ignore-initial").map_err(|_| invalid_cmp_skip_spec(value))?;
        Ok((first_skip, second_skip))
    } else {
        let skip =
            parse_cmp_count(value, "--ignore-initial").map_err(|_| invalid_cmp_skip_spec(value))?;
        Ok((skip, skip))
    }
}

fn parse_cmp_count(value: &str, flag: &str) -> io::Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {flag} value '{value}'"),
        ));
    }

    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (num_str, suffix) = value.split_at(split);
    let num = num_str.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {flag} value '{value}'"),
        )
    })?;
    let multiplier = cmp_suffix_multiplier(suffix).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {flag} value '{value}'"),
        )
    })?;
    num.checked_mul(multiplier).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {flag} value '{value}'"),
        )
    })
}

fn cmp_suffix_multiplier(suffix: &str) -> Option<u64> {
    match suffix.trim() {
        "" => Some(1),
        "kB" | "KB" => Some(1_000),
        "k" | "K" | "KiB" => Some(1 << 10),
        "MB" => Some(1_000_000),
        "M" | "MiB" => Some(1 << 20),
        "GB" => Some(1_000_000_000),
        "G" | "GiB" => Some(1 << 30),
        "TB" => Some(1_000_000_000_000),
        "T" | "TiB" => Some(1_u64 << 40),
        "PB" => Some(1_000_000_000_000_000),
        "P" | "PiB" => Some(1_u64 << 50),
        "EB" => Some(1_000_000_000_000_000_000),
        "E" | "EiB" => Some(1_u64 << 60),
        _ => None,
    }
}

fn invalid_cmp_skip_spec(value: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid --ignore-initial value '{value}'"),
    )
}

pub(super) fn run_cmp(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut io_mode = IOMode::Auto;
    let mut quiet = false;
    let mut verbose = false;
    let mut print_bytes = false;
    let mut limit = None::<u64>;
    let mut first_skip = 0u64;
    let mut second_skip = 0u64;
    let mut files = Vec::new();
    let mut end_of_options = false;
    let mut i = 1usize;
    while i < args.len() {
        if end_of_options {
            files.push(args[i].clone());
            i += 1;
            continue;
        }
        match args[i].as_str() {
            "--" => end_of_options = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-s" | "--quiet" | "--silent" => quiet = true,
            "-l" | "--verbose" => verbose = true,
            "-b" | "--print-bytes" => print_bytes = true,
            "-n" | "--bytes" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing --bytes value")
                })?;
                limit = Some(parse_cmp_limit(value)?);
            }
            "-i" | "--ignore-initial" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing --ignore-initial value",
                    )
                })?;
                (first_skip, second_skip) = parse_cmp_skip_spec(value)?;
            }
            other if other.starts_with("-n") && other.len() > 2 => {
                limit = Some(parse_cmp_limit(&other[2..])?);
            }
            other if other.starts_with("--bytes=") => {
                let value = &other["--bytes=".len()..];
                limit = Some(parse_cmp_limit(value)?);
            }
            other if other.starts_with("-i") && other.len() > 2 => {
                (first_skip, second_skip) = parse_cmp_skip_spec(&other[2..])?;
            }
            other if other.starts_with("--ignore-initial=") => {
                let value = &other["--ignore-initial=".len()..];
                (first_skip, second_skip) = parse_cmp_skip_spec(value)?;
            }
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    let files = ensure_files(
        program,
        files,
        "[-s|--quiet|--silent] [-l|--verbose] [-b|--print-bytes] [-i SKIP|--ignore-initial=SKIP] [-n LIMIT|--bytes=LIMIT] [--auto|--no-direct|--direct] [--] <file1> <file2>",
    )?;
    if files.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cmp requires exactly two file operands",
        ));
    }
    if !cmp_flags_are_compatible(quiet, verbose) {
        eprintln!("cmp: options -l and -s are incompatible");
        eprintln!("cmp: Try 'cmp --help' for more information.");
        return Ok(2);
    }

    let first_len = fs::metadata(&files[0])?.len();
    let second_len = fs::metadata(&files[1])?.len();
    let first_remaining = cmp_remaining_len_after_skip(first_len, first_skip);
    let second_remaining = cmp_remaining_len_after_skip(second_len, second_skip);
    let shared_remaining = first_remaining.min(second_remaining);
    let compare_len =
        cmp_effective_compare_len_with_skips(first_len, second_len, first_skip, second_skip, limit);
    if compare_len == 0 {
        if limit == Some(0) || first_remaining == second_remaining {
            return Ok(0);
        }
        if !quiet {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            eprintln!("cmp: EOF on {} which is empty", eof_file);
        }
        return Ok(1);
    }

    if verbose {
        let first = load_file_bytes(&files[0], io_mode, "read")?;
        let second = load_file_bytes(&files[1], io_mode, "read")?;
        let first_start = usize::try_from(first_skip)
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize")
            })?
            .min(first.data.len());
        let second_start = usize::try_from(second_skip)
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize")
            })?
            .min(second.data.len());
        let compare_len_usize = usize::try_from(compare_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize")
        })?;
        let first_slice = &first.data.as_slice()[first_start..first_start + compare_len_usize];
        let second_slice = &second.data.as_slice()[second_start..second_start + compare_len_usize];
        let byte_width = cmp_decimal_width(compare_len);
        let mut had_mismatch = false;
        for (idx, (&left, &right)) in first_slice.iter().zip(second_slice.iter()).enumerate() {
            if left != right {
                had_mismatch = true;
                if print_bytes {
                    println!(
                        "{:>width$} {:>3o} {:<4} {:>3o} {}",
                        idx + 1,
                        left,
                        cmp_render_byte_char(left),
                        right,
                        cmp_render_byte_char(right),
                        width = byte_width
                    );
                } else {
                    println!(
                        "{:>width$} {:>3o} {:>3o}",
                        idx + 1,
                        left,
                        right,
                        width = byte_width
                    );
                }
            }
        }
        if first_remaining != second_remaining
            && limit.map_or(true, |limit| limit > shared_remaining)
        {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            eprintln!("cmp: EOF on {} after byte {}", eof_file, shared_remaining);
            return Ok(1);
        }
        return Ok(if had_mismatch { 1 } else { 0 });
    }

    let config = load_config(None);
    let diff_page_cache = config.get_params_for_path("diff", false, &files[0]);
    let diff_direct = config.get_params_for_path("diff", true, &files[0]);
    let mismatch = diff_files_window(
        &files[0],
        &files[1],
        first_skip,
        second_skip,
        diff_page_cache.num_threads,
        diff_page_cache.block_size,
        diff_page_cache.qd,
        diff_direct.num_threads,
        diff_direct.block_size,
        diff_direct.qd,
        internal_io_mode(io_mode),
        false,
        false,
        Some(compare_len),
    )?;
    if mismatch != 0 {
        if !quiet {
            let index = mismatch as usize - 1;
            let line = 1 + count_newlines_in_range(
                &files[0],
                io_mode,
                "read",
                first_skip,
                first_skip + mismatch - 1,
            )?;
            if print_bytes {
                let left = cmp_read_byte_at(&files[0], first_skip + mismatch - 1)?;
                let right = cmp_read_byte_at(&files[1], second_skip + mismatch - 1)?;
                println!(
                    "{} {} differ: byte {}, line {} is {:>3o} {} {:>3o} {}",
                    files[0],
                    files[1],
                    index + 1,
                    line,
                    left,
                    cmp_render_byte_char(left),
                    right,
                    cmp_render_byte_char(right)
                );
            } else {
                println!(
                    "{} {} differ: byte {}, line {}",
                    files[0],
                    files[1],
                    index + 1,
                    line
                );
            }
        }
        return Ok(1);
    }

    if first_remaining != second_remaining && limit.map_or(true, |limit| limit > shared_remaining) {
        if !quiet {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            let eof_skip = if first_remaining < second_remaining {
                first_skip
            } else {
                second_skip
            };
            let eof_len = if first_remaining < second_remaining {
                first_remaining
            } else {
                second_remaining
            };
            let data = load_file_bytes(eof_file, io_mode, "read")?;
            let bytes = data.data.as_slice();
            let slice_start = usize::try_from(eof_skip)
                .unwrap_or(bytes.len())
                .min(bytes.len());
            let slice_end = usize::try_from(eof_skip + eof_len)
                .unwrap_or(bytes.len())
                .min(bytes.len());
            let newlines_before_eof =
                memchr_iter(b'\n', &bytes[slice_start..slice_end]).count() as u64;
            let ends_with_newline = slice_end > slice_start && bytes[slice_end - 1] == b'\n';
            let (line, phrase) = cmp_eof_line(newlines_before_eof, ends_with_newline);
            eprintln!(
                "cmp: EOF on {} after byte {}, {} {}",
                eof_file, shared_remaining, phrase, line
            );
        }
        return Ok(1);
    }

    Ok(0)
}

#[cfg(kani)]
mod kani_proofs {
    use super::{
        cmp_byte_display_parts, cmp_effective_compare_len, cmp_effective_compare_len_with_skips,
        cmp_flags_are_compatible, cmp_remaining_len_after_skip,
    };

    #[kani::proof]
    fn cmp_effective_compare_len_matches_min_formula() {
        let first_len: u64 = kani::any();
        let second_len: u64 = kani::any();
        let limit_present: bool = kani::any();
        let limit_value: u64 = kani::any();
        let limit = if limit_present {
            Some(limit_value)
        } else {
            None
        };
        assert_eq!(
            cmp_effective_compare_len(first_len, second_len, limit),
            first_len.min(second_len).min(limit.unwrap_or(u64::MAX))
        );
    }

    #[kani::proof]
    fn cmp_effective_compare_len_with_skips_matches_remaining_formula() {
        let first_len: u64 = kani::any();
        let second_len: u64 = kani::any();
        let first_skip: u64 = kani::any();
        let second_skip: u64 = kani::any();
        let limit_present: bool = kani::any();
        let limit_value: u64 = kani::any();
        let limit = if limit_present {
            Some(limit_value)
        } else {
            None
        };
        assert_eq!(
            cmp_effective_compare_len_with_skips(
                first_len,
                second_len,
                first_skip,
                second_skip,
                limit
            ),
            first_len
                .saturating_sub(first_skip)
                .min(second_len.saturating_sub(second_skip))
                .min(limit.unwrap_or(u64::MAX))
        );
    }

    #[kani::proof]
    fn cmp_remaining_len_after_skip_matches_saturating_sub() {
        let len: u64 = kani::any();
        let skip: u64 = kani::any();
        assert_eq!(
            cmp_remaining_len_after_skip(len, skip),
            len.saturating_sub(skip)
        );
    }

    #[kani::proof]
    fn cmp_flag_compatibility_matches_quiet_verbose_formula() {
        let quiet: bool = kani::any();
        let verbose: bool = kani::any();
        assert_eq!(
            cmp_flags_are_compatible(quiet, verbose),
            !(quiet && verbose)
        );
    }

    #[kani::proof]
    fn cmp_byte_display_parts_split_meta_bit() {
        let byte: u8 = kani::any();
        let (meta, core) = cmp_byte_display_parts(byte);
        assert_eq!(meta, byte >= 128);
        assert!(core <= 127);
        assert_eq!(byte, core + if meta { 128 } else { 0 });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cmp_byte_display_parts, cmp_decimal_width, cmp_effective_compare_len,
        cmp_effective_compare_len_with_skips, cmp_eof_line, cmp_flags_are_compatible,
        cmp_remaining_len_after_skip, cmp_render_byte_char, parse_cmp_limit, parse_cmp_skip_spec,
    };

    #[test]
    fn cmp_effective_compare_len_respects_shorter_file_and_limit() {
        assert_eq!(cmp_effective_compare_len(10, 12, None), 10);
        assert_eq!(cmp_effective_compare_len(10, 12, Some(4)), 4);
        assert_eq!(cmp_effective_compare_len(3, 9, Some(99)), 3);
        assert_eq!(cmp_effective_compare_len(3, 9, Some(0)), 0);
    }

    #[test]
    fn cmp_remaining_len_after_skip_saturates_at_zero() {
        assert_eq!(cmp_remaining_len_after_skip(10, 0), 10);
        assert_eq!(cmp_remaining_len_after_skip(10, 4), 6);
        assert_eq!(cmp_remaining_len_after_skip(3, 9), 0);
    }

    #[test]
    fn cmp_effective_compare_len_with_skips_respects_remaining_prefixes() {
        assert_eq!(cmp_effective_compare_len_with_skips(10, 12, 3, 4, None), 7);
        assert_eq!(
            cmp_effective_compare_len_with_skips(10, 12, 3, 4, Some(2)),
            2
        );
        assert_eq!(cmp_effective_compare_len_with_skips(3, 9, 6, 6, None), 0);
        assert_eq!(cmp_effective_compare_len_with_skips(3, 9, 4, 6, Some(9)), 0);
    }

    #[test]
    fn parse_cmp_limit_accepts_decimal_counts() {
        assert_eq!(parse_cmp_limit("0").unwrap(), 0);
        assert_eq!(parse_cmp_limit("17").unwrap(), 17);
        assert_eq!(parse_cmp_limit("1k").unwrap(), 1024);
        assert_eq!(parse_cmp_limit("1KB").unwrap(), 1000);
        assert_eq!(parse_cmp_limit("2MiB").unwrap(), 2 << 20);
        assert!(parse_cmp_limit("x").is_err());
        assert!(parse_cmp_limit("1mb").is_err());
    }

    #[test]
    fn parse_cmp_skip_spec_accepts_shared_and_split_skips() {
        assert_eq!(parse_cmp_skip_spec("4").unwrap(), (4, 4));
        assert_eq!(parse_cmp_skip_spec("3:9").unwrap(), (3, 9));
        assert_eq!(parse_cmp_skip_spec("1K:1KB").unwrap(), (1024, 1000));
        assert_eq!(parse_cmp_skip_spec("2MiB").unwrap(), (2 << 20, 2 << 20));
        assert!(parse_cmp_skip_spec("x").is_err());
        assert!(parse_cmp_skip_spec("1:x").is_err());
        assert!(parse_cmp_skip_spec("1mb").is_err());
    }

    #[test]
    fn cmp_eof_line_matches_gnu_newline_convention() {
        assert_eq!(cmp_eof_line(0, false), (1, "in line"));
        assert_eq!(cmp_eof_line(1, true), (1, "line"));
        assert_eq!(cmp_eof_line(1, false), (2, "in line"));
    }

    #[test]
    fn cmp_flag_compatibility_rejects_quiet_plus_verbose() {
        assert!(cmp_flags_are_compatible(false, false));
        assert!(cmp_flags_are_compatible(true, false));
        assert!(cmp_flags_are_compatible(false, true));
        assert!(!cmp_flags_are_compatible(true, true));
    }

    #[test]
    fn cmp_decimal_width_matches_decimal_digit_count() {
        assert_eq!(cmp_decimal_width(1), 1);
        assert_eq!(cmp_decimal_width(9), 1);
        assert_eq!(cmp_decimal_width(10), 2);
        assert_eq!(cmp_decimal_width(999), 3);
    }

    #[test]
    fn cmp_byte_display_parts_split_high_bit_from_render_core() {
        assert_eq!(cmp_byte_display_parts(0), (false, 0));
        assert_eq!(cmp_byte_display_parts(127), (false, 127));
        assert_eq!(cmp_byte_display_parts(128), (true, 0));
        assert_eq!(cmp_byte_display_parts(255), (true, 127));
    }

    #[test]
    fn cmp_render_byte_char_matches_gnu_style_examples() {
        assert_eq!(cmp_render_byte_char(b'Q'), "Q");
        assert_eq!(cmp_render_byte_char(b' '), " ");
        assert_eq!(cmp_render_byte_char(b'\t'), "^I");
        assert_eq!(cmp_render_byte_char(0), "^@");
        assert_eq!(cmp_render_byte_char(127), "^?");
        assert_eq!(cmp_render_byte_char(255), "M-^?");
    }
}
