use super::*;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const CMP_SMALL_REGULAR_FAST_PATH_LIMIT: u64 = 128 * 1024;

#[derive(Clone, Copy)]
struct CmpOptions {
    io_mode: IOMode,
    quiet: bool,
    verbose: bool,
    print_bytes: bool,
    report_throughput: bool,
    limit: Option<u64>,
    first_skip: u64,
    second_skip: u64,
}

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

fn cmp_read_small_range(path: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let file = fs::File::open(path)?;
    let mut bytes = vec![0_u8; len];
    if len > 0 {
        file.read_exact_at(&mut bytes, offset)?;
    }
    Ok(bytes)
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

fn cmp_directory_operand(files: &[String]) -> io::Result<Option<String>> {
    for path in files {
        if fs::metadata(path)?.file_type().is_dir() {
            return Ok(Some(path.clone()));
        }
    }
    Ok(None)
}

fn cmp_read_sorted_dir_entries(dir: &Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, io::Error>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn cmp_recursive_type_name(file_type: fs::FileType) -> &'static str {
    if file_type.is_dir() {
        "directory"
    } else if file_type.is_file() {
        "regular file"
    } else if file_type.is_symlink() {
        "symbolic link"
    } else {
        "special file"
    }
}

fn cmp_recursive_print_missing(dir: &Path, name: &std::ffi::OsStr) {
    fro::cio_println!("Only in {}: {}", dir.display(), Path::new(name).display());
}

fn cmp_recursive_print_type_mismatch(
    left: &Path,
    left_type: fs::FileType,
    right: &Path,
    right_type: fs::FileType,
) {
    fro::cio_println!(
        "cmp: {} is a {} while {} is a {}",
        left.display(),
        cmp_recursive_type_name(left_type),
        right.display(),
        cmp_recursive_type_name(right_type)
    );
}

fn cmp_recursive_compare_symlinks(
    left: &Path,
    right: &Path,
    options: CmpOptions,
) -> io::Result<i32> {
    let left_target = fs::read_link(left)?;
    let right_target = fs::read_link(right)?;
    if left_target == right_target {
        return Ok(0);
    }
    if !options.quiet {
        fro::cio_println!(
            "cmp: symbolic links {} and {} differ",
            left.display(),
            right.display()
        );
    }
    Ok(1)
}

#[allow(clippy::too_many_arguments)]
fn cmp_finish_with_loaded_bytes(
    files: &[String],
    first_bytes: &[u8],
    second_bytes: &[u8],
    first_remaining: u64,
    second_remaining: u64,
    shared_remaining: u64,
    quiet: bool,
    verbose: bool,
    print_bytes: bool,
    limit: Option<u64>,
    started_at: Option<std::time::Instant>,
) -> io::Result<i32> {
    if verbose {
        let compare_len = first_bytes.len() as u64;
        let byte_width = cmp_decimal_width(compare_len);
        let mut had_mismatch = false;
        for (idx, (&left, &right)) in first_bytes.iter().zip(second_bytes.iter()).enumerate() {
            if left != right {
                had_mismatch = true;
                if print_bytes {
                    fro::cio_println!(
                        "{:>width$} {:>3o} {:<4} {:>3o} {}",
                        idx + 1,
                        left,
                        cmp_render_byte_char(left),
                        right,
                        cmp_render_byte_char(right),
                        width = byte_width
                    );
                } else {
                    fro::cio_println!(
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
            fro::cio_eprintln!("cmp: EOF on {} after byte {}", eof_file, shared_remaining);
            return Ok(1);
        }
        if let Some(started_at) = started_at {
            report_gbps("cmp", compare_len, started_at);
        }
        return Ok(if had_mismatch { 1 } else { 0 });
    }

    if let Some(index) = first_bytes
        .iter()
        .zip(second_bytes.iter())
        .position(|(&left, &right)| left != right)
    {
        if !quiet {
            let line = 1 + memchr_iter(b'\n', &first_bytes[..index]).count() as u64;
            if print_bytes {
                let left = first_bytes[index];
                let right = second_bytes[index];
                fro::cio_println!(
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
                fro::cio_println!(
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
            let eof_bytes = if first_remaining < second_remaining {
                first_bytes
            } else {
                second_bytes
            };
            let newlines_before_eof = memchr_iter(b'\n', eof_bytes).count() as u64;
            let ends_with_newline = eof_bytes.last() == Some(&b'\n');
            let (line, phrase) = cmp_eof_line(newlines_before_eof, ends_with_newline);
            fro::cio_eprintln!(
                "cmp: EOF on {} after byte {}, {} {}",
                eof_file,
                shared_remaining,
                phrase,
                line
            );
        }
        return Ok(1);
    }

    if let Some(started_at) = started_at {
        report_gbps("cmp", first_bytes.len() as u64, started_at);
    }
    Ok(0)
}

#[allow(clippy::too_many_arguments)]
fn cmp_try_small_regular_fast_path(
    files: &[String],
    io_mode: IOMode,
    compare_len: u64,
    first_skip: u64,
    second_skip: u64,
    first_remaining: u64,
    second_remaining: u64,
    shared_remaining: u64,
    quiet: bool,
    verbose: bool,
    print_bytes: bool,
    limit: Option<u64>,
    started_at: Option<std::time::Instant>,
    first_is_regular: bool,
    second_is_regular: bool,
) -> io::Result<Option<i32>> {
    if io_mode == IOMode::Direct
        || compare_len > CMP_SMALL_REGULAR_FAST_PATH_LIMIT
        || !first_is_regular
        || !second_is_regular
    {
        return Ok(None);
    }

    let compare_len_usize = usize::try_from(compare_len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?;
    let first_bytes = cmp_read_small_range(&files[0], first_skip, compare_len_usize)?;
    let second_bytes = cmp_read_small_range(&files[1], second_skip, compare_len_usize)?;
    cmp_finish_with_loaded_bytes(
        files,
        &first_bytes,
        &second_bytes,
        first_remaining,
        second_remaining,
        shared_remaining,
        quiet,
        verbose,
        print_bytes,
        limit,
        started_at,
    )
    .map(Some)
}

fn run_cmp_pair(files: &[String], options: CmpOptions) -> io::Result<(i32, u64)> {
    let first_meta = fs::metadata(&files[0])?;
    let second_meta = fs::metadata(&files[1])?;
    let first_len = first_meta.len();
    let second_len = second_meta.len();
    let first_remaining = cmp_remaining_len_after_skip(first_len, options.first_skip);
    let second_remaining = cmp_remaining_len_after_skip(second_len, options.second_skip);
    let shared_remaining = first_remaining.min(second_remaining);
    let compare_len = cmp_effective_compare_len_with_skips(
        first_len,
        second_len,
        options.first_skip,
        options.second_skip,
        options.limit,
    );
    let started_at = options.report_throughput.then(std::time::Instant::now);
    if compare_len == 0 {
        if options.limit == Some(0) || first_remaining == second_remaining {
            if let Some(started_at) = started_at {
                report_gbps("cmp", 0, started_at);
            }
            return Ok((0, 0));
        }
        if !options.quiet {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            fro::cio_eprintln!("cmp: EOF on {} which is empty", eof_file);
        }
        return Ok((1, 0));
    }

    if let Some(code) = cmp_try_small_regular_fast_path(
        files,
        options.io_mode,
        compare_len,
        options.first_skip,
        options.second_skip,
        first_remaining,
        second_remaining,
        shared_remaining,
        options.quiet,
        options.verbose,
        options.print_bytes,
        options.limit,
        started_at,
        first_meta.file_type().is_file(),
        second_meta.file_type().is_file(),
    )? {
        return Ok((code, compare_len));
    }

    if options.verbose {
        let first = load_file_bytes(&files[0], options.io_mode, "read")?;
        let second = load_file_bytes(&files[1], options.io_mode, "read")?;
        let first_start = usize::try_from(options.first_skip)
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize")
            })?
            .min(first.data.len());
        let second_start = usize::try_from(options.second_skip)
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
                if options.print_bytes {
                    fro::cio_println!(
                        "{:>width$} {:>3o} {:<4} {:>3o} {}",
                        idx + 1,
                        left,
                        cmp_render_byte_char(left),
                        right,
                        cmp_render_byte_char(right),
                        width = byte_width
                    );
                } else {
                    fro::cio_println!(
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
            && options.limit.map_or(true, |limit| limit > shared_remaining)
        {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            fro::cio_eprintln!("cmp: EOF on {} after byte {}", eof_file, shared_remaining);
            return Ok((1, compare_len));
        }
        if let Some(started_at) = started_at {
            report_gbps("cmp", compare_len, started_at);
        }
        return Ok((if had_mismatch { 1 } else { 0 }, compare_len));
    }

    let config = load_config(None);
    let diff_page_cache = config.get_params_for_path("diff", false, &files[0]);
    let diff_direct = config.get_params_for_path("diff", true, &files[0]);
    let mismatch = diff_files_window(
        &files[0],
        &files[1],
        options.first_skip,
        options.second_skip,
        diff_page_cache.num_threads,
        diff_page_cache.block_size,
        diff_page_cache.qd,
        diff_direct.num_threads,
        diff_direct.block_size,
        diff_direct.qd,
        internal_io_mode(options.io_mode),
        false,
        false,
        Some(compare_len),
    )?;
    if mismatch != 0 {
        if !options.quiet {
            let index = mismatch as usize - 1;
            let line = 1 + count_newlines_in_range(
                &files[0],
                options.io_mode,
                "read",
                options.first_skip,
                options.first_skip + mismatch - 1,
            )?;
            if options.print_bytes {
                let left = cmp_read_byte_at(&files[0], options.first_skip + mismatch - 1)?;
                let right = cmp_read_byte_at(&files[1], options.second_skip + mismatch - 1)?;
                fro::cio_println!(
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
                fro::cio_println!(
                    "{} {} differ: byte {}, line {}",
                    files[0],
                    files[1],
                    index + 1,
                    line
                );
            }
        }
        return Ok((1, compare_len));
    }

    if first_remaining != second_remaining
        && options.limit.map_or(true, |limit| limit > shared_remaining)
    {
        if !options.quiet {
            let eof_file = if first_remaining < second_remaining {
                &files[0]
            } else {
                &files[1]
            };
            let eof_skip = if first_remaining < second_remaining {
                options.first_skip
            } else {
                options.second_skip
            };
            let eof_len = if first_remaining < second_remaining {
                first_remaining
            } else {
                second_remaining
            };
            let data = load_file_bytes(eof_file, options.io_mode, "read")?;
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
            fro::cio_eprintln!(
                "cmp: EOF on {} after byte {}, {} {}",
                eof_file,
                shared_remaining,
                phrase,
                line
            );
        }
        return Ok((1, compare_len));
    }

    if let Some(started_at) = started_at {
        report_gbps("cmp", compare_len, started_at);
    }
    Ok((0, compare_len))
}

fn run_cmp_recursive(files: &[String], options: CmpOptions) -> io::Result<i32> {
    let left_root = PathBuf::from(&files[0]);
    let right_root = PathBuf::from(&files[1]);
    let left_root_type = fs::symlink_metadata(&left_root)?.file_type();
    let right_root_type = fs::symlink_metadata(&right_root)?.file_type();
    if !left_root_type.is_dir() || !right_root_type.is_dir() {
        if left_root_type.is_dir() || right_root_type.is_dir() {
            fro::cio_eprintln!(
                "cmp: recursive comparison requires both operands to be directories"
            );
            return Ok(2);
        }
        return Ok(run_cmp_pair(files, options)?.0);
    }

    let started_at = options.report_throughput.then(std::time::Instant::now);
    let mut compared_bytes = 0_u64;
    let mut mismatch = false;
    let mut stack = vec![(left_root, right_root)];
    while let Some((left_dir, right_dir)) = stack.pop() {
        let left_entries = cmp_read_sorted_dir_entries(&left_dir)?;
        let right_entries = cmp_read_sorted_dir_entries(&right_dir)?;
        let mut left_index = 0usize;
        let mut right_index = 0usize;
        let mut child_dirs = Vec::new();
        while left_index < left_entries.len() || right_index < right_entries.len() {
            match (left_entries.get(left_index), right_entries.get(right_index)) {
                (Some(left), Some(right)) => {
                    let left_name = left.file_name();
                    let right_name = right.file_name();
                    match left_name.cmp(&right_name) {
                        std::cmp::Ordering::Less => {
                            mismatch = true;
                            if !options.quiet {
                                cmp_recursive_print_missing(&left_dir, &left_name);
                            }
                            left_index += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            mismatch = true;
                            if !options.quiet {
                                cmp_recursive_print_missing(&right_dir, &right_name);
                            }
                            right_index += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            let left_path = left.path();
                            let right_path = right.path();
                            let left_type = fs::symlink_metadata(&left_path)?.file_type();
                            let right_type = fs::symlink_metadata(&right_path)?.file_type();
                            if left_type.is_dir() && right_type.is_dir() {
                                child_dirs.push((left_path, right_path));
                            } else if left_type.is_file() && right_type.is_file() {
                                let child_files = [
                                    left_path.to_string_lossy().into_owned(),
                                    right_path.to_string_lossy().into_owned(),
                                ];
                                let (code, bytes) = run_cmp_pair(
                                    &child_files,
                                    CmpOptions {
                                        report_throughput: false,
                                        ..options
                                    },
                                )?;
                                compared_bytes = compared_bytes.saturating_add(bytes);
                                mismatch |= code != 0;
                            } else if left_type.is_symlink() && right_type.is_symlink() {
                                mismatch |= cmp_recursive_compare_symlinks(
                                    &left_path,
                                    &right_path,
                                    options,
                                )? != 0;
                            } else {
                                mismatch = true;
                                if !options.quiet {
                                    cmp_recursive_print_type_mismatch(
                                        &left_path,
                                        left_type,
                                        &right_path,
                                        right_type,
                                    );
                                }
                            }
                            left_index += 1;
                            right_index += 1;
                        }
                    }
                }
                (Some(left), None) => {
                    mismatch = true;
                    if !options.quiet {
                        cmp_recursive_print_missing(&left_dir, &left.file_name());
                    }
                    left_index += 1;
                }
                (None, Some(right)) => {
                    mismatch = true;
                    if !options.quiet {
                        cmp_recursive_print_missing(&right_dir, &right.file_name());
                    }
                    right_index += 1;
                }
                (None, None) => break,
            }
            if options.quiet && mismatch {
                return Ok(1);
            }
        }
        child_dirs.reverse();
        stack.extend(child_dirs);
    }
    if !mismatch {
        if let Some(started_at) = started_at {
            report_gbps("cmp", compared_bytes, started_at);
        }
        return Ok(0);
    }
    Ok(1)
}

pub(super) fn run_cmp(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut io_mode = IOMode::Auto;
    let mut quiet = false;
    let mut verbose = false;
    let mut print_bytes = false;
    let mut report_throughput = false;
    let mut limit = None::<u64>;
    let mut first_skip = 0u64;
    let mut second_skip = 0u64;
    let mut recursive = false;
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
            "-r" | "-R" | "--recursive" => recursive = true,
            "-l" | "--verbose" => verbose = true,
            "-b" | "--print-bytes" => print_bytes = true,
            "--report-gbps" => report_throughput = true,
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
        "[-s|--quiet|--silent] [-r|-R|--recursive] [-l|--verbose] [-b|--print-bytes] [-i SKIP|--ignore-initial=SKIP] [-n LIMIT|--bytes=LIMIT] [--auto|--no-direct|--direct] [--] <file1> <file2>",
    )?;
    if files.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cmp requires exactly two file operands",
        ));
    }
    if !cmp_flags_are_compatible(quiet, verbose) {
        fro::cio_eprintln!("cmp: options -l and -s are incompatible");
        fro::cio_eprintln!("cmp: Try 'cmp --help' for more information.");
        return Ok(2);
    }
    if !recursive {
        if let Some(dir) = cmp_directory_operand(&files)? {
            fro::cio_eprintln!("cmp: {}: Is a directory", dir);
            return Ok(2);
        }
    }
    let options = CmpOptions {
        io_mode,
        quiet,
        verbose,
        print_bytes,
        report_throughput,
        limit,
        first_skip,
        second_skip,
    };
    if recursive {
        run_cmp_recursive(&files, options)
    } else {
        Ok(run_cmp_pair(&files, options)?.0)
    }
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
#[path = "cmp/tests.rs"]
mod tests;
