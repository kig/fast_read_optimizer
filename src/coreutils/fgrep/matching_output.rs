use super::line_matching::{fgrep_line_matches, fgrep_line_matches_any, fgrep_select_line};
use super::runtime::{
    fgrep_record_selected_line, fgrep_suppresses_matching_line_output, write_count_line,
    write_matching_line,
};
use super::*;
use memchr::{memchr_iter, memmem::Finder};
use std::io::{self, Write};

pub(crate) fn count_literal_matching_lines(
    data: &[u8],
    pattern: &[u8],
    options: FgrepOptions,
) -> (bool, u64) {
    let finder = (!pattern.is_empty()).then(|| Finder::new(pattern));
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;

    for rel_end in memchr_iter(options.record_sep(), data) {
        let line_end = rel_end + 1;
        let is_match = pattern.is_empty()
            || finder
                .as_ref()
                .is_some_and(|finder| finder.find(&data[line_start..line_end]).is_some());
        if fgrep_select_line(is_match, options) {
            if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                return (matched_any, match_count);
            }
        }
        line_start = line_end;
    }

    if line_start < data.len() {
        let is_match = pattern.is_empty()
            || finder
                .as_ref()
                .is_some_and(|finder| finder.find(&data[line_start..]).is_some());
        if fgrep_select_line(is_match, options) {
            fgrep_record_selected_line(&mut match_count, &mut matched_any, options);
        }
    }

    (matched_any, match_count)
}

pub(crate) fn write_count_literal_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let (matched_any, match_count) = count_literal_matching_lines(data, pattern, options);
    write_count_line(out, Some(file), match_count, options, multi_file)?;
    Ok(matched_any)
}

pub(crate) fn write_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    matches: &[u64],
    multi_file: bool,
    options: FgrepOptions,
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
) -> io::Result<bool> {
    let bytes = data;
    let mut next_match = 0usize;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let sep = options.record_sep();
    while line_start < bytes.len() {
        let rel_end = bytes[line_start..]
            .iter()
            .position(|&byte| byte == sep)
            .map(|pos| pos + 1)
            .unwrap_or(bytes.len() - line_start);
        let line_end = line_start + rel_end;
        let mut matched = false;
        while next_match < matches.len() && matches[next_match] < line_end as u64 {
            if matches[next_match] >= line_start as u64 {
                matched = true;
            }
            next_match += 1;
        }
        if fgrep_select_line(matched, options) {
            if !fgrep_suppresses_matching_line_output(options) {
                write_matching_line(
                    out,
                    Some(file),
                    &bytes[line_start..line_end],
                    line_no,
                    line_start as u64,
                    multi_file,
                    options,
                    pattern,
                    patterns,
                )?;
            }
            if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                break;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, options, multi_file)?;
    }
    Ok(matched_any)
}

pub(crate) fn write_line_regexp_matches<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let highlight_pattern = FgrepPattern {
        raw: pattern.to_vec(),
        normalized: normalized_pattern.to_vec(),
    };
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    let sep = options.record_sep();
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == sep)
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(
            fgrep_line_matches(line, pattern, normalized_pattern, options),
            options,
        ) {
            if !fgrep_suppresses_matching_line_output(options) {
                write_matching_line(
                    out,
                    Some(file),
                    line,
                    line_no,
                    line_start as u64,
                    multi_file,
                    options,
                    &highlight_pattern,
                    std::slice::from_ref(&highlight_pattern),
                )?;
            }
            if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                break;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, options, multi_file)?;
    }
    Ok(matched_any)
}

pub(crate) fn write_filtered_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let highlight_pattern = FgrepPattern {
        raw: pattern.to_vec(),
        normalized: normalized_pattern.to_vec(),
    };
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    let sep = options.record_sep();
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == sep)
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(
            fgrep_line_matches(line, pattern, normalized_pattern, options),
            options,
        ) {
            if !fgrep_suppresses_matching_line_output(options) {
                write_matching_line(
                    out,
                    Some(file),
                    line,
                    line_no,
                    line_start as u64,
                    multi_file,
                    options,
                    &highlight_pattern,
                    std::slice::from_ref(&highlight_pattern),
                )?;
            }
            if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                break;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, options, multi_file)?;
    }
    Ok(matched_any)
}

pub(crate) fn write_filtered_lines_multi<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    let sep = options.record_sep();
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == sep)
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(fgrep_line_matches_any(line, patterns, options), options) {
            if !fgrep_suppresses_matching_line_output(options) {
                write_matching_line(
                    out,
                    Some(file),
                    line,
                    line_no,
                    line_start as u64,
                    multi_file,
                    options,
                    &patterns[0],
                    patterns,
                )?;
            }
            if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                break;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, options, multi_file)?;
    }
    Ok(matched_any)
}
