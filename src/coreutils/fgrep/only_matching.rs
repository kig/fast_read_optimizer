use super::context::{
    fgrep_context_enabled, fgrep_record_group_output, fgrep_write_group_separator,
};
use super::line_matching::{
    fgrep_line_matches, fgrep_line_matches_any, fgrep_select_line, fgrep_word_match_start,
    normalize_case,
};
use super::runtime::{
    fgrep_is_max_count_reached_error, fgrep_max_count_reached_error, fgrep_record_selected_line,
    fgrep_suppresses_matching_line_output, handle_loaded_match_result, write_count_line,
    write_matching_line,
};
use super::*;
use memchr::{memchr_iter, memmem::Finder};
use std::io::{self, Write};

#[derive(Clone, Copy)]
struct FgrepOnlyMatchingLine {
    start: usize,
    end: usize,
    line_no: u64,
    byte_offset: u64,
    selected: bool,
}

fn trim_trailing_record_sep(line: &[u8], sep: u8) -> &[u8] {
    line.strip_suffix(&[sep]).unwrap_or(line)
}

fn fgrep_word_match_ranges(
    line: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> Vec<(usize, usize)> {
    let candidate = if options.line_regexp {
        trim_trailing_record_sep(line, options.record_sep())
    } else {
        line
    };
    if patterns.len() == 1 {
        if pattern.raw.is_empty() {
            return Vec::new();
        }
        let normalized_line = normalize_case(candidate, options.ignore_case);
        let normalized_line = normalized_line.as_ref();
        let mut ranges = Vec::new();
        let mut search_start = 0usize;
        while search_start < candidate.len() {
            let Some(start) = fgrep_word_match_start(
                candidate,
                normalized_line,
                pattern.raw.as_slice(),
                pattern.normalized.as_slice(),
                search_start,
            ) else {
                break;
            };
            ranges.push((start, pattern.raw.len()));
            search_start = start + pattern.raw.len();
        }
        return ranges;
    }

    let normalized_line = normalize_case(candidate, options.ignore_case);
    let normalized_line = normalized_line.as_ref();
    let mut ranges = Vec::new();
    let mut search_start = 0usize;
    while search_start < candidate.len() {
        let mut best_start = None::<usize>;
        let mut best_len = 0usize;
        for pattern in patterns {
            if pattern.raw.is_empty() {
                continue;
            }
            let Some(start) = fgrep_word_match_start(
                candidate,
                normalized_line,
                pattern.raw.as_slice(),
                pattern.normalized.as_slice(),
                search_start,
            ) else {
                continue;
            };
            let len = pattern.raw.len();
            if best_start.is_none()
                || start < best_start.unwrap()
                || (start == best_start.unwrap() && len > best_len)
            {
                best_start = Some(start);
                best_len = len;
            }
        }
        let Some(start) = best_start else {
            break;
        };
        ranges.push((start, best_len));
        search_start = start + best_len;
    }
    ranges
}

fn only_matching_line_selected(
    line: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> bool {
    let is_match = if patterns.len() == 1 {
        fgrep_line_matches(
            line,
            pattern.raw.as_slice(),
            pattern.normalized.as_slice(),
            options,
        )
    } else {
        fgrep_line_matches_any(line, patterns, options)
    };
    fgrep_select_line(is_match, options)
}

pub(super) fn fgrep_match_ranges(
    line: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> Vec<(usize, usize)> {
    let candidate = if options.line_regexp {
        trim_trailing_record_sep(line, options.record_sep())
    } else {
        line
    };
    if options.line_regexp {
        let normalized_line = normalize_case(candidate, options.ignore_case);
        if patterns.iter().any(|pattern| {
            !pattern.raw.is_empty() && normalized_line.as_ref() == pattern.normalized.as_slice()
        }) {
            return vec![(0, candidate.len())];
        }
        return Vec::new();
    }

    if options.word_regexp {
        return fgrep_word_match_ranges(line, pattern, patterns, options);
    }

    if patterns.len() == 1 {
        if pattern.raw.is_empty() {
            return Vec::new();
        }
        let normalized_line = normalize_case(candidate, options.ignore_case);
        let finder = Finder::new(pattern.normalized.as_slice());
        let mut ranges = Vec::new();
        let mut search_start = 0usize;
        let haystack = normalized_line.as_ref();
        while search_start < haystack.len() {
            let Some(rel_start) = finder.find(&haystack[search_start..]) else {
                break;
            };
            let start = search_start + rel_start;
            ranges.push((start, pattern.raw.len()));
            search_start = start + pattern.raw.len();
        }
        return ranges;
    }

    let normalized_line = normalize_case(candidate, options.ignore_case);
    let haystack = normalized_line.as_ref();
    let mut ranges = Vec::new();
    let mut search_start = 0usize;
    while search_start < haystack.len() {
        let mut best_start = None::<usize>;
        let mut best_len = 0usize;
        for pattern in patterns {
            if pattern.raw.is_empty() {
                continue;
            }
            let finder = Finder::new(pattern.normalized.as_slice());
            let Some(rel_start) = finder.find(&haystack[search_start..]) else {
                continue;
            };
            let start = search_start + rel_start;
            let len = pattern.raw.len();
            if best_start.is_none()
                || start < best_start.unwrap()
                || (start == best_start.unwrap() && len > best_len)
            {
                best_start = Some(start);
                best_len = len;
            }
        }
        let Some(start) = best_start else {
            break;
        };
        ranges.push((start, best_len));
        search_start = start + best_len;
    }
    ranges
}

fn emit_only_matching_ranges<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    byte_offset: u64,
    multi_file: bool,
    options: FgrepOptions,
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    ranges: &[(usize, usize)],
) -> io::Result<()> {
    for (start, len) in ranges {
        write_matching_line(
            out,
            label,
            &line[*start..start + *len],
            line_no,
            byte_offset + *start as u64,
            multi_file,
            options,
            pattern,
            patterns,
        )?;
    }
    Ok(())
}

fn process_only_matching_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    byte_offset: u64,
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
    matched_any: &mut bool,
    match_count: &mut u64,
) -> io::Result<bool> {
    let selected = only_matching_line_selected(line, pattern, patterns, options);
    if !selected {
        return Ok(false);
    }
    if !fgrep_suppresses_matching_line_output(options) {
        let ranges = fgrep_match_ranges(line, pattern, patterns, options);
        emit_only_matching_ranges(
            out,
            label,
            line,
            line_no,
            byte_offset,
            multi_file,
            options,
            pattern,
            patterns,
            &ranges,
        )?;
    }
    Ok(fgrep_record_selected_line(
        match_count,
        matched_any,
        options,
    ))
}

fn collect_only_matching_lines(
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> Vec<FgrepOnlyMatchingLine> {
    let mut lines = Vec::new();
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    for rel_end in memchr_iter(options.record_sep(), data) {
        let line_end = rel_end + 1;
        lines.push(FgrepOnlyMatchingLine {
            start: line_start,
            end: line_end,
            line_no,
            byte_offset: line_start as u64,
            selected: only_matching_line_selected(
                &data[line_start..line_end],
                pattern,
                patterns,
                options,
            ),
        });
        line_start = line_end;
        line_no += 1;
    }
    if line_start < data.len() {
        lines.push(FgrepOnlyMatchingLine {
            start: line_start,
            end: data.len(),
            line_no,
            byte_offset: line_start as u64,
            selected: only_matching_line_selected(&data[line_start..], pattern, patterns, options),
        });
    }
    lines
}

fn write_only_matching_context_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let lines = collect_only_matching_lines(data, pattern, patterns, options);
    if lines.is_empty() {
        return Ok(false);
    }

    let selected_limit = options
        .max_count
        .map(|value| value.min(usize::MAX as u64) as usize)
        .unwrap_or(usize::MAX);
    let mut selected_indices = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.selected {
            selected_indices.push(index);
            if selected_indices.len() >= selected_limit {
                break;
            }
        }
    }
    if selected_indices.is_empty() {
        return Ok(false);
    }

    let before = options.before_context.min(usize::MAX as u64) as usize;
    let after = options.after_context.min(usize::MAX as u64) as usize;
    let mut groups: Vec<(usize, usize)> = Vec::new();
    for index in selected_indices {
        let start = index.saturating_sub(before);
        let end = index
            .saturating_add(after)
            .min(lines.len().saturating_sub(1));
        if let Some(last) = groups.last_mut() {
            if start <= last.1.saturating_add(1) {
                last.1 = last.1.max(end);
                continue;
            }
        }
        groups.push((start, end));
    }

    let emit_groups: Vec<(usize, usize)> = groups
        .into_iter()
        .filter(|(start, end)| {
            lines[*start..=*end].iter().any(|line| {
                !fgrep_match_ranges(&data[line.start..line.end], pattern, patterns, options)
                    .is_empty()
            })
        })
        .collect();
    if emit_groups.is_empty() {
        return Ok(true);
    }

    let prepend_file_separator = fgrep_record_group_output(true);
    if prepend_file_separator {
        fgrep_write_group_separator(out, options)?;
    }

    for (group_index, (start, end)) in emit_groups.iter().copied().enumerate() {
        if group_index > 0 {
            fgrep_write_group_separator(out, options)?;
        }
        for line in &lines[start..=end] {
            let ranges =
                fgrep_match_ranges(&data[line.start..line.end], pattern, patterns, options);
            if ranges.is_empty() {
                continue;
            }
            emit_only_matching_ranges(
                out,
                label,
                &data[line.start..line.end],
                line.line_no,
                line.byte_offset,
                multi_file,
                options,
                pattern,
                patterns,
                &ranges,
            )?;
        }
    }

    Ok(true)
}

fn write_only_matching_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
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
        if process_only_matching_line(
            out,
            label,
            &data[line_start..line_end],
            line_no,
            line_start as u64,
            pattern,
            patterns,
            multi_file,
            options,
            &mut matched_any,
            &mut match_count,
        )? {
            break;
        }
        line_start = line_end;
        line_no += 1;
    }

    if options.count_only {
        write_count_line(out, label, match_count, options, multi_file)?;
    }
    Ok(matched_any)
}

pub(super) fn write_only_matching_loaded_result<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    if fgrep_context_enabled(options) && !fgrep_suppresses_matching_line_output(options) {
        return write_only_matching_context_lines(
            out, label, data, pattern, patterns, multi_file, options,
        );
    }
    write_only_matching_lines(out, label, data, pattern, patterns, multi_file, options)
}

pub(super) fn write_only_matching_stream_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<(bool, u64)> {
    if !options.line_buffered
        || (fgrep_context_enabled(options) && !fgrep_suppresses_matching_line_output(options))
    {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let bytes = data.len() as u64;
        let matched = handle_loaded_match_result(
            out,
            label,
            data.as_slice(),
            pattern,
            patterns,
            super::fgrep_regular_file_path(options, patterns.len()),
            None,
            multi_file,
            options,
        )?;
        return Ok((matched, bytes));
    }

    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut pending_line = Vec::new();
    let mut line_no = 1_u64;
    let mut line_start_offset = 0_u64;
    let mut bytes = 0_u64;
    let mut stopped_early = false;

    let visit = visit_ordered_input(input, io_mode, |block| {
        bytes += block.len() as u64;
        let mut line_start = 0usize;
        for rel_end in memchr_iter(options.record_sep(), block) {
            let line_end = rel_end + 1;
            pending_line.extend_from_slice(&block[line_start..line_end]);
            if process_only_matching_line(
                out,
                label,
                &pending_line,
                line_no,
                line_start_offset,
                pattern,
                patterns,
                multi_file,
                options,
                &mut matched_any,
                &mut match_count,
            )? {
                return Err(fgrep_max_count_reached_error());
            }
            pending_line.clear();
            line_start_offset += (line_end - line_start) as u64;
            line_no += 1;
            line_start = line_end;
        }
        if line_start < block.len() {
            pending_line.extend_from_slice(&block[line_start..]);
        }
        Ok::<_, io::Error>(())
    });

    match visit {
        Ok(()) => {}
        Err(err) if fgrep_is_max_count_reached_error(&err) => {
            stopped_early = true;
        }
        Err(err) => return Err(err),
    }

    if !stopped_early && !pending_line.is_empty() {
        process_only_matching_line(
            out,
            label,
            &pending_line,
            line_no,
            line_start_offset,
            pattern,
            patterns,
            multi_file,
            options,
            &mut matched_any,
            &mut match_count,
        )?;
    }

    if options.count_only {
        write_count_line(out, label, match_count, options, multi_file)?;
    }
    Ok((matched_any, bytes))
}
