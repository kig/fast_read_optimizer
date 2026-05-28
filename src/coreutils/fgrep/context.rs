use super::color::write_colorized_group_separator;
use super::line_matching::{fgrep_line_matches, fgrep_line_matches_any, fgrep_select_line};
use super::runtime::{write_context_line, write_matching_line};
use super::{FgrepGroupSeparatorPolicy, *};
use memchr::memchr_iter;
use std::cell::Cell;
use std::io::{self, Write};

thread_local! {
    static CONTEXT_OUTPUT_SEEN: Cell<bool> = Cell::new(false);
}

#[derive(Clone, Copy)]
struct FgrepContextLine {
    start: usize,
    end: usize,
    line_no: u64,
    byte_offset: u64,
    selected: bool,
}

pub(super) fn fgrep_context_enabled(options: FgrepOptions) -> bool {
    options.before_context > 0 || options.after_context > 0
}

pub(super) fn fgrep_reset_context_output_state() {
    CONTEXT_OUTPUT_SEEN.with(|state| state.set(false));
}

pub(super) fn fgrep_record_group_output(matched_any: bool) -> bool {
    if !matched_any {
        return false;
    }
    CONTEXT_OUTPUT_SEEN.with(|state| {
        let seen_before = state.get();
        state.set(true);
        seen_before
    })
}

pub(super) fn fgrep_write_group_separator<W: Write>(
    out: &mut W,
    options: FgrepOptions,
) -> io::Result<()> {
    match options.group_separator {
        FgrepGroupSeparatorPolicy::Default => {
            write_colorized_group_separator(out, "--", options)?;
            out.write_all(b"\n")?;
        }
        FgrepGroupSeparatorPolicy::Disabled => return Ok(()),
        FgrepGroupSeparatorPolicy::Custom(separator) => {
            write_colorized_group_separator(out, separator, options)?;
            out.write_all(b"\n")?;
        }
    }
    if options.line_buffered {
        out.flush()?;
    }
    Ok(())
}

fn fgrep_context_line_selected(
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

fn fgrep_collect_context_lines(
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> Vec<FgrepContextLine> {
    let mut lines = Vec::new();
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    for rel_end in memchr_iter(options.record_sep(), data) {
        let line_end = rel_end + 1;
        lines.push(FgrepContextLine {
            start: line_start,
            end: line_end,
            line_no,
            byte_offset: line_start as u64,
            selected: fgrep_context_line_selected(
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
        lines.push(FgrepContextLine {
            start: line_start,
            end: data.len(),
            line_no,
            byte_offset: line_start as u64,
            selected: fgrep_context_line_selected(&data[line_start..], pattern, patterns, options),
        });
    }
    lines
}

pub(super) fn write_context_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let lines = fgrep_collect_context_lines(data, pattern, patterns, options);
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

    let prepend_file_separator = fgrep_record_group_output(true);
    if prepend_file_separator {
        fgrep_write_group_separator(out, options)?;
    }

    for (group_index, (start, end)) in groups.iter().copied().enumerate() {
        if group_index > 0 {
            fgrep_write_group_separator(out, options)?;
        }
        for line in &lines[start..=end] {
            if line.selected {
                write_matching_line(
                    out,
                    label,
                    &data[line.start..line.end],
                    line.line_no,
                    line.byte_offset,
                    multi_file,
                    options,
                    pattern,
                    patterns,
                )?;
            } else {
                write_context_line(
                    out,
                    label,
                    &data[line.start..line.end],
                    line.line_no,
                    line.byte_offset,
                    multi_file,
                    options,
                )?;
            }
        }
    }

    Ok(true)
}
