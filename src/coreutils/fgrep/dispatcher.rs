use super::context::{fgrep_context_enabled, write_context_lines};
use super::matching_output::{
    write_count_literal_matching_lines, write_filtered_lines, write_filtered_lines_multi,
    write_line_regexp_matches, write_matching_lines,
};
use super::only_matching::{write_only_matching_loaded_result, write_only_matching_stream_lines};
use super::runtime::{
    fgrep_display_label, fgrep_is_max_count_reached_error, fgrep_max_count_reached_error,
    fgrep_record_selected_line, fgrep_report_binary_match, fgrep_suppresses_matching_line_output,
    write_count_line, write_matching_line,
};
use super::line_matching::{fgrep_line_matches, fgrep_line_matches_any, fgrep_select_line, normalize_case};
use super::*;
use memchr::{memchr_iter, memmem::Finder};
use std::io::{self, Write};

pub(crate) fn write_loaded_match_result<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    regular_file_path: FgrepRegularFilePath,
    precomputed_matches: Option<&[u64]>,
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let display_label = fgrep_display_label(label);
    if options.only_matching {
        return write_only_matching_loaded_result(
            out, label, data, pattern, patterns, multi_file, options,
        );
    }
    if fgrep_context_enabled(options) && !fgrep_suppresses_matching_line_output(options) {
        return write_context_lines(
            out, label, data, pattern, patterns, multi_file, options,
        );
    }
    match regular_file_path {
        FgrepRegularFilePath::LiteralSearchOffsets => {
            if options.count_only {
                write_count_literal_matching_lines(
                    out,
                    display_label,
                    data,
                    pattern.raw.as_slice(),
                    multi_file,
                    options,
                )
            } else if let Some(matches) = precomputed_matches {
                write_matching_lines(
                    out,
                    display_label,
                    data,
                    matches,
                    multi_file,
                    options,
                    pattern,
                    patterns,
                )
            } else {
                write_filtered_lines(
                    out,
                    display_label,
                    data,
                    pattern.raw.as_slice(),
                    pattern.normalized.as_slice(),
                    multi_file,
                    options,
                )
            }
        }
        FgrepRegularFilePath::LineFilterSinglePattern => {
            if options.line_regexp {
                write_line_regexp_matches(
                    out,
                    display_label,
                    data,
                    pattern.raw.as_slice(),
                    pattern.normalized.as_slice(),
                    multi_file,
                    options,
                )
            } else {
                write_filtered_lines(
                    out,
                    display_label,
                    data,
                    pattern.raw.as_slice(),
                    pattern.normalized.as_slice(),
                    multi_file,
                    options,
                )
            }
        }
        FgrepRegularFilePath::LineFilterMultiPattern => {
            write_filtered_lines_multi(out, display_label, data, patterns, multi_file, options)
        }
    }
}

pub(crate) fn handle_loaded_match_result<W: Write>(
    out: &mut W,
    label: Option<&str>,
    data: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    regular_file_path: FgrepRegularFilePath,
    precomputed_matches: Option<&[u64]>,
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    if fgrep_data_is_binary(data, options) {
        if fgrep_binary_without_match(options) {
            if options.count_only {
                write_count_line(out, label, 0, options, multi_file)?;
            }
            return Ok(false);
        }
        if fgrep_binary_reports_match(options) {
            let mut sink = io::sink();
            let matched = write_loaded_match_result(
                &mut sink,
                label,
                data,
                pattern,
                patterns,
                regular_file_path,
                precomputed_matches,
                multi_file,
                fgrep_probe_options(options),
            )?;
            if matched {
                fgrep_report_binary_match(label, options);
            }
            return Ok(matched);
        }
    }
    write_loaded_match_result(
        out,
        label,
        data,
        pattern,
        patterns,
        regular_file_path,
        precomputed_matches,
        multi_file,
        options,
    )
}

pub(crate) fn finish_pending_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    pending_line: &mut Vec<u8>,
    pattern: &[u8],
    normalized_pattern: &[u8],
    line_no: u64,
    byte_offset: u64,
    multi_file: bool,
    options: FgrepOptions,
    matched_any: &mut bool,
    match_count: &mut u64,
) -> io::Result<bool> {
    let pattern = FgrepPattern {
        raw: pattern.to_vec(),
        normalized: normalized_pattern.to_vec(),
    };
    let mut stop = false;
    if fgrep_select_line(
        fgrep_line_matches(
            pending_line,
            pattern.raw.as_slice(),
            pattern.normalized.as_slice(),
            options,
        ),
        options,
    ) {
        if !fgrep_suppresses_matching_line_output(options) {
            write_matching_line(
                out,
                label,
                pending_line,
                line_no,
                byte_offset,
                multi_file,
                options,
                &pattern,
                std::slice::from_ref(&pattern),
            )?;
        }
        stop = fgrep_record_selected_line(match_count, matched_any, options);
    }
    pending_line.clear();
    Ok(stop)
}

pub(crate) fn write_matching_stream_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<(bool, u64)> {
    if options.only_matching {
        let only_matching_pattern = FgrepPattern {
            raw: pattern.to_vec(),
            normalized: normalized_pattern.to_vec(),
        };
        return write_only_matching_stream_lines(
            out,
            label,
            input,
            io_mode,
            &only_matching_pattern,
            std::slice::from_ref(&only_matching_pattern),
            multi_file,
            options,
        );
    }
    if !options.line_buffered {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let bytes = data.len() as u64;
        let pattern = FgrepPattern {
            raw: pattern.to_vec(),
            normalized: normalized_pattern.to_vec(),
        };
        let matched = handle_loaded_match_result(
            out,
            label,
            data.as_slice(),
            &pattern,
            std::slice::from_ref(&pattern),
            super::fgrep_regular_file_path(options, 1),
            None,
            multi_file,
            options,
        )?;
        return Ok((matched, bytes));
    }
    if fgrep_context_enabled(options) && !fgrep_suppresses_matching_line_output(options) {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let bytes = data.len() as u64;
        let context_pattern = FgrepPattern {
            raw: pattern.to_vec(),
            normalized: normalized_pattern.to_vec(),
        };
        let matched = write_context_lines(
            out,
            label,
            data.as_slice(),
            &context_pattern,
            std::slice::from_ref(&context_pattern),
            multi_file,
            options,
        )?;
        return Ok((matched, bytes));
    }
    if options.line_regexp {
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
                if finish_pending_line(
                    out,
                    label,
                    &mut pending_line,
                    pattern,
                    normalized_pattern,
                    line_no,
                    line_start_offset,
                    multi_file,
                    options,
                    &mut matched_any,
                    &mut match_count,
                )? {
                    return Err(fgrep_max_count_reached_error());
                }
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
            finish_pending_line(
                out,
                label,
                &mut pending_line,
                pattern,
                normalized_pattern,
                line_no,
                line_start_offset,
                multi_file,
                options,
                &mut matched_any,
                &mut match_count,
            )?;
        }
        if options.count_only {
            write_count_line(out, label, match_count, options, multi_file)?;
        }
        return Ok((matched_any, bytes));
    }

    let finder = Finder::new(normalized_pattern);
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut pending_line = Vec::new();
    let mut pending_line_has_match = pattern.is_empty();
    let mut boundary_tail = Vec::new();
    let mut line_no = 1_u64;
    let mut line_start_offset = 0_u64;
    let mut bytes = 0_u64;
    let mut stopped_early = false;
    let highlight_pattern = FgrepPattern {
        raw: pattern.to_vec(),
        normalized: normalized_pattern.to_vec(),
    };
    let visit = visit_ordered_input(input, io_mode, |block| {
        bytes += block.len() as u64;
        let normalized_block = normalize_case(block, options.ignore_case);
        let search_block = normalized_block.as_ref();
        let block_matches = if pattern.is_empty() {
            Vec::new()
        } else {
            finder.find_iter(search_block).collect::<Vec<_>>()
        };
        let mut next_match = 0usize;
        if !pattern.is_empty() && !boundary_tail.is_empty() {
            let prefix_len = search_block.len().min(pattern.len().saturating_sub(1));
            if prefix_len > 0 {
                let mut boundary = Vec::with_capacity(boundary_tail.len() + prefix_len);
                boundary.extend_from_slice(&boundary_tail);
                boundary.extend_from_slice(&search_block[..prefix_len]);
                pending_line_has_match |= finder.find_iter(&boundary).any(|offset| {
                    offset < boundary_tail.len() && offset + pattern.len() > boundary_tail.len()
                });
            }
        }
        let mut line_start = 0usize;
        for rel_end in memchr_iter(options.record_sep(), block) {
            let line_end = rel_end + 1;
            while next_match < block_matches.len() && block_matches[next_match] < line_end {
                if block_matches[next_match] >= line_start {
                    pending_line_has_match = true;
                }
                next_match += 1;
            }
            if pending_line.is_empty() {
                let line = &block[line_start..line_end];
                if fgrep_select_line(pending_line_has_match, options) {
                    if !fgrep_suppresses_matching_line_output(options) {
                        write_matching_line(
                            out,
                            label,
                            line,
                            line_no,
                            line_start_offset,
                            multi_file,
                            options,
                            &highlight_pattern,
                            std::slice::from_ref(&highlight_pattern),
                        )?;
                    }
                    if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                        return Err(fgrep_max_count_reached_error());
                    }
                }
            } else {
                pending_line.extend_from_slice(&block[line_start..line_end]);
                if fgrep_select_line(pending_line_has_match, options) {
                    if !fgrep_suppresses_matching_line_output(options) {
                        write_matching_line(
                            out,
                            label,
                            &pending_line,
                            line_no,
                            line_start_offset,
                            multi_file,
                            options,
                            &highlight_pattern,
                            std::slice::from_ref(&highlight_pattern),
                        )?;
                    }
                    if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                        return Err(fgrep_max_count_reached_error());
                    }
                }
                pending_line.clear();
            }
            pending_line_has_match = pattern.is_empty();
            line_start_offset += (line_end - line_start) as u64;
            line_no += 1;
            line_start = line_end;
        }
        if line_start < block.len() {
            pending_line.extend_from_slice(&block[line_start..]);
            while next_match < block_matches.len() {
                pending_line_has_match = true;
                next_match += 1;
            }
        }
        if pattern.is_empty() {
            boundary_tail.clear();
        } else {
            let tail_len = pattern.len().saturating_sub(1).min(search_block.len());
            boundary_tail.clear();
            boundary_tail.extend_from_slice(&search_block[search_block.len() - tail_len..]);
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
    if !stopped_early
        && !pending_line.is_empty()
        && fgrep_select_line(pending_line_has_match, options)
    {
        if !fgrep_suppresses_matching_line_output(options) {
            write_matching_line(
                out,
                label,
                &pending_line,
                line_no,
                line_start_offset,
                multi_file,
                options,
                &highlight_pattern,
                std::slice::from_ref(&highlight_pattern),
            )?;
        }
        fgrep_record_selected_line(&mut match_count, &mut matched_any, options);
    }
    if options.count_only {
        write_count_line(out, label, match_count, options, multi_file)?;
    }
    Ok((matched_any, bytes))
}

pub(crate) fn write_matching_stream_lines_multi<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<(bool, u64)> {
    if options.only_matching {
        return write_only_matching_stream_lines(
            out,
            label,
            input,
            io_mode,
            &patterns[0],
            patterns,
            multi_file,
            options,
        );
    }
    if !options.line_buffered {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let bytes = data.len() as u64;
        let matched = handle_loaded_match_result(
            out,
            label,
            data.as_slice(),
            &patterns[0],
            patterns,
            super::fgrep_regular_file_path(options, patterns.len()),
            None,
            multi_file,
            options,
        )?;
        return Ok((matched, bytes));
    }
    if fgrep_context_enabled(options) && !fgrep_suppresses_matching_line_output(options) {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let bytes = data.len() as u64;
        let matched = write_context_lines(
            out,
            label,
            data.as_slice(),
            &patterns[0],
            patterns,
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
            if fgrep_select_line(
                fgrep_line_matches_any(&pending_line, patterns, options),
                options,
            ) {
                if !fgrep_suppresses_matching_line_output(options) {
                    write_matching_line(
                        out,
                        label,
                        &pending_line,
                        line_no,
                        line_start_offset,
                        multi_file,
                        options,
                        &patterns[0],
                        patterns,
                    )?;
                }
                if fgrep_record_selected_line(&mut match_count, &mut matched_any, options) {
                    return Err(fgrep_max_count_reached_error());
                }
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
    if !stopped_early
        && !pending_line.is_empty()
        && fgrep_select_line(
            fgrep_line_matches_any(&pending_line, patterns, options),
            options,
        )
    {
        if !fgrep_suppresses_matching_line_output(options) {
            write_matching_line(
                out,
                label,
                &pending_line,
                line_no,
                line_start_offset,
                multi_file,
                options,
                &patterns[0],
                patterns,
            )?;
        }
        fgrep_record_selected_line(&mut match_count, &mut matched_any, options);
    }
    if options.count_only {
        write_count_line(out, label, match_count, options, multi_file)?;
    }
    Ok((matched_any, bytes))
}
