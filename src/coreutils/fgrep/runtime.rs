use super::color::{
    write_colorized_filename, write_colorized_match_line, write_colorized_number_prefix,
    write_colorized_separator,
};
use super::*;
use std::io::{self, Write};

#[path = "dispatcher.rs"]
mod dispatcher;
#[path = "matching_output.rs"]
mod matching_output;

#[allow(unused_imports)]
pub(crate) use self::dispatcher::{
    finish_pending_line, handle_loaded_match_result, write_loaded_match_result,
    write_matching_stream_lines, write_matching_stream_lines_multi,
};
#[allow(unused_imports)]
pub(crate) use self::matching_output::{
    count_literal_matching_lines, write_count_literal_matching_lines, write_filtered_lines,
    write_filtered_lines_multi, write_line_regexp_matches, write_matching_lines,
};

const FGREP_STDIN_LABEL: &str = "(standard input)";

pub(super) fn fgrep_display_label(label: Option<&str>) -> &str {
    match label {
        Some("-") | None => FGREP_STDIN_LABEL,
        Some(label) => label,
    }
}

pub(super) fn fgrep_report_binary_match(label: Option<&str>, options: FgrepOptions) {
    if !options.suppress_messages {
        fro::cio_eprintln!("grep: {}: binary file matches", fgrep_display_label(label));
    }
}

fn write_label_prefix<W: Write>(
    out: &mut W,
    label: Option<&str>,
    multi_file: bool,
    options: FgrepOptions,
    separator: u8,
) -> io::Result<bool> {
    if multi_file {
        write_colorized_filename(out, fgrep_display_label(label), options)?;
        write_colorized_separator(
            out,
            if options.null_terminate_filenames {
                b'\0'
            } else {
                separator
            },
            options,
        )?;
        return Ok(true);
    }
    Ok(false)
}

fn write_number_prefix<W: Write>(
    out: &mut W,
    value: u64,
    options: FgrepOptions,
    separator: u8,
) -> io::Result<()> {
    write_colorized_number_prefix(
        out,
        value,
        separator,
        options.initial_tab.then_some(options.offset_width.max(2)),
        options,
    )
}

fn flush_line_buffered_output<W: Write>(out: &mut W, options: FgrepOptions) -> io::Result<()> {
    if options.line_buffered {
        out.flush()?;
    }
    Ok(())
}

pub(super) fn write_filename_result<W: Write>(
    out: &mut W,
    label: Option<&str>,
    options: FgrepOptions,
) -> io::Result<()> {
    write_colorized_filename(out, fgrep_display_label(label), options)?;
    out.write_all(&[if options.null_terminate_filenames {
        b'\0'
    } else {
        b'\n'
    }])?;
    flush_line_buffered_output(out, options)?;
    Ok(())
}

fn write_line_with_separator<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    byte_offset: u64,
    multi_file: bool,
    options: FgrepOptions,
    highlight: Option<(&FgrepPattern, &[FgrepPattern])>,
    separator: u8,
) -> io::Result<()> {
    let mut wrote_prefix = write_label_prefix(out, label, multi_file, options, separator)?;
    if let Some(number) = fgrep_line_number_prefix(options.print_line_numbers, line_no) {
        write_number_prefix(out, number, options, separator)?;
        wrote_prefix = true;
    }
    if options.print_byte_offsets {
        write_number_prefix(out, byte_offset, options, separator)?;
        wrote_prefix = true;
    }
    if options.initial_tab && wrote_prefix {
        out.write_all(b"\t")?;
    }
    if let Some((pattern, patterns)) = highlight {
        write_colorized_match_line(out, line, pattern, patterns, options)?;
    } else {
        out.write_all(line)?;
    }
    let sep = options.record_sep();
    if !line.ends_with(&[sep]) {
        out.write_all(&[sep])?;
    }
    flush_line_buffered_output(out, options)?;
    Ok(())
}

pub(super) fn write_matching_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    byte_offset: u64,
    multi_file: bool,
    options: FgrepOptions,
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
) -> io::Result<()> {
    write_line_with_separator(
        out,
        label,
        line,
        line_no,
        byte_offset,
        multi_file,
        options,
        Some((pattern, patterns)),
        b':',
    )
}

pub(super) fn write_context_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    byte_offset: u64,
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<()> {
    write_line_with_separator(
        out,
        label,
        line,
        line_no,
        byte_offset,
        multi_file,
        options,
        None,
        b'-',
    )
}

pub(super) fn fgrep_suppresses_matching_line_output(options: FgrepOptions) -> bool {
    options.count_only || options.quiet || options.files_with_matches || options.files_without_match
}

pub(super) fn fgrep_stop_after_selected_line(selected_count: u64, options: FgrepOptions) -> bool {
    if options.quiet || options.files_with_matches || options.files_without_match {
        selected_count >= 1
    } else {
        options
            .max_count
            .is_some_and(|max_count| selected_count >= max_count)
    }
}

pub(super) fn fgrep_record_selected_line(
    match_count: &mut u64,
    matched_any: &mut bool,
    options: FgrepOptions,
) -> bool {
    *matched_any = true;
    *match_count += 1;
    fgrep_stop_after_selected_line(*match_count, options)
}

pub(super) fn fgrep_report_input_error(file: &str, err: &io::Error, options: FgrepOptions) {
    if !options.suppress_messages {
        // GNU grep omits " (os error N)" from error messages; strip it to match.
        let msg = err.to_string();
        let msg = if let Some(pos) = msg.rfind(" (os error ") {
            &msg[..pos]
        } else {
            msg.as_str()
        };
        fro::cio_eprintln!("grep: {file}: {msg}");
    }
}

pub(super) fn fgrep_exit_code(matched_any: bool, saw_error: bool, options: FgrepOptions) -> i32 {
    if options.quiet && matched_any {
        0
    } else if saw_error {
        2
    } else if matched_any {
        0
    } else {
        1
    }
}

pub(super) fn fgrep_max_count_reached_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, FGREP_MAX_COUNT_REACHED)
}

pub(super) fn fgrep_is_max_count_reached_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted && err.to_string() == FGREP_MAX_COUNT_REACHED
}

pub(super) fn write_count_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    count: u64,
    options: FgrepOptions,
    multi_file: bool,
) -> io::Result<()> {
    let _ = write_label_prefix(out, label, multi_file, options, b':')?;
    writeln!(out, "{count}")?;
    flush_line_buffered_output(out, options)?;
    Ok(())
}
