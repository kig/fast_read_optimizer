use super::only_matching::fgrep_match_ranges;
use super::*;
use std::io::{self, Write};

const MATCH_START: &[u8] = b"\x1b[01;31m\x1b[K";
const MATCH_END: &[u8] = b"\x1b[m\x1b[K";
const FILENAME_START: &[u8] = b"\x1b[35m\x1b[K";
const NUMBER_START: &[u8] = b"\x1b[32m\x1b[K";
const SEPARATOR_START: &[u8] = b"\x1b[36m\x1b[K";

pub(super) fn fgrep_color_enabled(options: FgrepOptions) -> bool {
    options.color
}

fn write_colored_text<W: Write>(
    out: &mut W,
    text: &[u8],
    start: &[u8],
    end: &[u8],
    options: FgrepOptions,
) -> io::Result<()> {
    if fgrep_color_enabled(options) {
        out.write_all(start)?;
        out.write_all(text)?;
        out.write_all(end)?;
    } else {
        out.write_all(text)?;
    }
    Ok(())
}

pub(super) fn write_colorized_filename<W: Write>(
    out: &mut W,
    label: &str,
    options: FgrepOptions,
) -> io::Result<()> {
    write_colored_text(out, label.as_bytes(), FILENAME_START, MATCH_END, options)
}

pub(super) fn write_colorized_number_prefix<W: Write>(
    out: &mut W,
    value: u64,
    separator: u8,
    width: Option<usize>,
    options: FgrepOptions,
) -> io::Result<()> {
    let width = width.unwrap_or(0);
    if fgrep_color_enabled(options) {
        out.write_all(NUMBER_START)?;
        if width > 0 {
            write!(out, "{value:>width$}")?;
        } else {
            write!(out, "{value}")?;
        }
        out.write_all(MATCH_END)?;
        out.write_all(SEPARATOR_START)?;
        out.write_all(&[separator])?;
        out.write_all(MATCH_END)?;
    } else {
        if width > 0 {
            write!(out, "{value:>width$}{}", separator as char)?;
        } else {
            write!(out, "{value}{}", separator as char)?;
        }
    }
    Ok(())
}

pub(super) fn write_colorized_group_separator<W: Write>(
    out: &mut W,
    separator: &str,
    options: FgrepOptions,
) -> io::Result<()> {
    write_colored_text(
        out,
        separator.as_bytes(),
        SEPARATOR_START,
        MATCH_END,
        options,
    )
}

pub(super) fn write_colorized_separator<W: Write>(
    out: &mut W,
    separator: u8,
    options: FgrepOptions,
) -> io::Result<()> {
    if separator == b'\0' {
        out.write_all(&[separator])?;
        return Ok(());
    }
    if fgrep_color_enabled(options) {
        out.write_all(SEPARATOR_START)?;
        out.write_all(&[separator])?;
        out.write_all(MATCH_END)?;
    } else {
        out.write_all(&[separator])?;
    }
    Ok(())
}

pub(super) fn write_colorized_match_line<W: Write>(
    out: &mut W,
    line: &[u8],
    pattern: &FgrepPattern,
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> io::Result<()> {
    if !fgrep_color_enabled(options) {
        out.write_all(line)?;
        return Ok(());
    }

    let ranges = fgrep_match_ranges(line, pattern, patterns, options);
    if ranges.is_empty() {
        out.write_all(line)?;
        return Ok(());
    }

    let mut cursor = 0usize;
    for (start, len) in ranges {
        if start > cursor {
            out.write_all(&line[cursor..start])?;
        }
        let end = start.saturating_add(len).min(line.len());
        out.write_all(MATCH_START)?;
        out.write_all(&line[start..end])?;
        out.write_all(MATCH_END)?;
        cursor = end;
    }
    if cursor < line.len() {
        out.write_all(&line[cursor..])?;
    }
    Ok(())
}
