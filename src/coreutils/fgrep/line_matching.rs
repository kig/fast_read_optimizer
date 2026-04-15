use super::{FgrepOptions, FgrepPattern};
use memchr::memmem::Finder;
use std::borrow::Cow;

pub(super) fn fgrep_short_flag_effect(flag: u8) -> Option<bool> {
    match flag {
        b'F' => Some(true),
        _ => None,
    }
}

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

pub(super) fn normalize_case<'a>(bytes: &'a [u8], ignore_case: bool) -> Cow<'a, [u8]> {
    if ignore_case {
        Cow::Owned(bytes.iter().map(u8::to_ascii_lowercase).collect())
    } else {
        Cow::Borrowed(bytes)
    }
}

pub(super) fn fgrep_line_matches(
    line: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    options: FgrepOptions,
) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_newline(line)
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    if options.line_regexp {
        normalized_line.as_ref() == normalized_pattern
    } else if pattern.is_empty() {
        true
    } else {
        Finder::new(normalized_pattern)
            .find(normalized_line.as_ref())
            .is_some()
    }
}

pub(super) fn fgrep_line_matches_any(
    line: &[u8],
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_newline(line)
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    let normalized_line = normalized_line.as_ref();
    patterns.iter().any(|pattern| {
        if options.line_regexp {
            normalized_line == pattern.normalized.as_slice()
        } else if pattern.raw.is_empty() {
            true
        } else {
            Finder::new(pattern.normalized.as_slice())
                .find(normalized_line)
                .is_some()
        }
    })
}

pub(super) fn fgrep_select_line(is_match: bool, options: FgrepOptions) -> bool {
    if options.invert_match {
        !is_match
    } else {
        is_match
    }
}
