use super::{FgrepOptions, FgrepPattern};
use memchr::memmem::Finder;
use std::borrow::Cow;

pub(super) fn fgrep_short_flag_effect(flag: u8) -> Option<bool> {
    match flag {
        b'F' => Some(true),
        _ => None,
    }
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn trim_trailing_record_sep(line: &[u8], sep: u8) -> &[u8] {
    line.strip_suffix(&[sep]).unwrap_or(line)
}

pub(super) fn normalize_case<'a>(bytes: &'a [u8], ignore_case: bool) -> Cow<'a, [u8]> {
    if ignore_case {
        Cow::Owned(bytes.iter().map(u8::to_ascii_lowercase).collect())
    } else {
        Cow::Borrowed(bytes)
    }
}

pub(super) fn fgrep_word_match_start(
    candidate: &[u8],
    normalized_candidate: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    search_start: usize,
) -> Option<usize> {
    if pattern.is_empty() {
        return (!candidate.iter().any(|byte| is_word_byte(*byte))).then_some(0);
    }

    let finder = Finder::new(normalized_pattern);
    let mut current = search_start;
    while current <= normalized_candidate.len() {
        let rel = finder.find(&normalized_candidate[current..])?;
        let start = current + rel;
        let end = start + pattern.len();
        let before_ok = start == 0 || !is_word_byte(candidate[start - 1]);
        let after_ok = end == candidate.len() || !is_word_byte(candidate[end]);
        if before_ok && after_ok {
            return Some(start);
        }
        current = start + 1;
    }
    None
}

fn fgrep_pattern_matches_candidate(
    candidate: &[u8],
    normalized_candidate: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    options: FgrepOptions,
) -> bool {
    if options.line_regexp {
        normalized_candidate == normalized_pattern
    } else if options.word_regexp {
        fgrep_word_match_start(
            candidate,
            normalized_candidate,
            pattern,
            normalized_pattern,
            0,
        )
        .is_some()
    } else if pattern.is_empty() {
        true
    } else {
        Finder::new(normalized_pattern)
            .find(normalized_candidate)
            .is_some()
    }
}

pub(super) fn fgrep_line_matches(
    line: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    options: FgrepOptions,
) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_record_sep(line, options.record_sep())
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    fgrep_pattern_matches_candidate(
        candidate,
        normalized_line.as_ref(),
        pattern,
        normalized_pattern,
        options,
    )
}

pub(super) fn fgrep_line_matches_any(
    line: &[u8],
    patterns: &[FgrepPattern],
    options: FgrepOptions,
) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_record_sep(line, options.record_sep())
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    let normalized_line = normalized_line.as_ref();
    patterns.iter().any(|pattern| {
        fgrep_pattern_matches_candidate(
            candidate,
            normalized_line,
            pattern.raw.as_slice(),
            pattern.normalized.as_slice(),
            options,
        )
    })
}

pub(super) fn fgrep_select_line(is_match: bool, options: FgrepOptions) -> bool {
    if options.invert_match {
        !is_match
    } else {
        is_match
    }
}
