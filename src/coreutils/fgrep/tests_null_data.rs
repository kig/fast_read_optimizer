// ── null-data unit tests ──────────────────────────────────────────────────

use super::line_matching::{fgrep_line_matches, fgrep_line_matches_any};
use super::runtime::{write_filtered_lines, write_filtered_lines_multi};
use super::{
    count_literal_matching_lines, parse_fgrep_args, FgrepBinaryMode, FgrepDevicePolicy,
    FgrepDirectoryPolicy, FgrepFilenameMode, FgrepGroupSeparatorPolicy, FgrepOptions, FgrepPattern,
};

fn null_data_base_options() -> FgrepOptions {
    FgrepOptions {
        count_only: false,
        quiet: false,
        files_with_matches: false,
        files_without_match: false,
        color: false,
        only_matching: false,
        suppress_messages: false,
        initial_tab: false,
        line_buffered: false,
        before_context: 0,
        after_context: 0,
        offset_width: 0,
        print_line_numbers: false,
        print_byte_offsets: false,
        line_regexp: false,
        word_regexp: false,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: true,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    }
}

#[test]
fn record_sep_returns_nul_when_null_data() {
    let opts = null_data_base_options();
    assert_eq!(opts.record_sep(), b'\0');
    assert_eq!(
        FgrepOptions {
            null_data: false,
            ..opts
        }
        .record_sep(),
        b'\n'
    );
}

#[test]
fn parse_fgrep_args_tracks_null_data_short_and_long() {
    for flag in ["-z", "--null-data"] {
        let parsed = parse_fgrep_args(&[
            "fgrep".to_string(),
            flag.to_string(),
            "needle".to_string(),
            "input.txt".to_string(),
        ])
        .unwrap();
        assert!(
            parsed.options.null_data,
            "flag {flag} did not set null_data"
        );
    }
    let no_flag = parse_fgrep_args(&[
        "fgrep".to_string(),
        "needle".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert!(!no_flag.options.null_data);
}

#[test]
fn count_literal_matching_lines_uses_nul_delimiter() {
    let opts = null_data_base_options();
    // Three NUL-delimited records: "alpha", "needle", "beta".
    let data = b"alpha\0needle\0beta\0";
    assert_eq!(
        count_literal_matching_lines(data, b"needle", opts),
        (true, 1)
    );
    // Inverted: two records that don't contain "needle".
    let inv = FgrepOptions {
        invert_match: true,
        ..opts
    };
    assert_eq!(
        count_literal_matching_lines(data, b"needle", inv),
        (true, 2)
    );
}

#[test]
fn count_literal_matching_lines_nul_does_not_split_on_newline() {
    let opts = null_data_base_options();
    // Data has embedded newlines but those are NOT record separators under -z.
    let data = b"foo\nbar\0needle";
    // One record containing "needle" (the unterminated last record).
    assert_eq!(
        count_literal_matching_lines(data, b"needle", opts),
        (true, 1)
    );
    // The record "foo\nbar" does not match "needle" but does match "bar".
    assert_eq!(count_literal_matching_lines(data, b"bar", opts), (true, 1));
}

#[test]
fn write_filtered_lines_outputs_nul_terminated_records() {
    let opts = null_data_base_options();
    let data = b"alpha\0needle\0beta\0";
    let mut out = Vec::new();
    let matched =
        write_filtered_lines(&mut out, "f", data, b"needle", b"needle", false, opts).unwrap();
    assert!(matched);
    // Output record should be NUL-terminated (same as input record).
    assert_eq!(out, b"needle\0");
}

#[test]
fn write_filtered_lines_nul_appends_terminator_to_unterminated_record() {
    let opts = null_data_base_options();
    // Last record has no trailing NUL.
    let data = b"alpha\0needle";
    let mut out = Vec::new();
    write_filtered_lines(&mut out, "f", data, b"needle", b"needle", false, opts).unwrap();
    assert_eq!(out, b"needle\0");
}

#[test]
fn write_filtered_lines_multi_nul_delimiter() {
    let opts = null_data_base_options();
    let data = b"hello\0world\0needle\0";
    let patterns = vec![
        FgrepPattern {
            raw: b"needle".to_vec(),
            normalized: b"needle".to_vec(),
        },
        FgrepPattern {
            raw: b"hello".to_vec(),
            normalized: b"hello".to_vec(),
        },
    ];
    let mut out = Vec::new();
    let matched = write_filtered_lines_multi(&mut out, "f", data, &patterns, false, opts).unwrap();
    assert!(matched);
    assert_eq!(out, b"hello\0needle\0");
}

#[test]
fn fgrep_line_matches_line_regexp_with_nul_delimiter() {
    // With -z and -x, the record terminator is NUL, not newline.
    let opts = FgrepOptions {
        line_regexp: true,
        ..null_data_base_options()
    };
    // "alpha\0" stripped of NUL terminator → "alpha", matches pattern "alpha".
    assert!(fgrep_line_matches(b"alpha\0", b"alpha", b"alpha", opts));
    // "alpha\n" is NOT stripped (NUL is the sep, not newline): "alpha\n" ≠ "alpha".
    assert!(!fgrep_line_matches(b"alpha\n", b"alpha", b"alpha", opts));
    // Exact match when there's no terminator.
    assert!(fgrep_line_matches(b"alpha", b"alpha", b"alpha", opts));
}
