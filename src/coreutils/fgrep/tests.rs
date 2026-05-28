use super::line_matching::{fgrep_line_matches, fgrep_line_matches_any, fgrep_short_flag_effect};
use super::runtime::{write_count_line, write_filename_result, write_matching_line};
use super::{
    compile_patterns, count_literal_matching_lines, fgrep_regular_file_path, parse_fgrep_args,
    parse_pattern_file_bytes, FgrepBinaryMode, FgrepDevicePolicy, FgrepDirectoryPolicy,
    FgrepFilenameMode, FgrepGroupSeparatorPolicy, FgrepOptions, FgrepPattern, FgrepRegularFilePath,
};
use std::io::{self, Write};

struct FlushCountingWriter {
    bytes: Vec<u8>,
    flushes: usize,
}

impl FlushCountingWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            flushes: 0,
        }
    }
}

impl Write for FlushCountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

fn fgrep_test_temp_file(name: &str) -> std::path::PathBuf {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    base.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
    assert_eq!(fgrep_short_flag_effect(b'F'), Some(true));
    assert_eq!(fgrep_short_flag_effect(b'n'), None);
}

#[test]
fn parse_fgrep_args_rejects_conflicting_matcher_flags() {
    for flag in [
        "-E",
        "--extended-regexp",
        "-G",
        "--basic-regexp",
        "-P",
        "--perl-regexp",
    ] {
        let err = match parse_fgrep_args(&[
            "fgrep".to_string(),
            flag.to_string(),
            "needle".to_string(),
            "file".to_string(),
        ]) {
            Ok(_) => panic!("expected conflicting matcher error for {flag}"),
            Err(err) => err,
        };
        assert_eq!(err.to_string(), "grep: conflicting matchers specified");
    }
}

#[test]
fn fgrep_line_matches_honors_line_regexp() {
    let options = FgrepOptions {
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
        line_regexp: true,
        word_regexp: false,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(b"alpha\n", b"alpha", b"alpha", options));
    assert!(fgrep_line_matches(b"alpha", b"alpha", b"alpha", options));
    assert!(!fgrep_line_matches(
        b"alpha beta\n",
        b"alpha",
        b"alpha",
        options
    ));
    assert!(!fgrep_line_matches(
        b"alpha\n", b"alpha\n", b"alpha\n", options
    ));
}

#[test]
fn fgrep_line_matches_honors_ignore_case() {
    let contains_options = FgrepOptions {
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
        ignore_case: true,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(
        b"Alpha beta\n",
        b"alpha",
        b"alpha",
        contains_options
    ));
    assert!(!fgrep_line_matches(
        b"beta\n",
        b"alpha",
        b"alpha",
        contains_options
    ));

    let line_options = FgrepOptions {
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
        line_regexp: true,
        word_regexp: false,
        ignore_case: true,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(
        b"Alpha\n",
        b"alpha",
        b"alpha",
        line_options
    ));
    assert!(!fgrep_line_matches(
        b"Alpha beta\n",
        b"alpha",
        b"alpha",
        line_options
    ));
}

#[test]
fn fgrep_line_matches_honors_word_regexp() {
    let options = FgrepOptions {
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
        word_regexp: true,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(
        b"needle beta\n",
        b"needle",
        b"needle",
        options
    ));
    assert!(fgrep_line_matches(
        b"alpha-needle\n",
        b"needle",
        b"needle",
        options
    ));
    assert!(!fgrep_line_matches(
        b"alpha_needle\n",
        b"needle",
        b"needle",
        options
    ));

    let empty_options = FgrepOptions {
        word_regexp: true,
        ..options
    };
    assert!(fgrep_line_matches(b"---\n", b"", b"", empty_options));
    assert!(!fgrep_line_matches(b"alpha\n", b"", b"", empty_options));
}

#[test]
fn parse_pattern_file_bytes_ignores_trailing_newline() {
    assert_eq!(
        parse_pattern_file_bytes(b"alpha\nbeta\n"),
        vec![b"alpha".to_vec(), b"beta".to_vec()]
    );
    assert_eq!(
        parse_pattern_file_bytes(b"\nalpha\n\n"),
        vec![Vec::new(), b"alpha".to_vec(), Vec::new()]
    );
}

#[test]
fn fgrep_line_matches_any_checks_all_patterns() {
    let options = FgrepOptions {
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
        ignore_case: true,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    let patterns = vec![
        FgrepPattern {
            raw: b"needle".to_vec(),
            normalized: b"needle".to_vec(),
        },
        FgrepPattern {
            raw: b"omega".to_vec(),
            normalized: b"omega".to_vec(),
        },
    ];
    assert!(fgrep_line_matches_any(b"OMEGA\n", &patterns, options));
    assert!(fgrep_line_matches_any(b"needle beta\n", &patterns, options));
    assert!(!fgrep_line_matches_any(b"alpha\n", &patterns, options));
}

#[test]
fn fgrep_regular_file_path_keeps_literal_backend_for_path_preserving_flags() {
    let base = FgrepOptions {
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
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    for options in [
        base,
        FgrepOptions {
            initial_tab: true,
            ..base
        },
        FgrepOptions {
            line_buffered: true,
            before_context: 0,
            after_context: 0,
            ..base
        },
        FgrepOptions {
            print_line_numbers: true,
            ..base
        },
        FgrepOptions {
            print_byte_offsets: true,
            ..base
        },
        FgrepOptions {
            count_only: true,
            ..base
        },
        FgrepOptions {
            invert_match: true,
            ..base
        },
        FgrepOptions {
            count_only: true,
            invert_match: true,
            print_line_numbers: true,
            ..base
        },
    ] {
        assert_eq!(
            fgrep_regular_file_path(options, 1),
            FgrepRegularFilePath::LiteralSearchOffsets
        );
    }
}

#[test]
fn fgrep_regular_file_path_tracks_single_pattern_sources_and_no_ignore_case() {
    let pattern_file = fgrep_test_temp_file("fro-fgrep-patterns");
    std::fs::write(&pattern_file, b"alpha\n").unwrap();

    for args in [
        vec![
            "fgrep".to_string(),
            "-e".to_string(),
            "alpha".to_string(),
            "input.txt".to_string(),
        ],
        vec![
            "fgrep".to_string(),
            "-f".to_string(),
            pattern_file.display().to_string(),
            "input.txt".to_string(),
        ],
        vec![
            "fgrep".to_string(),
            "-i".to_string(),
            "--no-ignore-case".to_string(),
            "-e".to_string(),
            "alpha".to_string(),
            "input.txt".to_string(),
        ],
    ] {
        let parsed = parse_fgrep_args(&args).unwrap();
        let pattern_count = compile_patterns(parsed.pattern_sources, parsed.options.ignore_case)
            .unwrap()
            .len();
        assert_eq!(
            fgrep_regular_file_path(parsed.options, pattern_count),
            FgrepRegularFilePath::LiteralSearchOffsets,
            "args: {args:?}"
        );
    }

    let _ = std::fs::remove_file(pattern_file);
}

#[test]
fn fgrep_regular_file_path_documents_line_filter_fallbacks() {
    let ignore_case = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-i".to_string(),
        "alpha".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert_eq!(
        fgrep_regular_file_path(ignore_case.options, 1),
        FgrepRegularFilePath::LineFilterSinglePattern
    );

    let line_regexp = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-x".to_string(),
        "alpha".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert_eq!(
        fgrep_regular_file_path(line_regexp.options, 1),
        FgrepRegularFilePath::LineFilterSinglePattern
    );

    let word_regexp = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-w".to_string(),
        "alpha".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert_eq!(
        fgrep_regular_file_path(word_regexp.options, 1),
        FgrepRegularFilePath::LineFilterSinglePattern
    );

    let multi_pattern = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-e".to_string(),
        "alpha".to_string(),
        "-e".to_string(),
        "beta".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    let pattern_count = compile_patterns(
        multi_pattern.pattern_sources,
        multi_pattern.options.ignore_case,
    )
    .unwrap()
    .len();
    assert_eq!(
        fgrep_regular_file_path(multi_pattern.options, pattern_count),
        FgrepRegularFilePath::LineFilterMultiPattern
    );
}

#[test]
fn count_literal_matching_lines_counts_once_per_matching_line() {
    let options = FgrepOptions {
        count_only: true,
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
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert_eq!(
        count_literal_matching_lines(b"needle needle\nalpha\nneedle\n", b"needle", options),
        (true, 2)
    );
}

#[test]
fn count_literal_matching_lines_handles_invert_and_trailing_line() {
    let options = FgrepOptions {
        count_only: true,
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
        invert_match: true,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert_eq!(
        count_literal_matching_lines(b"needle\nalpha\nomega", b"needle", options),
        (true, 2)
    );
}

#[test]
fn count_literal_matching_lines_honors_max_count() {
    let options = FgrepOptions {
        count_only: true,
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
        max_count: Some(2),
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert_eq!(
        count_literal_matching_lines(b"needle\nneedle\nneedle\n", b"needle", options),
        (true, 2)
    );
}

#[test]
fn parse_fgrep_args_tracks_max_count_and_no_messages() {
    let parsed = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-s".to_string(),
        "--max-count=3".to_string(),
        "needle".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert!(parsed.options.suppress_messages);
    assert_eq!(parsed.options.max_count, Some(3));
}

#[test]
fn parse_fgrep_args_tracks_file_kind_policies_and_stdin_label() {
    let parsed = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-Dskip".to_string(),
        "--directories=skip".to_string(),
        "--label=stdin-label".to_string(),
        "needle".to_string(),
        "-".to_string(),
    ])
    .unwrap();
    assert_eq!(parsed.options.device_policy, FgrepDevicePolicy::Skip);
    assert_eq!(parsed.options.directory_policy, FgrepDirectoryPolicy::Skip);
    assert_eq!(parsed.stdin_label.as_deref(), Some("stdin-label"));
}

#[test]
fn parse_fgrep_args_tracks_byte_offset_and_null_filename_flags() {
    let parsed = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-b".to_string(),
        "--null".to_string(),
        "needle".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert!(parsed.options.print_byte_offsets);
    assert!(parsed.options.null_terminate_filenames);
}

#[test]
fn parse_fgrep_args_tracks_initial_tab_and_line_buffered() {
    let parsed = parse_fgrep_args(&[
        "fgrep".to_string(),
        "-T".to_string(),
        "--line-buffered".to_string(),
        "needle".to_string(),
        "input.txt".to_string(),
    ])
    .unwrap();
    assert!(parsed.options.initial_tab);
    assert!(parsed.options.line_buffered);
}

#[test]
fn parse_fgrep_args_tracks_binary_mode_flags_and_precedence() {
    for (args, expected) in [
        (
            vec!["fgrep", "--binary-files=text", "needle", "input.txt"],
            FgrepBinaryMode::Text,
        ),
        (
            vec!["fgrep", "-a", "needle", "input.txt"],
            FgrepBinaryMode::Text,
        ),
        (
            vec!["fgrep", "-I", "needle", "input.txt"],
            FgrepBinaryMode::WithoutMatch,
        ),
        (
            vec![
                "fgrep",
                "--binary-files=without-match",
                "needle",
                "input.txt",
            ],
            FgrepBinaryMode::WithoutMatch,
        ),
        (
            vec!["fgrep", "-U", "needle", "input.txt"],
            FgrepBinaryMode::Binary,
        ),
        (
            vec!["fgrep", "--binary-files=binary", "needle", "input.txt"],
            FgrepBinaryMode::Binary,
        ),
        (
            vec!["fgrep", "-I", "-a", "needle", "input.txt"],
            FgrepBinaryMode::Text,
        ),
        (
            vec!["fgrep", "-a", "-I", "needle", "input.txt"],
            FgrepBinaryMode::WithoutMatch,
        ),
    ] {
        let parsed = parse_fgrep_args(
            &args
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<String>>(),
        )
        .unwrap();
        assert_eq!(parsed.options.binary_mode, expected);
    }
}

#[test]
fn parse_fgrep_args_tracks_group_separator_policy() {
    for (args, expected) in [
        (
            vec!["fgrep", "--group-separator=SEP", "needle", "input.txt"],
            FgrepGroupSeparatorPolicy::Custom("SEP"),
        ),
        (
            vec!["fgrep", "--group-separator", "SEP", "needle", "input.txt"],
            FgrepGroupSeparatorPolicy::Custom("SEP"),
        ),
        (
            vec![
                "fgrep",
                "--group-separator=SEP",
                "--no-group-separator",
                "needle",
                "input.txt",
            ],
            FgrepGroupSeparatorPolicy::Disabled,
        ),
        (
            vec![
                "fgrep",
                "--no-group-separator",
                "--group-separator=SEP",
                "needle",
                "input.txt",
            ],
            FgrepGroupSeparatorPolicy::Custom("SEP"),
        ),
    ] {
        let parsed = parse_fgrep_args(
            &args
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<String>>(),
        )
        .unwrap();
        assert_eq!(parsed.options.group_separator, expected);
    }
}

#[test]
fn write_matching_line_only_flushes_when_line_buffered() {
    let options = FgrepOptions {
        count_only: false,
        quiet: false,
        files_with_matches: false,
        files_without_match: false,
        color: false,
        only_matching: false,
        suppress_messages: false,
        initial_tab: true,
        line_buffered: true,
        before_context: 0,
        after_context: 0,
        offset_width: 2,
        print_line_numbers: true,
        print_byte_offsets: true,
        line_regexp: false,
        word_regexp: false,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Always,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    let pattern = FgrepPattern {
        raw: b"needle".to_vec(),
        normalized: b"needle".to_vec(),
    };
    let mut writer = FlushCountingWriter::new();
    write_matching_line(
        &mut writer,
        Some("file.txt"),
        b"needle\n",
        2,
        6,
        true,
        options,
        &pattern,
        std::slice::from_ref(&pattern),
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(writer.bytes).unwrap(),
        "file.txt: 2: 6:\tneedle\n"
    );
    assert_eq!(writer.flushes, 1);

    let mut writer = FlushCountingWriter::new();
    write_matching_line(
        &mut writer,
        Some("file.txt"),
        b"needle\n",
        2,
        6,
        true,
        FgrepOptions {
            line_buffered: false,
            before_context: 0,
            after_context: 0,
            ..options
        },
        &pattern,
        std::slice::from_ref(&pattern),
    )
    .unwrap();
    assert_eq!(writer.flushes, 0);
}

#[test]
fn line_buffered_flushes_count_and_filename_outputs() {
    let options = FgrepOptions {
        count_only: false,
        quiet: false,
        files_with_matches: false,
        files_without_match: false,
        color: false,
        only_matching: false,
        suppress_messages: false,
        initial_tab: false,
        line_buffered: true,
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
        null_data: false,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Always,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    let mut count_writer = FlushCountingWriter::new();
    write_count_line(&mut count_writer, Some("file.txt"), 2, options, true).unwrap();
    assert_eq!(
        String::from_utf8(count_writer.bytes).unwrap(),
        "file.txt:2\n"
    );
    assert_eq!(count_writer.flushes, 1);

    let mut file_writer = FlushCountingWriter::new();
    write_filename_result(&mut file_writer, Some("file.txt"), options).unwrap();
    assert_eq!(String::from_utf8(file_writer.bytes).unwrap(), "file.txt\n");
    assert_eq!(file_writer.flushes, 1);
}
