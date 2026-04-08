use super::{
    compile_patterns, fgrep_line_matches, fgrep_line_matches_any, fgrep_regular_file_path,
    fgrep_short_flag_effect, parse_fgrep_args, parse_pattern_file_bytes, FgrepOptions,
    FgrepPattern, FgrepRegularFilePath,
};

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
fn fgrep_line_matches_honors_line_regexp() {
    let options = FgrepOptions {
        count_only: false,
        print_line_numbers: false,
        line_regexp: true,
        ignore_case: false,
        invert_match: false,
        report_gbps: false,
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
        print_line_numbers: false,
        line_regexp: false,
        ignore_case: true,
        invert_match: false,
        report_gbps: false,
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
        print_line_numbers: false,
        line_regexp: true,
        ignore_case: true,
        invert_match: false,
        report_gbps: false,
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
        print_line_numbers: false,
        line_regexp: false,
        ignore_case: true,
        invert_match: false,
        report_gbps: false,
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
        print_line_numbers: false,
        line_regexp: false,
        ignore_case: false,
        invert_match: false,
        report_gbps: false,
    };
    for options in [
        base,
        FgrepOptions {
            print_line_numbers: true,
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
