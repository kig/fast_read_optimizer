use super::line_matching::{fgrep_line_matches, fgrep_select_line, fgrep_short_flag_effect};
use super::{
    fgrep_line_number_prefix, FgrepBinaryMode, FgrepDevicePolicy, FgrepDirectoryPolicy,
    FgrepFilenameMode, FgrepGroupSeparatorPolicy, FgrepOptions,
};

#[kani::proof]
fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
    let flag: u8 = kani::any();
    let expected = match flag {
        b'F' => Some(true),
        _ => None,
    };
    assert_eq!(fgrep_short_flag_effect(flag), expected);
}

#[kani::proof]
fn fgrep_line_number_prefix_matches_boolean_gate() {
    let print_line_numbers: bool = kani::any();
    let line_no: u64 = kani::any();
    assert_eq!(
        fgrep_line_number_prefix(print_line_numbers, line_no),
        if print_line_numbers {
            Some(line_no)
        } else {
            None
        }
    );
}

#[kani::proof]
fn fgrep_line_matches_line_regexp_trims_one_newline() {
    let payload = [b'a', b'\n'];
    let options = FgrepOptions {
        count_only: false,
        quiet: false,
        files_with_matches: false,
        files_without_match: false,
        color: false,
        only_matching: false,
        suppress_messages: false,
        print_line_numbers: false,
        print_byte_offsets: false,
        line_regexp: true,
        word_regexp: false,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        before_context: 0,
        after_context: 0,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(&payload, b"a", b"a", options));
    assert!(!fgrep_line_matches(&payload, b"a\n", b"a\n", options));
}

#[kani::proof]
fn fgrep_line_matches_word_regexp_handles_non_word_only_lines() {
    let empty_line = [b'\n'];
    let word_line = [b'a', b'\n'];
    let options = FgrepOptions {
        count_only: false,
        quiet: false,
        files_with_matches: false,
        files_without_match: false,
        color: false,
        only_matching: false,
        suppress_messages: false,
        print_line_numbers: false,
        print_byte_offsets: false,
        line_regexp: false,
        word_regexp: true,
        ignore_case: false,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        before_context: 0,
        after_context: 0,
        report_gbps: false,
        group_separator: FgrepGroupSeparatorPolicy::Default,
        binary_mode: FgrepBinaryMode::Default,
        filename_mode: FgrepFilenameMode::Auto,
        device_policy: FgrepDevicePolicy::Read,
        directory_policy: FgrepDirectoryPolicy::Read,
    };
    assert!(fgrep_line_matches(&empty_line, b"", b"", options));
    assert!(!fgrep_line_matches(&word_line, b"", b"", options));
}
