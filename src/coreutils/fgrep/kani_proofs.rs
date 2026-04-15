use super::line_matching::{fgrep_line_matches, fgrep_select_line, fgrep_short_flag_effect};
use super::{fgrep_line_number_prefix, FgrepOptions};

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
        print_line_numbers: false,
        line_regexp: true,
        ignore_case: false,
        invert_match: false,
        report_gbps: false,
    };
    assert!(fgrep_line_matches(&payload, b"a", b"a", options));
    assert!(!fgrep_line_matches(&payload, b"a\n", b"a\n", options));
}
