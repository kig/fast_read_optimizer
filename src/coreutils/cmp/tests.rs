use super::{
    cmp_byte_display_parts, cmp_decimal_width, cmp_effective_compare_len,
    cmp_effective_compare_len_with_skips, cmp_eof_line, cmp_flags_are_compatible,
    cmp_remaining_len_after_skip, cmp_render_byte_char, parse_cmp_limit, parse_cmp_skip_spec,
    CMP_SMALL_REGULAR_FAST_PATH_LIMIT,
};

#[test]
fn cmp_effective_compare_len_respects_shorter_file_and_limit() {
    assert_eq!(cmp_effective_compare_len(10, 12, None), 10);
    assert_eq!(cmp_effective_compare_len(10, 12, Some(4)), 4);
    assert_eq!(cmp_effective_compare_len(3, 9, Some(99)), 3);
    assert_eq!(cmp_effective_compare_len(3, 9, Some(0)), 0);
}

#[test]
fn cmp_remaining_len_after_skip_saturates_at_zero() {
    assert_eq!(cmp_remaining_len_after_skip(10, 0), 10);
    assert_eq!(cmp_remaining_len_after_skip(10, 4), 6);
    assert_eq!(cmp_remaining_len_after_skip(3, 9), 0);
}

#[test]
fn cmp_effective_compare_len_with_skips_respects_remaining_prefixes() {
    assert_eq!(cmp_effective_compare_len_with_skips(10, 12, 3, 4, None), 7);
    assert_eq!(
        cmp_effective_compare_len_with_skips(10, 12, 3, 4, Some(2)),
        2
    );
    assert_eq!(cmp_effective_compare_len_with_skips(3, 9, 6, 6, None), 0);
    assert_eq!(cmp_effective_compare_len_with_skips(3, 9, 4, 6, Some(9)), 0);
}

#[test]
fn parse_cmp_limit_accepts_decimal_counts() {
    assert_eq!(parse_cmp_limit("0").unwrap(), 0);
    assert_eq!(parse_cmp_limit("17").unwrap(), 17);
    assert_eq!(parse_cmp_limit("1k").unwrap(), 1024);
    assert_eq!(parse_cmp_limit("1KB").unwrap(), 1000);
    assert_eq!(parse_cmp_limit("2MiB").unwrap(), 2 << 20);
    assert!(parse_cmp_limit("x").is_err());
    assert!(parse_cmp_limit("1mb").is_err());
}

#[test]
fn parse_cmp_skip_spec_accepts_shared_and_split_skips() {
    assert_eq!(parse_cmp_skip_spec("4").unwrap(), (4, 4));
    assert_eq!(parse_cmp_skip_spec("3:9").unwrap(), (3, 9));
    assert_eq!(parse_cmp_skip_spec("1K:1KB").unwrap(), (1024, 1000));
    assert_eq!(parse_cmp_skip_spec("2MiB").unwrap(), (2 << 20, 2 << 20));
    assert!(parse_cmp_skip_spec("x").is_err());
    assert!(parse_cmp_skip_spec("1:x").is_err());
    assert!(parse_cmp_skip_spec("1mb").is_err());
}

#[test]
fn cmp_eof_line_matches_gnu_newline_convention() {
    assert_eq!(cmp_eof_line(0, false), (1, "in line"));
    assert_eq!(cmp_eof_line(1, true), (1, "line"));
    assert_eq!(cmp_eof_line(1, false), (2, "in line"));
}

#[test]
fn cmp_flag_compatibility_rejects_quiet_plus_verbose() {
    assert!(cmp_flags_are_compatible(false, false));
    assert!(cmp_flags_are_compatible(true, false));
    assert!(cmp_flags_are_compatible(false, true));
    assert!(!cmp_flags_are_compatible(true, true));
}

#[test]
fn cmp_decimal_width_matches_decimal_digit_count() {
    assert_eq!(cmp_decimal_width(1), 1);
    assert_eq!(cmp_decimal_width(9), 1);
    assert_eq!(cmp_decimal_width(10), 2);
    assert_eq!(cmp_decimal_width(999), 3);
}

#[test]
fn cmp_byte_display_parts_split_high_bit_from_render_core() {
    assert_eq!(cmp_byte_display_parts(0), (false, 0));
    assert_eq!(cmp_byte_display_parts(127), (false, 127));
    assert_eq!(cmp_byte_display_parts(128), (true, 0));
    assert_eq!(cmp_byte_display_parts(255), (true, 127));
}

#[test]
fn cmp_render_byte_char_matches_gnu_style_examples() {
    assert_eq!(cmp_render_byte_char(b'Q'), "Q");
    assert_eq!(cmp_render_byte_char(b' '), " ");
    assert_eq!(cmp_render_byte_char(b'\t'), "^I");
    assert_eq!(cmp_render_byte_char(0), "^@");
    assert_eq!(cmp_render_byte_char(127), "^?");
    assert_eq!(cmp_render_byte_char(255), "M-^?");
}

#[test]
fn cmp_small_regular_fast_path_limit_is_tiny_file_sized() {
    assert!(CMP_SMALL_REGULAR_FAST_PATH_LIMIT >= 4 * 1024);
    assert!(CMP_SMALL_REGULAR_FAST_PATH_LIMIT <= 1024 * 1024);
}
