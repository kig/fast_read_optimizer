use super::compare::{
    compare_line_bytes, compare_output_lines, compare_sort_keys, same_sort_key,
};
use super::*;

fn refs_for_lines(lines: &[&[u8]]) -> (Vec<u8>, Vec<SortLineRef>) {
    let mut storage = Vec::new();
    let mut refs = Vec::new();
    for (sequence, line) in lines.iter().enumerate() {
        let start = storage.len();
        storage.extend_from_slice(line);
        refs.push(SortLineRef {
            start,
            len: line.len(),
            sequence: sequence as u64,
        });
    }
    (storage, refs)
}

fn comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, false, false, false, false, false)
}

fn stable_comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, true, false, false, false, false)
}

fn dictionary_order_comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, false, true, false, false, false)
}

fn ignore_case_comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, false, false, true, false, false)
}

fn ignore_leading_blanks_comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, false, false, false, true, false)
}

fn ignore_nonprinting_comparator(mode: SortMode) -> SortComparator {
    SortComparator::new(mode, Vec::new(), None, false, false, false, false, true)
}

#[test]
fn parse_sort_buffer_size_accepts_sizes_and_percentages() {
    assert_eq!(
        external::parse_sort_buffer_size("1K", "--buffer-size").unwrap(),
        1024
    );
    assert_eq!(
        external::parse_sort_buffer_size("2MiB", "--buffer-size").unwrap(),
        2 * 1024 * 1024
    );
    assert_eq!(
        external::parse_sort_buffer_size("0", "--buffer-size").unwrap(),
        0
    );
    assert_eq!(
        external::parse_sort_buffer_size("50%", "--buffer-size").unwrap(),
        external::mem_available_bytes()
            .unwrap_or(128 << 20)
            .checked_mul(50)
            .unwrap()
            / 100
    );
}

#[test]
fn parse_sort_buffer_size_rejects_invalid_values() {
    assert_eq!(
        external::parse_sort_buffer_size("", "--buffer-size")
            .unwrap_err()
            .to_string(),
        "invalid --buffer-size argument ''"
    );
    assert_eq!(
        external::parse_sort_buffer_size("bad", "--buffer-size")
            .unwrap_err()
            .to_string(),
        "invalid --buffer-size argument 'bad'"
    );
    assert_eq!(
        external::parse_sort_buffer_size("1ZiB", "--buffer-size")
            .unwrap_err()
            .to_string(),
        "invalid --buffer-size argument '1ZiB'"
    );
}

#[test]
fn sort_buffer_size_override_wins_over_environment_limit() {
    let previous = std::env::var_os("FRO_SORT_MAX_IN_MEMORY_BYTES");
    std::env::set_var("FRO_SORT_MAX_IN_MEMORY_BYTES", "1048576");
    assert_eq!(external::sort_memory_budget_bytes(None).unwrap(), 1_048_576);
    assert_eq!(
        external::sort_memory_budget_bytes(Some(65_536)).unwrap(),
        65_536
    );
    if let Some(value) = previous {
        std::env::set_var("FRO_SORT_MAX_IN_MEMORY_BYTES", value);
    } else {
        std::env::remove_var("FRO_SORT_MAX_IN_MEMORY_BYTES");
    }
}

fn field_separator_comparator(mode: SortMode, separator: u8) -> SortComparator {
    SortComparator::new(
        mode,
        Vec::new(),
        Some(separator),
        false,
        false,
        false,
        false,
        false,
    )
}

#[test]
fn radix_sort_matches_bytewise_order_for_prefixes_and_empty_lines() {
    let input = [
        b"beta".as_slice(),
        b"".as_slice(),
        b"alpha".as_slice(),
        b"alph".as_slice(),
        b"alpha".as_slice(),
        b"z".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    sort_line_refs(
        &mut refs,
        &storage,
        &comparator(SortMode::Bytewise),
        false,
        false,
    )
    .unwrap();
    let sorted = refs
        .iter()
        .map(|line| line.bytes(&storage).to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        sorted,
        vec![
            b"".to_vec(),
            b"alph".to_vec(),
            b"alpha".to_vec(),
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"z".to_vec(),
        ]
    );
}

#[test]
fn append_input_lines_keeps_missing_final_newline_as_a_record() {
    let mut storage = Vec::new();
    let mut refs = Vec::new();
    let mut next_sequence = 0;
    append_input_lines(
        &mut storage,
        &mut refs,
        b"beta\nalpha",
        &mut next_sequence,
        RecordTerminator::Newline,
    )
    .unwrap();
    let lines = refs
        .iter()
        .map(|line| line.bytes(&storage).to_vec())
        .collect::<Vec<_>>();
    assert_eq!(lines, vec![b"beta".to_vec(), b"alpha".to_vec()]);
}

#[test]
fn append_input_lines_splits_nul_terminated_records() {
    let mut storage = Vec::new();
    let mut refs = Vec::new();
    let mut next_sequence = 0;
    append_input_lines(
        &mut storage,
        &mut refs,
        b"beta\0alpha",
        &mut next_sequence,
        RecordTerminator::Nul,
    )
    .unwrap();
    let lines = refs
        .iter()
        .map(|line| line.bytes(&storage).to_vec())
        .collect::<Vec<_>>();
    assert_eq!(lines, vec![b"beta".to_vec(), b"alpha".to_vec()]);
}

#[test]
fn finalize_sorted_lines_applies_unique_and_reverse_after_sorting() {
    let input = [
        b"beta".as_slice(),
        b"alpha".as_slice(),
        b"beta".as_slice(),
        b"alpha".as_slice(),
        b"".as_slice(),
    ];
    let (storage, refs) = refs_for_lines(&input);

    let mut sorted = refs.clone();
    finalize_sorted_lines(
        &mut sorted,
        &storage,
        &comparator(SortMode::Bytewise),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        sorted
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"".to_vec(),
            b"alpha".to_vec(),
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"beta".to_vec(),
        ]
    );

    let mut unique_only = refs.clone();
    finalize_sorted_lines(
        &mut unique_only,
        &storage,
        &comparator(SortMode::Bytewise),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        unique_only
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"".to_vec(), b"alpha".to_vec(), b"beta".to_vec()]
    );

    let mut unique_reverse = refs;
    finalize_sorted_lines(
        &mut unique_reverse,
        &storage,
        &comparator(SortMode::Bytewise),
        true,
        true,
    )
    .unwrap();
    assert_eq!(
        unique_reverse
            .iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"beta".to_vec(), b"alpha".to_vec(), b"".to_vec()]
    );
}

#[test]
fn ignore_nonprinting_uses_last_resort_line_order_when_keys_match() {
    assert!(ignore_nonprinting_comparator(SortMode::Bytewise).uses_last_resort_line_order(false));
    assert_eq!(
        compare_sort_keys(
            b"Alpha\x01",
            b"Alpha",
            &ignore_nonprinting_comparator(SortMode::Bytewise),
        ),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_line_bytes(
            b"a\t",
            b"a ",
            &ignore_nonprinting_comparator(SortMode::Bytewise),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
}

#[test]
fn ignore_nonprinting_unique_keeps_the_first_equal_key_record() {
    let input = [
        b"Alpha\x01".as_slice(),
        b"Alpha".as_slice(),
        b"Beta".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    finalize_sorted_lines(
        &mut refs,
        &storage,
        &ignore_nonprinting_comparator(SortMode::Bytewise),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"Alpha\x01".to_vec(), b"Beta".to_vec()]
    );
}

#[test]
fn numeric_compare_matches_expected_prefix_ordering() {
    let lines = [
        b"x".as_slice(),
        b"10".as_slice(),
        b"2".as_slice(),
        b"-3".as_slice(),
        b".5".as_slice(),
        b"02".as_slice(),
        b"2a".as_slice(),
        b"+2".as_slice(),
        b"  10".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&lines);
    sort_line_refs(
        &mut refs,
        &storage,
        &comparator(SortMode::Numeric),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"-3".to_vec(),
            b"+2".to_vec(),
            b"x".to_vec(),
            b".5".to_vec(),
            b"02".to_vec(),
            b"2".to_vec(),
            b"2a".to_vec(),
            b"  10".to_vec(),
            b"10".to_vec(),
        ]
    );
}

#[test]
fn numeric_unique_keeps_first_line_for_equal_numeric_keys() {
    let input = [
        b"1.0".as_slice(),
        b"1".as_slice(),
        b"1.00".as_slice(),
        b"2".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    finalize_sorted_lines(
        &mut refs,
        &storage,
        &comparator(SortMode::Numeric),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"1.0".to_vec(), b"2".to_vec()]
    );
}

#[test]
fn general_numeric_compare_orders_invalid_nan_and_numbers() {
    let lines = [
        b"x".as_slice(),
        b"NaN".as_slice(),
        b"-inf".as_slice(),
        b"-3".as_slice(),
        b".5".as_slice(),
        b"0x10".as_slice(),
        b"1e2".as_slice(),
        b"+inf".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&lines);
    sort_line_refs(
        &mut refs,
        &storage,
        &comparator(SortMode::GeneralNumeric),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"x".to_vec(),
            b"NaN".to_vec(),
            b"-inf".to_vec(),
            b"-3".to_vec(),
            b".5".to_vec(),
            b"0x10".to_vec(),
            b"1e2".to_vec(),
            b"+inf".to_vec(),
        ]
    );
}

#[test]
fn human_numeric_unique_compares_suffix_families() {
    let input = [
        b"1KiB".as_slice(),
        b"1K".as_slice(),
        b"1024".as_slice(),
        b"1000".as_slice(),
        b"1024K".as_slice(),
        b"1M".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    finalize_sorted_lines(
        &mut refs,
        &storage,
        &comparator(SortMode::HumanNumeric),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"1000".to_vec(),
            b"1024".to_vec(),
            b"1KiB".to_vec(),
            b"1024K".to_vec(),
            b"1M".to_vec(),
        ]
    );
}

#[test]
fn month_sort_groups_by_month_prefix_and_treats_invalid_as_equal_keys() {
    let input = [
        b"foo".as_slice(),
        b"Jan".as_slice(),
        b"January".as_slice(),
        b"  feb".as_slice(),
        b"Feb".as_slice(),
        b"Dec".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    finalize_sorted_lines(
        &mut refs,
        &storage,
        &comparator(SortMode::Month),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"foo".to_vec(),
            b"Jan".to_vec(),
            b"  feb".to_vec(),
            b"Dec".to_vec()
        ]
    );
}

#[test]
fn version_sort_matches_strverscmp_style_digit_ordering() {
    let input = [
        b"v1".as_slice(),
        b"v01".as_slice(),
        b"v1.0".as_slice(),
        b"v1.0.02".as_slice(),
        b"v1.0.2".as_slice(),
        b"v1.0.10".as_slice(),
        b"v1~".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    sort_line_refs(
        &mut refs,
        &storage,
        &comparator(SortMode::Version),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"v1~".to_vec(),
            b"v01".to_vec(),
            b"v1".to_vec(),
            b"v1.0".to_vec(),
            b"v1.0.02".to_vec(),
            b"v1.0.2".to_vec(),
            b"v1.0.10".to_vec(),
        ]
    );
}

#[test]
fn compare_line_bytes_matches_numeric_last_resort_ordering() {
    assert_eq!(
        compare_line_bytes(b"1", b"1.0", &comparator(SortMode::Numeric), false, false),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_line_bytes(
            b"x",
            b"NaN",
            &comparator(SortMode::GeneralNumeric),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_line_bytes(
            b"1KiB",
            b"1024K",
            &comparator(SortMode::HumanNumeric),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_line_bytes(b"JAN", b"Jan", &comparator(SortMode::Month), false, false),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_line_bytes(b"v01", b"v1", &comparator(SortMode::Version), false, false),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_line_bytes(
            b"beta",
            b"alpha",
            &comparator(SortMode::Bytewise),
            false,
            true
        ),
        std::cmp::Ordering::Less
    );
}

#[test]
fn stable_sort_preserves_input_order_for_equal_keys() {
    let input = [
        b"2 b".as_slice(),
        b"1 c".as_slice(),
        b"2 a".as_slice(),
        b"2 d".as_slice(),
    ];
    let (storage, mut refs) = refs_for_lines(&input);
    let mut comparator = stable_comparator(SortMode::Bytewise);
    comparator.keys = vec![SortKeySpec {
        start: SortKeyPosition {
            field: 1,
            char_offset: 0,
        },
        end: SortKeyEnd::FieldEnd { field: 1 },
    }];
    finalize_sorted_lines(&mut refs, &storage, &comparator, false, false).unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"1 c".to_vec(),
            b"2 b".to_vec(),
            b"2 a".to_vec(),
            b"2 d".to_vec(),
        ]
    );
}

#[test]
fn stable_compare_line_bytes_suppresses_last_resort_tie_break() {
    assert_eq!(
        compare_line_bytes(
            b"2 b",
            b"2 a",
            &stable_comparator(SortMode::Numeric),
            false,
            false,
        ),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_output_lines(
            b"2 b",
            0,
            b"2 a",
            1,
            &stable_comparator(SortMode::Numeric),
            false,
            true,
        ),
        std::cmp::Ordering::Less
    );
}

#[test]
fn ignore_case_bytewise_uses_case_folded_primary_order() {
    assert_eq!(
        compare_line_bytes(
            b"alpha",
            b"ALPHA",
            &ignore_case_comparator(SortMode::Bytewise),
            false,
            false,
        ),
        std::cmp::Ordering::Greater
    );
    let input = [b"b".as_slice(), b"A".as_slice(), b"a".as_slice()];
    let (storage, mut refs) = refs_for_lines(&input);
    sort_line_refs(
        &mut refs,
        &storage,
        &ignore_case_comparator(SortMode::Bytewise),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"A".to_vec(), b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn ignore_leading_blanks_trims_default_bytewise_keys_only() {
    assert_eq!(
        compare_sort_keys(
            b"  alpha",
            b"alpha",
            &ignore_leading_blanks_comparator(SortMode::Bytewise),
        ),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        compare_line_bytes(
            b"  alpha",
            b"alpha",
            &ignore_leading_blanks_comparator(SortMode::Bytewise),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_output_lines(
            b"  alpha",
            0,
            b"alpha",
            1,
            &ignore_leading_blanks_comparator(SortMode::Bytewise),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
}

#[test]
fn ignore_leading_blanks_shifts_key_char_positions() {
    let input = [b"aa   bc".as_slice(), b"aa  ad".as_slice()];
    let (storage, mut refs) = refs_for_lines(&input);
    let mut comparator = ignore_leading_blanks_comparator(SortMode::Bytewise);
    comparator.keys = vec![parse_sort_key_spec("2.1,2.1").unwrap()];
    finalize_sorted_lines(&mut refs, &storage, &comparator, false, false).unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"aa  ad".to_vec(), b"aa   bc".to_vec()]
    );
}

#[test]
fn dictionary_order_filters_non_dictionary_bytes_before_bytewise_compare() {
    assert_eq!(
        compare_line_bytes(
            b"a-1",
            b"a1",
            &dictionary_order_comparator(SortMode::Bytewise),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
    assert!(same_sort_key(
        b"a-1",
        b"a1",
        &dictionary_order_comparator(SortMode::Bytewise)
    ));
}

#[test]
fn dictionary_order_combines_with_keys_and_version_mode() {
    let mut keyed = dictionary_order_comparator(SortMode::Bytewise);
    keyed.keys = vec![parse_sort_key_spec("2,2").unwrap()];
    assert_eq!(
        compare_line_bytes(b"x a-1", b"x a1", &keyed, false, false),
        std::cmp::Ordering::Less
    );
    assert!(same_sort_key(b"x a-1", b"x a1", &keyed));

    assert_eq!(
        compare_line_bytes(
            b"v1-1",
            b"v11",
            &dictionary_order_comparator(SortMode::Version),
            false,
            false,
        ),
        std::cmp::Ordering::Less
    );
}

#[test]
fn parse_sort_key_spec_supports_bounded_field_ranges() {
    assert_eq!(
        parse_sort_key_spec("2.3,4.5").unwrap(),
        SortKeySpec {
            start: SortKeyPosition {
                field: 2,
                char_offset: 2,
            },
            end: SortKeyEnd::Char {
                field: 4,
                char_end: 5,
            },
        }
    );
    assert_eq!(
        parse_sort_key_spec("3,3.0").unwrap(),
        SortKeySpec {
            start: SortKeyPosition {
                field: 3,
                char_offset: 0,
            },
            end: SortKeyEnd::FieldEnd { field: 3 },
        }
    );
    assert!(parse_sort_key_spec("0,1").is_err());
    assert!(parse_sort_key_spec("1.0,1").is_err());
    assert!(parse_sort_key_spec("1b,1").is_err());
}

#[test]
fn parse_field_separator_accepts_one_byte_and_rejects_invalid_values() {
    assert_eq!(parse_field_separator(":").unwrap(), b':');
    assert_eq!(parse_field_separator("\0").unwrap(), b'\0');
    assert_eq!(
        parse_field_separator("").unwrap_err().to_string(),
        "empty tab"
    );
    assert_eq!(
        parse_field_separator("ab").unwrap_err().to_string(),
        "multi-character tab 'ab'"
    );
}

#[test]
fn parse_sort_parallel_accepts_positive_counts_and_rejects_invalid_values() {
    assert_eq!(parse_sort_parallel("1").unwrap(), 1);
    assert_eq!(parse_sort_parallel("8").unwrap(), 8);
    assert_eq!(
        parse_sort_parallel("").unwrap_err().to_string(),
        "invalid --parallel argument ''"
    );
    assert_eq!(
        parse_sort_parallel("0").unwrap_err().to_string(),
        "number in parallel must be nonzero"
    );
    assert_eq!(
        parse_sort_parallel("bad").unwrap_err().to_string(),
        "invalid --parallel argument 'bad'"
    );
}

#[test]
fn parse_sort_batch_size_accepts_two_or_more_and_rejects_invalid_values() {
    assert_eq!(parse_sort_batch_size("2").unwrap(), 2);
    assert_eq!(parse_sort_batch_size("8").unwrap(), 8);
    assert_eq!(
        parse_sort_batch_size("").unwrap_err(),
        SortBatchSizeParseError::InvalidArgument(String::new())
    );
    assert_eq!(
        parse_sort_batch_size("bad").unwrap_err(),
        SortBatchSizeParseError::InvalidArgument("bad".to_string())
    );
    assert_eq!(
        parse_sort_batch_size("1").unwrap_err(),
        SortBatchSizeParseError::TooSmall("1".to_string())
    );
    assert_eq!(
        parse_sort_batch_size("0").unwrap_err(),
        SortBatchSizeParseError::TooSmall("0".to_string())
    );
}

#[test]
fn field_separator_keys_include_empty_fields_and_exact_byte_splits() {
    let input = [b"a::2".as_slice(), b"a:1:0".as_slice(), b":a:1".as_slice()];
    let (storage, mut refs) = refs_for_lines(&input);
    let mut comparator = field_separator_comparator(SortMode::Bytewise, b':');
    comparator.keys = vec![parse_sort_key_spec("2,2").unwrap()];
    finalize_sorted_lines(&mut refs, &storage, &comparator, false, false).unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"a::2".to_vec(), b"a:1:0".to_vec(), b":a:1".to_vec()]
    );
}

#[test]
fn field_separator_combines_with_numeric_and_ignore_leading_blanks_keys() {
    let mut numeric = field_separator_comparator(SortMode::Numeric, b':');
    numeric.keys = vec![parse_sort_key_spec("2,2").unwrap()];
    assert_eq!(
        compare_line_bytes(b"row: 2:a", b"row:10:b", &numeric, false, false),
        std::cmp::Ordering::Less
    );

    let input = [b"row:  bc:x".as_slice(), b"row: ad:y".as_slice()];
    let (storage, mut refs) = refs_for_lines(&input);
    let mut blanks = field_separator_comparator(SortMode::Bytewise, b':');
    blanks.ignore_leading_blanks = true;
    blanks.keys = vec![parse_sort_key_spec("2.1,2.1").unwrap()];
    finalize_sorted_lines(&mut refs, &storage, &blanks, false, false).unwrap();
    assert_eq!(
        refs.iter()
            .map(|line| line.bytes(&storage).to_vec())
            .collect::<Vec<_>>(),
        vec![b"row: ad:y".to_vec(), b"row:  bc:x".to_vec()]
    );
}
