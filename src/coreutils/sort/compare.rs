use super::{SortComparator, SortKeyEnd, SortKeySpec, SortLineRef, SortMode};
use openssl::hash::{Hasher, MessageDigest};
use std::borrow::Cow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SortDebugKeySelection {
    pub(super) offset: usize,
    pub(super) visual_len: usize,
    pub(super) matched: bool,
    pub(super) preserve_offsets: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NumericPrefix<'a> {
    negative: bool,
    int_digits: &'a [u8],
    frac_digits: &'a [u8],
}

impl<'a> NumericPrefix<'a> {
    fn is_zero(self) -> bool {
        self.int_digits.is_empty() && self.frac_digits.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GeneralNaNSign {
    ExplicitPlus,
    None,
    Negative,
}

#[derive(Clone, Copy, Debug)]
enum GeneralNumericPrefix<'a> {
    Invalid,
    NaN {
        sign: GeneralNaNSign,
        token: &'a [u8],
    },
    Number(f64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HumanNumericPrefix<'a> {
    negative: bool,
    suffix_rank: u8,
    number: NumericPrefix<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MonthPrefix {
    Invalid,
    Month(u8),
}

fn trim_leading_zeros(bytes: &[u8]) -> &[u8] {
    let first_non_zero = bytes
        .iter()
        .position(|&byte| byte != b'0')
        .unwrap_or(bytes.len());
    &bytes[first_non_zero..]
}

fn trim_trailing_zeros(bytes: &[u8]) -> &[u8] {
    let last_non_zero = bytes
        .iter()
        .rposition(|&byte| byte != b'0')
        .map(|idx| idx + 1)
        .unwrap_or(0);
    &bytes[..last_non_zero]
}

fn trim_leading_blanks(bytes: &[u8]) -> &[u8] {
    let first_non_blank = bytes
        .iter()
        .position(|&byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(bytes.len());
    &bytes[first_non_blank..]
}

fn selected_sort_key<'a>(bytes: &'a [u8], comparator: &SortComparator) -> &'a [u8] {
    if comparator.ignore_leading_blanks {
        trim_leading_blanks(bytes)
    } else {
        bytes
    }
}

fn is_dictionary_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b' ' | b'\t')
}

fn is_printable_byte(byte: u8) -> bool {
    byte == b' ' || byte.is_ascii_graphic()
}

fn normalized_sort_key<'a>(bytes: &'a [u8], comparator: &SortComparator) -> Cow<'a, [u8]> {
    let selected = selected_sort_key(bytes, comparator);
    if !comparator.dictionary_order && !comparator.ignore_nonprinting {
        return Cow::Borrowed(selected);
    }

    let mut normalized = Vec::with_capacity(selected.len());
    for &byte in selected {
        if comparator.ignore_nonprinting && !is_printable_byte(byte) {
            continue;
        }
        if comparator.dictionary_order && !is_dictionary_byte(byte) {
            continue;
        }
        normalized.push(byte);
    }
    Cow::Owned(normalized)
}

fn random_sort_key<'a>(bytes: &'a [u8], comparator: &SortComparator) -> Cow<'a, [u8]> {
    let selected = selected_sort_key(bytes, comparator);
    if !comparator.dictionary_order && !comparator.ignore_nonprinting && !comparator.ignore_case {
        return Cow::Borrowed(selected);
    }

    let mut normalized = Vec::with_capacity(selected.len());
    for &byte in selected {
        if comparator.ignore_nonprinting && !is_printable_byte(byte) {
            continue;
        }
        if comparator.dictionary_order && !is_dictionary_byte(byte) {
            continue;
        }
        normalized.push(if comparator.ignore_case {
            byte.to_ascii_uppercase()
        } else {
            byte
        });
    }
    Cow::Owned(normalized)
}

fn random_digest(bytes: &[u8], comparator: &SortComparator) -> [u8; 16] {
    let mut hasher = Hasher::new(MessageDigest::md5()).expect("md5 hasher");
    hasher
        .update(&comparator.random_seed)
        .expect("md5 seed update");
    hasher.update(bytes).expect("md5 data update");
    let digest = hasher.finish().expect("md5 finish");
    let mut out = [0u8; 16];
    out.copy_from_slice(digest.as_ref());
    out
}
fn compare_lowercase_preferring_lower(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let max_len = left.len().max(right.len());
    for idx in 0..max_len {
        let Some(&left_byte) = left.get(idx) else {
            return std::cmp::Ordering::Less;
        };
        let Some(&right_byte) = right.get(idx) else {
            return std::cmp::Ordering::Greater;
        };
        let left_fold = left_byte.to_ascii_lowercase();
        let right_fold = right_byte.to_ascii_lowercase();
        match left_fold.cmp(&right_fold) {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
        match (
            left_byte.is_ascii_lowercase(),
            right_byte.is_ascii_lowercase(),
        ) {
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            _ => {}
        }
        match left_byte.cmp(&right_byte) {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_ascii_folded(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let max_len = left.len().max(right.len());
    for idx in 0..max_len {
        let Some(&left_byte) = left.get(idx) else {
            return std::cmp::Ordering::Less;
        };
        let Some(&right_byte) = right.get(idx) else {
            return std::cmp::Ordering::Greater;
        };
        match left_byte
            .to_ascii_uppercase()
            .cmp(&right_byte.to_ascii_uppercase())
        {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}

include!("compare/numeric.rs");
include!("compare/version.rs");
include!("compare/key.rs");
pub(super) fn compare_sort_keys(
    left: &[u8],
    right: &[u8],
    comparator: &SortComparator,
) -> std::cmp::Ordering {
    let left_key = normalized_sort_key(left, comparator);
    let right_key = normalized_sort_key(right, comparator);
    let left = left_key.as_ref();
    let right = right_key.as_ref();
    match comparator.mode {
        SortMode::Bytewise if comparator.ignore_case => compare_ascii_folded(left, right),
        SortMode::Bytewise => left.cmp(right),
        SortMode::Numeric => compare_numeric_lines(left, right),
        SortMode::GeneralNumeric => {
            compare_general_numeric_lines(left, right, comparator.ignore_case)
        }
        SortMode::HumanNumeric => compare_human_numeric_lines(left, right),
        SortMode::Month => compare_month_lines(left, right),
        SortMode::Random => {
            let left_key = random_sort_key(left, comparator);
            let right_key = random_sort_key(right, comparator);
            let left = left_key.as_ref();
            let right = right_key.as_ref();
            random_digest(left, comparator)
                .cmp(&random_digest(right, comparator))
                .then_with(|| left.cmp(right))
        }
        SortMode::Version => compare_version_lines(left, right, comparator.ignore_case),
    }
}

fn same_single_sort_key(left: &[u8], right: &[u8], comparator: &SortComparator) -> bool {
    let left_key = normalized_sort_key(left, comparator);
    let right_key = normalized_sort_key(right, comparator);
    let left = left_key.as_ref();
    let right = right_key.as_ref();
    match comparator.mode {
        SortMode::Bytewise if comparator.ignore_case => compare_ascii_folded(left, right).is_eq(),
        SortMode::Bytewise => left == right,
        SortMode::Numeric => compare_numeric_lines(left, right) == std::cmp::Ordering::Equal,
        SortMode::GeneralNumeric => match (
            parse_general_numeric_prefix(left),
            parse_general_numeric_prefix(right),
        ) {
            (GeneralNumericPrefix::Invalid, GeneralNumericPrefix::Invalid) => left == right,
            (
                GeneralNumericPrefix::Number(left_value),
                GeneralNumericPrefix::Number(right_value),
            ) => left_value == right_value,
            _ => false,
        },
        SortMode::HumanNumeric => match (
            parse_human_numeric_prefix(left),
            parse_human_numeric_prefix(right),
        ) {
            (None, None) => true,
            (None, Some(right_key)) | (Some(right_key), None) => right_key.number.is_zero(),
            (Some(left_key), Some(right_key)) => {
                if left_key.number.is_zero() && right_key.number.is_zero() {
                    true
                } else {
                    left_key.negative == right_key.negative
                        && left_key.suffix_rank == right_key.suffix_rank
                        && compare_numeric_prefixes(left_key.number, right_key.number)
                            == std::cmp::Ordering::Equal
                }
            }
        },
        SortMode::Month => match (parse_month_prefix(left), parse_month_prefix(right)) {
            (MonthPrefix::Invalid, MonthPrefix::Invalid) => true,
            (MonthPrefix::Month(left_month), MonthPrefix::Month(right_month)) => {
                left_month == right_month
            }
            _ => false,
        },
        SortMode::Random => random_sort_key(left, comparator) == random_sort_key(right, comparator),
        SortMode::Version => {
            compare_version_lines(left, right, comparator.ignore_case) == std::cmp::Ordering::Equal
        }
    }
}
