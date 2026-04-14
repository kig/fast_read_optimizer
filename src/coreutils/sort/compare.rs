use super::{SortComparator, SortKeyEnd, SortKeySpec, SortLineRef, SortMode};

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

fn parse_numeric_prefix(line: &[u8]) -> NumericPrefix<'_> {
    let line = trim_leading_blanks(line);
    let mut idx = 0usize;

    let negative = idx < line.len() && line[idx] == b'-';
    if negative {
        idx += 1;
    }

    let int_start = idx;
    while idx < line.len() && line[idx].is_ascii_digit() {
        idx += 1;
    }
    let int_digits = &line[int_start..idx];

    let frac_digits = if idx < line.len() && line[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < line.len() && line[idx].is_ascii_digit() {
            idx += 1;
        }
        &line[frac_start..idx]
    } else {
        &[]
    };

    let has_digits = !int_digits.is_empty() || !frac_digits.is_empty();
    if !has_digits {
        return NumericPrefix {
            negative: false,
            int_digits: &[],
            frac_digits: &[],
        };
    }

    NumericPrefix {
        negative,
        int_digits: trim_leading_zeros(int_digits),
        frac_digits: trim_trailing_zeros(frac_digits),
    }
}

fn compare_numeric_magnitude(
    left: NumericPrefix<'_>,
    right: NumericPrefix<'_>,
) -> std::cmp::Ordering {
    left.int_digits
        .len()
        .cmp(&right.int_digits.len())
        .then_with(|| left.int_digits.cmp(right.int_digits))
        .then_with(|| {
            let max_frac_len = left.frac_digits.len().max(right.frac_digits.len());
            for idx in 0..max_frac_len {
                let left_digit = left.frac_digits.get(idx).copied().unwrap_or(b'0');
                let right_digit = right.frac_digits.get(idx).copied().unwrap_or(b'0');
                match left_digit.cmp(&right_digit) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        })
}

fn compare_numeric_prefixes(
    left: NumericPrefix<'_>,
    right: NumericPrefix<'_>,
) -> std::cmp::Ordering {
    if left.is_zero() && right.is_zero() {
        return std::cmp::Ordering::Equal;
    }
    match (left.negative, right.negative) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => compare_numeric_magnitude(left, right),
        (true, true) => compare_numeric_magnitude(right, left),
    }
}

fn compare_numeric_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    compare_numeric_prefixes(parse_numeric_prefix(left), parse_numeric_prefix(right))
}

fn parse_general_numeric_prefix(line: &[u8]) -> GeneralNumericPrefix<'_> {
    let trimmed = trim_leading_blanks(line);
    if trimmed.is_empty() {
        return GeneralNumericPrefix::Invalid;
    }

    let nul_cutoff = trimmed
        .iter()
        .position(|&byte| byte == b'\0')
        .unwrap_or(trimmed.len());
    let mut buffer = Vec::with_capacity(nul_cutoff + 1);
    buffer.extend_from_slice(&trimmed[..nul_cutoff]);
    buffer.push(0);

    let start = buffer.as_ptr() as *const libc::c_char;
    let mut end = std::ptr::null_mut();
    let value = unsafe { libc::strtod(start, &mut end) };
    if std::ptr::eq(end as *const libc::c_char, start) {
        return GeneralNumericPrefix::Invalid;
    }
    let consumed = unsafe { end.offset_from(start) as usize }.min(nul_cutoff);
    if value.is_nan() {
        let sign = match trimmed.first().copied() {
            Some(b'+') => GeneralNaNSign::ExplicitPlus,
            Some(b'-') => GeneralNaNSign::Negative,
            _ => GeneralNaNSign::None,
        };
        return GeneralNumericPrefix::NaN {
            sign,
            token: &trimmed[..consumed],
        };
    }
    GeneralNumericPrefix::Number(value)
}

fn human_suffix_rank(byte: u8) -> Option<u8> {
    Some(match byte {
        b'k' | b'K' => 1,
        b'm' | b'M' => 2,
        b'g' | b'G' => 3,
        b't' | b'T' => 4,
        b'p' | b'P' => 5,
        b'e' | b'E' => 6,
        b'z' | b'Z' => 7,
        b'y' | b'Y' => 8,
        _ => return None,
    })
}

fn parse_human_numeric_prefix(line: &[u8]) -> Option<HumanNumericPrefix<'_>> {
    let line = trim_leading_blanks(line);
    let mut idx = 0usize;
    if matches!(line.first(), Some(b'+')) {
        return None;
    }

    let negative = idx < line.len() && line[idx] == b'-';
    if negative {
        idx += 1;
    }

    let int_start = idx;
    while idx < line.len() && line[idx].is_ascii_digit() {
        idx += 1;
    }
    let int_digits = &line[int_start..idx];

    let frac_digits = if idx < line.len() && line[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < line.len() && line[idx].is_ascii_digit() {
            idx += 1;
        }
        &line[frac_start..idx]
    } else {
        &[]
    };

    if int_digits.is_empty() && frac_digits.is_empty() {
        return None;
    }

    let suffix_rank = line
        .get(idx)
        .copied()
        .and_then(human_suffix_rank)
        .unwrap_or(0);

    Some(HumanNumericPrefix {
        negative,
        suffix_rank,
        number: NumericPrefix {
            negative,
            int_digits: trim_leading_zeros(int_digits),
            frac_digits: trim_trailing_zeros(frac_digits),
        },
    })
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

fn compare_general_numeric_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    match (
        parse_general_numeric_prefix(left),
        parse_general_numeric_prefix(right),
    ) {
        (GeneralNumericPrefix::Invalid, GeneralNumericPrefix::Invalid) => std::cmp::Ordering::Equal,
        (GeneralNumericPrefix::Invalid, _) => std::cmp::Ordering::Less,
        (_, GeneralNumericPrefix::Invalid) => std::cmp::Ordering::Greater,
        (
            GeneralNumericPrefix::NaN {
                sign: left_sign,
                token: left_token,
            },
            GeneralNumericPrefix::NaN {
                sign: right_sign,
                token: right_token,
            },
        ) => left_sign
            .cmp(&right_sign)
            .then_with(|| compare_lowercase_preferring_lower(left_token, right_token)),
        (GeneralNumericPrefix::NaN { .. }, GeneralNumericPrefix::Number(_)) => {
            std::cmp::Ordering::Less
        }
        (GeneralNumericPrefix::Number(_), GeneralNumericPrefix::NaN { .. }) => {
            std::cmp::Ordering::Greater
        }
        (GeneralNumericPrefix::Number(left_value), GeneralNumericPrefix::Number(right_value)) => {
            left_value
                .partial_cmp(&right_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
    }
}

fn compare_human_numeric_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let invalid_vs_valid = |valid: HumanNumericPrefix<'_>| {
        if valid.number.is_zero() {
            std::cmp::Ordering::Equal
        } else if valid.negative {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        }
    };
    match (
        parse_human_numeric_prefix(left),
        parse_human_numeric_prefix(right),
    ) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(valid)) => invalid_vs_valid(valid),
        (Some(valid), None) => invalid_vs_valid(valid).reverse(),
        (Some(left_key), Some(right_key)) => match (left_key.negative, right_key.negative) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => left_key
                .suffix_rank
                .cmp(&right_key.suffix_rank)
                .then_with(|| compare_numeric_prefixes(left_key.number, right_key.number)),
            (true, true) => right_key
                .suffix_rank
                .cmp(&left_key.suffix_rank)
                .then_with(|| compare_numeric_prefixes(left_key.number, right_key.number)),
        },
    }
}

fn parse_month_prefix(line: &[u8]) -> MonthPrefix {
    let trimmed = trim_leading_blanks(line);
    let Some(prefix) = trimmed.get(..3) else {
        return MonthPrefix::Invalid;
    };
    let month = if prefix.eq_ignore_ascii_case(b"jan") {
        0
    } else if prefix.eq_ignore_ascii_case(b"feb") {
        1
    } else if prefix.eq_ignore_ascii_case(b"mar") {
        2
    } else if prefix.eq_ignore_ascii_case(b"apr") {
        3
    } else if prefix.eq_ignore_ascii_case(b"may") {
        4
    } else if prefix.eq_ignore_ascii_case(b"jun") {
        5
    } else if prefix.eq_ignore_ascii_case(b"jul") {
        6
    } else if prefix.eq_ignore_ascii_case(b"aug") {
        7
    } else if prefix.eq_ignore_ascii_case(b"sep") {
        8
    } else if prefix.eq_ignore_ascii_case(b"oct") {
        9
    } else if prefix.eq_ignore_ascii_case(b"nov") {
        10
    } else if prefix.eq_ignore_ascii_case(b"dec") {
        11
    } else {
        return MonthPrefix::Invalid;
    };
    MonthPrefix::Month(month)
}

fn compare_month_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    match (parse_month_prefix(left), parse_month_prefix(right)) {
        (MonthPrefix::Invalid, MonthPrefix::Invalid) => std::cmp::Ordering::Equal,
        (MonthPrefix::Invalid, MonthPrefix::Month(_)) => std::cmp::Ordering::Less,
        (MonthPrefix::Month(_), MonthPrefix::Invalid) => std::cmp::Ordering::Greater,
        (MonthPrefix::Month(left_month), MonthPrefix::Month(right_month)) => {
            left_month.cmp(&right_month)
        }
    }
}

fn version_order(byte: u8) -> i32 {
    if byte.is_ascii_digit() {
        0
    } else if byte.is_ascii_alphabetic() {
        i32::from(byte)
    } else if byte == b'~' {
        -1
    } else {
        i32::from(byte) + i32::from(u8::MAX) + 1
    }
}

fn version_digit(bytes: &[u8], idx: usize) -> bool {
    bytes
        .get(idx)
        .copied()
        .is_some_and(|byte| byte.is_ascii_digit())
}

fn compare_version_core(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let mut left_pos = 0usize;
    let mut right_pos = 0usize;

    while left_pos < left.len() || right_pos < right.len() {
        let mut first_digit_diff = 0i32;

        while (left_pos < left.len() && !version_digit(left, left_pos))
            || (right_pos < right.len() && !version_digit(right, right_pos))
        {
            let left_order = if left_pos < left.len() {
                version_order(left[left_pos])
            } else {
                0
            };
            let right_order = if right_pos < right.len() {
                version_order(right[right_pos])
            } else {
                0
            };
            match left_order.cmp(&right_order) {
                std::cmp::Ordering::Equal => {
                    left_pos += 1;
                    right_pos += 1;
                }
                other => return other,
            }
        }

        while left_pos < left.len() && left[left_pos] == b'0' {
            left_pos += 1;
        }
        while right_pos < right.len() && right[right_pos] == b'0' {
            right_pos += 1;
        }

        while version_digit(left, left_pos) && version_digit(right, right_pos) {
            if first_digit_diff == 0 {
                first_digit_diff = i32::from(left[left_pos]) - i32::from(right[right_pos]);
            }
            left_pos += 1;
            right_pos += 1;
        }

        if version_digit(left, left_pos) {
            return std::cmp::Ordering::Greater;
        }
        if version_digit(right, right_pos) {
            return std::cmp::Ordering::Less;
        }
        if first_digit_diff != 0 {
            return first_digit_diff.cmp(&0);
        }
    }

    std::cmp::Ordering::Equal
}

fn version_suffix_start(bytes: &[u8]) -> Option<usize> {
    let mut suffix_start = None;
    let mut expecting_alpha = false;
    for (idx, &byte) in bytes.iter().enumerate() {
        if expecting_alpha {
            expecting_alpha = false;
            if !(byte.is_ascii_alphabetic() || byte == b'~') {
                suffix_start = None;
            }
        } else if byte == b'.' {
            expecting_alpha = true;
            suffix_start.get_or_insert(idx);
        } else if !(byte.is_ascii_alphanumeric() || byte == b'~') {
            suffix_start = None;
        }
    }
    suffix_start
}

fn compare_version_lines(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    if left == right {
        return std::cmp::Ordering::Equal;
    }
    if left.is_empty() {
        return std::cmp::Ordering::Less;
    }
    if right.is_empty() {
        return std::cmp::Ordering::Greater;
    }
    if left == b"." {
        return std::cmp::Ordering::Less;
    }
    if right == b"." {
        return std::cmp::Ordering::Greater;
    }
    if left == b".." {
        return std::cmp::Ordering::Less;
    }
    if right == b".." {
        return std::cmp::Ordering::Greater;
    }
    if left[0] == b'.' && right[0] != b'.' {
        return std::cmp::Ordering::Less;
    }
    if left[0] != b'.' && right[0] == b'.' {
        return std::cmp::Ordering::Greater;
    }

    let (left_body, right_body) = if left[0] == b'.' && right[0] == b'.' {
        (&left[1..], &right[1..])
    } else {
        (left, right)
    };

    let mut left_len = version_suffix_start(left_body).unwrap_or(left_body.len());
    let mut right_len = version_suffix_start(right_body).unwrap_or(right_body.len());
    if (left_len != left_body.len() || right_len != right_body.len())
        && left_len == right_len
        && left_body[..left_len] == right_body[..right_len]
    {
        left_len = left_body.len();
        right_len = right_body.len();
    }

    let primary = compare_version_core(&left_body[..left_len], &right_body[..right_len]);
    if primary == std::cmp::Ordering::Equal {
        left.cmp(right)
    } else {
        primary
    }
}

pub(super) fn compare_sort_keys(left: &[u8], right: &[u8], mode: SortMode) -> std::cmp::Ordering {
    match mode {
        SortMode::Bytewise => left.cmp(right),
        SortMode::Numeric => compare_numeric_lines(left, right),
        SortMode::GeneralNumeric => compare_general_numeric_lines(left, right),
        SortMode::HumanNumeric => compare_human_numeric_lines(left, right),
        SortMode::Month => compare_month_lines(left, right),
        SortMode::Version => compare_version_lines(left, right),
    }
}

fn same_single_sort_key(left: &[u8], right: &[u8], mode: SortMode) -> bool {
    match mode {
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
        SortMode::Version => compare_version_lines(left, right) == std::cmp::Ordering::Equal,
    }
}

fn is_blank(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
}

fn nth_field_bounds(line: &[u8], field: usize) -> Option<(usize, usize)> {
    let mut idx = 0usize;
    let mut seen = 0usize;
    while idx < line.len() {
        let start = idx;
        while idx < line.len() && is_blank(line[idx]) {
            idx += 1;
        }
        if idx == line.len() {
            break;
        }
        while idx < line.len() && !is_blank(line[idx]) {
            idx += 1;
        }
        seen += 1;
        if seen == field {
            return Some((start, idx));
        }
    }
    None
}

fn key_range(line: &[u8], key: &SortKeySpec) -> (usize, usize) {
    let Some((start_field, start_field_end)) = nth_field_bounds(line, key.start.field) else {
        return (line.len(), line.len());
    };
    let start = (start_field + key.start.char_offset).min(start_field_end);
    let end = match key.end {
        SortKeyEnd::EndOfLine => line.len(),
        SortKeyEnd::FieldEnd { field } => nth_field_bounds(line, field)
            .map(|(_, field_end)| field_end)
            .unwrap_or(line.len()),
        SortKeyEnd::Char { field, char_end } => nth_field_bounds(line, field)
            .map(|(field_start, field_end)| (field_start + char_end).min(field_end))
            .unwrap_or(line.len()),
    };
    (start, end.max(start))
}

fn key_slice<'a>(line: &'a [u8], key: &SortKeySpec) -> &'a [u8] {
    let (start, end) = key_range(line, key);
    &line[start..end]
}

fn compare_selected_keys(
    left: &[u8],
    right: &[u8],
    comparator: &SortComparator,
) -> std::cmp::Ordering {
    if comparator.keys.is_empty() {
        return compare_sort_keys(left, right, comparator.mode);
    }
    for key in &comparator.keys {
        let order = compare_sort_keys(key_slice(left, key), key_slice(right, key), comparator.mode);
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

fn same_selected_keys(left: &[u8], right: &[u8], comparator: &SortComparator) -> bool {
    if comparator.keys.is_empty() {
        return same_single_sort_key(left, right, comparator.mode);
    }
    comparator.keys.iter().all(|key| {
        same_single_sort_key(key_slice(left, key), key_slice(right, key), comparator.mode)
    })
}

pub(super) fn compare_line_refs(
    left: SortLineRef,
    right: SortLineRef,
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
) -> std::cmp::Ordering {
    let key_order = compare_selected_keys(left.bytes(storage), right.bytes(storage), comparator);
    if key_order != std::cmp::Ordering::Equal {
        return key_order;
    }
    if unique {
        left.sequence.cmp(&right.sequence)
    } else {
        left.bytes(storage)
            .cmp(right.bytes(storage))
            .then_with(|| left.sequence.cmp(&right.sequence))
    }
}

pub(super) fn compare_output_lines(
    left: &[u8],
    left_sequence: u64,
    right: &[u8],
    right_sequence: u64,
    comparator: &SortComparator,
    _unique: bool,
    reverse: bool,
) -> std::cmp::Ordering {
    let asc = compare_line_bytes(left, right, comparator, false)
        .then_with(|| left_sequence.cmp(&right_sequence));
    if reverse {
        asc.reverse()
    } else {
        asc
    }
}

pub(super) fn compare_line_bytes(
    left: &[u8],
    right: &[u8],
    comparator: &SortComparator,
    reverse: bool,
) -> std::cmp::Ordering {
    let asc = match compare_selected_keys(left, right, comparator) {
        std::cmp::Ordering::Equal
            if comparator.mode != SortMode::Bytewise || !comparator.keys.is_empty() =>
        {
            left.cmp(right)
        }
        other => other,
    };
    if reverse {
        asc.reverse()
    } else {
        asc
    }
}

pub(super) fn same_sort_key(left: &[u8], right: &[u8], comparator: &SortComparator) -> bool {
    same_selected_keys(left, right, comparator)
}
