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
fn compare_general_numeric_lines(
    left: &[u8],
    right: &[u8],
    ignore_case: bool,
) -> std::cmp::Ordering {
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
        ) => left_sign.cmp(&right_sign).then_with(|| {
            if ignore_case {
                compare_ascii_folded(left_token, right_token)
            } else {
                compare_lowercase_preferring_lower(left_token, right_token)
            }
        }),
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
