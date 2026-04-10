use super::*;
use std::io::Write;
use std::path::Path;
use stringzilla::stringzilla as sz;

mod external;

const SORT_WRITE_BUFFER_SIZE: usize = 2 << 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SortLineRef {
    start: usize,
    len: usize,
    sequence: u64,
}

impl SortLineRef {
    fn bytes<'a>(self, storage: &'a [u8]) -> &'a [u8] {
        &storage[self.start..self.start + self.len]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortMode {
    Bytewise,
    Numeric,
    GeneralNumeric,
    HumanNumeric,
    Month,
    Version,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RecordTerminator {
    #[default]
    Newline,
    Nul,
}

impl RecordTerminator {
    fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
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

fn compare_sort_keys(left: &[u8], right: &[u8], mode: SortMode) -> std::cmp::Ordering {
    match mode {
        SortMode::Bytewise => left.cmp(right),
        SortMode::Numeric => compare_numeric_lines(left, right),
        SortMode::GeneralNumeric => compare_general_numeric_lines(left, right),
        SortMode::HumanNumeric => compare_human_numeric_lines(left, right),
        SortMode::Month => compare_month_lines(left, right),
        SortMode::Version => compare_version_lines(left, right),
    }
}

fn compare_line_refs(
    left: SortLineRef,
    right: SortLineRef,
    storage: &[u8],
    mode: SortMode,
    unique: bool,
) -> std::cmp::Ordering {
    match mode {
        SortMode::Bytewise => left
            .bytes(storage)
            .cmp(right.bytes(storage))
            .then_with(|| left.sequence.cmp(&right.sequence)),
        SortMode::Numeric
        | SortMode::GeneralNumeric
        | SortMode::HumanNumeric
        | SortMode::Month
        | SortMode::Version => {
            let key_order = compare_sort_keys(left.bytes(storage), right.bytes(storage), mode);
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
    }
}

fn compare_output_lines(
    left: &[u8],
    left_sequence: u64,
    right: &[u8],
    right_sequence: u64,
    mode: SortMode,
    _unique: bool,
    reverse: bool,
) -> std::cmp::Ordering {
    let asc = compare_line_bytes(left, right, mode, false)
        .then_with(|| left_sequence.cmp(&right_sequence));
    if reverse {
        asc.reverse()
    } else {
        asc
    }
}

fn compare_line_bytes(
    left: &[u8],
    right: &[u8],
    mode: SortMode,
    reverse: bool,
) -> std::cmp::Ordering {
    let asc = match compare_sort_keys(left, right, mode) {
        std::cmp::Ordering::Equal if mode != SortMode::Bytewise => left.cmp(right),
        other => other,
    };
    if reverse {
        asc.reverse()
    } else {
        asc
    }
}

fn same_sort_key(left: &[u8], right: &[u8], mode: SortMode) -> bool {
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

fn print_sort_help(program: &str) {
    fro::cio_println!("sort - Sort newline-delimited records (or NUL-delimited with -z)");
    fro::cio_println!();
    fro::cio_println!("Usage: {program} [OPTION]... [FILE]...");
    fro::cio_println!();
    fro::cio_println!(
        "This bounded slice sorts locale-independent byte, numeric, general-numeric, human-numeric, month, or version records, using newlines by default and NULs with -z."
    );
    fro::cio_println!(
        "It currently supports the default case plus -g/-h/-M/-n/-V, -z, reverse/unique, merge/check, and -o output."
    );
    fro::cio_println!();
    fro::cio_println!("Supported options:");
    fro::cio_println!("  -c, --check          check whether one input is already sorted");
    fro::cio_println!("  -g, --general-numeric-sort");
    fro::cio_println!("                       compare leading C-locale floating-point prefixes");
    fro::cio_println!("  -h, --human-numeric-sort");
    fro::cio_println!("                       compare leading numbers grouped by human suffix family");
    fro::cio_println!("  -m, --merge          merge already sorted inputs without resorting");
    fro::cio_println!("  -M, --month-sort     compare leading month abbreviations like GNU sort -M");
    fro::cio_println!("  -n, --numeric-sort   compare leading numeric prefixes in C-locale style");
    fro::cio_println!("  -r, --reverse        reverse the result of comparisons");
    fro::cio_println!("  -u, --unique         output only the first of an equal run");
    fro::cio_println!("  -V, --version-sort   compare digit runs with GNU version-order semantics");
    fro::cio_println!("  -z, --zero-terminated  use NUL as the input and output record terminator");
    fro::cio_println!("  -o FILE              write result to FILE after reading all input");
    fro::cio_println!("      --output=FILE    same as -o FILE");
    fro::cio_println!("  -T DIR               write spill files under DIR when out-of-core merge is needed");
    fro::cio_println!("      --temporary-directory=DIR");
    fro::cio_println!("                       same as -T DIR");
    fro::cio_println!("      --auto           choose direct IO automatically for regular files");
    fro::cio_println!("      --direct         force direct IO for regular files when possible");
    fro::cio_println!("      --no-direct      force page-cache IO for regular files");
    fro::cio_println!("      --report-gbps    print aggregate input throughput to stderr");
    fro::cio_println!("      --help           display this help and exit");
    fro::cio_println!();
    fro::cio_println!("Notes:");
    fro::cio_println!("  - Use '-' once to read stdin.");
    fro::cio_println!("  - Use '--' before file names that start with '-'.");
    fro::cio_println!("  - Bytewise in-memory sorting uses a StringZilla argsort fast path.");
    fro::cio_println!("  - Inputs larger than available memory spill sorted runs and merge them.");
    fro::cio_println!("  - -T only matters when spill temp files are actually created.");
    fro::cio_println!("  - -m reuses the spill/merge backend on already sorted inputs.");
    fro::cio_println!("  - -c validates one input stream and exits 1 on the first disorder.");
    fro::cio_println!("  - -g uses C-locale strtod-style prefixes; NaNs sort after non-numbers and before infinities.");
    fro::cio_println!("  - -h compares the leading numeric prefix plus an optional K/M/G/T/P/E/Z/Y suffix family.");
    fro::cio_println!("  - -M looks at the first nonblank three-letter month abbreviation and treats other lines as invalid month keys.");
    fro::cio_println!("  - -V uses GNU/libc version-order comparisons while preserving the existing spill, merge, and check backend.");
    fro::cio_println!("  - Unsupported GNU sort features currently return an error:");
    fro::cio_println!("    key selection (-k) and locale collation.");
}

fn sort_input_label(input: &StreamInput) -> &str {
    match input {
        StreamInput::File(path) => path.as_str(),
        StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
    }
}

fn append_input_lines(
    storage: &mut Vec<u8>,
    lines: &mut Vec<SortLineRef>,
    input: &[u8],
    next_sequence: &mut u64,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let base = storage.len();
    storage.extend_from_slice(input);

    let mut line_start = 0usize;
    let terminator = terminator.byte();
    for (idx, &byte) in input.iter().enumerate() {
        if byte == terminator {
            lines.push(SortLineRef {
                start: base + line_start,
                len: idx - line_start,
                sequence: *next_sequence,
            });
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
            line_start = idx + 1;
        }
    }
    if line_start < input.len() {
        lines.push(SortLineRef {
            start: base + line_start,
            len: input.len() - line_start,
            sequence: *next_sequence,
        });
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
    }

    Ok(())
}

fn sort_line_refs(
    lines: &mut [SortLineRef],
    storage: &[u8],
    mode: SortMode,
    unique: bool,
) -> io::Result<()> {
    if lines.len() < 2 {
        return Ok(());
    }
    match mode {
        SortMode::Bytewise => {
            let snapshot = lines.to_vec();
            let mut order = vec![0 as sz::SortedIdx; snapshot.len()];
            sz::argsort_permutation_by(|idx| snapshot[idx].bytes(storage), &mut order).map_err(
                |status| io::Error::other(format!("StringZilla sort failed: {status:?}")),
            )?;
            for (dst, sorted_idx) in lines.iter_mut().zip(order.into_iter()) {
                *dst = snapshot[sorted_idx];
            }
        }
        SortMode::Numeric
        | SortMode::GeneralNumeric
        | SortMode::HumanNumeric
        | SortMode::Month
        | SortMode::Version => {
            lines.sort_unstable_by(|left, right| {
                compare_line_refs(*left, *right, storage, mode, unique)
            });
        }
    }
    Ok(())
}

fn finalize_sorted_lines(
    lines: &mut Vec<SortLineRef>,
    storage: &[u8],
    mode: SortMode,
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    sort_line_refs(lines, storage, mode, unique)?;
    if unique {
        lines
            .dedup_by(|left, right| same_sort_key(left.bytes(storage), right.bytes(storage), mode));
    }
    if reverse {
        lines.reverse();
    }
    Ok(())
}

fn apply_short_sort_flags(
    arg: &str,
    check: &mut bool,
    merge: &mut bool,
    mode: &mut SortMode,
    reverse: &mut bool,
    unique: &mut bool,
    terminator: &mut RecordTerminator,
) -> bool {
    if !arg.starts_with('-') || arg.starts_with("--") || arg == "-" {
        return false;
    }
    for flag in arg[1..].bytes() {
        match flag {
            b'c' => *check = true,
            b'g' => *mode = SortMode::GeneralNumeric,
            b'h' => *mode = SortMode::HumanNumeric,
            b'M' => *mode = SortMode::Month,
            b'm' => *merge = true,
            b'n' => *mode = SortMode::Numeric,
            b'r' => *reverse = true,
            b'u' => *unique = true,
            b'V' => *mode = SortMode::Version,
            b'z' => *terminator = RecordTerminator::Nul,
            _ => return false,
        }
    }
    true
}

fn write_sorted_lines<W: Write>(
    mut out: W,
    lines: &[SortLineRef],
    storage: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    for line in lines {
        buffered_write_sort_line(&mut out, &mut buffer, line.bytes(storage), terminator)?;
    }
    flush_sort_output_buffer(&mut out, &mut buffer)?;
    out.flush()
}

fn buffered_write_sort_line<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
    line: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let terminator = terminator.byte();
    if buffer.len() + line.len() + 1 > SORT_WRITE_BUFFER_SIZE && !buffer.is_empty() {
        flush_sort_output_buffer(out, buffer)?;
    }
    if line.len() + 1 >= SORT_WRITE_BUFFER_SIZE {
        out.write_all(line)?;
        out.write_all(&[terminator])?;
        return Ok(());
    }
    buffer.extend_from_slice(line);
    buffer.push(terminator);
    Ok(())
}

fn report_sort_disorder(
    label: &str,
    disorder: &external::SortCheckFailure,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut stderr = fro::command_io::stderr_buf_writer(4096)?;
    write!(stderr, "sort: {label}:{}: disorder: ", disorder.line_number)?;
    stderr.write_all(String::from_utf8_lossy(&disorder.line).as_bytes())?;
    if terminator == RecordTerminator::Nul {
        stderr.write_all(b"\0")
    } else {
        stderr.write_all(b"\n")
    }
}

fn flush_sort_output_buffer<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    if !buffer.is_empty() {
        out.write_all(buffer)?;
        buffer.clear();
    }
    Ok(())
}

pub(super) fn run_sort(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut check = false;
    let mut merge = false;
    let mut mode = SortMode::Bytewise;
    let mut reverse = false;
    let mut unique = false;
    let mut terminator = RecordTerminator::Newline;
    let mut output_path = None;
    let mut temporary_directory = None;
    let mut files = Vec::new();
    let mut end_flags = false;
    let mut idx = 1usize;

    while idx < args.len() {
        let arg = &args[idx];
        match arg.as_str() {
            "--" if !end_flags => end_flags = true,
            "--help" if !end_flags => {
                print_sort_help(args[0].as_str());
                return Ok(0);
            }
            "-c" | "--check" if !end_flags => check = true,
            "-g" | "--general-numeric-sort" if !end_flags => mode = SortMode::GeneralNumeric,
            "-h" | "--human-numeric-sort" if !end_flags => mode = SortMode::HumanNumeric,
            "-M" | "--month-sort" if !end_flags => mode = SortMode::Month,
            "-m" | "--merge" if !end_flags => merge = true,
            "-n" | "--numeric-sort" if !end_flags => mode = SortMode::Numeric,
            "-r" | "--reverse" if !end_flags => reverse = true,
            "-u" | "--unique" if !end_flags => unique = true,
            "-V" | "--version-sort" if !end_flags => mode = SortMode::Version,
            "-z" | "--zero-terminated" if !end_flags => terminator = RecordTerminator::Nul,
            "-o" | "--output" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'o'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                output_path = Some(path.clone());
                idx += 1;
            }
            "-T" | "--temporary-directory" if !end_flags => {
                let Some(path) = args.get(idx + 1) else {
                    fro::cio_eprintln!("sort: option requires an argument -- 'T'");
                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                    return Ok(2);
                };
                temporary_directory = Some(path.clone());
                idx += 1;
            }
            "--auto" if !end_flags => io_mode = IOMode::Auto,
            "--direct" if !end_flags => io_mode = IOMode::Direct,
            "--no-direct" if !end_flags => io_mode = IOMode::PageCache,
            "--report-gbps" if !end_flags => report_throughput = true,
            other if !end_flags && other.starts_with("--output=") => {
                output_path = Some(other["--output=".len()..].to_string());
            }
            other if !end_flags && other.starts_with("--temporary-directory=") => {
                temporary_directory = Some(other["--temporary-directory=".len()..].to_string());
            }
            other
                if !end_flags
                    && apply_short_sort_flags(
                        other,
                        &mut check,
                        &mut merge,
                        &mut mode,
                        &mut reverse,
                        &mut unique,
                        &mut terminator,
                    ) => {}
            other
                if !end_flags
                    && other.starts_with('-')
                    && !other.starts_with("--")
                    && other != "-" =>
            {
                let mut handled = true;
                let mut consumed_next = false;
                for (pos, flag) in other[1..].char_indices() {
                    match flag {
                        'c' => check = true,
                        'g' => mode = SortMode::GeneralNumeric,
                        'h' => mode = SortMode::HumanNumeric,
                        'M' => mode = SortMode::Month,
                        'm' => merge = true,
                        'n' => mode = SortMode::Numeric,
                        'r' => reverse = true,
                        'u' => unique = true,
                        'V' => mode = SortMode::Version,
                        'z' => terminator = RecordTerminator::Nul,
                        'o' => {
                            let value_start = 2 + pos;
                            if value_start < other.len() {
                                output_path = Some(other[value_start..].to_string());
                            } else {
                                let Some(path) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'o'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                output_path = Some(path.clone());
                                consumed_next = true;
                            }
                            break;
                        }
                        'T' => {
                            let value_start = 2 + pos;
                            if value_start < other.len() {
                                temporary_directory = Some(other[value_start..].to_string());
                            } else {
                                let Some(path) = args.get(idx + 1) else {
                                    fro::cio_eprintln!("sort: option requires an argument -- 'T'");
                                    fro::cio_eprintln!("Try 'sort --help' for more information.");
                                    return Ok(2);
                                };
                                temporary_directory = Some(path.clone());
                                consumed_next = true;
                            }
                            break;
                        }
                        _ => {
                            handled = false;
                            break;
                        }
                    }
                }
                if !handled {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported sort flag: {other}"),
                    ));
                }
                if consumed_next {
                    idx += 1;
                }
            }
            other if !end_flags && other.starts_with('-') && other != "-" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported sort flag: {other}"),
                ));
            }
            other => files.push(other.to_string()),
        }
        idx += 1;
    }

    if check && files.len() > 1 {
        fro::cio_eprintln!("sort: extra operand '{}' not allowed with -c", files[1]);
        return Ok(2);
    }
    if check && output_path.is_some() {
        fro::cio_eprintln!("sort: options '-co' are incompatible");
        return Ok(2);
    }

    let inputs = parse_stream_inputs(files);
    if inputs
        .iter()
        .filter(|input| matches!(input, StreamInput::Stdin { .. }))
        .count()
        > 1
    {
        fro::cio_eprintln!("sort: repeated '-' operands are not supported");
        fro::cio_eprintln!("Try 'sort --help' for more information.");
        return Ok(2);
    }

    let started_at = std::time::Instant::now();
    let total_bytes = if check {
        match external::check_input_sorted(&inputs[0], io_mode, mode, unique, reverse, terminator) {
            Ok(result) => {
                if let Some(disorder) = result.disorder {
                    report_sort_disorder(sort_input_label(&inputs[0]), &disorder, terminator)?;
                    return Ok(1);
                }
                result.total_bytes
            }
            Err(err) => {
                fro::cio_eprintln!("sort: {err}");
                return Ok(2);
            }
        }
    } else {
        match if merge {
            external::merge_presorted_inputs(
                &inputs,
                io_mode,
                mode,
                unique,
                reverse,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
            )
        } else {
            external::sort_inputs(
                &inputs,
                io_mode,
                mode,
                unique,
                reverse,
                terminator,
                output_path.as_deref(),
                temporary_directory.as_deref().map(Path::new),
            )
        } {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(path) = output_path.as_deref() {
                    if err.kind() == io::ErrorKind::PermissionDenied {
                        fro::cio_eprintln!("sort: cannot write '{path}': {err}");
                    } else {
                        fro::cio_eprintln!("sort: {err}");
                    }
                } else {
                    fro::cio_eprintln!("sort: {err}");
                }
                return Ok(2);
            }
        }
    };

    if report_throughput {
        report_gbps("sort", total_bytes, started_at);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
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
        sort_line_refs(&mut refs, &storage, SortMode::Bytewise, false).unwrap();
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
        finalize_sorted_lines(&mut sorted, &storage, SortMode::Bytewise, false, false).unwrap();
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
        finalize_sorted_lines(&mut unique_only, &storage, SortMode::Bytewise, true, false).unwrap();
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
            SortMode::Bytewise,
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
        sort_line_refs(&mut refs, &storage, SortMode::Numeric, false).unwrap();
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
        finalize_sorted_lines(&mut refs, &storage, SortMode::Numeric, true, false).unwrap();
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
        sort_line_refs(&mut refs, &storage, SortMode::GeneralNumeric, false).unwrap();
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
        finalize_sorted_lines(&mut refs, &storage, SortMode::HumanNumeric, true, false).unwrap();
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
        finalize_sorted_lines(&mut refs, &storage, SortMode::Month, true, false).unwrap();
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
        sort_line_refs(&mut refs, &storage, SortMode::Version, false).unwrap();
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
            compare_line_bytes(b"1", b"1.0", SortMode::Numeric, false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"x", b"NaN", SortMode::GeneralNumeric, false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"1KiB", b"1024K", SortMode::HumanNumeric, false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"JAN", b"Jan", SortMode::Month, false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"v01", b"v1", SortMode::Version, false),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_line_bytes(b"beta", b"alpha", SortMode::Bytewise, true),
            std::cmp::Ordering::Less
        );
    }
}
