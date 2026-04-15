fn is_blank(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
}

fn nth_blank_field_bounds(line: &[u8], field: usize) -> Option<(usize, usize)> {
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

fn nth_separator_field_bounds(line: &[u8], field: usize, separator: u8) -> Option<(usize, usize)> {
    let mut current_field = 1usize;
    let mut start = 0usize;
    for idx in 0..=line.len() {
        if idx == line.len() || line[idx] == separator {
            if current_field == field {
                return Some((start, idx));
            }
            current_field += 1;
            start = idx + 1;
        }
    }
    None
}

fn nth_field_bounds(line: &[u8], field: usize, separator: Option<u8>) -> Option<(usize, usize)> {
    match separator {
        Some(separator) => nth_separator_field_bounds(line, field, separator),
        None => nth_blank_field_bounds(line, field),
    }
}

fn key_field_start(
    line: &[u8],
    field: usize,
    field_separator: Option<u8>,
    ignore_leading_blanks: bool,
) -> Option<(usize, usize)> {
    nth_field_bounds(line, field, field_separator).map(|(field_start, field_end)| {
        let key_start = if ignore_leading_blanks {
            let mut idx = field_start;
            while idx < field_end && is_blank(line[idx]) {
                idx += 1;
            }
            idx
        } else {
            field_start
        };
        (key_start, field_end)
    })
}

fn key_range(line: &[u8], key: &SortKeySpec, comparator: &SortComparator) -> (usize, usize) {
    let Some((start_field, start_field_end)) = key_field_start(
        line,
        key.start.field,
        comparator.field_separator,
        comparator.ignore_leading_blanks,
    ) else {
        return (line.len(), line.len());
    };
    let start = match comparator.field_separator {
        Some(_) => start_field
            .saturating_add(key.start.char_offset)
            .min(line.len()),
        None => (start_field + key.start.char_offset).min(start_field_end),
    };
    let end = match key.end {
        SortKeyEnd::EndOfLine => line.len(),
        SortKeyEnd::FieldEnd { field } => nth_field_bounds(line, field, comparator.field_separator)
            .map(|(_, field_end)| field_end)
            .unwrap_or(line.len()),
        SortKeyEnd::Char { field, char_end } => key_field_start(
            line,
            field,
            comparator.field_separator,
            comparator.ignore_leading_blanks,
        )
        .map(
            |(field_start, field_end)| match comparator.field_separator {
                Some(_) => field_start.saturating_add(char_end).min(line.len()),
                None => (field_start + char_end).min(field_end),
            },
        )
        .unwrap_or(line.len()),
    };
    (start, end.max(start))
}

fn leading_blank_count(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .position(|&byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(bytes.len())
}

fn numeric_prefix_span(bytes: &[u8]) -> (usize, Option<usize>) {
    let start = leading_blank_count(bytes);
    let trimmed = &bytes[start..];
    let mut idx = 0usize;
    if idx < trimmed.len() && trimmed[idx] == b'-' {
        idx += 1;
    }
    let digit_start = idx;
    while idx < trimmed.len() && trimmed[idx].is_ascii_digit() {
        idx += 1;
    }
    let mut matched = idx > digit_start;
    if idx < trimmed.len() && trimmed[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < trimmed.len() && trimmed[idx].is_ascii_digit() {
            idx += 1;
        }
        matched |= idx > frac_start;
    }
    (start, matched.then_some(idx))
}

fn general_numeric_prefix_span(bytes: &[u8]) -> (usize, Option<usize>) {
    let start = leading_blank_count(bytes);
    let trimmed = &bytes[start..];
    if trimmed.is_empty() {
        return (start, None);
    }
    let nul_cutoff = trimmed
        .iter()
        .position(|&byte| byte == b'\0')
        .unwrap_or(trimmed.len());
    let mut buffer = Vec::with_capacity(nul_cutoff + 1);
    buffer.extend_from_slice(&trimmed[..nul_cutoff]);
    buffer.push(0);
    let start_ptr = buffer.as_ptr() as *const libc::c_char;
    let mut end_ptr = std::ptr::null_mut();
    let _ = unsafe { libc::strtod(start_ptr, &mut end_ptr) };
    if std::ptr::eq(end_ptr as *const libc::c_char, start_ptr) {
        return (start, None);
    }
    let consumed = unsafe { end_ptr.offset_from(start_ptr) as usize }.min(nul_cutoff);
    (start, Some(consumed))
}

fn human_numeric_prefix_span(bytes: &[u8]) -> (usize, Option<usize>) {
    let start = leading_blank_count(bytes);
    let trimmed = &bytes[start..];
    let mut idx = 0usize;
    if matches!(trimmed.first(), Some(b'+')) {
        return (start, None);
    }
    if idx < trimmed.len() && trimmed[idx] == b'-' {
        idx += 1;
    }
    let digit_start = idx;
    while idx < trimmed.len() && trimmed[idx].is_ascii_digit() {
        idx += 1;
    }
    let mut matched = idx > digit_start;
    if idx < trimmed.len() && trimmed[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < trimmed.len() && trimmed[idx].is_ascii_digit() {
            idx += 1;
        }
        matched |= idx > frac_start;
    }
    if !matched {
        return (start, None);
    }
    if trimmed
        .get(idx)
        .copied()
        .and_then(human_suffix_rank)
        .is_some()
    {
        idx += 1;
    }
    (start, Some(idx))
}

fn month_prefix_span(bytes: &[u8]) -> (usize, Option<usize>) {
    let start = leading_blank_count(bytes);
    let trimmed = &bytes[start..];
    if matches!(parse_month_prefix(bytes), MonthPrefix::Month(_)) {
        (start, Some(trimmed.len().min(3)))
    } else {
        (start, None)
    }
}

fn build_debug_key_selection(
    key_slice: &[u8],
    offset: usize,
    comparator: &SortComparator,
) -> SortDebugKeySelection {
    let (offset_delta, matched_len) = match comparator.mode {
        SortMode::Bytewise | SortMode::Random | SortMode::Version => (0, None),
        SortMode::Numeric => numeric_prefix_span(key_slice),
        SortMode::GeneralNumeric => general_numeric_prefix_span(key_slice),
        SortMode::HumanNumeric => human_numeric_prefix_span(key_slice),
        SortMode::Month => month_prefix_span(key_slice),
    };
    let printable_len = key_slice
        .iter()
        .filter(|&&byte| is_printable_byte(byte))
        .count();
    let matched = match comparator.mode {
        SortMode::Bytewise | SortMode::Random | SortMode::Version => {
            !normalized_sort_key(key_slice, comparator).is_empty()
        }
        _ => matched_len.is_some(),
    };
    SortDebugKeySelection {
        offset: offset.saturating_add(offset_delta),
        visual_len: matched_len.unwrap_or(if comparator.ignore_nonprinting {
            printable_len
        } else {
            key_slice.len()
        }),
        matched,
        preserve_offsets: !comparator.ignore_nonprinting,
    }
}

pub(super) fn debug_key_selections(
    line: &[u8],
    comparator: &SortComparator,
) -> Vec<SortDebugKeySelection> {
    if comparator.keys.is_empty() {
        let offset = if comparator.ignore_leading_blanks {
            line.iter()
                .position(|&byte| !matches!(byte, b' ' | b'\t'))
                .unwrap_or(line.len())
        } else {
            0
        };
        return vec![build_debug_key_selection(
            selected_sort_key(line, comparator),
            offset,
            comparator,
        )];
    }

    comparator
        .keys
        .iter()
        .map(|key| {
            let (start, end) = key_range(line, key, comparator);
            build_debug_key_selection(&line[start..end], start, comparator)
        })
        .collect()
}

fn key_slice<'a>(line: &'a [u8], key: &SortKeySpec, comparator: &SortComparator) -> &'a [u8] {
    let (start, end) = key_range(line, key, comparator);
    &line[start..end]
}

fn compare_selected_keys(
    left: &[u8],
    right: &[u8],
    comparator: &SortComparator,
) -> std::cmp::Ordering {
    if comparator.keys.is_empty() {
        return compare_sort_keys(left, right, comparator);
    }
    for key in &comparator.keys {
        let order = compare_sort_keys(
            key_slice(left, key, comparator),
            key_slice(right, key, comparator),
            comparator,
        );
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

fn same_selected_keys(left: &[u8], right: &[u8], comparator: &SortComparator) -> bool {
    if comparator.keys.is_empty() {
        return same_single_sort_key(left, right, comparator);
    }
    comparator.keys.iter().all(|key| {
        same_single_sort_key(
            key_slice(left, key, comparator),
            key_slice(right, key, comparator),
            comparator,
        )
    })
}

pub(super) fn compare_line_refs(
    left: SortLineRef,
    right: SortLineRef,
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> std::cmp::Ordering {
    let key_order = compare_selected_keys(left.bytes(storage), right.bytes(storage), comparator);
    if key_order != std::cmp::Ordering::Equal {
        return if reverse {
            key_order.reverse()
        } else {
            key_order
        };
    }
    if comparator.uses_last_resort_line_order(unique) {
        let line_order = left
            .bytes(storage)
            .cmp(right.bytes(storage))
            .then_with(|| left.sequence.cmp(&right.sequence));
        if reverse {
            line_order.reverse()
        } else {
            line_order
        }
    } else {
        left.sequence.cmp(&right.sequence)
    }
}

pub(super) fn compare_output_lines(
    left: &[u8],
    left_sequence: u64,
    right: &[u8],
    right_sequence: u64,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> std::cmp::Ordering {
    let key_order = compare_selected_keys(left, right, comparator);
    if key_order != std::cmp::Ordering::Equal {
        return if reverse {
            key_order.reverse()
        } else {
            key_order
        };
    }
    if comparator.uses_last_resort_line_order(unique) {
        let line_order = left.cmp(right);
        if line_order != std::cmp::Ordering::Equal {
            return if reverse {
                line_order.reverse()
            } else {
                line_order
            };
        }
    }
    left_sequence.cmp(&right_sequence)
}

pub(super) fn compare_line_bytes(
    left: &[u8],
    right: &[u8],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> std::cmp::Ordering {
    let asc = match compare_selected_keys(left, right, comparator) {
        std::cmp::Ordering::Equal if comparator.uses_last_resort_line_order(unique) => {
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
