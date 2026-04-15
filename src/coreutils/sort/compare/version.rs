fn version_order(byte: u8, ignore_case: bool) -> i32 {
    if byte.is_ascii_digit() {
        0
    } else if byte.is_ascii_alphabetic() {
        i32::from(if ignore_case {
            byte.to_ascii_uppercase()
        } else {
            byte
        })
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

fn compare_version_core(left: &[u8], right: &[u8], ignore_case: bool) -> std::cmp::Ordering {
    let mut left_pos = 0usize;
    let mut right_pos = 0usize;

    while left_pos < left.len() || right_pos < right.len() {
        let mut first_digit_diff = 0i32;

        while (left_pos < left.len() && !version_digit(left, left_pos))
            || (right_pos < right.len() && !version_digit(right, right_pos))
        {
            let left_order = if left_pos < left.len() {
                version_order(left[left_pos], ignore_case)
            } else {
                0
            };
            let right_order = if right_pos < right.len() {
                version_order(right[right_pos], ignore_case)
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

fn compare_version_lines(left: &[u8], right: &[u8], ignore_case: bool) -> std::cmp::Ordering {
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

    let primary = compare_version_core(
        &left_body[..left_len],
        &right_body[..right_len],
        ignore_case,
    );
    if primary == std::cmp::Ordering::Equal {
        if ignore_case {
            compare_ascii_folded(left, right)
        } else {
            left.cmp(right)
        }
    } else {
        primary
    }
}
