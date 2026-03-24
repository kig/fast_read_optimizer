use super::*;
use memchr::memchr_iter;

pub(super) fn cat_numbering_step(
    next_line_number: u64,
    at_line_start: bool,
    byte: u8,
) -> Option<(u64, bool, Option<u64>)> {
    let emitted = if at_line_start {
        Some(next_line_number)
    } else {
        None
    };
    let next_line_number = if at_line_start {
        next_line_number.checked_add(1)?
    } else {
        next_line_number
    };
    Some((next_line_number, byte == b'\n', emitted))
}

pub(super) fn cat_squeeze_blank_step(
    previous_blank_line: bool,
    current_blank_line: bool,
) -> (bool, bool) {
    let emit_line = !(previous_blank_line && current_blank_line);
    let next_previous_blank_line = if emit_line { current_blank_line } else { true };
    (emit_line, next_previous_blank_line)
}

pub(super) fn cat_should_number_line(number: bool, number_nonblank: bool, line: &[u8]) -> bool {
    if number_nonblank {
        line != b"\n"
    } else {
        number
    }
}

pub(super) fn cat_show_ends_rendered_len(
    original_len: usize,
    ends_with_newline: bool,
) -> Option<usize> {
    if ends_with_newline {
        original_len.checked_add(1)
    } else {
        Some(original_len)
    }
}

#[cfg(any(test, kani))]
pub(super) fn cat_show_tabs_rendered_len(original_len: usize, tab_count: usize) -> Option<usize> {
    original_len.checked_add(tab_count)
}

pub(super) fn cat_visible_byte_rendered_len(
    byte: u8,
    show_tabs: bool,
    show_nonprinting: bool,
) -> usize {
    if show_tabs && byte == b'\t' {
        return 2;
    }
    if !show_nonprinting {
        return 1;
    }
    match byte {
        0..=31 => {
            if byte == b'\t' || byte == b'\n' {
                1
            } else {
                2
            }
        }
        32..=126 => 1,
        127 => 2,
        128..=159 => 4,
        160..=254 => 3,
        255 => 4,
    }
}

pub(super) fn cat_uses_transform_path(
    number: bool,
    number_nonblank: bool,
    show_ends: bool,
    show_tabs: bool,
    show_nonprinting: bool,
    squeeze_blank: bool,
) -> bool {
    number || number_nonblank || show_ends || show_tabs || show_nonprinting || squeeze_blank
}

pub(super) fn cat_short_visual_flag_effect(flag: u8) -> Option<(bool, bool, bool)> {
    match flag {
        b'E' => Some((true, false, false)),
        b'T' => Some((false, true, false)),
        b'v' => Some((false, false, true)),
        b'e' => Some((true, false, true)),
        b't' => Some((false, true, true)),
        b'A' => Some((true, true, true)),
        _ => None,
    }
}

fn cat_write_visible_byte<W: Write>(
    out: &mut W,
    byte: u8,
    show_tabs: bool,
    show_nonprinting: bool,
) -> io::Result<()> {
    if show_tabs && byte == b'\t' {
        return out.write_all(b"^I");
    }
    if !show_nonprinting {
        return out.write_all(&[byte]);
    }
    match byte {
        0..=31 => {
            if byte == b'\t' || byte == b'\n' {
                out.write_all(&[byte])
            } else {
                out.write_all(&[b'^', byte + 64])
            }
        }
        32..=126 => out.write_all(&[byte]),
        127 => out.write_all(b"^?"),
        128..=255 => {
            out.write_all(b"M-")?;
            let low = byte - 128;
            match low {
                0..=31 => out.write_all(&[b'^', low + 64]),
                32..=126 => out.write_all(&[low]),
                127 => out.write_all(b"^?"),
                _ => unreachable!(),
            }
        }
    }
}

fn cat_write_transformed_line<W: Write>(
    out: &mut W,
    line: &[u8],
    number: bool,
    number_nonblank: bool,
    show_ends: bool,
    show_tabs: bool,
    show_nonprinting: bool,
    squeeze_blank: bool,
    next_line_number: &mut u64,
    previous_blank_line: &mut bool,
) -> io::Result<()> {
    let current_blank_line = line == b"\n";
    if squeeze_blank {
        let (emit_line, next_previous) =
            cat_squeeze_blank_step(*previous_blank_line, current_blank_line);
        *previous_blank_line = next_previous;
        if !emit_line {
            return Ok(());
        }
    } else {
        *previous_blank_line = current_blank_line;
    }
    if cat_should_number_line(number, number_nonblank, line) {
        let (updated_next_line_number, _, emitted_line_number) =
            cat_numbering_step(*next_line_number, true, line[0]).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "cat line number overflow")
            })?;
        *next_line_number = updated_next_line_number;
        if let Some(line_number) = emitted_line_number {
            write!(out, "{line_number:>6}\t")?;
        }
    }
    let ends_with_newline = line.ends_with(b"\n");
    if show_tabs || show_nonprinting {
        let body = if show_ends && ends_with_newline {
            &line[..line.len().saturating_sub(1)]
        } else {
            line
        };
        let mut rendered_len = 0usize;
        for &byte in body {
            rendered_len = rendered_len
                .checked_add(cat_visible_byte_rendered_len(
                    byte,
                    show_tabs,
                    show_nonprinting,
                ))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "cat byte rendering overflow")
                })?;
        }
        if show_ends && ends_with_newline {
            rendered_len = rendered_len.checked_add(2).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cat show-ends rendering overflow",
                )
            })?;
        }
        let _ = rendered_len;
        for &byte in body {
            cat_write_visible_byte(out, byte, show_tabs, show_nonprinting)?;
        }
        if show_ends && ends_with_newline {
            out.write_all(b"$\n")?;
        }
        return Ok(());
    }
    let rendered_len =
        cat_show_ends_rendered_len(line.len(), ends_with_newline).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cat show-ends rendering overflow",
            )
        })?;
    if show_ends && rendered_len != line.len() {
        let split = line.len().saturating_sub(1);
        out.write_all(&line[..split])?;
        out.write_all(b"$\n")
    } else {
        out.write_all(line)
    }
}

pub(super) fn run_cat(args: &[String]) -> io::Result<()> {
    let mut io_mode = IOMode::Auto;
    let mut number = false;
    let mut number_nonblank = false;
    let mut show_ends = false;
    let mut show_tabs = false;
    let mut show_nonprinting = false;
    let mut squeeze_blank = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-n" | "--number" => number = true,
            "-b" | "--number-nonblank" => number_nonblank = true,
            "-E" | "--show-ends" => show_ends = true,
            "-T" | "--show-tabs" => show_tabs = true,
            "-t" => {
                show_tabs = true;
                show_nonprinting = true;
            }
            "-u" => {}
            "--show-nonprinting" => show_nonprinting = true,
            "--show-all" => {
                let (flag_show_ends, flag_show_tabs, flag_show_nonprinting) =
                    cat_short_visual_flag_effect(b'A').unwrap();
                show_ends |= flag_show_ends;
                show_tabs |= flag_show_tabs;
                show_nonprinting |= flag_show_nonprinting;
            }
            "-s" | "--squeeze-blank" => squeeze_blank = true,
            other => {
                if let [b'-', flag] = other.as_bytes() {
                    if let Some((flag_show_ends, flag_show_tabs, flag_show_nonprinting)) =
                        cat_short_visual_flag_effect(*flag)
                    {
                        show_ends |= flag_show_ends;
                        show_tabs |= flag_show_tabs;
                        show_nonprinting |= flag_show_nonprinting;
                        continue;
                    }
                }
                files.push(other.to_string());
            }
        }
    }
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    if cat_uses_transform_path(
        number,
        number_nonblank,
        show_ends,
        show_tabs,
        show_nonprinting,
        squeeze_blank,
    ) {
        let mut next_line_number = 1u64;
        let mut previous_blank_line = false;
        let mut pending_line = Vec::new();
        for input in inputs {
            visit_ordered_input(&input, io_mode, |block| {
                let mut line_start = 0usize;
                for newline_offset in memchr_iter(b'\n', block) {
                    pending_line.extend_from_slice(&block[line_start..=newline_offset]);
                    cat_write_transformed_line(
                        &mut out,
                        &pending_line,
                        number,
                        number_nonblank,
                        show_ends,
                        show_tabs,
                        show_nonprinting,
                        squeeze_blank,
                        &mut next_line_number,
                        &mut previous_blank_line,
                    )?;
                    pending_line.clear();
                    line_start = newline_offset + 1;
                }
                if line_start < block.len() {
                    pending_line.extend_from_slice(&block[line_start..]);
                }
                Ok(())
            })?;
        }
        if !pending_line.is_empty() {
            cat_write_transformed_line(
                &mut out,
                &pending_line,
                number,
                number_nonblank,
                show_ends,
                show_tabs,
                show_nonprinting,
                squeeze_blank,
                &mut next_line_number,
                &mut previous_blank_line,
            )?;
        }
        return out.into_inner();
    }
    for input in inputs {
        if try_fast_cat_copy(&input, io_mode)? {
            continue;
        }
        copy_file_like_to_output(&mut out, &input)?;
    }
    out.into_inner()
}

#[cfg(kani)]
mod kani_proofs {
    use super::{
        cat_numbering_step, cat_short_visual_flag_effect, cat_should_number_line,
        cat_show_ends_rendered_len, cat_show_tabs_rendered_len, cat_squeeze_blank_step,
        cat_uses_transform_path, cat_visible_byte_rendered_len,
    };

    #[kani::proof]
    fn cat_numbering_step_matches_start_of_line_formula() {
        let next_line_number: u64 = kani::any();
        let at_line_start: bool = kani::any();
        let byte: u8 = kani::any();
        let step = cat_numbering_step(next_line_number, at_line_start, byte);
        if at_line_start && next_line_number == u64::MAX {
            assert!(step.is_none());
            return;
        }
        let (next_number, next_at_line_start, emitted) = step.unwrap();
        assert_eq!(emitted, at_line_start.then_some(next_line_number));
        assert_eq!(
            next_number,
            if at_line_start {
                next_line_number + 1
            } else {
                next_line_number
            }
        );
        assert_eq!(next_at_line_start, byte == b'\n');
    }

    #[kani::proof]
    fn cat_squeeze_blank_step_matches_blank_run_formula() {
        let previous_blank_line: bool = kani::any();
        let current_blank_line: bool = kani::any();
        let (emit_line, next_previous_blank_line) =
            cat_squeeze_blank_step(previous_blank_line, current_blank_line);
        assert_eq!(emit_line, !(previous_blank_line && current_blank_line));
        assert_eq!(
            next_previous_blank_line,
            if emit_line { current_blank_line } else { true }
        );
    }

    #[kani::proof]
    fn cat_should_number_line_matches_nonblank_precedence() {
        let number: bool = kani::any();
        let number_nonblank: bool = kani::any();
        let blank: bool = kani::any();
        let line = if blank {
            b"\n".as_slice()
        } else {
            b"x\n".as_slice()
        };
        assert_eq!(
            cat_should_number_line(number, number_nonblank, line),
            if number_nonblank { !blank } else { number }
        );
    }

    #[kani::proof]
    fn cat_show_ends_rendered_len_matches_newline_formula() {
        let original_len: usize = kani::any();
        let ends_with_newline: bool = kani::any();
        let rendered_len = cat_show_ends_rendered_len(original_len, ends_with_newline);
        if ends_with_newline && original_len == usize::MAX {
            assert!(rendered_len.is_none());
            return;
        }
        assert_eq!(
            rendered_len,
            Some(if ends_with_newline {
                original_len + 1
            } else {
                original_len
            })
        );
    }

    #[kani::proof]
    fn cat_show_tabs_rendered_len_matches_tab_formula() {
        let original_len: usize = kani::any();
        let tab_count: usize = kani::any();
        let rendered_len = cat_show_tabs_rendered_len(original_len, tab_count);
        if original_len > usize::MAX - tab_count {
            assert!(rendered_len.is_none());
            return;
        }
        assert_eq!(rendered_len, Some(original_len + tab_count));
    }

    #[kani::proof]
    fn cat_visible_byte_rendered_len_matches_gnu_byte_classes() {
        let byte: u8 = kani::any();
        let show_tabs: bool = kani::any();
        let show_nonprinting: bool = kani::any();
        let rendered_len = cat_visible_byte_rendered_len(byte, show_tabs, show_nonprinting);
        let expected = if show_tabs && byte == b'\t' {
            2
        } else if !show_nonprinting {
            1
        } else {
            match byte {
                0..=31 => {
                    if byte == b'\t' || byte == b'\n' {
                        1
                    } else {
                        2
                    }
                }
                32..=126 => 1,
                127 => 2,
                128..=159 => 4,
                160..=254 => 3,
                255 => 4,
            }
        };
        assert_eq!(rendered_len, expected);
    }

    #[kani::proof]
    fn cat_visible_byte_rendered_len_keeps_layout_bytes_single_width_for_v_mode() {
        let keep_newline: bool = kani::any();
        let byte = if keep_newline { b'\n' } else { b'\t' };
        assert_eq!(cat_visible_byte_rendered_len(byte, false, true), 1);
    }

    #[kani::proof]
    fn cat_uses_transform_path_matches_disjunction() {
        let number: bool = kani::any();
        let number_nonblank: bool = kani::any();
        let show_ends: bool = kani::any();
        let show_tabs: bool = kani::any();
        let show_nonprinting: bool = kani::any();
        let squeeze_blank: bool = kani::any();
        assert_eq!(
            cat_uses_transform_path(
                number,
                number_nonblank,
                show_ends,
                show_tabs,
                show_nonprinting,
                squeeze_blank,
            ),
            number
                || number_nonblank
                || show_ends
                || show_tabs
                || show_nonprinting
                || squeeze_blank
        );
    }

    #[kani::proof]
    fn cat_short_visual_flag_effect_matches_gnu_composites() {
        let flag: u8 = kani::any();
        let effect = cat_short_visual_flag_effect(flag);
        let expected = match flag {
            b'E' => Some((true, false, false)),
            b'T' => Some((false, true, false)),
            b'v' => Some((false, false, true)),
            b'e' => Some((true, false, true)),
            b't' => Some((false, true, true)),
            b'A' => Some((true, true, true)),
            _ => None,
        };
        assert_eq!(effect, expected);
    }

    #[kani::proof]
    fn cat_short_visual_flag_effect_maps_show_all_to_all_visual_bits() {
        assert_eq!(cat_short_visual_flag_effect(b'A'), Some((true, true, true)));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cat_numbering_step, cat_short_visual_flag_effect, cat_should_number_line,
        cat_show_ends_rendered_len, cat_show_tabs_rendered_len, cat_squeeze_blank_step,
        cat_uses_transform_path, cat_visible_byte_rendered_len,
    };

    #[test]
    fn cat_numbering_step_numbers_only_at_line_starts() {
        assert_eq!(
            cat_numbering_step(1, true, b'a').unwrap(),
            (2, false, Some(1))
        );
        assert_eq!(
            cat_numbering_step(2, false, b'b').unwrap(),
            (2, false, None)
        );
        assert_eq!(
            cat_numbering_step(2, false, b'\n').unwrap(),
            (2, true, None)
        );
        assert_eq!(
            cat_numbering_step(2, true, b'\n').unwrap(),
            (3, true, Some(2))
        );
    }

    #[test]
    fn cat_numbering_step_rejects_line_number_overflow() {
        assert!(cat_numbering_step(u64::MAX, true, b'x').is_none());
        assert_eq!(
            cat_numbering_step(u64::MAX, false, b'x').unwrap(),
            (u64::MAX, false, None)
        );
    }

    #[test]
    fn cat_squeeze_blank_step_emits_only_first_blank_in_run() {
        assert_eq!(cat_squeeze_blank_step(false, false), (true, false));
        assert_eq!(cat_squeeze_blank_step(false, true), (true, true));
        assert_eq!(cat_squeeze_blank_step(true, false), (true, false));
        assert_eq!(cat_squeeze_blank_step(true, true), (false, true));
    }

    #[test]
    fn cat_should_number_line_gives_nonblank_mode_precedence() {
        assert!(cat_should_number_line(true, false, b"\n"));
        assert!(cat_should_number_line(true, false, b"x\n"));
        assert!(!cat_should_number_line(false, false, b"x\n"));
        assert!(!cat_should_number_line(true, true, b"\n"));
        assert!(cat_should_number_line(false, true, b"x\n"));
    }

    #[test]
    fn cat_show_ends_rendered_len_adds_one_byte_only_for_newline_terminated_lines() {
        assert_eq!(cat_show_ends_rendered_len(0, false), Some(0));
        assert_eq!(cat_show_ends_rendered_len(0, true), Some(1));
        assert_eq!(cat_show_ends_rendered_len(4, false), Some(4));
        assert_eq!(cat_show_ends_rendered_len(4, true), Some(5));
        assert_eq!(cat_show_ends_rendered_len(usize::MAX, true), None);
    }

    #[test]
    fn cat_show_tabs_rendered_len_adds_one_byte_per_tab() {
        assert_eq!(cat_show_tabs_rendered_len(0, 0), Some(0));
        assert_eq!(cat_show_tabs_rendered_len(4, 0), Some(4));
        assert_eq!(cat_show_tabs_rendered_len(4, 1), Some(5));
        assert_eq!(cat_show_tabs_rendered_len(4, 3), Some(7));
        assert_eq!(cat_show_tabs_rendered_len(usize::MAX, 1), None);
    }

    #[test]
    fn cat_visible_byte_rendered_len_matches_expected_classes() {
        assert_eq!(cat_visible_byte_rendered_len(b'\t', true, false), 2);
        assert_eq!(cat_visible_byte_rendered_len(b'\t', false, true), 1);
        assert_eq!(cat_visible_byte_rendered_len(b'\n', false, true), 1);
        assert_eq!(cat_visible_byte_rendered_len(0x01, false, true), 2);
        assert_eq!(cat_visible_byte_rendered_len(0x7f, false, true), 2);
        assert_eq!(cat_visible_byte_rendered_len(0x80, false, true), 4);
        assert_eq!(cat_visible_byte_rendered_len(0xa0, false, true), 3);
        assert_eq!(cat_visible_byte_rendered_len(0xff, false, true), 4);
        assert_eq!(cat_visible_byte_rendered_len(b'A', false, true), 1);
    }

    #[test]
    fn cat_visible_byte_rendered_len_keeps_tab_and_newline_single_width_without_show_tabs() {
        assert_eq!(cat_visible_byte_rendered_len(b'\t', false, true), 1);
        assert_eq!(cat_visible_byte_rendered_len(b'\n', false, true), 1);
    }

    #[test]
    fn cat_uses_transform_path_is_false_for_plain_unbuffered_mode() {
        assert!(!cat_uses_transform_path(
            false, false, false, false, false, false
        ));
        assert!(cat_uses_transform_path(
            true, false, false, false, false, false
        ));
    }

    #[test]
    fn cat_short_visual_flag_effect_matches_expected_composites() {
        assert_eq!(
            cat_short_visual_flag_effect(b'E'),
            Some((true, false, false))
        );
        assert_eq!(
            cat_short_visual_flag_effect(b'T'),
            Some((false, true, false))
        );
        assert_eq!(
            cat_short_visual_flag_effect(b'v'),
            Some((false, false, true))
        );
        assert_eq!(
            cat_short_visual_flag_effect(b'e'),
            Some((true, false, true))
        );
        assert_eq!(
            cat_short_visual_flag_effect(b't'),
            Some((false, true, true))
        );
        assert_eq!(cat_short_visual_flag_effect(b'A'), Some((true, true, true)));
        assert_eq!(cat_short_visual_flag_effect(b'x'), None);
    }

    #[test]
    fn cat_short_visual_flag_effect_maps_show_all_to_all_visual_bits() {
        assert_eq!(cat_short_visual_flag_effect(b'A'), Some((true, true, true)));
    }
}
