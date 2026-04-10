use super::*;
use memchr::memchr_iter;

#[derive(Clone)]
struct CatArgs {
    io_mode: IOMode,
    report_gbps: bool,
    number: bool,
    number_nonblank: bool,
    show_ends: bool,
    show_tabs: bool,
    show_nonprinting: bool,
    squeeze_blank: bool,
    files: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatExecutionBackend {
    OrderedTransform,
    FastCopyToStdout,
    BufferedCopy,
}

fn parse_short_cat_flags(arg: &str, parsed: &mut CatArgs) -> io::Result<bool> {
    if !arg.starts_with('-') || arg.len() <= 1 || arg.starts_with("--") {
        return Ok(false);
    }
    for flag in arg[1..].bytes() {
        match flag {
            b'n' => parsed.number = true,
            b'b' => parsed.number_nonblank = true,
            b'E' => parsed.show_ends = true,
            b'T' => parsed.show_tabs = true,
            b'v' => parsed.show_nonprinting = true,
            b's' => parsed.squeeze_blank = true,
            b'u' => {}
            b'e' | b't' | b'A' => {
                let (flag_show_ends, flag_show_tabs, flag_show_nonprinting) =
                    cat_short_visual_flag_effect(flag).unwrap();
                parsed.show_ends |= flag_show_ends;
                parsed.show_tabs |= flag_show_tabs;
                parsed.show_nonprinting |= flag_show_nonprinting;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported cat flag -{}", flag as char),
                ))
            }
        }
    }
    Ok(true)
}

fn parse_cat_args(args: &[String]) -> io::Result<CatArgs> {
    let mut parsed = CatArgs {
        io_mode: IOMode::Auto,
        report_gbps: false,
        number: false,
        number_nonblank: false,
        show_ends: false,
        show_tabs: false,
        show_nonprinting: false,
        squeeze_blank: false,
        files: Vec::new(),
    };
    let mut end_flags = false;
    for arg in &args[1..] {
        match arg.as_str() {
            "--" if !end_flags => end_flags = true,
            "--auto" if !end_flags => parsed.io_mode = IOMode::Auto,
            "--direct" if !end_flags => parsed.io_mode = IOMode::Direct,
            "--no-direct" if !end_flags => parsed.io_mode = IOMode::PageCache,
            "--report-gbps" if !end_flags => parsed.report_gbps = true,
            "-n" | "--number" if !end_flags => parsed.number = true,
            "-b" | "--number-nonblank" if !end_flags => parsed.number_nonblank = true,
            "-E" | "--show-ends" if !end_flags => parsed.show_ends = true,
            "-T" | "--show-tabs" if !end_flags => parsed.show_tabs = true,
            "-u" if !end_flags => {}
            "--show-nonprinting" if !end_flags => parsed.show_nonprinting = true,
            "--show-all" if !end_flags => {
                let (flag_show_ends, flag_show_tabs, flag_show_nonprinting) =
                    cat_short_visual_flag_effect(b'A').unwrap();
                parsed.show_ends |= flag_show_ends;
                parsed.show_tabs |= flag_show_tabs;
                parsed.show_nonprinting |= flag_show_nonprinting;
            }
            "-s" | "--squeeze-blank" if !end_flags => parsed.squeeze_blank = true,
            other => {
                if !end_flags && parse_short_cat_flags(other, &mut parsed)? {
                    continue;
                }
                parsed.files.push(other.to_string());
            }
        }
    }
    Ok(parsed)
}

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

fn cat_execution_backend(input: &StreamInput, args: &CatArgs) -> io::Result<CatExecutionBackend> {
    if cat_uses_transform_path(
        args.number,
        args.number_nonblank,
        args.show_ends,
        args.show_tabs,
        args.show_nonprinting,
        args.squeeze_blank,
    ) {
        return Ok(CatExecutionBackend::OrderedTransform);
    }
    if args.io_mode == IOMode::Direct {
        return Ok(CatExecutionBackend::BufferedCopy);
    }
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            Ok(CatExecutionBackend::FastCopyToStdout)
        }
        StreamInput::Stdin { .. } => {
            if fd_is_regular(fro::command_io::stdin_fd())?
                || fd_is_fifo(fro::command_io::stdin_fd())?
            {
                Ok(CatExecutionBackend::FastCopyToStdout)
            } else {
                Ok(CatExecutionBackend::BufferedCopy)
            }
        }
        StreamInput::File(path) => {
            let file_type = fs::metadata(path)?.file_type();
            if file_type.is_fifo() {
                Ok(CatExecutionBackend::FastCopyToStdout)
            } else {
                Ok(CatExecutionBackend::BufferedCopy)
            }
        }
    }
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
    let mut parsed = parse_cat_args(args)?;
    let io_mode = parsed.io_mode;
    let report_throughput = parsed.report_gbps;
    let number = parsed.number;
    let number_nonblank = parsed.number_nonblank;
    let show_ends = parsed.show_ends;
    let show_tabs = parsed.show_tabs;
    let show_nonprinting = parsed.show_nonprinting;
    let squeeze_blank = parsed.squeeze_blank;
    let inputs = parse_stream_inputs(std::mem::take(&mut parsed.files));
    let started_at = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    if cat_uses_transform_path(
        number,
        number_nonblank,
        show_ends,
        show_tabs,
        show_nonprinting,
        squeeze_blank,
    ) {
        let mut out = stdout_buf_writer()?;
        let mut next_line_number = 1u64;
        let mut previous_blank_line = false;
        let mut pending_line = Vec::new();
        for input in inputs {
            total_bytes += visit_ordered_input_counted(&input, io_mode, |block| {
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
        out.into_inner()?;
        if report_throughput {
            report_gbps("cat", total_bytes, started_at);
        }
        return Ok(());
    }
    let mut out = None;
    for input in inputs {
        let backend = cat_execution_backend(&input, &parsed)?;
        debug_assert_ne!(backend, CatExecutionBackend::OrderedTransform);
        let mut copied = 0_u64;
        if backend == CatExecutionBackend::FastCopyToStdout
            && try_fast_copy_to_stdout_counted(&input, io_mode, &mut |bytes| {
                copied = bytes;
                Ok(())
            })?
            .is_some()
        {
            total_bytes += copied;
            continue;
        }
        let out = out.get_or_insert(stdout_buf_writer()?);
        total_bytes += copy_file_like_to_output_counted(out, &input)?;
    }
    match out {
        Some(out) => out.into_inner()?,
        None => {}
    }
    if report_throughput {
        report_gbps("cat", total_bytes, started_at);
    }
    Ok(())
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
        cat_execution_backend, cat_numbering_step, cat_short_visual_flag_effect,
        cat_should_number_line, cat_show_ends_rendered_len, cat_show_tabs_rendered_len,
        cat_squeeze_blank_step, cat_uses_transform_path, cat_visible_byte_rendered_len,
        parse_cat_args, parse_short_cat_flags, CatArgs, CatExecutionBackend, StreamInput,
    };
    use std::io;

    fn cat_test_temp_file(name: &str) -> std::path::PathBuf {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

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

    #[test]
    fn plain_and_unbuffered_cat_keep_regular_files_on_fast_copy_backend() {
        let tmp = cat_test_temp_file("fro-cat-fast-backend");
        std::fs::write(&tmp, b"plain cat backend selection\n").unwrap();
        let file = tmp.display().to_string();
        let input = StreamInput::File(file.clone());

        for args in [
            vec!["cat".to_string(), file.clone()],
            vec!["cat".to_string(), "-u".to_string(), file.clone()],
            vec!["cat".to_string(), "--no-direct".to_string(), file.clone()],
        ] {
            let parsed = parse_cat_args(&args).unwrap();
            assert_eq!(
                cat_execution_backend(&input, &parsed).unwrap(),
                CatExecutionBackend::FastCopyToStdout,
                "args {args:?} should stay on the fast copy backend"
            );
        }

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn formatting_flags_intentionally_leave_the_fast_copy_backend() {
        let tmp = cat_test_temp_file("fro-cat-transform-backend");
        std::fs::write(&tmp, b"alpha\nbeta\n").unwrap();
        let file = tmp.display().to_string();
        let input = StreamInput::File(file.clone());

        for args in [
            vec!["cat".to_string(), "-n".to_string(), file.clone()],
            vec!["cat".to_string(), "-A".to_string(), file.clone()],
            vec!["cat".to_string(), "--show-tabs".to_string(), file.clone()],
        ] {
            let parsed = parse_cat_args(&args).unwrap();
            assert_eq!(
                cat_execution_backend(&input, &parsed).unwrap(),
                CatExecutionBackend::OrderedTransform,
                "args {args:?} should leave the fast copy backend"
            );
        }

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn direct_mode_plain_cat_uses_buffered_copy_backend() {
        let tmp = cat_test_temp_file("fro-cat-direct-backend");
        std::fs::write(&tmp, b"direct backend selection\n").unwrap();
        let file = tmp.display().to_string();
        let input = StreamInput::File(file.clone());
        let args = vec!["cat".to_string(), "--direct".to_string(), file];
        let parsed = parse_cat_args(&args).unwrap();

        assert_eq!(
            cat_execution_backend(&input, &parsed).unwrap(),
            CatExecutionBackend::BufferedCopy
        );

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn parse_short_cat_flags_supports_combined_common_flags() {
        let mut parsed = CatArgs {
            io_mode: super::IOMode::Auto,
            report_gbps: false,
            number: false,
            number_nonblank: false,
            show_ends: false,
            show_tabs: false,
            show_nonprinting: false,
            squeeze_blank: false,
            files: Vec::new(),
        };
        assert!(parse_short_cat_flags("-benst", &mut parsed).unwrap());
        assert!(parsed.number);
        assert!(parsed.number_nonblank);
        assert!(parsed.show_ends);
        assert!(parsed.show_tabs);
        assert!(parsed.show_nonprinting);
        assert!(parsed.squeeze_blank);
    }

    #[test]
    fn parse_short_cat_flags_rejects_unknown_combined_flags() {
        let mut parsed = CatArgs {
            io_mode: super::IOMode::Auto,
            report_gbps: false,
            number: false,
            number_nonblank: false,
            show_ends: false,
            show_tabs: false,
            show_nonprinting: false,
            squeeze_blank: false,
            files: Vec::new(),
        };
        let err = parse_short_cat_flags("-nx", &mut parsed).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("unsupported cat flag -x"));
    }

    #[test]
    fn parse_cat_args_honors_double_dash_and_combined_flags() {
        let args = vec![
            "cat".to_string(),
            "-ben".to_string(),
            "--".to_string(),
            "--show-all".to_string(),
            "-".to_string(),
        ];
        let parsed = parse_cat_args(&args).unwrap();
        assert!(matches!(parsed.io_mode, super::IOMode::Auto));
        assert!(parsed.number);
        assert!(parsed.number_nonblank);
        assert!(parsed.show_ends);
        assert!(!parsed.show_tabs);
        assert!(parsed.show_nonprinting);
        assert!(!parsed.squeeze_blank);
        assert_eq!(
            parsed.files,
            vec!["--show-all".to_string(), "-".to_string()]
        );
    }
}
