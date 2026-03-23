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

fn cat_write_transformed_line<W: Write>(
    out: &mut W,
    line: &[u8],
    number: bool,
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
    if number {
        let (updated_next_line_number, _, emitted_line_number) =
            cat_numbering_step(*next_line_number, true, line[0]).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "cat line number overflow")
            })?;
        *next_line_number = updated_next_line_number;
        if let Some(line_number) = emitted_line_number {
            write!(out, "{line_number:>6}\t")?;
        }
    }
    out.write_all(line)
}

pub(super) fn run_cat(args: &[String]) -> io::Result<()> {
    let mut io_mode = IOMode::Auto;
    let mut number = false;
    let mut squeeze_blank = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-n" | "--number" => number = true,
            "-s" | "--squeeze-blank" => squeeze_blank = true,
            other => files.push(other.to_string()),
        }
    }
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    if number || squeeze_blank {
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
    use super::{cat_numbering_step, cat_squeeze_blank_step};

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
}

#[cfg(test)]
mod tests {
    use super::{cat_numbering_step, cat_squeeze_blank_step};

    #[test]
    fn cat_numbering_step_numbers_only_at_line_starts() {
        assert_eq!(cat_numbering_step(1, true, b'a').unwrap(), (2, false, Some(1)));
        assert_eq!(cat_numbering_step(2, false, b'b').unwrap(), (2, false, None));
        assert_eq!(cat_numbering_step(2, false, b'\n').unwrap(), (2, true, None));
        assert_eq!(cat_numbering_step(2, true, b'\n').unwrap(), (3, true, Some(2)));
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
}
