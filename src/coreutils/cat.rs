use super::*;
use memchr::memchr;

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

pub(super) fn run_cat(args: &[String]) -> io::Result<()> {
    let mut io_mode = IOMode::Auto;
    let mut number = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-n" | "--number" => number = true,
            other => files.push(other.to_string()),
        }
    }
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    if number {
        let mut next_line_number = 1u64;
        let mut at_line_start = true;
        for input in inputs {
            visit_ordered_input(&input, io_mode, |block| {
                let mut offset = 0usize;
                while offset < block.len() {
                    let step =
                        cat_numbering_step(next_line_number, at_line_start, block[offset])
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "cat line number overflow",
                                )
                            })?;
                    next_line_number = step.0;
                    at_line_start = step.1;
                    if let Some(line_number) = step.2 {
                        write!(out, "{line_number:>6}\t")?;
                    }
                    let segment_end = match memchr(b'\n', &block[offset..]) {
                        Some(pos) => offset + pos + 1,
                        None => block.len(),
                    };
                    out.write_all(&block[offset..segment_end])?;
                    at_line_start = segment_end > offset && block[segment_end - 1] == b'\n';
                    offset = segment_end;
                }
                Ok(())
            })?;
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
    use super::cat_numbering_step;

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
}

#[cfg(test)]
mod tests {
    use super::cat_numbering_step;

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
}
