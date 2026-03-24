use super::*;

pub(super) fn fgrep_short_flag_effect(flag: u8) -> Option<bool> {
    match flag {
        b'F' => Some(true),
        _ => None,
    }
}

pub(super) fn fgrep_line_number_prefix(print_line_numbers: bool, line_no: u64) -> Option<u64> {
    print_line_numbers.then_some(line_no)
}

fn write_matching_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<()> {
    if multi_file {
        if let Some(label) = label {
            write!(out, "{label}:")?;
        }
    }
    if let Some(number) = fgrep_line_number_prefix(print_line_numbers, line_no) {
        write!(out, "{number}:")?;
    }
    out.write_all(line)
}

fn write_matching_stream_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    pattern: &[u8],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<bool> {
    let finder = Finder::new(pattern);
    let mut matched_any = false;
    let mut pending_line = Vec::new();
    let mut pending_line_has_match = pattern.is_empty();
    let mut boundary_tail = Vec::new();
    let mut line_no = 1_u64;
    visit_ordered_input(input, io_mode, |block| {
        let block_matches = finder.find_iter(block).collect::<Vec<_>>();
        let mut next_match = 0usize;
        if !pattern.is_empty() && !boundary_tail.is_empty() {
            let prefix_len = block.len().min(pattern.len().saturating_sub(1));
            if prefix_len > 0 {
                let mut boundary = Vec::with_capacity(boundary_tail.len() + prefix_len);
                boundary.extend_from_slice(&boundary_tail);
                boundary.extend_from_slice(&block[..prefix_len]);
                pending_line_has_match |= finder.find_iter(&boundary).any(|offset| {
                    offset < boundary_tail.len() && offset + pattern.len() > boundary_tail.len()
                });
            }
        }
        let mut line_start = 0usize;
        for rel_end in memchr_iter(b'\n', block) {
            let line_end = rel_end + 1;
            while next_match < block_matches.len() && block_matches[next_match] < line_end {
                if block_matches[next_match] >= line_start {
                    pending_line_has_match = true;
                }
                next_match += 1;
            }
            if pending_line.is_empty() {
                let line = &block[line_start..line_end];
                if pending_line_has_match {
                    matched_any = true;
                    write_matching_line(out, label, line, line_no, multi_file, print_line_numbers)?;
                }
            } else {
                pending_line.extend_from_slice(&block[line_start..line_end]);
                if pending_line_has_match {
                    matched_any = true;
                    write_matching_line(
                        out,
                        label,
                        &pending_line,
                        line_no,
                        multi_file,
                        print_line_numbers,
                    )?;
                }
                pending_line.clear();
            }
            pending_line_has_match = pattern.is_empty();
            line_no += 1;
            line_start = line_end;
        }
        if line_start < block.len() {
            pending_line.extend_from_slice(&block[line_start..]);
            while next_match < block_matches.len() {
                pending_line_has_match = true;
                next_match += 1;
            }
        }
        if pattern.is_empty() {
            boundary_tail.clear();
        } else {
            let tail_len = pattern.len().saturating_sub(1).min(block.len());
            boundary_tail.clear();
            boundary_tail.extend_from_slice(&block[block.len() - tail_len..]);
        }
        Ok::<_, io::Error>(())
    })?;
    if !pending_line.is_empty() && pending_line_has_match {
        matched_any = true;
        write_matching_line(
            out,
            label,
            &pending_line,
            line_no,
            multi_file,
            print_line_numbers,
        )?;
    }
    Ok(matched_any)
}

fn write_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    matches: &[u64],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<()> {
    let bytes = data;
    let mut next_match = 0usize;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < bytes.len() {
        let rel_end = bytes[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(bytes.len() - line_start);
        let line_end = line_start + rel_end;
        let mut matched = false;
        while next_match < matches.len() && matches[next_match] < line_end as u64 {
            if matches[next_match] >= line_start as u64 {
                matched = true;
            }
            next_match += 1;
        }
        if matched {
            if multi_file {
                write!(out, "{}:", file)?;
            }
            if let Some(number) = fgrep_line_number_prefix(print_line_numbers, line_no) {
                write!(out, "{number}:")?;
            }
            out.write_all(&bytes[line_start..line_end])?;
        }
        line_start = line_end;
        line_no += 1;
    }
    Ok(())
}

pub(super) fn run_fgrep(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut print_line_numbers = false;
    let mut pattern = None::<String>;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-n" => print_line_numbers = true,
            "--line-number" => print_line_numbers = true,
            "--fixed-strings" => {}
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => {
                if let [b'-', flag] = other.as_bytes() {
                    if fgrep_short_flag_effect(*flag).is_some() {
                        continue;
                    }
                }
                if pattern.is_none() {
                    pattern = Some(other.to_string());
                } else {
                    files.push(other.to_string());
                }
            }
        }
    }
    let pattern = pattern.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep requires a search pattern",
        )
    })?;
    let inputs = parse_stream_inputs(files);
    let mut out = stdout_buf_writer()?;
    let mut matched_any = false;
    let multi_file = inputs.len() > 1;
    let config = load_config(None);
    for input in inputs {
        match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => {
                let (matches, _) = grep_match_offsets_for_mode(
                    &config,
                    "grep",
                    file,
                    internal_io_mode(io_mode),
                    pattern.as_bytes(),
                )?;
                if matches.is_empty() {
                    continue;
                }
                matched_any = true;
                let data = load_file_bytes(file, io_mode, "read_to_memory")?;
                write_matching_lines(
                    &mut out,
                    file,
                    data.data.as_slice(),
                    &matches,
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::File(file) => {
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    Some(file),
                    &input,
                    io_mode,
                    pattern.as_bytes(),
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::Stdin { label } => {
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    label.as_deref(),
                    &input,
                    io_mode,
                    pattern.as_bytes(),
                    multi_file,
                    print_line_numbers,
                )?;
            }
        }
    }
    out.into_inner()?;
    Ok(if matched_any { 0 } else { 1 })
}

#[cfg(kani)]
mod kani_proofs {
    use super::{fgrep_line_number_prefix, fgrep_short_flag_effect};

    #[kani::proof]
    fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
        let flag: u8 = kani::any();
        let expected = match flag {
            b'F' => Some(true),
            _ => None,
        };
        assert_eq!(fgrep_short_flag_effect(flag), expected);
    }

    #[kani::proof]
    fn fgrep_line_number_prefix_matches_boolean_gate() {
        let print_line_numbers: bool = kani::any();
        let line_no: u64 = kani::any();
        assert_eq!(
            fgrep_line_number_prefix(print_line_numbers, line_no),
            if print_line_numbers {
                Some(line_no)
            } else {
                None
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{fgrep_line_number_prefix, fgrep_short_flag_effect};

    #[test]
    fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
        assert_eq!(fgrep_short_flag_effect(b'F'), Some(true));
        assert_eq!(fgrep_short_flag_effect(b'n'), None);
    }

    #[test]
    fn fgrep_line_number_prefix_matches_boolean_gate() {
        assert_eq!(fgrep_line_number_prefix(false, 7), None);
        assert_eq!(fgrep_line_number_prefix(true, 7), Some(7));
    }
}
