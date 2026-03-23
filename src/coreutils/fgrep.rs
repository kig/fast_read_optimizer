use super::*;

pub(super) fn fgrep_short_flag_effect(flag: u8) -> Option<bool> {
    match flag {
        b'F' => Some(true),
        _ => None,
    }
}

fn write_matching_stream_lines<R: BufRead, W: Write>(
    out: &mut W,
    label: Option<&str>,
    reader: &mut R,
    pattern: &[u8],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<bool> {
    let finder = Finder::new(pattern);
    let mut matched_any = false;
    let mut line = Vec::new();
    let mut line_no = 1_u64;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(matched_any);
        }
        if finder.find(&line).is_some() {
            matched_any = true;
            if multi_file {
                if let Some(label) = label {
                    write!(out, "{label}:")?;
                }
            }
            if print_line_numbers {
                write!(out, "{line_no}:")?;
            }
            out.write_all(&line)?;
        }
        line_no += 1;
    }
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
            if print_line_numbers {
                write!(out, "{}:", line_no)?;
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
        match input {
            StreamInput::File(file) if is_regular_input_path(&file)? => {
                let (matches, _) = grep_match_offsets_for_mode(
                    &config,
                    "grep",
                    &file,
                    internal_io_mode(io_mode),
                    pattern.as_bytes(),
                )?;
                if matches.is_empty() {
                    continue;
                }
                matched_any = true;
                let data = load_file_bytes(&file, io_mode, "read_to_memory")?;
                write_matching_lines(
                    &mut out,
                    &file,
                    data.data.as_slice(),
                    &matches,
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::File(file) => {
                let mut reader = BufReader::new(std::fs::File::open(&file)?);
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    Some(&file),
                    &mut reader,
                    pattern.as_bytes(),
                    multi_file,
                    print_line_numbers,
                )?;
            }
            StreamInput::Stdin { label } => {
                let mut reader = stdin_buf_reader()?;
                matched_any |= write_matching_stream_lines(
                    &mut out,
                    label.as_deref(),
                    &mut reader,
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
    use super::fgrep_short_flag_effect;

    #[kani::proof]
    fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
        let flag: u8 = kani::any();
        let expected = match flag {
            b'F' => Some(true),
            _ => None,
        };
        assert_eq!(fgrep_short_flag_effect(flag), expected);
    }
}

#[cfg(test)]
mod tests {
    use super::fgrep_short_flag_effect;

    #[test]
    fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
        assert_eq!(fgrep_short_flag_effect(b'F'), Some(true));
        assert_eq!(fgrep_short_flag_effect(b'n'), None);
    }
}
