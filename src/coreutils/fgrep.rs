use super::*;
use std::borrow::Cow;

#[derive(Clone, Copy)]
struct FgrepOptions {
    count_only: bool,
    print_line_numbers: bool,
    line_regexp: bool,
    ignore_case: bool,
    invert_match: bool,
}

#[derive(Clone)]
struct FgrepPattern {
    raw: Vec<u8>,
    normalized: Vec<u8>,
}

enum PatternSource {
    Inline(String),
    File(String),
}

const FGREP_STREAM_OUTPUT_BATCH_BYTES: usize = 1 << 20;

pub(super) fn fgrep_short_flag_effect(flag: u8) -> Option<bool> {
    match flag {
        b'F' => Some(true),
        _ => None,
    }
}

pub(super) fn fgrep_line_number_prefix(print_line_numbers: bool, line_no: u64) -> Option<u64> {
    print_line_numbers.then_some(line_no)
}

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

fn normalize_case<'a>(bytes: &'a [u8], ignore_case: bool) -> Cow<'a, [u8]> {
    if ignore_case {
        Cow::Owned(bytes.iter().map(u8::to_ascii_lowercase).collect())
    } else {
        Cow::Borrowed(bytes)
    }
}

fn fgrep_line_matches(
    line: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    options: FgrepOptions,
) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_newline(line)
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    if options.line_regexp {
        normalized_line.as_ref() == normalized_pattern
    } else if pattern.is_empty() {
        true
    } else {
        Finder::new(normalized_pattern)
            .find(normalized_line.as_ref())
            .is_some()
    }
}

fn fgrep_line_matches_any(line: &[u8], patterns: &[FgrepPattern], options: FgrepOptions) -> bool {
    let candidate = if options.line_regexp {
        trim_trailing_newline(line)
    } else {
        line
    };
    let normalized_line = normalize_case(candidate, options.ignore_case);
    let normalized_line = normalized_line.as_ref();
    patterns.iter().any(|pattern| {
        if options.line_regexp {
            normalized_line == pattern.normalized.as_slice()
        } else if pattern.raw.is_empty() {
            true
        } else {
            Finder::new(pattern.normalized.as_slice())
                .find(normalized_line)
                .is_some()
        }
    })
}

fn fgrep_select_line(is_match: bool, options: FgrepOptions) -> bool {
    if options.invert_match {
        !is_match
    } else {
        is_match
    }
}

fn parse_pattern_file_bytes(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut patterns = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            patterns.push(bytes[start..index].to_vec());
            start = index + 1;
        }
    }
    if start < bytes.len() {
        patterns.push(bytes[start..].to_vec());
    }
    patterns
}

fn compile_patterns(
    sources: Vec<PatternSource>,
    ignore_case: bool,
) -> io::Result<Vec<FgrepPattern>> {
    let mut patterns = Vec::new();
    for source in sources {
        match source {
            PatternSource::Inline(pattern) => {
                patterns.push(FgrepPattern {
                    normalized: normalize_case(pattern.as_bytes(), ignore_case).into_owned(),
                    raw: pattern.into_bytes(),
                });
            }
            PatternSource::File(path) => {
                let bytes = fs::read(&path)?;
                patterns.extend(parse_pattern_file_bytes(&bytes).into_iter().map(|pattern| {
                    let normalized = normalize_case(&pattern, ignore_case).into_owned();
                    FgrepPattern {
                        raw: pattern,
                        normalized,
                    }
                }));
            }
        }
    }
    Ok(patterns)
}

fn parse_option_value(
    args: &[String],
    index: &mut usize,
    value: Option<&str>,
    option_name: &str,
) -> io::Result<String> {
    if let Some(value) = value {
        return Ok(value.to_string());
    }
    *index += 1;
    args.get(*index).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: option '{option_name}' requires an argument"),
        )
    })
}

fn write_matching_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    line: &[u8],
    line_no: u64,
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<()> {
    if multi_file {
        if let Some(label) = label {
            write!(out, "{label}:")?;
        }
    }
    if let Some(number) = fgrep_line_number_prefix(options.print_line_numbers, line_no) {
        write!(out, "{number}:")?;
    }
    out.write_all(line)?;
    if !line.ends_with(b"\n") {
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn write_count_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    count: u64,
    multi_file: bool,
) -> io::Result<()> {
    if multi_file {
        if let Some(label) = label {
            write!(out, "{label}:")?;
        }
    }
    writeln!(out, "{count}")?;
    Ok(())
}

struct BufferedMatchOutput {
    bytes: Vec<u8>,
}

impl BufferedMatchOutput {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(FGREP_STREAM_OUTPUT_BATCH_BYTES),
        }
    }

    fn flush<W: Write>(&mut self, out: &mut W) -> io::Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        out.write_all(&self.bytes)?;
        self.bytes.clear();
        Ok(())
    }

    fn write_matching_line<W: Write>(
        &mut self,
        out: &mut W,
        label: Option<&str>,
        line: &[u8],
        line_no: u64,
        multi_file: bool,
        options: FgrepOptions,
    ) -> io::Result<()> {
        if self.bytes.len() >= FGREP_STREAM_OUTPUT_BATCH_BYTES {
            self.flush(out)?;
        }
        if multi_file {
            if let Some(label) = label {
                write!(&mut self.bytes, "{label}:")?;
            }
        }
        if let Some(number) = fgrep_line_number_prefix(options.print_line_numbers, line_no) {
            write!(&mut self.bytes, "{number}:")?;
        }
        self.bytes.extend_from_slice(line);
        if !line.ends_with(b"\n") {
            self.bytes.push(b'\n');
        }
        if self.bytes.len() >= FGREP_STREAM_OUTPUT_BATCH_BYTES {
            self.flush(out)?;
        }
        Ok(())
    }

    fn write_count_line<W: Write>(
        &mut self,
        out: &mut W,
        label: Option<&str>,
        count: u64,
        multi_file: bool,
    ) -> io::Result<()> {
        if self.bytes.len() >= FGREP_STREAM_OUTPUT_BATCH_BYTES {
            self.flush(out)?;
        }
        if multi_file {
            if let Some(label) = label {
                write!(&mut self.bytes, "{label}:")?;
            }
        }
        writeln!(&mut self.bytes, "{count}")?;
        if self.bytes.len() >= FGREP_STREAM_OUTPUT_BATCH_BYTES {
            self.flush(out)?;
        }
        Ok(())
    }
}

fn finish_pending_line<W: Write>(
    out: &mut W,
    label: Option<&str>,
    pending_line: &mut Vec<u8>,
    buffered_output: &mut BufferedMatchOutput,
    pattern: &[u8],
    normalized_pattern: &[u8],
    line_no: u64,
    multi_file: bool,
    options: FgrepOptions,
    matched_any: &mut bool,
    match_count: &mut u64,
) -> io::Result<()> {
    if fgrep_select_line(
        fgrep_line_matches(pending_line, pattern, normalized_pattern, options),
        options,
    ) {
        *matched_any = true;
        *match_count += 1;
        if !options.count_only {
            buffered_output.write_matching_line(
                out,
                label,
                pending_line,
                line_no,
                multi_file,
                options,
            )?;
        }
    }
    pending_line.clear();
    Ok(())
}

fn write_matching_stream_lines<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    if options.line_regexp {
        let mut matched_any = false;
        let mut match_count = 0_u64;
        let mut pending_line = Vec::new();
        let mut buffered_output = BufferedMatchOutput::new();
        let mut line_no = 1_u64;
        visit_ordered_input(input, io_mode, |block| {
            let mut line_start = 0usize;
            for rel_end in memchr_iter(b'\n', block) {
                let line_end = rel_end + 1;
                pending_line.extend_from_slice(&block[line_start..line_end]);
                finish_pending_line(
                    out,
                    label,
                    &mut pending_line,
                    &mut buffered_output,
                    pattern,
                    normalized_pattern,
                    line_no,
                    multi_file,
                    options,
                    &mut matched_any,
                    &mut match_count,
                )?;
                line_no += 1;
                line_start = line_end;
            }
            if line_start < block.len() {
                pending_line.extend_from_slice(&block[line_start..]);
            }
            Ok::<_, io::Error>(())
        })?;
        if !pending_line.is_empty() {
            finish_pending_line(
                out,
                label,
                &mut pending_line,
                &mut buffered_output,
                pattern,
                normalized_pattern,
                line_no,
                multi_file,
                options,
                &mut matched_any,
                &mut match_count,
            )?;
        }
        if options.count_only {
            buffered_output.write_count_line(out, label, match_count, multi_file)?;
        }
        buffered_output.flush(out)?;
        return Ok(matched_any);
    }

    let finder = Finder::new(normalized_pattern);
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut pending_line = Vec::new();
    let mut buffered_output = BufferedMatchOutput::new();
    let mut pending_line_has_match = pattern.is_empty();
    let mut boundary_tail = Vec::new();
    let mut line_no = 1_u64;
    visit_ordered_input(input, io_mode, |block| {
        let normalized_block = normalize_case(block, options.ignore_case);
        let search_block = normalized_block.as_ref();
        let block_matches = if pattern.is_empty() {
            Vec::new()
        } else {
            finder.find_iter(search_block).collect::<Vec<_>>()
        };
        let mut next_match = 0usize;
        if !pattern.is_empty() && !boundary_tail.is_empty() {
            let prefix_len = search_block.len().min(pattern.len().saturating_sub(1));
            if prefix_len > 0 {
                let mut boundary = Vec::with_capacity(boundary_tail.len() + prefix_len);
                boundary.extend_from_slice(&boundary_tail);
                boundary.extend_from_slice(&search_block[..prefix_len]);
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
                if fgrep_select_line(pending_line_has_match, options) {
                    matched_any = true;
                    match_count += 1;
                    if !options.count_only {
                        buffered_output
                            .write_matching_line(out, label, line, line_no, multi_file, options)?;
                    }
                }
            } else {
                pending_line.extend_from_slice(&block[line_start..line_end]);
                if fgrep_select_line(pending_line_has_match, options) {
                    matched_any = true;
                    match_count += 1;
                    if !options.count_only {
                        buffered_output.write_matching_line(
                            out,
                            label,
                            &pending_line,
                            line_no,
                            multi_file,
                            options,
                        )?;
                    }
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
            let tail_len = pattern.len().saturating_sub(1).min(search_block.len());
            boundary_tail.clear();
            boundary_tail.extend_from_slice(&search_block[search_block.len() - tail_len..]);
        }
        Ok::<_, io::Error>(())
    })?;
    if !pending_line.is_empty() && fgrep_select_line(pending_line_has_match, options) {
        matched_any = true;
        match_count += 1;
        if !options.count_only {
            buffered_output.write_matching_line(
                out,
                label,
                &pending_line,
                line_no,
                multi_file,
                options,
            )?;
        }
    }
    if options.count_only {
        buffered_output.write_count_line(out, label, match_count, multi_file)?;
    }
    buffered_output.flush(out)?;
    Ok(matched_any)
}

fn write_matching_stream_lines_multi<W: Write>(
    out: &mut W,
    label: Option<&str>,
    input: &StreamInput,
    io_mode: IOMode,
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut pending_line = Vec::new();
    let mut buffered_output = BufferedMatchOutput::new();
    let mut line_no = 1_u64;
    visit_ordered_input(input, io_mode, |block| {
        let mut line_start = 0usize;
        for rel_end in memchr_iter(b'\n', block) {
            let line_end = rel_end + 1;
            pending_line.extend_from_slice(&block[line_start..line_end]);
            if fgrep_select_line(
                fgrep_line_matches_any(&pending_line, patterns, options),
                options,
            ) {
                matched_any = true;
                match_count += 1;
                if !options.count_only {
                    buffered_output.write_matching_line(
                        out,
                        label,
                        &pending_line,
                        line_no,
                        multi_file,
                        options,
                    )?;
                }
            }
            pending_line.clear();
            line_no += 1;
            line_start = line_end;
        }
        if line_start < block.len() {
            pending_line.extend_from_slice(&block[line_start..]);
        }
        Ok::<_, io::Error>(())
    })?;
    if !pending_line.is_empty()
        && fgrep_select_line(
            fgrep_line_matches_any(&pending_line, patterns, options),
            options,
        )
    {
        matched_any = true;
        match_count += 1;
        if !options.count_only {
            buffered_output.write_matching_line(
                out,
                label,
                &pending_line,
                line_no,
                multi_file,
                options,
            )?;
        }
    }
    if options.count_only {
        buffered_output.write_count_line(out, label, match_count, multi_file)?;
    }
    buffered_output.flush(out)?;
    Ok(matched_any)
}

fn write_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    matches: &[u64],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let bytes = data;
    let mut next_match = 0usize;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    let mut matched_any = false;
    let mut match_count = 0_u64;
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
        if fgrep_select_line(matched, options) {
            matched_any = true;
            match_count += 1;
            if !options.count_only {
                write_matching_line(
                    out,
                    Some(file),
                    &bytes[line_start..line_end],
                    line_no,
                    multi_file,
                    options,
                )?;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, multi_file)?;
    }
    Ok(matched_any)
}

fn write_line_regexp_matches<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(
            fgrep_line_matches(line, pattern, normalized_pattern, options),
            options,
        ) {
            matched_any = true;
            match_count += 1;
            if !options.count_only {
                write_matching_line(out, Some(file), line, line_no, multi_file, options)?;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, multi_file)?;
    }
    Ok(matched_any)
}

fn write_filtered_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    pattern: &[u8],
    normalized_pattern: &[u8],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(
            fgrep_line_matches(line, pattern, normalized_pattern, options),
            options,
        ) {
            matched_any = true;
            match_count += 1;
            if !options.count_only {
                write_matching_line(out, Some(file), line, line_no, multi_file, options)?;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, multi_file)?;
    }
    Ok(matched_any)
}

fn write_filtered_lines_multi<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    patterns: &[FgrepPattern],
    multi_file: bool,
    options: FgrepOptions,
) -> io::Result<bool> {
    let mut matched_any = false;
    let mut match_count = 0_u64;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < data.len() {
        let rel_end = data[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(data.len() - line_start);
        let line_end = line_start + rel_end;
        let line = &data[line_start..line_end];
        if fgrep_select_line(fgrep_line_matches_any(line, patterns, options), options) {
            matched_any = true;
            match_count += 1;
            if !options.count_only {
                write_matching_line(out, Some(file), line, line_no, multi_file, options)?;
            }
        }
        line_start = line_end;
        line_no += 1;
    }
    if options.count_only {
        write_count_line(out, Some(file), match_count, multi_file)?;
    }
    Ok(matched_any)
}

pub(super) fn run_fgrep(args: &[String]) -> io::Result<i32> {
    let mut io_mode = IOMode::Auto;
    let mut options = FgrepOptions {
        count_only: false,
        print_line_numbers: false,
        line_regexp: false,
        ignore_case: false,
        invert_match: false,
    };
    let mut pattern_sources = Vec::new();
    let mut positional_pattern = None::<String>;
    let mut files = Vec::new();
    let mut end_flags = false;
    let mut index = 1usize;
    while index < args.len() {
        let arg = &args[index];
        if end_flags {
            if positional_pattern.is_none() && pattern_sources.is_empty() {
                positional_pattern = Some(arg.clone());
            } else {
                files.push(arg.clone());
            }
            index += 1;
            continue;
        }
        match arg.as_str() {
            "-c" => options.count_only = true,
            "--count" => options.count_only = true,
            "-n" => options.print_line_numbers = true,
            "--line-number" => options.print_line_numbers = true,
            "-x" => options.line_regexp = true,
            "--line-regexp" => options.line_regexp = true,
            "-i" => options.ignore_case = true,
            "--ignore-case" => options.ignore_case = true,
            "--no-ignore-case" => options.ignore_case = false,
            "-v" => options.invert_match = true,
            "--invert-match" => options.invert_match = true,
            "-e" => pattern_sources.push(PatternSource::Inline(parse_option_value(
                args, &mut index, None, "-e",
            )?)),
            "--regexp" => pattern_sources.push(PatternSource::Inline(parse_option_value(
                args, &mut index, None, "--regexp",
            )?)),
            "-f" => pattern_sources.push(PatternSource::File(parse_option_value(
                args, &mut index, None, "-f",
            )?)),
            "--file" => pattern_sources.push(PatternSource::File(parse_option_value(
                args, &mut index, None, "--file",
            )?)),
            "--fixed-strings" => {}
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--" => end_flags = true,
            other => {
                if let Some(value) = other.strip_prefix("--regexp=") {
                    pattern_sources.push(PatternSource::Inline(value.to_string()));
                    index += 1;
                    continue;
                }
                if let Some(value) = other.strip_prefix("--file=") {
                    pattern_sources.push(PatternSource::File(value.to_string()));
                    index += 1;
                    continue;
                }
                if let Some(value) = other.strip_prefix("-e") {
                    if !value.is_empty() {
                        pattern_sources.push(PatternSource::Inline(value.to_string()));
                        index += 1;
                        continue;
                    }
                }
                if let Some(value) = other.strip_prefix("-f") {
                    if !value.is_empty() {
                        pattern_sources.push(PatternSource::File(value.to_string()));
                        index += 1;
                        continue;
                    }
                }
                if let [b'-', flag] = other.as_bytes() {
                    if fgrep_short_flag_effect(*flag).is_some() {
                        index += 1;
                        continue;
                    }
                }
                if positional_pattern.is_none() && pattern_sources.is_empty() {
                    positional_pattern = Some(other.to_string());
                } else {
                    files.push(other.to_string());
                }
            }
        }
        index += 1;
    }
    if let Some(pattern) = positional_pattern {
        pattern_sources.push(PatternSource::Inline(pattern));
    }
    if pattern_sources.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep requires a search pattern",
        ));
    }
    let patterns = compile_patterns(pattern_sources, options.ignore_case)?;
    let use_single_pattern_fast_path = patterns.len() == 1;
    let inputs = parse_stream_inputs(files);
    if patterns.is_empty() {
        for input in &inputs {
            if let StreamInput::File(file) = input {
                let _ = is_regular_input_path(file)?;
            }
        }
        return Ok(1);
    }
    let pattern = &patterns[0];
    let mut out = stdout_buf_writer()?;
    let mut matched_any = false;
    let multi_file = inputs.len() > 1;
    let config = load_config(None);
    for input in inputs {
        match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => {
                if options.line_regexp || options.ignore_case || !use_single_pattern_fast_path {
                    let data = load_file_bytes(file, io_mode, "read_to_memory")?;
                    matched_any |= if use_single_pattern_fast_path {
                        if options.line_regexp {
                            write_line_regexp_matches(
                                &mut out,
                                file,
                                data.data.as_slice(),
                                pattern.raw.as_slice(),
                                pattern.normalized.as_slice(),
                                multi_file,
                                options,
                            )?
                        } else {
                            write_filtered_lines(
                                &mut out,
                                file,
                                data.data.as_slice(),
                                pattern.raw.as_slice(),
                                pattern.normalized.as_slice(),
                                multi_file,
                                options,
                            )?
                        }
                    } else {
                        write_filtered_lines_multi(
                            &mut out,
                            file,
                            data.data.as_slice(),
                            &patterns,
                            multi_file,
                            options,
                        )?
                    };
                } else {
                    let (matches, _) = grep_match_offsets_for_mode(
                        &config,
                        "grep",
                        file,
                        internal_io_mode(io_mode),
                        pattern.raw.as_slice(),
                    )?;
                    if matches.is_empty() {
                        if !options.invert_match {
                            if options.count_only {
                                write_count_line(&mut out, Some(file), 0, multi_file)?;
                            }
                            continue;
                        }
                    }
                    let data = load_file_bytes(file, io_mode, "read_to_memory")?;
                    matched_any |= write_matching_lines(
                        &mut out,
                        file,
                        data.data.as_slice(),
                        &matches,
                        multi_file,
                        options,
                    )?;
                }
            }
            StreamInput::File(file) => {
                matched_any |= if use_single_pattern_fast_path {
                    write_matching_stream_lines(
                        &mut out,
                        Some(file),
                        &input,
                        io_mode,
                        pattern.raw.as_slice(),
                        pattern.normalized.as_slice(),
                        multi_file,
                        options,
                    )?
                } else {
                    write_matching_stream_lines_multi(
                        &mut out,
                        Some(file),
                        &input,
                        io_mode,
                        &patterns,
                        multi_file,
                        options,
                    )?
                };
            }
            StreamInput::Stdin { label } => {
                matched_any |= if use_single_pattern_fast_path {
                    write_matching_stream_lines(
                        &mut out,
                        label.as_deref(),
                        &input,
                        io_mode,
                        pattern.raw.as_slice(),
                        pattern.normalized.as_slice(),
                        multi_file,
                        options,
                    )?
                } else {
                    write_matching_stream_lines_multi(
                        &mut out,
                        label.as_deref(),
                        &input,
                        io_mode,
                        &patterns,
                        multi_file,
                        options,
                    )?
                };
            }
        }
    }
    out.into_inner()?;
    Ok(if matched_any { 0 } else { 1 })
}

#[cfg(kani)]
mod kani_proofs {
    use super::{
        fgrep_line_matches, fgrep_line_number_prefix, fgrep_select_line, fgrep_short_flag_effect,
        FgrepOptions,
    };

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

    #[kani::proof]
    fn fgrep_line_matches_line_regexp_trims_one_newline() {
        let payload = [b'a', b'\n'];
        let options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: true,
            ignore_case: false,
            invert_match: false,
        };
        assert!(fgrep_line_matches(&payload, b"a", b"a", options));
        assert!(!fgrep_line_matches(&payload, b"a\n", b"a\n", options));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        fgrep_line_matches, fgrep_line_matches_any, fgrep_short_flag_effect,
        parse_pattern_file_bytes, BufferedMatchOutput, FgrepOptions, FgrepPattern,
    };
    use std::io::{self, Write};

    #[derive(Default)]
    struct CountingWriter {
        writes: usize,
        bytes: Vec<u8>,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn fgrep_short_flag_effect_maps_fixed_strings_flag() {
        assert_eq!(fgrep_short_flag_effect(b'F'), Some(true));
        assert_eq!(fgrep_short_flag_effect(b'n'), None);
    }

    #[test]
    fn fgrep_line_matches_honors_line_regexp() {
        let options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: true,
            ignore_case: false,
            invert_match: false,
        };
        assert!(fgrep_line_matches(b"alpha\n", b"alpha", b"alpha", options));
        assert!(fgrep_line_matches(b"alpha", b"alpha", b"alpha", options));
        assert!(!fgrep_line_matches(
            b"alpha beta\n",
            b"alpha",
            b"alpha",
            options
        ));
        assert!(!fgrep_line_matches(
            b"alpha\n", b"alpha\n", b"alpha\n", options
        ));
    }

    #[test]
    fn fgrep_line_matches_honors_ignore_case() {
        let contains_options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: false,
            ignore_case: true,
            invert_match: false,
        };
        assert!(fgrep_line_matches(
            b"Alpha beta\n",
            b"alpha",
            b"alpha",
            contains_options
        ));
        assert!(!fgrep_line_matches(
            b"beta\n",
            b"alpha",
            b"alpha",
            contains_options
        ));

        let line_options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: true,
            ignore_case: true,
            invert_match: false,
        };
        assert!(fgrep_line_matches(
            b"Alpha\n",
            b"alpha",
            b"alpha",
            line_options
        ));
        assert!(!fgrep_line_matches(
            b"Alpha beta\n",
            b"alpha",
            b"alpha",
            line_options
        ));
    }

    #[test]
    fn parse_pattern_file_bytes_ignores_trailing_newline() {
        assert_eq!(
            parse_pattern_file_bytes(b"alpha\nbeta\n"),
            vec![b"alpha".to_vec(), b"beta".to_vec()]
        );
        assert_eq!(
            parse_pattern_file_bytes(b"\nalpha\n\n"),
            vec![Vec::new(), b"alpha".to_vec(), Vec::new()]
        );
    }

    #[test]
    fn fgrep_line_matches_any_checks_all_patterns() {
        let options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: false,
            ignore_case: true,
            invert_match: false,
        };
        let patterns = vec![
            FgrepPattern {
                raw: b"needle".to_vec(),
                normalized: b"needle".to_vec(),
            },
            FgrepPattern {
                raw: b"omega".to_vec(),
                normalized: b"omega".to_vec(),
            },
        ];
        assert!(fgrep_line_matches_any(b"OMEGA\n", &patterns, options));
        assert!(fgrep_line_matches_any(b"needle beta\n", &patterns, options));
        assert!(!fgrep_line_matches_any(b"alpha\n", &patterns, options));
    }

    #[test]
    fn buffered_match_output_batches_multiple_lines_into_one_write() {
        let options = FgrepOptions {
            count_only: false,
            print_line_numbers: false,
            line_regexp: false,
            ignore_case: false,
            invert_match: false,
        };
        let mut writer = CountingWriter::default();
        let mut buffered = BufferedMatchOutput::new();

        buffered
            .write_matching_line(&mut writer, None, b"alpha\n", 1, false, options)
            .unwrap();
        buffered
            .write_matching_line(&mut writer, None, b"beta", 2, false, options)
            .unwrap();
        buffered.flush(&mut writer).unwrap();

        assert_eq!(writer.writes, 1);
        assert_eq!(writer.bytes, b"alpha\nbeta\n");
    }

    #[test]
    fn buffered_match_output_preserves_prefixes_in_batched_output() {
        let options = FgrepOptions {
            count_only: false,
            print_line_numbers: true,
            line_regexp: false,
            ignore_case: false,
            invert_match: false,
        };
        let mut writer = CountingWriter::default();
        let mut buffered = BufferedMatchOutput::new();

        buffered
            .write_matching_line(&mut writer, Some("stdin"), b"alpha\n", 7, true, options)
            .unwrap();
        buffered
            .write_count_line(&mut writer, Some("stdin"), 1, true)
            .unwrap();
        buffered.flush(&mut writer).unwrap();

        assert_eq!(writer.bytes, b"stdin:7:alpha\nstdin:1\n");
    }
}
