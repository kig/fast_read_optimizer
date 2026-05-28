use super::line_matching::normalize_case;
use super::*;

pub(super) fn parse_pattern_file_bytes(bytes: &[u8]) -> Vec<Vec<u8>> {
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

pub(super) fn load_exclude_from(glob_filter: &mut FgrepGlobFilter, path: &str) -> io::Result<()> {
    let bytes = fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("fgrep: --exclude-from: {path}: {e}")))?;
    for line in parse_pattern_file_bytes(&bytes) {
        if !line.is_empty() {
            glob_filter.exclude.push(FgrepGlobFilter::parse_glob(
                "--exclude-from",
                &String::from_utf8_lossy(&line),
            )?);
        }
    }
    Ok(())
}

pub(super) fn compile_patterns(
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

pub(super) fn parse_option_value(
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

pub(super) fn parse_max_count_value(value: &str) -> io::Result<u64> {
    value.parse::<u64>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: invalid max count '{value}': {err}"),
        )
    })
}

pub(super) fn parse_context_count_value(value: &str) -> io::Result<u64> {
    value.parse::<u64>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: invalid context count '{value}': {err}"),
        )
    })
}

pub(super) fn fgrep_conflicting_matchers_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, FGREP_CONFLICTING_MATCHERS)
}

pub(super) fn fgrep_is_conflicting_matchers_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::InvalidInput && err.to_string() == FGREP_CONFLICTING_MATCHERS
}

pub(super) fn parse_binary_files_value(value: &str) -> io::Result<FgrepBinaryMode> {
    match value {
        "binary" => Ok(FgrepBinaryMode::Binary),
        "text" => Ok(FgrepBinaryMode::Text),
        "without-match" => Ok(FgrepBinaryMode::WithoutMatch),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown binary-files type",
        )),
    }
}

pub(super) fn parse_color_mode_value(
    value: Option<&str>,
    option_name: &str,
) -> io::Result<FgrepColorMode> {
    match value {
        None => Ok(FgrepColorMode::Auto),
        Some("always") => Ok(FgrepColorMode::Always),
        Some("auto") => Ok(FgrepColorMode::Auto),
        Some("never") => Ok(FgrepColorMode::Never),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: invalid color mode for '{option_name}'"),
        )),
    }
}
