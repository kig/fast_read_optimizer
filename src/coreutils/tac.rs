use super::*;
use memchr::{memmem, memrchr};
use regex::bytes::Regex;

const TAC_SIMPLE_STDOUT_MAX_BYTES: u64 = 64 * 1024;

struct TacOptions {
    io_mode: IOMode,
    before: bool,
    separator: TacSeparator,
    files: Vec<String>,
}

enum TacSeparator {
    Newline,
    Literal(Vec<u8>),
    Regex(Regex),
}

impl TacSeparator {
    fn is_fast_newline_path(&self, before: bool) -> bool {
        !before && matches!(self, Self::Newline)
    }
}

fn parse_tac_args(args: &[String]) -> io::Result<TacOptions> {
    let mut io_mode = IOMode::Auto;
    let mut before = false;
    let mut regex = false;
    let mut separator: Option<String> = None;
    let mut files = Vec::new();
    let mut parse_options = true;
    let mut index = 1;

    while index < args.len() {
        let arg = &args[index];
        if parse_options {
            match arg.as_str() {
                "--auto" => {
                    io_mode = IOMode::Auto;
                    index += 1;
                    continue;
                }
                "--direct" => {
                    io_mode = IOMode::Direct;
                    index += 1;
                    continue;
                }
                "--no-direct" => {
                    io_mode = IOMode::PageCache;
                    index += 1;
                    continue;
                }
                "-b" | "--before" => {
                    before = true;
                    index += 1;
                    continue;
                }
                "-r" | "--regex" => {
                    regex = true;
                    index += 1;
                    continue;
                }
                "-s" | "--separator" => {
                    index += 1;
                    let Some(value) = args.get(index) else {
                        return tac_missing_option_argument(arg);
                    };
                    separator = Some(value.clone());
                    index += 1;
                    continue;
                }
                "--" => {
                    parse_options = false;
                    index += 1;
                    continue;
                }
                _ if arg.starts_with('-') && arg != "-" => {
                    if let Some(value) = arg.strip_prefix("--separator=") {
                        separator = Some(value.to_string());
                        index += 1;
                        continue;
                    }
                    fro::cio_eprintln!("tac: unrecognized option '{arg}'");
                    fro::cio_eprintln!("Try 'tac --help' for more information.");
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unrecognized option",
                    ));
                }
                _ => {}
            }
        }
        files.push(arg.clone());
        index += 1;
    }

    Ok(TacOptions {
        io_mode,
        before,
        separator: build_tac_separator(separator, regex)?,
        files,
    })
}

fn tac_missing_option_argument(option: &str) -> io::Result<TacOptions> {
    fro::cio_eprintln!("tac: option '{option}' requires an argument");
    fro::cio_eprintln!("Try 'tac --help' for more information.");
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "missing option argument",
    ))
}

fn build_tac_separator(separator: Option<String>, regex: bool) -> io::Result<TacSeparator> {
    let separator = separator.unwrap_or_else(|| "\n".to_string());
    if regex {
        if separator.is_empty() {
            fro::cio_eprintln!("tac: separator cannot be empty");
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "separator cannot be empty",
            ));
        }
        let pattern = format!(r"\A(?:{separator})");
        let regex = Regex::new(&pattern).map_err(|_| {
            fro::cio_eprintln!("tac: Invalid regular expression");
            io::Error::new(io::ErrorKind::InvalidInput, "invalid regular expression")
        })?;
        Ok(TacSeparator::Regex(regex))
    } else if separator == "\n" {
        Ok(TacSeparator::Newline)
    } else {
        Ok(TacSeparator::Literal(separator.into_bytes()))
    }
}

pub(super) fn run_tac(args: &[String]) -> io::Result<()> {
    let options = parse_tac_args(args)?;
    let inputs = parse_stream_inputs(options.files.clone());
    let mut out = TacOutput::for_inputs(&inputs)?;
    for input in &inputs {
        write_reversed_input(&mut out, input, &options)?;
    }
    out.finish()
}

fn write_reversed_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    options: &TacOptions,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let data = load_file_bytes(path, options.io_mode, "read_to_memory")?;
            write_reversed_data(
                out,
                data.data.as_slice(),
                &options.separator,
                options.before,
            )
        }
        _ => {
            let data = loaded_or_stream_bytes(input, options.io_mode)?;
            write_reversed_data(out, &data, &options.separator, options.before)
        }
    }
}

fn write_reversed_data<W: Write>(
    out: &mut W,
    data: &[u8],
    separator: &TacSeparator,
    before: bool,
) -> io::Result<()> {
    if separator.is_fast_newline_path(before) {
        return write_reversed_lines(out, data);
    }
    if matches!(separator, TacSeparator::Literal(bytes) if bytes.is_empty()) {
        out.write_all(data)?;
        return Ok(());
    }

    let mut chunk_end = data.len();
    while chunk_end > 0 {
        let chunk_start = if before {
            find_before_chunk_start(data, chunk_end, separator)
        } else {
            find_after_chunk_start(data, chunk_end, separator)
        };
        out.write_all(&data[chunk_start..chunk_end])?;
        chunk_end = chunk_start;
    }
    Ok(())
}

fn write_reversed_lines<W: Write>(out: &mut W, data: &[u8]) -> io::Result<()> {
    let mut line_end = data.len();
    while line_end > 0 {
        let search_end = if data[line_end - 1] == b'\n' {
            line_end - 1
        } else {
            line_end
        };
        let line_start = memrchr(b'\n', &data[..search_end]).map_or(0, |offset| offset + 1);
        out.write_all(&data[line_start..line_end])?;
        line_end = line_start;
    }
    Ok(())
}

fn find_after_chunk_start(data: &[u8], chunk_end: usize, separator: &TacSeparator) -> usize {
    let mut max_start = chunk_end.saturating_sub(1);
    loop {
        let Some((separator_start, separator_end)) =
            find_last_separator(data, chunk_end, max_start, separator)
        else {
            return 0;
        };
        if separator_end == chunk_end {
            if separator_start == 0 {
                return 0;
            }
            max_start = separator_start - 1;
            continue;
        }
        return separator_end;
    }
}

fn find_before_chunk_start(data: &[u8], chunk_end: usize, separator: &TacSeparator) -> usize {
    if chunk_end == 0 {
        return 0;
    }
    find_last_separator(data, chunk_end, chunk_end - 1, separator)
        .map_or(0, |(separator_start, _)| separator_start)
}

fn find_last_separator(
    data: &[u8],
    chunk_end: usize,
    max_start: usize,
    separator: &TacSeparator,
) -> Option<(usize, usize)> {
    match separator {
        TacSeparator::Newline => {
            let search_end = chunk_end.min(max_start.saturating_add(2));
            memrchr(b'\n', &data[..search_end]).map(|start| (start, start + 1))
        }
        TacSeparator::Literal(needle) => {
            if needle.is_empty() || chunk_end < needle.len() {
                return None;
            }
            let search_end = chunk_end.min(max_start.saturating_add(needle.len()));
            memmem::rfind(&data[..search_end], needle).map(|start| (start, start + needle.len()))
        }
        TacSeparator::Regex(regex) => {
            for start in (0..=max_start).rev() {
                if let Some(relative_end) = regex.shortest_match(&data[start..chunk_end]) {
                    return Some((start, start + relative_end));
                }
            }
            None
        }
    }
}

enum TacOutput {
    Buffered(BufWriter),
    Stdout(std::fs::File),
}

impl TacOutput {
    fn for_inputs(inputs: &[StreamInput]) -> io::Result<Self> {
        if tac_prefers_simple_stdout(inputs)? {
            Ok(Self::Stdout(fro::command_io::stdout_file()?))
        } else {
            Ok(Self::Buffered(stdout_buf_writer()?))
        }
    }

    fn finish(self) -> io::Result<()> {
        match self {
            Self::Buffered(out) => out.into_inner(),
            Self::Stdout(mut out) => out.flush(),
        }
    }
}

impl Write for TacOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Buffered(out) => out.write(buf),
            Self::Stdout(out) => out.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Buffered(out) => out.flush(),
            Self::Stdout(out) => out.flush(),
        }
    }
}

fn tac_prefers_simple_stdout(inputs: &[StreamInput]) -> io::Result<bool> {
    let mut total_bytes = 0_u64;
    for input in inputs {
        let StreamInput::File(path) = input else {
            return Ok(false);
        };
        let metadata = fs::metadata(path)?;
        if !metadata.file_type().is_file() {
            return Ok(false);
        }
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .ok_or_else(|| io::Error::other("tac input byte count overflow"))?;
        if total_bytes > TAC_SIMPLE_STDOUT_MAX_BYTES {
            return Ok(false);
        }
    }
    Ok(!inputs.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tac_args_supports_separator_bundle_and_double_dash() {
        let args = vec![
            "tac".to_string(),
            "--direct".to_string(),
            "-b".to_string(),
            "--regex".to_string(),
            "--separator=ab+".to_string(),
            "--".to_string(),
            "--auto".to_string(),
            "-file".to_string(),
            "-".to_string(),
        ];

        let options = parse_tac_args(&args).unwrap();
        assert!(matches!(options.io_mode, IOMode::Direct));
        assert!(options.before);
        assert!(matches!(options.separator, TacSeparator::Regex(_)));
        assert_eq!(options.files, vec!["--auto", "-file", "-"]);
    }

    #[test]
    fn parse_tac_args_rejects_unknown_options_before_double_dash() {
        let args = vec!["tac".to_string(), "--bogus".to_string()];

        let err = parse_tac_args(&args)
            .err()
            .expect("expected option parse failure");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn parse_tac_args_rejects_empty_regex_separator() {
        let args = vec![
            "tac".to_string(),
            "--regex".to_string(),
            "--separator=".to_string(),
        ];

        let err = parse_tac_args(&args)
            .err()
            .expect("expected empty separator failure");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn write_reversed_lines_handles_trailing_newline() {
        let mut out = Vec::new();
        write_reversed_lines(&mut out, b"a\nb\n").unwrap();
        assert_eq!(out, b"b\na\n");
    }

    #[test]
    fn write_reversed_lines_handles_missing_trailing_newline() {
        let mut out = Vec::new();
        write_reversed_lines(&mut out, b"a\nb").unwrap();
        assert_eq!(out, b"ba\n");
    }

    #[test]
    fn write_reversed_lines_preserves_blank_lines() {
        let mut out = Vec::new();
        write_reversed_lines(&mut out, b"a\n\nb\n").unwrap();
        assert_eq!(out, b"b\n\na\n");
    }

    #[test]
    fn write_reversed_data_supports_literal_separator_bundle() {
        let mut out = Vec::new();
        write_reversed_data(
            &mut out,
            b"a::b::c::",
            &TacSeparator::Literal(b"::".to_vec()),
            false,
        )
        .unwrap();
        assert_eq!(out, b"c::b::a::");
    }

    #[test]
    fn write_reversed_data_supports_before_separator_bundle() {
        let mut out = Vec::new();
        write_reversed_data(
            &mut out,
            b"a::b::c",
            &TacSeparator::Literal(b"::".to_vec()),
            true,
        )
        .unwrap();
        assert_eq!(out, b"::c::ba");
    }

    #[test]
    fn write_reversed_data_supports_regex_separator_bundle() {
        let mut out = Vec::new();
        let separator = build_tac_separator(Some("[0-9][0-9]*".to_string()), true).unwrap();
        write_reversed_data(&mut out, b"a12b345c", &separator, false).unwrap();
        assert_eq!(out, b"c54b32a1");
    }

    #[test]
    fn write_reversed_data_keeps_empty_literal_separator_identity() {
        let mut out = Vec::new();
        write_reversed_data(&mut out, b"abc", &TacSeparator::Literal(Vec::new()), true).unwrap();
        assert_eq!(out, b"abc");
    }

    #[test]
    fn tac_prefers_simple_stdout_for_tiny_regular_files() {
        let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fro-tac-unit-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("tiny.txt");
        std::fs::write(&path, b"a\nb\n").unwrap();

        assert!(tac_prefers_simple_stdout(&[StreamInput::File(
            path.to_string_lossy().into_owned(),
        )])
        .unwrap());
    }

    #[test]
    fn tac_prefers_simple_stdout_rejects_stdin_inputs() {
        assert!(!tac_prefers_simple_stdout(&[StreamInput::Stdin { label: None }]).unwrap());
    }
}
