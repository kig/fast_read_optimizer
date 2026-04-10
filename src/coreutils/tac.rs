use super::*;
use memchr::memrchr;

const TAC_SIMPLE_STDOUT_MAX_BYTES: u64 = 64 * 1024;

fn parse_tac_args(args: &[String]) -> io::Result<(IOMode, Vec<String>)> {
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut parse_options = true;

    for arg in &args[1..] {
        if parse_options {
            match arg.as_str() {
                "--auto" => {
                    io_mode = IOMode::Auto;
                    continue;
                }
                "--direct" => {
                    io_mode = IOMode::Direct;
                    continue;
                }
                "--no-direct" => {
                    io_mode = IOMode::PageCache;
                    continue;
                }
                "--" => {
                    parse_options = false;
                    continue;
                }
                _ if arg.starts_with('-') && arg != "-" => {
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
    }

    Ok((io_mode, files))
}

pub(super) fn run_tac(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_tac_args(args)?;
    let inputs = parse_stream_inputs(files);
    let mut out = TacOutput::for_inputs(&inputs)?;
    for input in &inputs {
        write_reversed_input(&mut out, input, io_mode)?;
    }
    out.finish()
}

fn write_reversed_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
) -> io::Result<()> {
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            let data = load_file_bytes(path, io_mode, "read_to_memory")?;
            write_reversed_lines(out, data.data.as_slice())
        }
        _ => {
            let data = loaded_or_stream_bytes(input, io_mode)?;
            write_reversed_lines(out, &data)
        }
    }
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
    fn parse_tac_args_supports_double_dash_for_dash_prefixed_files() {
        let args = vec![
            "tac".to_string(),
            "--direct".to_string(),
            "--".to_string(),
            "--auto".to_string(),
            "-file".to_string(),
            "-".to_string(),
        ];

        let (io_mode, files) = parse_tac_args(&args).unwrap();
        assert!(matches!(io_mode, IOMode::Direct));
        assert_eq!(files, vec!["--auto", "-file", "-"]);
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
