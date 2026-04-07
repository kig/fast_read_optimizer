use super::*;

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
                    eprintln!("tac: unrecognized option '{arg}'");
                    eprintln!("Try 'tac --help' for more information.");
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
    let out = stdout_buf_writer()?;
    for input in &inputs {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let mut parts = data
            .split_inclusive(|&byte| byte == b'\n')
            .collect::<Vec<_>>();
        if parts.is_empty() && !data.is_empty() {
            parts.push(data.as_slice());
        }
        for part in parts.into_iter().rev() {
            out.write_all(part)?;
        }
    }
    out.into_inner()
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
}
