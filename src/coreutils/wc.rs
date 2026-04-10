use super::*;

mod count;

use self::count::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WcExecutionBackend {
    MetadataFastPath,
    MappedBlocks,
    FdParallel,
}

#[derive(Debug, PartialEq, Eq)]
struct WcInputs {
    inputs: Vec<StreamInput>,
    requested_count: usize,
    exit_code: i32,
}

fn wc_os_error_message(err: &io::Error) -> String {
    err.raw_os_error()
        .map(|errno| {
            unsafe { CStr::from_ptr(libc::strerror(errno)) }
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| err.to_string())
}

fn wc_error_label(input: &StreamInput) -> &str {
    match input {
        StreamInput::File(path) => path.as_str(),
        StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
    }
}

fn read_files0_inputs<R: Read>(reader: &mut R, reject_stdin_name: bool) -> io::Result<WcInputs> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;

    let mut inputs = Vec::new();
    let mut requested_count = 0usize;
    let mut exit_code = 0;
    let mut start = 0usize;
    while start < bytes.len() {
        let end = bytes[start..]
            .iter()
            .position(|&byte| byte == 0)
            .map(|offset| start + offset)
            .unwrap_or(bytes.len());
        let name = &bytes[start..end];
        requested_count += 1;
        if name == b"-" {
            if reject_stdin_name {
                fro::cio_eprintln!("wc: when reading file names from stdin, no file name of '-' allowed");
                exit_code = 1;
            } else {
                inputs.push(StreamInput::Stdin {
                    label: Some("-".to_string()),
                });
            }
        } else {
            inputs.push(StreamInput::File(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }
        start = end.saturating_add(1);
    }

    Ok(WcInputs {
        inputs,
        requested_count,
        exit_code,
    })
}

fn wc_inputs_from_args(
    files: Vec<String>,
    files0_from: Option<String>,
) -> io::Result<Result<WcInputs, i32>> {
    if let Some(files0_from) = files0_from {
        if !files.is_empty() {
            fro::cio_eprintln!("wc: extra operand '{}'", files[0]);
            fro::cio_eprintln!("file operands cannot be combined with --files0-from");
            fro::cio_eprintln!("Try 'wc --help' for more information.");
            return Ok(Err(1));
        }
        if files0_from == "-" {
            return read_files0_inputs(&mut fro::command_io::stdin_file()?, true).map(Ok);
        }
        let mut list_file = match std::fs::File::open(&files0_from) {
            Ok(file) => file,
            Err(err) => {
                fro::cio_eprintln!(
                    "wc: cannot open '{}' for reading: {}",
                    files0_from,
                    wc_os_error_message(&err)
                );
                return Ok(Err(1));
            }
        };
        return read_files0_inputs(&mut list_file, false).map(Ok);
    }

    let inputs = parse_stream_inputs(files);
    Ok(Ok(WcInputs {
        requested_count: inputs.len(),
        inputs,
        exit_code: 0,
    }))
}

fn wc_totals_for_input(
    input: &StreamInput,
    options: WcCountOptions,
    config: &crate::config::LoadedConfig,
    io_mode: IOMode,
) -> io::Result<WcTotals> {
    match wc_execution_backend(input, options)? {
        WcExecutionBackend::MetadataFastPath => {
            return wc_metadata_totals(input, options)?.ok_or_else(|| {
                io::Error::other("wc metadata backend selected without metadata totals")
            });
        }
        WcExecutionBackend::MappedBlocks => {
            let StreamInput::File(file) = input else {
                return Err(io::Error::other(
                    "wc mapped-block backend selected for non-file input",
                ));
            };
            let count_options = WcCountOptions {
                max_line_length: false,
                ..options
            };
            let blocks = map_file_blocks_for_mode(
                config,
                "read",
                file,
                internal_io_mode(io_mode),
                move |block| Ok::<_, io::Error>(count_wc_block(block.data, count_options)),
            )?;
            Ok(reduce_wc_counts(&blocks.blocks))
        }
        WcExecutionBackend::FdParallel => match input {
            StreamInput::File(file) => {
                let mut reader = std::fs::File::open(file)?;
                wc_totals_from_fd_parallel(&mut reader, options, config, io_mode)
            }
            StreamInput::Stdin { .. } => {
                wc_totals_from_fd_parallel(
                    &mut fro::command_io::stdin_file()?,
                    options,
                    config,
                    io_mode,
                )
            }
        },
    }
}

fn wc_execution_backend(
    input: &StreamInput,
    options: WcCountOptions,
) -> io::Result<WcExecutionBackend> {
    if options.bytes
        && !options.lines
        && !options.words
        && !options.chars
        && !options.max_line_length
        && matches!(input, StreamInput::File(path) if is_regular_input_path(path)?)
    {
        return Ok(WcExecutionBackend::MetadataFastPath);
    }

    match input {
        StreamInput::File(file)
            if !options.chars && !options.max_line_length && is_regular_input_path(file)? =>
        {
            Ok(WcExecutionBackend::MappedBlocks)
        }
        StreamInput::File(_) | StreamInput::Stdin { .. } => Ok(WcExecutionBackend::FdParallel),
    }
}

fn apply_wc_short_flag_bundle(
    arg: &str,
    print_lines: &mut bool,
    print_words: &mut bool,
    print_chars: &mut bool,
    print_bytes: &mut bool,
    print_max_line_length: &mut bool,
) -> bool {
    let Some(bundle) = arg.strip_prefix('-') else {
        return false;
    };
    if bundle.is_empty() || bundle.starts_with('-') {
        return false;
    }

    let mut next_lines = *print_lines;
    let mut next_words = *print_words;
    let mut next_chars = *print_chars;
    let mut next_bytes = *print_bytes;
    let mut next_max_line_length = *print_max_line_length;

    for flag in bundle.bytes() {
        match flag {
            b'l' => next_lines = true,
            b'w' => next_words = true,
            b'm' => next_chars = true,
            b'c' => next_bytes = true,
            b'L' => next_max_line_length = true,
            _ => return false,
        }
    }

    *print_lines = next_lines;
    *print_words = next_words;
    *print_chars = next_chars;
    *print_bytes = next_bytes;
    *print_max_line_length = next_max_line_length;
    true
}

pub(super) fn run_wc(args: &[String]) -> io::Result<i32> {
    let mut print_lines = false;
    let mut print_words = false;
    let mut print_chars = false;
    let mut print_bytes = false;
    let mut print_max_line_length = false;
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut files0_from = None::<String>;
    let mut files = Vec::new();
    let mut stop_parsing_flags = false;
    let mut i = 1usize;
    while i < args.len() {
        let arg = args[i].as_str();
        if stop_parsing_flags {
            files.push(arg.to_string());
            i += 1;
            continue;
        }
        match arg {
            "--" => stop_parsing_flags = true,
            "-l" | "--lines" => print_lines = true,
            "-w" | "--words" => print_words = true,
            "-m" | "--chars" => print_chars = true,
            "-c" | "--bytes" => print_bytes = true,
            "-L" | "--max-line-length" => print_max_line_length = true,
            "--files0-from" => {
                i += 1;
                if i >= args.len() {
                    fro::cio_eprintln!("wc: option '--files0-from' requires an argument");
                    fro::cio_eprintln!("Try 'wc --help' for more information.");
                    return Ok(1);
                }
                files0_from = Some(args[i].clone());
            }
            other if other.starts_with("--files0-from=") => {
                files0_from = Some(other["--files0-from=".len()..].to_string());
            }
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--report-gbps" => report_throughput = true,
            other
                if apply_wc_short_flag_bundle(
                    other,
                    &mut print_lines,
                    &mut print_words,
                    &mut print_chars,
                    &mut print_bytes,
                    &mut print_max_line_length,
                ) => {}
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    if !print_lines && !print_words && !print_chars && !print_bytes && !print_max_line_length {
        print_lines = true;
        print_words = true;
        print_bytes = true;
    }
    let options = WcCountOptions {
        lines: print_lines,
        words: print_words,
        chars: print_chars,
        bytes: print_bytes,
        max_line_length: print_max_line_length,
    };

    let parsed_inputs = match wc_inputs_from_args(files, files0_from)? {
        Ok(parsed) => parsed,
        Err(code) => return Ok(code),
    };
    let WcInputs {
        inputs,
        requested_count,
        mut exit_code,
    } = parsed_inputs;
    let started_at = std::time::Instant::now();
    let config = load_config(None);
    let mut out = stdout_buf_writer()?;
    let mut grand_total = WcTotals {
        lines: 0,
        words: 0,
        chars: 0,
        bytes: 0,
        max_line_length: 0,
    };
    let print_total = requested_count > 1;
    for input in inputs {
        let label = match &input {
            StreamInput::File(file) => Some(file.as_str()),
            StreamInput::Stdin { label } => label.as_deref(),
        };
        let totals = match wc_totals_for_input(&input, options, &config, io_mode) {
            Ok(totals) => totals,
            Err(err) => {
                fro::cio_eprintln!(
                    "wc: {}: {}",
                    wc_error_label(&input),
                    wc_os_error_message(&err)
                );
                exit_code = 1;
                continue;
            }
        };
        grand_total.lines += totals.lines;
        grand_total.words += totals.words;
        grand_total.chars += totals.chars;
        grand_total.bytes += totals.bytes;
        grand_total.max_line_length = grand_total.max_line_length.max(totals.max_line_length);
        write_wc_result(
            &mut out,
            totals,
            label,
            print_lines,
            print_words,
            print_chars,
            print_bytes,
            print_max_line_length,
        )?;
    }
    if print_total {
        write_wc_result(
            &mut out,
            grand_total,
            Some("total"),
            print_lines,
            print_words,
            print_chars,
            print_bytes,
            print_max_line_length,
        )?;
    }
    out.into_inner()?;
    if report_throughput {
        report_gbps("wc", grand_total.bytes, started_at);
    }
    Ok(exit_code)
}

#[cfg(test)]
mod tests;
