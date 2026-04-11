use super::check::{self, ManifestEntry};
use super::*;
use std::fs;
use std::io;

struct CksumOptions {
    io_mode: IOMode,
    report_gbps: bool,
    check: bool,
    check_options: check::CheckOptions,
    inputs: Vec<StreamInput>,
}

fn parse_cksum_options(args: &[String]) -> io::Result<CksumOptions> {
    let mut io_mode = IOMode::Auto;
    let mut report_gbps = false;
    let mut check = false;
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
                "--report-gbps" => {
                    report_gbps = true;
                    continue;
                }
                "-c" | "--check" => {
                    check = true;
                    continue;
                }
                other if check::is_check_behavior_flag(other) => continue,
                "--" => {
                    parse_options = false;
                    continue;
                }
                "-" => {}
                other if other.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported cksum flag: {other}"),
                    ));
                }
                _ => {}
            }
        }
        files.push(arg.clone());
    }
    Ok(CksumOptions {
        io_mode,
        report_gbps,
        check,
        check_options: check::parse_check_options(&args[1..]),
        inputs: parse_stream_inputs(files),
    })
}

fn parse_cksum_check_line(line: &str) -> Option<ManifestEntry> {
    let first_space = line.find(' ')?;
    let rest = &line[(first_space + 1)..];
    let second_space = rest.find(' ')?;
    let crc = &line[..first_space];
    let bytes = &rest[..second_space];
    let path = &rest[(second_space + 1)..];
    if path.is_empty() {
        return None;
    }
    crc.parse::<u32>().ok()?;
    bytes.parse::<u64>().ok()?;
    Some(ManifestEntry {
        expected: format!("{crc} {bytes}"),
        path: path.to_string(),
    })
}

fn cksum_input(input: &StreamInput, io_mode: IOMode) -> io::Result<(u32, u64)> {
    match input {
        StreamInput::File(file) if is_regular_input_path(file)? => Ok((
            u32::from_be_bytes(
                hash_file(file, HashAlgorithm::CRC32, io_mode)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| io::Error::other("unexpected CRC32 digest length"))?,
            ),
            fs::metadata(file)?.len(),
        )),
        StreamInput::Stdin { .. } => match regular_stdin_path()? {
            Some(path) => Ok((
                u32::from_be_bytes(
                    hash_file(&path, HashAlgorithm::CRC32, io_mode)?
                        .as_slice()
                        .try_into()
                        .map_err(|_| io::Error::other("unexpected CRC32 digest length"))?,
                ),
                fs::metadata(path)?.len(),
            )),
            None => cksum_stream_input(input, io_mode),
        },
        _ => cksum_stream_input(input, io_mode),
    }
}

fn run_cksum_check(options: &CksumOptions) -> io::Result<i32> {
    check::run_manifest_check(
        hash_sum_program_name(HashAlgorithm::CRC32),
        "cksum",
        &options.inputs,
        options.check_options,
        parse_cksum_check_line,
        |path| {
            let (crc, bytes) = cksum_input(&StreamInput::File(path.to_string()), options.io_mode)?;
            Ok(format!("{crc} {bytes}"))
        },
    )
}

pub(crate) fn run_cksum(args: &[String]) -> io::Result<i32> {
    let options = parse_cksum_options(args)?;
    if options.check {
        return run_cksum_check(&options);
    }
    let started_at = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    for input in options.inputs {
        let (crc, bytes) = cksum_input(&input, options.io_mode)?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("cksum byte count overflow"))?;
        match input {
            StreamInput::File(file) => fro::cio_println!("{} {} {}", crc, bytes, file),
            StreamInput::Stdin { label: Some(label) } => {
                fro::cio_println!("{} {} {}", crc, bytes, label)
            }
            StreamInput::Stdin { label: None } => fro::cio_println!("{} {}", crc, bytes),
        }
    }
    if options.report_gbps {
        report_gbps("cksum", total_bytes, started_at);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cksum_check_line_accepts_space_prefixed_paths() {
        let entry = parse_cksum_check_line("123 456   leading space.txt").unwrap();
        assert_eq!(entry.expected, "123 456");
        assert_eq!(entry.path, "  leading space.txt");
    }

    #[test]
    fn parse_cksum_options_supports_check_flags_and_double_dash() {
        let args = vec![
            "cksum".to_string(),
            "--quiet".to_string(),
            "--check".to_string(),
            "--".to_string(),
            "--manifest".to_string(),
        ];

        let options = parse_cksum_options(&args).unwrap();
        assert!(options.check);
        assert!(options.check_options.quiet);
        assert_eq!(
            options.inputs,
            vec![StreamInput::File("--manifest".to_string())]
        );
    }
}
