use crate::coreutils::{stdin_buf_reader, stdout_buf_writer, StreamInput};
use std::fs;
use std::io::{self, Read};

#[derive(Clone, Copy)]
pub(super) struct CheckOptions {
    pub quiet: bool,
    pub status_only: bool,
    pub warn: bool,
    pub strict: bool,
    pub ignore_missing: bool,
}

pub(super) struct ManifestEntry {
    pub expected: String,
    pub path: String,
}

pub(super) fn is_check_behavior_flag(arg: &str) -> bool {
    matches!(
        arg,
        "--quiet" | "--status" | "-w" | "--warn" | "--strict" | "--ignore-missing"
    )
}

pub(super) fn parse_check_options(args: &[String]) -> CheckOptions {
    CheckOptions {
        quiet: args.iter().any(|arg| arg == "--quiet"),
        status_only: args.iter().any(|arg| arg == "--status"),
        warn: args
            .iter()
            .any(|arg| matches!(arg.as_str(), "-w" | "--warn")),
        strict: args.iter().any(|arg| arg == "--strict"),
        ignore_missing: args.iter().any(|arg| arg == "--ignore-missing"),
    }
}

pub(super) fn escaped_hash_check_display(label: &str) -> String {
    if label.contains('\n') {
        format!("\\{}", super::escape_hash_sum_label(label))
    } else {
        label.to_string()
    }
}

pub(crate) fn hash_check_should_print_result(
    success: bool,
    quiet: bool,
    status_only: bool,
) -> bool {
    !status_only && (!success || !quiet)
}

pub(super) fn hash_check_should_report_malformed_line(warn: bool, status_only: bool) -> bool {
    warn && !status_only
}

pub(super) fn hash_check_exit_code(
    had_failure: bool,
    malformed_lines: usize,
    strict: bool,
    no_verified_files: bool,
) -> i32 {
    if had_failure || (strict && malformed_lines != 0) || no_verified_files {
        1
    } else {
        0
    }
}

pub(super) fn is_not_found_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::NotFound || err.raw_os_error() == Some(libc::ENOENT)
}

fn input_label(input: &StreamInput) -> &str {
    match input {
        StreamInput::File(file) => file.as_str(),
        StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
    }
}

fn read_manifest_input(input: &StreamInput) -> io::Result<String> {
    match input {
        StreamInput::File(file) => fs::read_to_string(file),
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            let mut text = String::new();
            reader.read_to_string(&mut text)?;
            Ok(text)
        }
    }
}

pub(super) fn run_manifest_check<P, C>(
    program: &str,
    check_name: &str,
    inputs: &[StreamInput],
    options: CheckOptions,
    parse_line: P,
    compute_actual: C,
) -> io::Result<i32>
where
    P: Fn(&str) -> Option<ManifestEntry>,
    C: Fn(&ManifestEntry) -> io::Result<String>,
{
    if inputs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing checksum file operand",
        ));
    }
    let mut out = stdout_buf_writer()?;
    let mut had_failure = false;
    let mut had_checksum_failure = false;
    let mut had_valid_line = false;
    let mut verified_files = 0usize;
    let mut unread_files = 0usize;
    let mut malformed_lines = 0usize;
    let mut no_valid_input = None::<String>;
    for input in inputs {
        let mut input_had_valid_line = false;
        let data = read_manifest_input(input)?;
        for (line_no, line) in data.lines().enumerate() {
            let Some(entry) = parse_line(line) else {
                malformed_lines += 1;
                if hash_check_should_report_malformed_line(options.warn, options.status_only) {
                    fro::cio_eprintln!(
                        "{program}: {}: {}: improperly formatted {check_name} checksum line",
                        input_label(input),
                        line_no + 1
                    );
                }
                continue;
            };
            had_valid_line = true;
            input_had_valid_line = true;
            let actual = match compute_actual(&entry) {
                Ok(actual) => actual,
                Err(err) if options.ignore_missing && is_not_found_error(&err) => continue,
                Err(err) if is_not_found_error(&err) => {
                    unread_files += 1;
                    had_failure = true;
                    let display_path = escaped_hash_check_display(&entry.path);
                    if hash_check_should_print_result(false, options.quiet, options.status_only) {
                        out.write_all(format!("{display_path}: FAILED open or read\n").as_bytes())?;
                    }
                    fro::cio_eprintln!("{program}: {display_path}: No such file or directory");
                    continue;
                }
                Err(err) => return Err(err),
            };
            let success = actual == entry.expected;
            if !success {
                had_failure = true;
                had_checksum_failure = true;
            } else {
                verified_files += 1;
            }
            if hash_check_should_print_result(success, options.quiet, options.status_only) {
                let status = if success { "OK" } else { "FAILED" };
                let display_path = escaped_hash_check_display(&entry.path);
                out.write_all(format!("{display_path}: {status}\n").as_bytes())?;
            }
        }
        if !input_had_valid_line {
            no_valid_input = Some(input_label(input).to_string());
            break;
        }
    }
    out.into_inner()?;
    if let Some(input_label) = no_valid_input {
        fro::cio_eprintln!(
            "{program}: {input_label}: no properly formatted {check_name} checksum lines found"
        );
        return Ok(1);
    }
    let no_verified_files = options.ignore_missing && had_valid_line && verified_files == 0;
    if malformed_lines != 0 && had_valid_line && !options.status_only {
        let phrase = if malformed_lines == 1 {
            "line is"
        } else {
            "lines are"
        };
        fro::cio_eprintln!("{program}: WARNING: {malformed_lines} {phrase} improperly formatted");
    }
    if unread_files != 0 && !options.status_only {
        let phrase = if unread_files == 1 {
            "listed file could not be read"
        } else {
            "listed files could not be read"
        };
        fro::cio_eprintln!("{program}: WARNING: {unread_files} {phrase}");
    }
    if had_checksum_failure && !options.status_only {
        fro::cio_eprintln!("{program}: WARNING: 1 computed checksum did NOT match");
    }
    if no_verified_files && !options.status_only {
        fro::cio_eprintln!(
            "{program}: {}: no file was verified",
            input_label(&inputs[0])
        );
    }
    Ok(hash_check_exit_code(
        had_failure,
        malformed_lines,
        options.strict,
        no_verified_files,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_check_display_only_reescapes_newlines() {
        assert_eq!(
            escaped_hash_check_display("dir\\line\nfile.txt"),
            "\\dir\\\\line\\nfile.txt"
        );
        assert_eq!(escaped_hash_check_display("dir\\line.txt"), "dir\\line.txt");
    }

    #[test]
    fn hash_check_print_policy_matches_gnu_quiet_and_status_rules() {
        assert!(hash_check_should_print_result(true, false, false));
        assert!(!hash_check_should_print_result(true, true, false));
        assert!(hash_check_should_print_result(false, true, false));
        assert!(!hash_check_should_print_result(true, false, true));
        assert!(!hash_check_should_print_result(false, false, true));
    }

    #[test]
    fn hash_check_malformed_line_policy_matches_warn_and_status_rules() {
        assert!(hash_check_should_report_malformed_line(true, false));
        assert!(!hash_check_should_report_malformed_line(false, false));
        assert!(!hash_check_should_report_malformed_line(true, true));
    }

    #[test]
    fn hash_check_exit_code_matches_failure_and_strict_rules() {
        assert_eq!(hash_check_exit_code(false, 0, false, false), 0);
        assert_eq!(hash_check_exit_code(false, 1, false, false), 0);
        assert_eq!(hash_check_exit_code(false, 1, true, false), 1);
        assert_eq!(hash_check_exit_code(true, 0, false, false), 1);
        assert_eq!(hash_check_exit_code(true, 1, true, false), 1);
        assert_eq!(hash_check_exit_code(false, 0, false, true), 1);
    }
}
