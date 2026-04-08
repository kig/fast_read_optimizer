use super::*;
use crc_fast::{CrcAlgorithm as FastCrcAlgorithm, Digest as CrcDigest};
use fro::finalize_cksum_crc;
use openssl::hash::Hasher;

fn hash_sum_needs_escape(label: &str) -> bool {
    label.bytes().any(|byte| matches!(byte, b'\\' | b'\n'))
}

fn escape_hash_sum_label(label: &str) -> String {
    let mut escaped = String::with_capacity(label.len());
    for ch in label.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn unescape_hash_sum_label(label: &str) -> Option<String> {
    let mut unescaped = String::with_capacity(label.len());
    let mut chars = label.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            unescaped.push(ch);
            continue;
        }
        match chars.next()? {
            '\\' => unescaped.push('\\'),
            'n' => unescaped.push('\n'),
            _ => return None,
        }
    }
    Some(unescaped)
}

fn escaped_hash_sum_display(label: &str, zero_terminated: bool) -> (bool, String) {
    if zero_terminated || !hash_sum_needs_escape(label) {
        (false, label.to_string())
    } else {
        (true, escape_hash_sum_label(label))
    }
}

fn escaped_hash_check_display(label: &str) -> String {
    if label.contains('\n') {
        format!("\\{}", escape_hash_sum_label(label))
    } else {
        label.to_string()
    }
}

fn regular_stdin_path() -> io::Result<Option<String>> {
    if fd_is_regular(libc::STDIN_FILENO)? {
        Ok(Some("/proc/self/fd/0".to_string()))
    } else {
        Ok(None)
    }
}

fn hash_stream_input(
    input: &StreamInput,
    algorithm: HashAlgorithm,
    io_mode: IOMode,
) -> io::Result<(Vec<u8>, u64)> {
    match algorithm {
        HashAlgorithm::Md5
        | HashAlgorithm::Blake2b512
        | HashAlgorithm::Sha224
        | HashAlgorithm::Sha256
        | HashAlgorithm::Sha384
        | HashAlgorithm::Sha512 => {
            let mut hasher = Hasher::new(ordered_digest(algorithm).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "unsupported digest")
            })?)
            .map_err(io::Error::other)?;
            let bytes = visit_ordered_input_counted(input, io_mode, |block| {
                hasher.update(block).map_err(io::Error::other)
            })?;
            hasher
                .finish()
                .map_err(io::Error::other)
                .map(|d| (d.to_vec(), bytes))
        }
        HashAlgorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            let bytes = visit_ordered_input_counted(input, io_mode, |block| {
                hasher.update(block);
                Ok(())
            })?;
            Ok((hasher.finalize().as_bytes().to_vec(), bytes))
        }
        HashAlgorithm::CRC32 => {
            let mut digest = CrcDigest::new(FastCrcAlgorithm::Crc32Cksum);
            let bytes = visit_ordered_input_counted(input, io_mode, |block| {
                digest.update(block);
                Ok(())
            })?;
            let finalized = finalize_cksum_crc(digest.finalize(), bytes);
            Ok((finalized.to_be_bytes().to_vec(), bytes))
        }
        HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "block hash sums do not support stream input",
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashSumFormat {
    Default,
    Binary,
    Tag,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HashCheckLineKind {
    Tagged,
    UntaggedText,
    UntaggedBinary,
    Invalid,
}

struct HashSumOptions {
    io_mode: IOMode,
    report_gbps: bool,
    format: HashSumFormat,
    zero_terminated: bool,
    check: bool,
    tag_with_check: bool,
    quiet: bool,
    status_only: bool,
    warn: bool,
    strict: bool,
    ignore_missing: bool,
    inputs: Vec<StreamInput>,
}

fn parse_hash_sum_options(args: &[String]) -> io::Result<HashSumOptions> {
    let mut io_mode = IOMode::Auto;
    let mut report_gbps = false;
    let mut format = HashSumFormat::Default;
    let mut zero_terminated = false;
    let mut check = false;
    let mut saw_tag = false;
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
                "-b" | "--binary" => {
                    format = HashSumFormat::Binary;
                    continue;
                }
                "-t" | "--text" => {
                    format = HashSumFormat::Default;
                    continue;
                }
                "--tag" => {
                    saw_tag = true;
                    format = HashSumFormat::Tag;
                    continue;
                }
                "-z" | "--zero" => {
                    zero_terminated = true;
                    continue;
                }
                "-c" | "--check" => {
                    check = true;
                    format = HashSumFormat::Default;
                    continue;
                }
                "--quiet" | "--status" | "-w" | "--warn" | "--strict" | "--ignore-missing" => {
                    continue;
                }
                "--" => {
                    parse_options = false;
                    continue;
                }
                "-" => {}
                other if other.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported hash flag: {other}"),
                    ));
                }
                _ => {}
            }
        }
        files.push(arg.clone());
    }
    Ok(HashSumOptions {
        io_mode,
        report_gbps,
        format,
        zero_terminated,
        check,
        tag_with_check: check && saw_tag,
        quiet: args[1..].iter().any(|arg| arg == "--quiet"),
        status_only: args[1..].iter().any(|arg| arg == "--status"),
        warn: args[1..]
            .iter()
            .any(|arg| matches!(arg.as_str(), "-w" | "--warn")),
        strict: args[1..].iter().any(|arg| arg == "--strict"),
        ignore_missing: args[1..].iter().any(|arg| arg == "--ignore-missing"),
        inputs: parse_stream_inputs(files),
    })
}

fn hash_sum_tag_name(algorithm: HashAlgorithm) -> Option<&'static str> {
    match algorithm {
        HashAlgorithm::Md5 => Some("MD5"),
        HashAlgorithm::Blake2b512 => Some("BLAKE2b"),
        HashAlgorithm::Sha224 => Some("SHA224"),
        HashAlgorithm::Sha256 => Some("SHA256"),
        HashAlgorithm::Sha384 => Some("SHA384"),
        HashAlgorithm::Sha512 => Some("SHA512"),
        HashAlgorithm::Blake3
        | HashAlgorithm::CRC32
        | HashAlgorithm::FroBlockXxh3
        | HashAlgorithm::FroBlockSha256 => None,
    }
}

fn hash_sum_program_name(algorithm: HashAlgorithm) -> &'static str {
    match algorithm {
        HashAlgorithm::Md5 => "md5sum",
        HashAlgorithm::Blake2b512 => "b2sum",
        HashAlgorithm::Sha224 => "sha224sum",
        HashAlgorithm::Sha256 => "sha256sum",
        HashAlgorithm::Sha384 => "sha384sum",
        HashAlgorithm::Sha512 => "sha512sum",
        HashAlgorithm::Blake3 => "b3sum",
        HashAlgorithm::CRC32 => "cksum",
        HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => "hash",
    }
}

fn hash_check_algorithm_name(algorithm: HashAlgorithm) -> &'static str {
    match algorithm {
        HashAlgorithm::Blake2b512 => "BLAKE2",
        _ => hash_sum_tag_name(algorithm).unwrap_or("digest"),
    }
}

pub(super) fn hash_check_untagged_kind(separator: u8, has_filename: bool) -> HashCheckLineKind {
    if !has_filename {
        return HashCheckLineKind::Invalid;
    }
    match separator {
        b' ' => HashCheckLineKind::UntaggedText,
        b'*' => HashCheckLineKind::UntaggedBinary,
        _ => HashCheckLineKind::Invalid,
    }
}

pub(super) fn hash_check_should_print_result(
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

fn is_not_found_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::NotFound || err.raw_os_error() == Some(libc::ENOENT)
}

fn hash_check_line_kind(line: &str) -> HashCheckLineKind {
    if line.is_empty() {
        return HashCheckLineKind::Invalid;
    }
    if let Some((left, _)) = line.split_once(" = ") {
        if left.ends_with(')') && left.contains(" (") {
            return HashCheckLineKind::Tagged;
        }
    }
    let bytes = line.as_bytes();
    if let Some(space_pos) = bytes.iter().position(|&byte| byte == b' ') {
        let has_filename = space_pos + 2 < bytes.len();
        let separator = bytes.get(space_pos + 1).copied().unwrap_or_default();
        return hash_check_untagged_kind(separator, has_filename);
    }
    HashCheckLineKind::Invalid
}

fn write_hash_sum_line(
    out: &mut dyn Write,
    algorithm: HashAlgorithm,
    format: HashSumFormat,
    zero_terminated: bool,
    digest: &[u8],
    label: &str,
) -> io::Result<()> {
    let (escaped, label) = escaped_hash_sum_display(label, zero_terminated);
    let line = match format {
        HashSumFormat::Default => format!(
            "{}{}  {}",
            if escaped { "\\" } else { "" },
            hex_digest(digest),
            label
        ),
        HashSumFormat::Binary => format!(
            "{}{} *{}",
            if escaped { "\\" } else { "" },
            hex_digest(digest),
            label
        ),
        HashSumFormat::Tag => format!(
            "{}{} ({}) = {}",
            if escaped { "\\" } else { "" },
            hash_sum_tag_name(algorithm).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "unsupported tagged digest")
            })?,
            label,
            hex_digest(digest)
        ),
    };
    out.write_all(line.as_bytes())?;
    out.write_all(if zero_terminated { b"\0" } else { b"\n" })
}

fn parse_tagged_hash_check_line<'a>(
    line: &'a str,
    algorithm: HashAlgorithm,
) -> Option<(&'a str, &'a str, bool)> {
    let escaped = line.starts_with('\\');
    let tag = hash_sum_tag_name(algorithm)?;
    let line = if escaped {
        line.strip_prefix('\\')?
    } else {
        line
    };
    let rest = line.strip_prefix(tag)?.strip_prefix(" (")?;
    let (file, digest) = rest.rsplit_once(") = ")?;
    Some((digest, file, escaped))
}

fn parse_hash_check_line(line: &str, algorithm: HashAlgorithm) -> Option<(String, String)> {
    match hash_check_line_kind(line) {
        HashCheckLineKind::Tagged => {
            let (digest, file, escaped) = parse_tagged_hash_check_line(line, algorithm)?;
            let file = if escaped {
                unescape_hash_sum_label(file)?
            } else {
                file.to_string()
            };
            Some((digest.to_string(), file))
        }
        HashCheckLineKind::UntaggedText | HashCheckLineKind::UntaggedBinary => {
            let space_pos = line.as_bytes().iter().position(|&byte| byte == b' ')?;
            let escaped = line.starts_with('\\');
            let digest = if escaped {
                &line[1..space_pos]
            } else {
                &line[..space_pos]
            };
            let file = &line[(space_pos + 2)..];
            let file = if escaped {
                unescape_hash_sum_label(file)?
            } else {
                file.to_string()
            };
            Some((digest.to_string(), file))
        }
        HashCheckLineKind::Invalid => None,
    }
}

fn run_hash_sum_check(options: &HashSumOptions, algorithm: HashAlgorithm) -> io::Result<i32> {
    if options.inputs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing checksum file operand",
        ));
    }
    let out = stdout_buf_writer()?;
    let mut had_failure = false;
    let mut had_checksum_failure = false;
    let mut had_valid_line = false;
    let mut verified_files = 0usize;
    let mut unread_files = 0usize;
    let mut malformed_lines = 0usize;
    let mut no_valid_input = None::<String>;
    let algorithm_name = hash_check_algorithm_name(algorithm);
    for input in &options.inputs {
        let input_label = match input {
            StreamInput::File(file) => file.as_str(),
            StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
        };
        let mut input_had_valid_line = false;
        let data = match input {
            StreamInput::File(ref file) => fs::read_to_string(file)?,
            StreamInput::Stdin { .. } => {
                let mut reader = stdin_buf_reader()?;
                let mut text = String::new();
                reader.read_to_string(&mut text)?;
                text
            }
        };
        for (line_no, line) in data.lines().enumerate() {
            let Some((expected_hex, path)) = parse_hash_check_line(line, algorithm) else {
                malformed_lines += 1;
                if hash_check_should_report_malformed_line(options.warn, options.status_only) {
                    eprintln!(
                        "{}: {}: {}: improperly formatted {} checksum line",
                        hash_sum_program_name(algorithm),
                        input_label,
                        line_no + 1,
                        algorithm_name
                    );
                }
                continue;
            };
            had_valid_line = true;
            input_had_valid_line = true;
            let actual = match hash_file(&path, algorithm, options.io_mode) {
                Ok(actual) => actual,
                Err(err) if options.ignore_missing && is_not_found_error(&err) => continue,
                Err(err) if is_not_found_error(&err) => {
                    unread_files += 1;
                    had_failure = true;
                    let display_path = escaped_hash_check_display(&path);
                    if hash_check_should_print_result(false, options.quiet, options.status_only) {
                        out.write_all(format!("{display_path}: FAILED open or read\n").as_bytes())?;
                    }
                    let message = if is_not_found_error(&err) {
                        "No such file or directory".to_string()
                    } else {
                        err.to_string()
                    };
                    eprintln!(
                        "{}: {}: {}",
                        hash_sum_program_name(algorithm),
                        display_path,
                        message
                    );
                    continue;
                }
                Err(err) => return Err(err),
            };
            let success = hex_digest(&actual) == expected_hex;
            if !success {
                had_failure = true;
                had_checksum_failure = true;
            } else {
                verified_files += 1;
            }
            if hash_check_should_print_result(success, options.quiet, options.status_only) {
                let status = if success { "OK" } else { "FAILED" };
                let display_path = escaped_hash_check_display(&path);
                out.write_all(format!("{display_path}: {status}\n").as_bytes())?;
            }
        }
        if !input_had_valid_line {
            no_valid_input = Some(input_label.to_string());
            break;
        }
    }
    out.into_inner()?;
    if let Some(input_label) = no_valid_input {
        eprintln!(
            "{}: {}: no properly formatted {} checksum lines found",
            hash_sum_program_name(algorithm),
            input_label,
            algorithm_name
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
        eprintln!(
            "{}: WARNING: {} {} improperly formatted",
            hash_sum_program_name(algorithm),
            malformed_lines,
            phrase
        );
    }
    if unread_files != 0 && !options.status_only {
        let phrase = if unread_files == 1 {
            "listed file could not be read"
        } else {
            "listed files could not be read"
        };
        eprintln!(
            "{}: WARNING: {} {}",
            hash_sum_program_name(algorithm),
            unread_files,
            phrase
        );
    }
    if had_checksum_failure && !options.status_only {
        eprintln!(
            "{}: WARNING: 1 computed checksum did NOT match",
            hash_sum_program_name(algorithm)
        );
    }
    if no_verified_files && !options.status_only {
        let input_label = match options.inputs.first() {
            Some(StreamInput::File(file)) => file.as_str(),
            Some(StreamInput::Stdin { label }) => label.as_deref().unwrap_or("-"),
            None => "-",
        };
        eprintln!(
            "{}: {}: no file was verified",
            hash_sum_program_name(algorithm),
            input_label
        );
    }
    Ok(hash_check_exit_code(
        had_failure,
        malformed_lines,
        options.strict,
        no_verified_files,
    ))
}

pub(super) fn run_hash_sum(args: &[String], algorithm: HashAlgorithm) -> io::Result<i32> {
    let options = parse_hash_sum_options(args)?;
    if options.tag_with_check {
        let program = hash_sum_program_name(algorithm);
        eprintln!("{program}: the --tag option is meaningless when verifying checksums");
        eprintln!("Try '{program} --help' for more information.");
        return Ok(1);
    }
    if options.check {
        return run_hash_sum_check(&options, algorithm);
    }
    let started_at = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    let mut out = stdout_buf_writer()?;
    for input in options.inputs {
        let label = match &input {
            StreamInput::File(file) => Some(file.as_str()),
            StreamInput::Stdin { label } => Some(label.as_deref().unwrap_or("-")),
        };
        let (digest, bytes) = match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => (
                hash_file(file, algorithm, options.io_mode)?,
                fs::metadata(file)?.len(),
            ),
            StreamInput::Stdin { .. } => match regular_stdin_path()? {
                Some(path) => (
                    hash_file(&path, algorithm, options.io_mode)?,
                    fs::metadata(path)?.len(),
                ),
                None => hash_stream_input(&input, algorithm, options.io_mode)?,
            },
            _ => hash_stream_input(&input, algorithm, options.io_mode)?,
        };
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("hash byte count overflow"))?;
        if let Some(label) = label {
            write_hash_sum_line(
                &mut out,
                algorithm,
                options.format,
                options.zero_terminated,
                &digest,
                label,
            )?;
        } else {
            out.write_all(hex_digest(&digest).as_bytes())?;
            out.write_all(if options.zero_terminated {
                b"\0"
            } else {
                b"\n"
            })?;
        }
    }
    out.into_inner()?;
    if options.report_gbps {
        report_gbps(hash_sum_program_name(algorithm), total_bytes, started_at);
    }
    Ok(0)
}

fn cksum_stream_input(input: &StreamInput, io_mode: IOMode) -> io::Result<(u32, u64)> {
    let (digest, bytes) = hash_stream_input(input, HashAlgorithm::CRC32, io_mode)?;
    let crc = u32::from_be_bytes(digest.as_slice().try_into().map_err(|_| {
        io::Error::other(format!(
            "unexpected CRC32 digest length: {} bytes",
            digest.len()
        ))
    })?);
    Ok((crc, bytes))
}

pub(super) fn run_cksum(args: &[String]) -> io::Result<()> {
    let mut io_mode = IOMode::Auto;
    let mut report_throughput = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--report-gbps" => report_throughput = true,
            other => files.push(other.to_string()),
        }
    }
    let inputs = parse_stream_inputs(files);
    let started_at = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    for input in inputs {
        let (crc, bytes) = match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => (
                u32::from_be_bytes(
                    hash_file(file, HashAlgorithm::CRC32, io_mode)?
                        .as_slice()
                        .try_into()
                        .map_err(|_| io::Error::other("unexpected CRC32 digest length"))?,
                ),
                fs::metadata(file)?.len(),
            ),
            StreamInput::Stdin { .. } => match regular_stdin_path()? {
                Some(path) => (
                    u32::from_be_bytes(
                        hash_file(&path, HashAlgorithm::CRC32, io_mode)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| io::Error::other("unexpected CRC32 digest length"))?,
                    ),
                    fs::metadata(path)?.len(),
                ),
                None => cksum_stream_input(&input, io_mode)?,
            },
            _ => cksum_stream_input(&input, io_mode)?,
        };
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("cksum byte count overflow"))?;
        match input {
            StreamInput::File(file) => println!("{} {} {}", crc, bytes, file),
            StreamInput::Stdin { label: Some(label) } => println!("{} {} {}", crc, bytes, label),
            StreamInput::Stdin { label: None } => println!("{} {}", crc, bytes),
        }
    }
    if report_throughput {
        report_gbps("cksum", total_bytes, started_at);
    }
    Ok(())
}

fn ordered_digest(algorithm: HashAlgorithm) -> Option<openssl::hash::MessageDigest> {
    match algorithm {
        HashAlgorithm::Md5 => Some(openssl::hash::MessageDigest::md5()),
        HashAlgorithm::Blake2b512 => openssl::hash::MessageDigest::from_name("BLAKE2b512"),
        HashAlgorithm::Sha224 => Some(openssl::hash::MessageDigest::sha224()),
        HashAlgorithm::Sha256 => Some(openssl::hash::MessageDigest::sha256()),
        HashAlgorithm::Sha384 => Some(openssl::hash::MessageDigest::sha384()),
        HashAlgorithm::Sha512 => Some(openssl::hash::MessageDigest::sha512()),
        HashAlgorithm::Blake3
        | HashAlgorithm::CRC32
        | HashAlgorithm::FroBlockXxh3
        | HashAlgorithm::FroBlockSha256 => None,
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

#[cfg(kani)]
mod kani_proofs {
    use super::{hash_check_exit_code, hash_check_should_report_malformed_line};

    #[kani::proof]
    fn hash_check_malformed_line_policy_matches_flag_formula() {
        let warn: bool = kani::any();
        let status_only: bool = kani::any();
        assert_eq!(
            hash_check_should_report_malformed_line(warn, status_only),
            warn && !status_only
        );
    }

    #[kani::proof]
    fn hash_check_exit_code_matches_failure_formula() {
        let had_failure: bool = kani::any();
        let malformed_lines: usize = kani::any();
        let strict: bool = kani::any();
        let no_verified_files: bool = kani::any();
        assert_eq!(
            hash_check_exit_code(had_failure, malformed_lines, strict, no_verified_files),
            if had_failure || (strict && malformed_lines != 0) || no_verified_files {
                1
            } else {
                0
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fro::cksum_crc_block;

    #[test]
    fn hash_check_line_kind_classifies_supported_layouts() {
        assert_eq!(
            hash_check_line_kind("abc123  file.txt"),
            HashCheckLineKind::UntaggedText
        );
        assert_eq!(
            hash_check_line_kind("abc123 *file.txt"),
            HashCheckLineKind::UntaggedBinary
        );
        assert_eq!(
            hash_check_line_kind("SHA256 (file.txt) = abc123"),
            HashCheckLineKind::Tagged
        );
        assert_eq!(hash_check_line_kind(""), HashCheckLineKind::Invalid);
        assert_eq!(hash_check_line_kind("nonsense"), HashCheckLineKind::Invalid);
    }

    #[test]
    fn parse_hash_sum_options_supports_double_dash_for_dash_prefixed_files() {
        let args = vec![
            "sha256sum".to_string(),
            "--direct".to_string(),
            "--".to_string(),
            "--check".to_string(),
            "-file".to_string(),
            "-".to_string(),
        ];

        let options = parse_hash_sum_options(&args).unwrap();
        assert!(matches!(options.io_mode, IOMode::Direct));
        assert!(!options.check);
        assert_eq!(
            options.inputs,
            vec![
                StreamInput::File("--check".to_string()),
                StreamInput::File("-file".to_string()),
                StreamInput::Stdin {
                    label: Some("-".to_string())
                }
            ]
        );
    }

    #[test]
    fn parse_hash_check_line_accepts_tagged_lines_for_matching_algorithm() {
        assert_eq!(
            parse_hash_check_line("SHA256 (dir/file).txt) = abc123", HashAlgorithm::Sha256),
            Some(("abc123".to_string(), "dir/file).txt".to_string()))
        );
        assert_eq!(
            parse_hash_check_line(
                "BLAKE2b (hash file.txt) = deadbeef",
                HashAlgorithm::Blake2b512
            ),
            Some(("deadbeef".to_string(), "hash file.txt".to_string()))
        );
    }

    #[test]
    fn parse_hash_check_line_rejects_tagged_lines_for_other_algorithms() {
        assert_eq!(
            parse_hash_check_line("SHA256 (file.txt) = abc123", HashAlgorithm::Md5),
            None
        );
    }

    #[test]
    fn parse_hash_check_line_unescapes_gnu_escaped_paths() {
        assert_eq!(
            parse_hash_check_line("\\abc123  dir\\\\line\\nfile.txt", HashAlgorithm::Sha256),
            Some(("abc123".to_string(), "dir\\line\nfile.txt".to_string()))
        );
        assert_eq!(
            parse_hash_check_line(
                "\\SHA256 (dir\\\\line\\nfile.txt) = deadbeef",
                HashAlgorithm::Sha256
            ),
            Some(("deadbeef".to_string(), "dir\\line\nfile.txt".to_string()))
        );
    }

    #[test]
    fn write_hash_sum_line_escapes_backslash_and_newline_like_gnu() {
        let mut out = Vec::new();
        write_hash_sum_line(
            &mut out,
            HashAlgorithm::Sha256,
            HashSumFormat::Default,
            false,
            &[0xab, 0xcd],
            "dir\\line\nfile.txt",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "\\abcd  dir\\\\line\\nfile.txt\n"
        );
    }

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

    #[test]
    fn hash_stream_input_matches_hash_file_for_sha256() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "fro-hash-stream-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bytes = (0..(1024 * 1024 + 123))
            .map(|i| ((i * 17) % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &bytes).unwrap();
        let stream_input = StreamInput::File(path.to_string_lossy().into_owned());
        let (streamed, streamed_bytes) =
            hash_stream_input(&stream_input, HashAlgorithm::Sha256, IOMode::PageCache).unwrap();
        let file = hash_file(
            path.to_str().unwrap(),
            HashAlgorithm::Sha256,
            IOMode::PageCache,
        )
        .unwrap();
        assert_eq!(streamed_bytes, bytes.len() as u64);
        assert_eq!(streamed, file);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cksum_stream_input_matches_hash_file_crc32() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "fro-cksum-regular-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bytes = (0..(3 * 1024 * 1024 + 517))
            .map(|i| ((i * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &bytes).unwrap();

        let regular = u32::from_be_bytes(
            hash_file(
                path.to_str().unwrap(),
                HashAlgorithm::CRC32,
                IOMode::PageCache,
            )
            .unwrap()
            .as_slice()
            .try_into()
            .unwrap(),
        );
        let streamed = cksum_stream_input(
            &StreamInput::File(path.to_string_lossy().into_owned()),
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(streamed.1, bytes.len() as u64);
        assert_eq!(
            streamed.0,
            finalize_cksum_crc(cksum_crc_block(&bytes), bytes.len() as u64)
        );
        assert_eq!(regular, streamed.0);

        let _ = std::fs::remove_file(path);
    }
}
