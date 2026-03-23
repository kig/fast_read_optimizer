use super::*;

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
    format: HashSumFormat,
    zero_terminated: bool,
    check: bool,
    quiet: bool,
    status_only: bool,
    warn: bool,
    inputs: Vec<StreamInput>,
}

fn parse_hash_sum_options(args: &[String]) -> io::Result<HashSumOptions> {
    let mut io_mode = IOMode::Auto;
    let mut format = HashSumFormat::Default;
    let mut zero_terminated = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-b" | "--binary" => format = HashSumFormat::Binary,
            "-t" | "--text" => format = HashSumFormat::Default,
            "--tag" => format = HashSumFormat::Tag,
            "-z" | "--zero" => zero_terminated = true,
            "-c" | "--check" => {
                if format == HashSumFormat::Tag {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--check does not support --tag input",
                    ));
                }
                format = HashSumFormat::Default;
            }
            "--quiet" | "--status" | "-w" | "--warn" => {}
            "-" => files.push(arg.clone()),
            other if other.starts_with('-') => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported hash flag: {other}"),
                ));
            }
            _ => files.push(arg.clone()),
        }
    }
    Ok(HashSumOptions {
        io_mode,
        format,
        zero_terminated,
        check: args[1..]
            .iter()
            .any(|arg| matches!(arg.as_str(), "-c" | "--check")),
        quiet: args[1..].iter().any(|arg| arg == "--quiet"),
        status_only: args[1..].iter().any(|arg| arg == "--status"),
        warn: args[1..]
            .iter()
            .any(|arg| matches!(arg.as_str(), "-w" | "--warn")),
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
        HashAlgorithm::Blake3 | HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => None,
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
    let line = match format {
        HashSumFormat::Default => format!("{}  {}", hex_digest(digest), label),
        HashSumFormat::Binary => format!("{} *{}", hex_digest(digest), label),
        HashSumFormat::Tag => format!(
            "{} ({}) = {}",
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

fn parse_hash_check_line(line: &str) -> Option<(&str, &str)> {
    match hash_check_line_kind(line) {
        HashCheckLineKind::UntaggedText | HashCheckLineKind::UntaggedBinary => {
            let space_pos = line.as_bytes().iter().position(|&byte| byte == b' ')?;
            let digest = &line[..space_pos];
            let file = &line[(space_pos + 2)..];
            Some((digest, file))
        }
        _ => None,
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
    let mut had_valid_line = false;
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
            let Some((expected_hex, path)) = parse_hash_check_line(line) else {
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
            let actual = hash_file(path, algorithm, options.io_mode)?;
            let success = hex_digest(&actual) == expected_hex;
            if !success {
                had_failure = true;
            }
            if hash_check_should_print_result(success, options.quiet, options.status_only) {
                let status = if success { "OK" } else { "FAILED" };
                out.write_all(format!("{path}: {status}\n").as_bytes())?;
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
    if had_failure && !options.status_only {
        eprintln!(
            "{}: WARNING: 1 computed checksum did NOT match",
            hash_sum_program_name(algorithm)
        );
    }
    Ok(if had_failure { 1 } else { 0 })
}

pub(super) fn run_hash_sum(args: &[String], algorithm: HashAlgorithm) -> io::Result<i32> {
    let options = parse_hash_sum_options(args)?;
    if options.check {
        return run_hash_sum_check(&options, algorithm);
    }
    let mut out = stdout_buf_writer()?;
    for input in options.inputs {
        let label = match &input {
            StreamInput::File(file) => Some(file.as_str()),
            StreamInput::Stdin { label } => Some(label.as_deref().unwrap_or("-")),
        };
        let digest = match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => {
                hash_file(file, algorithm, options.io_mode)?
            }
            _ => {
                let mut data = Vec::new();
                visit_ordered_input(&input, options.io_mode, |block| {
                    data.extend_from_slice(block);
                    Ok(())
                })?;
                match algorithm {
                    HashAlgorithm::Md5
                    | HashAlgorithm::Blake2b512
                    | HashAlgorithm::Sha224
                    | HashAlgorithm::Sha256
                    | HashAlgorithm::Sha384
                    | HashAlgorithm::Sha512 => {
                        let digest = openssl::hash::hash(
                            ordered_digest(algorithm).ok_or_else(|| {
                                io::Error::new(io::ErrorKind::InvalidInput, "unsupported digest")
                            })?,
                            &data,
                        )
                        .map_err(io::Error::other)?;
                        digest.to_vec()
                    }
                    HashAlgorithm::Blake3 => {
                        let mut hasher = blake3::Hasher::new();
                        hasher.update(&data);
                        hasher.finalize().as_bytes().to_vec()
                    }
                    HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "block hash sums do not support stream input",
                        ));
                    }
                }
            }
        };
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
    Ok(0)
}

fn crc32_cksum_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

pub(super) fn run_cksum(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    for input in inputs {
        let mut crc = 0_u32;
        let mut bytes = 0_u64;
        visit_ordered_input(&input, io_mode, |block| {
            crc = crc32_cksum_update(crc, block);
            bytes += block.len() as u64;
            Ok(())
        })?;
        let mut length = bytes;
        while length != 0 {
            crc = crc32_cksum_update(crc, &[(length & 0xff) as u8]);
            length >>= 8;
        }
        match input {
            StreamInput::File(file) => println!("{} {} {}", !crc, bytes, file),
            StreamInput::Stdin { label: Some(label) } => println!("{} {} {}", !crc, bytes, label),
            StreamInput::Stdin { label: None } => println!("{} {}", !crc, bytes),
        }
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
        HashAlgorithm::Blake3 | HashAlgorithm::FroBlockXxh3 | HashAlgorithm::FroBlockSha256 => None,
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
    use super::hash_check_should_report_malformed_line;

    #[kani::proof]
    fn hash_check_malformed_line_policy_matches_flag_formula() {
        let warn: bool = kani::any();
        let status_only: bool = kani::any();
        assert_eq!(
            hash_check_should_report_malformed_line(warn, status_only),
            warn && !status_only
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
