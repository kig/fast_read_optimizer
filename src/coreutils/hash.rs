use super::*;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use crc_fast::{CrcAlgorithm as FastCrcAlgorithm, Digest as CrcDigest};
use fro::finalize_cksum_crc;
use openssl::hash::Hasher;

mod check;
pub(crate) mod cksum;

use self::check::{is_check_behavior_flag, parse_check_options, CheckOptions, ManifestEntry};

const BLAKE2B_DEFAULT_LENGTH_BITS: usize = 512;
const BLAKE2B_MAX_LENGTH_BITS: usize = 512;

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

fn regular_stdin_path() -> io::Result<Option<String>> {
    if fd_is_regular(fro::command_io::stdin_fd())? {
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
    blake2_length_bits: usize,
    zero_terminated: bool,
    check: bool,
    tag_with_check: bool,
    check_options: CheckOptions,
    inputs: Vec<StreamInput>,
}

fn parse_hash_sum_options(args: &[String], algorithm: HashAlgorithm) -> io::Result<HashSumOptions> {
    let mut io_mode = IOMode::Auto;
    let mut report_gbps = false;
    let mut format = HashSumFormat::Default;
    let mut blake2_length_bits = BLAKE2B_DEFAULT_LENGTH_BITS;
    let mut zero_terminated = false;
    let mut check = false;
    let mut saw_tag = false;
    let mut files = Vec::new();
    let mut parse_options = true;
    let mut index = 1usize;
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
                "--report-gbps" => {
                    report_gbps = true;
                    index += 1;
                    continue;
                }
                "-b" | "--binary" => {
                    format = HashSumFormat::Binary;
                    index += 1;
                    continue;
                }
                "-t" | "--text" => {
                    format = HashSumFormat::Default;
                    index += 1;
                    continue;
                }
                "--tag" => {
                    saw_tag = true;
                    format = HashSumFormat::Tag;
                    index += 1;
                    continue;
                }
                "-l" if algorithm == HashAlgorithm::Blake2b512 => {
                    let Some(value) = args.get(index + 1) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "option requires an argument -- 'l'\nTry 'b2sum --help' for more information.",
                        ));
                    };
                    blake2_length_bits = parse_b2sum_length_bits(value)?;
                    index += 2;
                    continue;
                }
                "--length" if algorithm == HashAlgorithm::Blake2b512 => {
                    let Some(value) = args.get(index + 1) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "option '--length' requires an argument\nTry 'b2sum --help' for more information.",
                        ));
                    };
                    blake2_length_bits = parse_b2sum_length_bits(value)?;
                    index += 2;
                    continue;
                }
                "-z" | "--zero" => {
                    zero_terminated = true;
                    index += 1;
                    continue;
                }
                "-c" | "--check" => {
                    check = true;
                    format = HashSumFormat::Default;
                    index += 1;
                    continue;
                }
                other if is_check_behavior_flag(other) => {
                    index += 1;
                    continue;
                }
                other
                    if algorithm == HashAlgorithm::Blake2b512 && other.starts_with("--length=") =>
                {
                    blake2_length_bits = parse_b2sum_length_bits(&other["--length=".len()..])?;
                    index += 1;
                    continue;
                }
                other
                    if algorithm == HashAlgorithm::Blake2b512
                        && other.starts_with("-l")
                        && other.len() > 2 =>
                {
                    blake2_length_bits = parse_b2sum_length_bits(&other[2..])?;
                    index += 1;
                    continue;
                }
                "--" => {
                    parse_options = false;
                    index += 1;
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
        index += 1;
    }
    Ok(HashSumOptions {
        io_mode,
        report_gbps,
        format,
        blake2_length_bits,
        zero_terminated,
        check,
        tag_with_check: check && saw_tag,
        check_options: parse_check_options(&args[1..]),
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

fn hash_sum_tag_label(algorithm: HashAlgorithm, digest_len_bytes: usize) -> Option<String> {
    match algorithm {
        HashAlgorithm::Blake2b512 => {
            let bits = digest_len_bytes * 8;
            if bits == BLAKE2B_DEFAULT_LENGTH_BITS {
                Some("BLAKE2b".to_string())
            } else {
                Some(format!("BLAKE2b-{bits}"))
            }
        }
        _ => hash_sum_tag_name(algorithm).map(str::to_string),
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
            hash_sum_tag_label(algorithm, digest.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "unsupported tagged digest")
            })?,
            label,
            hex_digest(digest)
        ),
    };
    out.write_all(line.as_bytes())?;
    out.write_all(if zero_terminated { b"\0" } else { b"\n" })
}

fn parse_b2sum_length_bits(value: &str) -> io::Result<usize> {
    let bits = value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid length: ‘{value}’"),
        )
    })?;
    if bits == 0 {
        return Ok(BLAKE2B_DEFAULT_LENGTH_BITS);
    }
    if bits % 8 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid length: ‘{value}’\nlength is not a multiple of 8"),
        ));
    }
    if bits > BLAKE2B_MAX_LENGTH_BITS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid length: ‘{value}’\nmaximum digest length for ‘blake2b’ is {BLAKE2B_MAX_LENGTH_BITS} bits"
            ),
        ));
    }
    Ok(bits)
}

fn hash_blake2b_input(
    input: &StreamInput,
    io_mode: IOMode,
    digest_len_bytes: usize,
) -> io::Result<(Vec<u8>, u64)> {
    let mut hasher = Blake2bVar::new(digest_len_bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let bytes = visit_ordered_input_counted(input, io_mode, |block| {
        hasher.update(block);
        Ok(())
    })?;
    let mut digest = vec![0_u8; digest_len_bytes];
    hasher
        .finalize_variable(&mut digest)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    Ok((digest, bytes))
}

fn hash_sum_input(
    input: &StreamInput,
    algorithm: HashAlgorithm,
    io_mode: IOMode,
    blake2_length_bits: usize,
) -> io::Result<(Vec<u8>, u64)> {
    if algorithm == HashAlgorithm::Blake2b512 && blake2_length_bits != BLAKE2B_DEFAULT_LENGTH_BITS {
        return match input {
            StreamInput::File(file) => hash_blake2b_input(
                &StreamInput::File(file.clone()),
                io_mode,
                blake2_length_bits / 8,
            ),
            StreamInput::Stdin { .. } => match regular_stdin_path()? {
                Some(path) => {
                    hash_blake2b_input(&StreamInput::File(path), io_mode, blake2_length_bits / 8)
                }
                None => hash_blake2b_input(input, io_mode, blake2_length_bits / 8),
            },
        };
    }

    match input {
        StreamInput::File(file) if is_regular_input_path(file)? => Ok((
            hash_file(file, algorithm, io_mode)?,
            fs::metadata(file)?.len(),
        )),
        StreamInput::Stdin { .. } => match regular_stdin_path()? {
            Some(path) => Ok((
                hash_file(&path, algorithm, io_mode)?,
                fs::metadata(path)?.len(),
            )),
            None => hash_stream_input(input, algorithm, io_mode),
        },
        _ => hash_stream_input(input, algorithm, io_mode),
    }
}

fn parse_tagged_hash_check_line<'a>(
    line: &'a str,
    algorithm: HashAlgorithm,
) -> Option<(&'a str, &'a str, bool, usize)> {
    let escaped = line.starts_with('\\');
    let line = if escaped {
        line.strip_prefix('\\')?
    } else {
        line
    };
    let (rest, digest_bits) = match algorithm {
        HashAlgorithm::Blake2b512 => {
            if let Some(rest) = line.strip_prefix("BLAKE2b (") {
                (rest, BLAKE2B_DEFAULT_LENGTH_BITS)
            } else {
                let rest = line.strip_prefix("BLAKE2b-")?;
                let (bits_text, rest) = rest.split_once(" (")?;
                let digest_bits = parse_b2sum_length_bits(bits_text).ok()?;
                if digest_bits == BLAKE2B_DEFAULT_LENGTH_BITS {
                    return None;
                }
                (rest, digest_bits)
            }
        }
        _ => {
            let tag = hash_sum_tag_name(algorithm)?;
            (line.strip_prefix(tag)?.strip_prefix(" (")?, 0)
        }
    };
    let (file, digest) = rest.rsplit_once(") = ")?;
    Some((digest, file, escaped, digest_bits))
}

fn is_valid_b2sum_digest(digest: &str, expected_bits: Option<usize>) -> bool {
    let expected_len = expected_bits.map(|bits| bits / 4);
    !digest.is_empty()
        && digest.len() % 2 == 0
        && digest.len() <= BLAKE2B_MAX_LENGTH_BITS / 4
        && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        && expected_len.is_none_or(|expected_len| digest.len() == expected_len)
}

fn parse_hash_check_line(line: &str, algorithm: HashAlgorithm) -> Option<(String, String)> {
    match hash_check_line_kind(line) {
        HashCheckLineKind::Tagged => {
            let (digest, file, escaped, digest_bits) =
                parse_tagged_hash_check_line(line, algorithm)?;
            if algorithm == HashAlgorithm::Blake2b512
                && !is_valid_b2sum_digest(digest, Some(digest_bits))
            {
                return None;
            }
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
            if algorithm == HashAlgorithm::Blake2b512 && !is_valid_b2sum_digest(digest, None) {
                return None;
            }
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
    check::run_manifest_check(
        hash_sum_program_name(algorithm),
        hash_check_algorithm_name(algorithm),
        &options.inputs,
        options.check_options,
        |line| {
            parse_hash_check_line(line, algorithm)
                .map(|(expected, path)| ManifestEntry { expected, path })
        },
        |entry| {
            if algorithm == HashAlgorithm::Blake2b512 && entry.expected.len() != 128 {
                let digest_len_bits = entry.expected.len() * 4;
                let (digest, _) = hash_sum_input(
                    &StreamInput::File(entry.path.clone()),
                    algorithm,
                    options.io_mode,
                    digest_len_bits,
                )?;
                Ok(hex_digest(&digest))
            } else {
                hash_file(&entry.path, algorithm, options.io_mode).map(|digest| hex_digest(&digest))
            }
        },
    )
}

pub(super) fn run_hash_sum(args: &[String], algorithm: HashAlgorithm) -> io::Result<i32> {
    let options = parse_hash_sum_options(args, algorithm)?;
    if options.tag_with_check {
        let program = hash_sum_program_name(algorithm);
        fro::cio_eprintln!("{program}: the --tag option is meaningless when verifying checksums");
        fro::cio_eprintln!("Try '{program} --help' for more information.");
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
        let (digest, bytes) = hash_sum_input(
            &input,
            algorithm,
            options.io_mode,
            options.blake2_length_bits,
        )?;
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
    use super::check::{hash_check_exit_code, hash_check_should_report_malformed_line};

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

        let options = parse_hash_sum_options(&args, HashAlgorithm::Sha256).unwrap();
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
    fn parse_b2sum_length_bits_accepts_default_and_truncated_ranges() {
        assert_eq!(
            parse_b2sum_length_bits("0").unwrap(),
            BLAKE2B_DEFAULT_LENGTH_BITS
        );
        assert_eq!(parse_b2sum_length_bits("8").unwrap(), 8);
        assert_eq!(parse_b2sum_length_bits("504").unwrap(), 504);
        assert_eq!(
            parse_b2sum_length_bits("512").unwrap(),
            BLAKE2B_DEFAULT_LENGTH_BITS
        );
    }

    #[test]
    fn parse_b2sum_length_bits_rejects_invalid_ranges() {
        for value in ["", "foo", "-8"] {
            assert_eq!(
                parse_b2sum_length_bits(value).unwrap_err().to_string(),
                format!("invalid length: ‘{value}’")
            );
        }
        assert_eq!(
            parse_b2sum_length_bits("9").unwrap_err().to_string(),
            "invalid length: ‘9’\nlength is not a multiple of 8"
        );
        assert_eq!(
            parse_b2sum_length_bits("520").unwrap_err().to_string(),
            "invalid length: ‘520’\nmaximum digest length for ‘blake2b’ is 512 bits"
        );
    }

    #[test]
    fn parse_hash_sum_options_parses_b2sum_length_forms() {
        for args in [
            vec![
                "b2sum".to_string(),
                "--length".to_string(),
                "72".to_string(),
            ],
            vec!["b2sum".to_string(), "--length=72".to_string()],
            vec!["b2sum".to_string(), "-l".to_string(), "72".to_string()],
            vec!["b2sum".to_string(), "-l72".to_string()],
        ] {
            let options = parse_hash_sum_options(&args, HashAlgorithm::Blake2b512).unwrap();
            assert_eq!(options.blake2_length_bits, 72);
        }
    }

    #[test]
    fn parse_hash_check_line_accepts_tagged_lines_for_matching_algorithm() {
        assert_eq!(
            parse_hash_check_line("SHA256 (dir/file).txt) = abc123", HashAlgorithm::Sha256),
            Some(("abc123".to_string(), "dir/file).txt".to_string()))
        );
        assert_eq!(
            parse_hash_check_line(
                "BLAKE2b (hash file.txt) = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                HashAlgorithm::Blake2b512
            ),
            Some((
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                "hash file.txt".to_string()
            ))
        );
        assert_eq!(
            parse_hash_check_line(
                "BLAKE2b-72 (hash file.txt) = 70687daa6157af27dc",
                HashAlgorithm::Blake2b512
            ),
            Some((
                "70687daa6157af27dc".to_string(),
                "hash file.txt".to_string()
            ))
        );
    }

    #[test]
    fn parse_hash_check_line_rejects_tagged_lines_for_other_algorithms() {
        assert_eq!(
            parse_hash_check_line("SHA256 (file.txt) = abc123", HashAlgorithm::Md5),
            None
        );
        assert_eq!(
            parse_hash_check_line("BLAKE2b (file.txt) = 6b", HashAlgorithm::Blake2b512),
            None
        );
        assert_eq!(
            parse_hash_check_line("BLAKE2b-16 (file.txt) = 6b", HashAlgorithm::Blake2b512),
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
            check::escaped_hash_check_display("dir\\line\nfile.txt"),
            "\\dir\\\\line\\nfile.txt"
        );
        assert_eq!(
            check::escaped_hash_check_display("dir\\line.txt"),
            "dir\\line.txt"
        );
    }

    #[test]
    fn b2sum_digest_validation_accepts_truncated_lengths() {
        assert!(is_valid_b2sum_digest("6b", None));
        assert!(is_valid_b2sum_digest("70687daa6157af27dc", Some(72)));
        assert!(!is_valid_b2sum_digest("6", None));
        assert!(!is_valid_b2sum_digest("zz", None));
        assert!(!is_valid_b2sum_digest(&"a".repeat(130), None));
    }

    #[test]
    fn write_hash_sum_line_uses_b2sum_length_suffix_for_tagged_output() {
        let mut out = Vec::new();
        write_hash_sum_line(
            &mut out,
            HashAlgorithm::Blake2b512,
            HashSumFormat::Tag,
            false,
            &[0x6b],
            "file.txt",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "BLAKE2b-8 (file.txt) = 6b\n"
        );
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
