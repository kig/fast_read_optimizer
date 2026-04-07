#![cfg(unix)]

use blake3::Hasher as Blake3Hasher;
use openssl::hash::{hash, MessageDigest};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn run_fro(command: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .output()
        .expect("failed to run fro coreutils command")
}

fn run_system(program: &str, args: &[&str]) -> Output {
    Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"))
}

fn run_system_with_stdin(program: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn {program}: {err}"));
    let mut stdin = child.stdin.take().expect("missing system stdin");
    let input = stdin_bytes.to_vec();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&input).or_else(|err| match err.kind() {
            std::io::ErrorKind::BrokenPipe => Ok(()),
            _ => Err(err),
        })
    });
    let output = child
        .wait_with_output()
        .unwrap_or_else(|err| panic!("failed to collect {program} output: {err}"));
    writer
        .join()
        .expect("stdin writer thread panicked")
        .expect("failed to write system stdin");
    output
}

fn run_fro_with_stdin(command: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro coreutils command");
    let mut stdin = child.stdin.take().expect("missing child stdin");
    let input = stdin_bytes.to_vec();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&input).or_else(|err| match err.kind() {
            std::io::ErrorKind::BrokenPipe => Ok(()),
            _ => Err(err),
        })
    });
    let output = child
        .wait_with_output()
        .expect("failed to read child output");
    writer
        .join()
        .expect("stdin writer thread panicked")
        .expect("failed to write child stdin");
    output
}

fn assert_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    out
}

fn cksum_crc(bytes: &[u8]) -> u32 {
    fn update(mut crc: u32, data: &[u8]) -> u32 {
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

    let mut crc = update(0, bytes);
    let mut len = bytes.len() as u64;
    while len != 0 {
        crc = update(crc, &[(len & 0xff) as u8]);
        len >>= 8;
    }
    !crc
}

#[test]
fn multicall_aliases_cover_existing_copy_diff_and_grep_modes() {
    let tmp = unique_temp_dir("fro-coreutils-existing");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let grep_target = tmp.join("grep.txt");
    let bytes = b"alpha\nneedle beta\nomega\n".to_vec();
    fs::write(&source, &bytes).unwrap();
    fs::write(&grep_target, &bytes).unwrap();

    assert_success(run_fro(
        "cp",
        &[
            "--no-direct",
            source.to_str().unwrap(),
            target.to_str().unwrap(),
        ],
    ));
    assert_eq!(fs::read(&target).unwrap(), bytes);

    assert_success(run_fro(
        "cmp",
        &[source.to_str().unwrap(), target.to_str().unwrap()],
    ));

    let moved = tmp.join("moved.bin");
    assert_success(run_fro(
        "mv",
        &[target.to_str().unwrap(), moved.to_str().unwrap()],
    ));
    assert_eq!(fs::read(&moved).unwrap(), bytes);
    assert!(!target.exists());

    let grep = assert_success(run_fro(
        "fgrep",
        &["-n", "needle", grep_target.to_str().unwrap()],
    ));
    assert_eq!(String::from_utf8_lossy(&grep.stdout), "2:needle beta\n");

    assert_success(run_fro("rm", &[moved.to_str().unwrap()]));
    assert!(!moved.exists());

    let archive = tmp.join("bundle.tar");
    assert_success(run_fro(
        "tar",
        &["-cf", archive.to_str().unwrap(), source.to_str().unwrap()],
    ));
    let listing = assert_success(run_system("tar", &["-tf", archive.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&listing.stdout).trim(),
        source.file_name().unwrap().to_string_lossy()
    );
}

#[test]
fn multicall_rm_help_and_force_zero_operands_work() {
    let help = run_fro("rm", &["--help"]);
    let help_stdout = String::from_utf8_lossy(&help.stdout);
    assert_eq!(help.status.code(), Some(0));
    assert!(help_stdout.contains("rm - Remove files or directory trees"));
    assert!(help_stdout.contains("[-f] [-d] [-r|-R|--recursive] [-v] <file> [file ...]"));
    assert!(help_stdout.contains("ignores missing operands and missing files"));
    assert!(help_stdout.contains("-d/--dir removes empty directories"));
    assert!(help.stderr.is_empty());

    assert_success(run_fro("rm", &["-f"]));
}

#[test]
fn multicall_cat_tac_and_wc_match_expected_text_behavior() {
    let tmp = unique_temp_dir("fro-coreutils-text");
    let path = tmp.join("text.txt");
    let bytes = b"one two\nthree\n".to_vec();
    fs::write(&path, &bytes).unwrap();

    let cat_out = assert_success(run_fro("cat", &[path.to_str().unwrap()]));
    assert_eq!(cat_out.stdout, bytes);

    let tac_out = assert_success(run_fro("tac", &[path.to_str().unwrap()]));
    assert_eq!(tac_out.stdout, b"three\none two\n");

    let wc_out = assert_success(run_fro("wc", &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&wc_out.stdout).trim(),
        format!("2 3 14 {}", path.to_str().unwrap())
    );

    let base64_out = assert_success(run_fro("base64", &["-w", "0", path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&base64_out.stdout).trim(),
        "b25lIHR3bwp0aHJlZQo="
    );

    let du_out = assert_success(run_fro("du", &[path.to_str().unwrap()]));
    assert!(String::from_utf8_lossy(&du_out.stdout).contains(path.to_str().unwrap()));
}

#[test]
fn multicall_head_supports_negative_counts_and_headers() {
    let tmp = unique_temp_dir("fro-coreutils-head");
    let file_a = tmp.join("a.txt");
    let file_b = tmp.join("b.txt");
    fs::write(&file_a, b"zero\none\ntwo\nthree\n").unwrap();
    fs::write(&file_b, b"apple\nbanana\ncarrot\n").unwrap();

    let minus_lines = assert_success(run_fro("head", &["-n", "-1", file_a.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&minus_lines.stdout),
        "zero\none\ntwo\n"
    );

    let minus_bytes = assert_success(run_fro("head", &["-c", "-2", file_a.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&minus_bytes.stdout),
        "zero\none\ntwo\nthre"
    );

    let long_lines = assert_success(run_fro("head", &["--lines=-1", file_a.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&long_lines.stdout),
        "zero\none\ntwo\n"
    );

    let long_bytes = assert_success(run_fro("head", &["--bytes=4", file_a.to_str().unwrap()]));
    assert_eq!(String::from_utf8_lossy(&long_bytes.stdout), "zero");

    let verbose = assert_success(run_fro(
        "head",
        &["-v", file_a.to_str().unwrap(), file_b.to_str().unwrap()],
    ));
    let verbose_stdout = String::from_utf8_lossy(&verbose.stdout);
    assert!(verbose_stdout.contains(&format!("==> {} <==", file_a.display())));
    assert!(verbose_stdout.contains(&format!("==> {} <==", file_b.display())));
}

#[test]
fn multicall_dd_copies_requested_range() {
    let tmp = unique_temp_dir("fro-coreutils-dd");
    let input = tmp.join("input.bin");
    let output = tmp.join("output.bin");
    let bytes = (0..97).map(|i| ((i * 13) % 251) as u8).collect::<Vec<_>>();
    fs::write(&input, &bytes).unwrap();

    let out = assert_success(run_fro(
        "dd",
        &[
            &format!("if={}", input.display()),
            &format!("of={}", output.display()),
            "bs=7",
            "count=5",
            "status=none",
        ],
    ));
    assert!(
        out.stderr.is_empty(),
        "stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(fs::read(&output).unwrap(), bytes[..35].to_vec());
}

#[test]
fn multicall_dd_status_noxfer_prints_records_only() {
    let tmp = unique_temp_dir("fro-coreutils-dd-noxfer");
    let input = tmp.join("input.bin");
    let output = tmp.join("output.bin");
    let bytes = (0..97).map(|i| ((i * 13) % 251) as u8).collect::<Vec<_>>();
    fs::write(&input, &bytes).unwrap();

    let out = assert_success(run_fro(
        "dd",
        &[
            &format!("if={}", input.display()),
            &format!("of={}", output.display()),
            "bs=10",
            "status=noxfer",
        ],
    ));
    assert_eq!(fs::read(&output).unwrap(), bytes);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("9+1 records in"),
        "stderr:
{stderr}"
    );
    assert!(
        stderr.contains("9+1 records out"),
        "stderr:
{stderr}"
    );
    assert!(
        !stderr.contains("bytes copied"),
        "stderr:
{stderr}"
    );
}

#[test]
fn multicall_cat_and_wc_accept_stdin_and_dash() {
    let bytes = b"one two\nthree\n";

    let cat_stdin = assert_success(run_fro_with_stdin("cat", &[], bytes));
    assert_eq!(cat_stdin.stdout, bytes);

    let cat_dash = assert_success(run_fro_with_stdin("cat", &["-"], bytes));
    assert_eq!(cat_dash.stdout, bytes);

    let wc_stdin = assert_success(run_fro_with_stdin("wc", &[], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_stdin.stdout).trim(), "2 3 14");

    let wc_dash = assert_success(run_fro_with_stdin("wc", &["-"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_dash.stdout).trim(), "2 3 14 -");
}

#[test]
fn multicall_wc_byte_count_pipe_matches_system_for_large_stdin() {
    let bytes = (0..(3 * 1024 * 1024 + 123))
        .map(|idx| ((idx * 17 + 31) % 251) as u8)
        .collect::<Vec<_>>();

    let fro = assert_success(run_fro_with_stdin("wc", &["-c"], &bytes));
    let system = run_system_with_stdin("wc", &["-c"], &bytes);
    assert!(
        system.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&system.stdout)
    );
}

#[test]
fn multicall_wc_default_counts_large_streamed_stdin_matches_system() {
    let line = b"alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega\n";
    let mut bytes = Vec::with_capacity(line.len() * 16384);
    for _ in 0..16384 {
        bytes.extend_from_slice(line);
    }

    let fro = assert_success(run_fro_with_stdin("wc", &[], &bytes));
    let system = run_system_with_stdin("wc", &[], &bytes);
    assert!(
        system.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr)
    );
    let fro_tokens = String::from_utf8_lossy(&fro.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let system_tokens = String::from_utf8_lossy(&system.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(fro_tokens, system_tokens);
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn multicall_head_accepts_stdin_negative_counts() {
    let bytes = b"one\ntwo\nthree\n";

    let head_stdin = assert_success(run_fro_with_stdin("head", &["-n", "-1"], bytes));
    assert_eq!(String::from_utf8_lossy(&head_stdin.stdout), "one\ntwo\n");

    let head_dash = assert_success(run_fro_with_stdin("head", &["-c", "-2", "-"], bytes));
    assert_eq!(String::from_utf8_lossy(&head_dash.stdout), "one\ntwo\nthre");

    let head_long_lines = assert_success(run_fro_with_stdin("head", &["--lines", "2"], bytes));
    assert_eq!(
        String::from_utf8_lossy(&head_long_lines.stdout),
        "one\ntwo\n"
    );

    let head_long_bytes = assert_success(run_fro_with_stdin("head", &["--bytes=-2"], bytes));
    assert_eq!(
        String::from_utf8_lossy(&head_long_bytes.stdout),
        "one\ntwo\nthre"
    );
}

#[test]
fn multicall_head_byte_prefix_matches_system_for_large_stdin_binary() {
    let bytes = (0..(256 * 1024))
        .map(|idx| ((idx * 17 + 31) % 251) as u8)
        .collect::<Vec<_>>();

    let fro = assert_success(run_fro_with_stdin("head", &["-c", "65536"], &bytes));
    let system = run_system_with_stdin("head", &["-c", "65536"], &bytes);
    assert!(
        system.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(fro.stdout, system.stdout);
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn multicall_head_line_prefix_matches_system_for_large_stdin_text() {
    let line = b"needle alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\n";
    let mut bytes = Vec::with_capacity(line.len() * 16384);
    for _ in 0..16384 {
        bytes.extend_from_slice(line);
    }

    let fro = assert_success(run_fro_with_stdin("head", &["-n", "4096"], &bytes));
    let system = run_system_with_stdin("head", &["-n", "4096"], &bytes);
    assert!(
        system.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(fro.stdout, system.stdout);
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn multicall_wc_supports_character_counts_on_stdin() {
    let bytes = "aé🙂\n".as_bytes();

    let wc_chars = assert_success(run_fro_with_stdin("wc", &["-m"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_chars.stdout).trim(), "4");

    let wc_chars_dash = assert_success(run_fro_with_stdin("wc", &["-m", "-"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_chars_dash.stdout).trim(), "4 -");
}

#[test]
fn multicall_wc_supports_long_bytes_flag_on_stdin() {
    let bytes = b"one two\nthree\n";

    let wc_bytes = assert_success(run_fro_with_stdin("wc", &["--bytes"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_bytes.stdout).trim(), "14");

    let wc_bytes_dash = assert_success(run_fro_with_stdin("wc", &["--bytes", "-"], bytes));
    assert_eq!(
        String::from_utf8_lossy(&wc_bytes_dash.stdout).trim(),
        "14 -"
    );
}

#[test]
fn multicall_wc_supports_max_line_length_on_stdin() {
    let bytes = "e\u{0301}\n1234567\tX\n".as_bytes();

    let wc_max = assert_success(run_fro_with_stdin("wc", &["-L"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_max.stdout).trim(), "9");

    let wc_max_dash = assert_success(run_fro_with_stdin("wc", &["--max-line-length", "-"], bytes));
    assert_eq!(String::from_utf8_lossy(&wc_max_dash.stdout).trim(), "9 -");
}

#[test]
fn multicall_wc_supports_files0_from_list_files() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0");
    let one = tmp.join("one.txt");
    let two = tmp.join("two.txt");
    let list = tmp.join("inputs.list0");
    fs::write(&one, b"one two\n").unwrap();
    fs::write(&two, b"alpha\nbeta\n").unwrap();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(one.as_os_str().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(two.as_os_str().as_bytes());
    bytes.push(0);
    fs::write(&list, bytes).unwrap();

    let out = assert_success(run_fro(
        "wc",
        &["--files0-from", list.to_str().unwrap(), "-l", "-w"],
    ));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("1 2 {}\n2 2 {}\n3 4 total\n", one.display(), two.display())
    );
}

#[test]
fn multicall_wc_supports_files0_from_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0-stdin");
    let file = tmp.join("stdin.txt");
    fs::write(&file, b"alpha beta\n").unwrap();

    let mut list = Vec::new();
    list.extend_from_slice(file.as_os_str().as_bytes());
    list.push(0);

    let out = assert_success(run_fro_with_stdin("wc", &["--files0-from=-", "-w"], &list));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("2 {}\n", file.display())
    );
}

#[test]
fn multicall_pv_passes_through_stdin_and_reports_progress() {
    let bytes = b"one two\nthree\n";
    let out = assert_success(run_fro_with_stdin("pv", &[], bytes));
    assert_eq!(out.stdout, bytes);
    assert!(
        !out.stderr.is_empty(),
        "expected progress output on stderr, got none"
    );
}

#[test]
fn multicall_fgrep_prints_matching_line_once() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep");
    let path = tmp.join("grep.txt");
    fs::write(&path, b"needle needle\nother\n").unwrap();

    let out = assert_success(run_fro("fgrep", &["needle", path.to_str().unwrap()]));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "needle needle\n");
}

#[test]
fn multicall_fgrep_and_tac_accept_stdin() {
    let grep_input = b"alpha\nneedle beta\nomega\n";
    let grep = assert_success(run_fro_with_stdin("fgrep", &["needle"], grep_input));
    assert_eq!(String::from_utf8_lossy(&grep.stdout), "needle beta\n");

    let grep_dash = assert_success(run_fro_with_stdin("fgrep", &["needle", "-"], grep_input));
    assert_eq!(String::from_utf8_lossy(&grep_dash.stdout), "needle beta\n");

    let tac_input = b"one\ntwo\n";
    let tac = assert_success(run_fro_with_stdin("tac", &[], tac_input));
    assert_eq!(String::from_utf8_lossy(&tac.stdout), "two\none\n");
}

#[test]
fn multicall_tac_supports_double_dash_and_rejects_unknown_options() {
    let tmp = unique_temp_dir("fro-coreutils-tac-options");
    let path = tmp.join("-dash.txt");
    fs::write(&path, b"one\ntwo\n").unwrap();

    let out = assert_success(run_fro("tac", &["--", path.to_str().unwrap()]));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "two\none\n");

    let fro = run_fro("tac", &["--bogus", path.to_str().unwrap()]);
    let sys = run_system("tac", &["--bogus", path.to_str().unwrap()]);
    assert_eq!(fro.status.success(), sys.status.success());
    assert_eq!(fro.status.code(), sys.status.code());
    let fro_stderr = String::from_utf8_lossy(&fro.stderr);
    let sys_stderr = String::from_utf8_lossy(&sys.stderr);
    assert!(fro_stderr.starts_with(sys_stderr.as_ref()));
    assert!(fro_stderr.contains("Error: unrecognized option"));
}

#[test]
fn multicall_hash_sums_print_expected_digests() {
    let tmp = unique_temp_dir("fro-coreutils-hash");
    let path = tmp.join("hash.bin");
    let bytes = (0..(128 * 1024 + 7))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    let sha_out = assert_success(run_fro("sha256sum", &[path.to_str().unwrap()]));
    let expected_sha = format!("{:x}  {}", Sha256::digest(&bytes), path.to_str().unwrap());
    assert_eq!(
        String::from_utf8_lossy(&sha_out.stdout).trim(),
        expected_sha
    );

    let mut b3 = Blake3Hasher::new();
    b3.update(&bytes);
    let b3_out = assert_success(run_fro("b3sum", &[path.to_str().unwrap()]));
    let expected_b3 = format!("{}  {}", b3.finalize().to_hex(), path.to_str().unwrap());
    assert_eq!(String::from_utf8_lossy(&b3_out.stdout).trim(), expected_b3);

    let b2_out = assert_success(run_fro("b2sum", &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&b2_out.stdout).trim(),
        format!(
            "{}  {}",
            hex_digest(&hash(MessageDigest::from_name("BLAKE2b512").unwrap(), &bytes).unwrap()),
            path.to_str().unwrap()
        )
    );

    let md5_out = assert_success(run_fro("md5sum", &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&md5_out.stdout).trim(),
        format!(
            "{}  {}",
            hex_digest(&hash(MessageDigest::md5(), &bytes).unwrap()),
            path.to_str().unwrap()
        )
    );

    let cksum_out = assert_success(run_fro("cksum", &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&cksum_out.stdout).trim(),
        format!(
            "{} {} {}",
            cksum_crc(&bytes),
            bytes.len(),
            path.to_str().unwrap()
        )
    );
}

#[test]
fn multicall_hash_sums_support_double_dash_for_dash_prefixed_files() {
    let tmp = unique_temp_dir("fro-coreutils-hash-double-dash");
    let path = tmp.join("-hash.bin");
    let bytes = (0..8193)
        .map(|i| ((i * 41) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    for tool in ["sha256sum", "md5sum", "b2sum", "b3sum"] {
        let fro = assert_success(run_fro(tool, &["--", path.to_str().unwrap()]));
        let sys = assert_success(run_system(tool, &["--", path.to_str().unwrap()]));
        assert_eq!(
            String::from_utf8_lossy(&fro.stdout),
            String::from_utf8_lossy(&sys.stdout),
            "{tool} stdout mismatch"
        );
        assert_eq!(fro.stderr, sys.stderr, "{tool} stderr mismatch");
    }
}

#[test]
fn multicall_and_subcommand_version_flags_print_version() {
    let pkg_version = env!("CARGO_PKG_VERSION");

    let subcommand = assert_success(run_fro("cat", &["--version"]));
    let subcommand_stdout = String::from_utf8_lossy(&subcommand.stdout);
    assert!(subcommand_stdout.contains(pkg_version));
    assert!(subcommand_stdout.contains("cat"));

    let head_subcommand = assert_success(run_fro("head", &["--version"]));
    let head_subcommand_stdout = String::from_utf8_lossy(&head_subcommand.stdout);
    assert!(head_subcommand_stdout.contains(pkg_version));
    assert!(head_subcommand_stdout.contains("head"));

    let alias_dir = unique_temp_dir("fro-coreutils-version");
    let alias_path = alias_dir.join("cat");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_fro"), &alias_path).unwrap();
    let alias = Command::new(&alias_path)
        .arg("--version")
        .output()
        .expect("failed to run multicall alias --version");
    let alias = assert_success(alias);
    let alias_stdout = String::from_utf8_lossy(&alias.stdout);
    assert!(alias_stdout.contains(pkg_version));
    assert!(alias_stdout.contains("cat"));

    let head_alias_path = alias_dir.join("head");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_fro"), &head_alias_path).unwrap();
    let head_alias = Command::new(&head_alias_path)
        .arg("--version")
        .output()
        .expect("failed to run head multicall alias --version");
    let head_alias = assert_success(head_alias);
    let head_alias_stdout = String::from_utf8_lossy(&head_alias.stdout);
    assert!(head_alias_stdout.contains(pkg_version));
    assert!(head_alias_stdout.contains("head"));

    let encrypt_subcommand = assert_success(run_fro("encrypt", &["--version"]));
    let encrypt_subcommand_stdout = String::from_utf8_lossy(&encrypt_subcommand.stdout);
    assert!(encrypt_subcommand_stdout.contains(pkg_version));
    assert!(encrypt_subcommand_stdout.contains("encrypt"));

    let encrypt_alias_path = alias_dir.join("encrypt");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_fro"), &encrypt_alias_path).unwrap();
    let encrypt_alias = Command::new(&encrypt_alias_path)
        .arg("--version")
        .output()
        .expect("failed to run encrypt multicall alias --version");
    let encrypt_alias = assert_success(encrypt_alias);
    let encrypt_alias_stdout = String::from_utf8_lossy(&encrypt_alias.stdout);
    assert!(encrypt_alias_stdout.contains(pkg_version));
    assert!(encrypt_alias_stdout.contains("encrypt"));
}

#[test]
fn multicall_shred_can_zero_and_remove_file() {
    let tmp = unique_temp_dir("fro-coreutils-shred");
    let keep = tmp.join("keep.bin");
    let remove = tmp.join("remove.bin");
    fs::write(&keep, vec![0x5a; 64 * 1024]).unwrap();
    fs::write(&remove, vec![0x33; 64 * 1024]).unwrap();

    assert_success(run_fro("shred", &["-n", "1", "-z", keep.to_str().unwrap()]));
    assert_eq!(fs::metadata(&keep).unwrap().len(), 64 * 1024);
    assert!(fs::read(&keep).unwrap().iter().all(|&byte| byte == 0));

    assert_success(run_fro(
        "shred",
        &["-n", "1", "-u", remove.to_str().unwrap()],
    ));
    assert!(!remove.exists());
}
