#![cfg(unix)]

use blake3::Hasher as Blake3Hasher;
use openssl::hash::{hash, MessageDigest};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
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

    let grep = assert_success(run_fro(
        "fgrep",
        &["-n", "needle", grep_target.to_str().unwrap()],
    ));
    assert_eq!(String::from_utf8_lossy(&grep.stdout), "2:needle beta\n");
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
