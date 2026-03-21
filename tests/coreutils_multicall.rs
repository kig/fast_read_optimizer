#![cfg(unix)]

use blake3::Hasher as Blake3Hasher;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
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

fn alias_path(tmp: &Path, name: &str) -> PathBuf {
    let path = tmp.join(name);
    symlink(env!("CARGO_BIN_EXE_fro"), &path).unwrap();
    path
}

fn run_alias(alias: &Path, args: &[&str]) -> Output {
    Command::new(alias)
        .args(args)
        .output()
        .expect("failed to run multicall alias")
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

fn bsd_sum(bytes: &[u8]) -> u16 {
    let mut checksum = 0_u16;
    for &byte in bytes {
        checksum = checksum.rotate_right(1).wrapping_add(u16::from(byte));
    }
    checksum
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
    let cp = alias_path(&tmp, "cp");
    let cmp = alias_path(&tmp, "cmp");
    let fgrep = alias_path(&tmp, "fgrep");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let grep_target = tmp.join("grep.txt");
    let bytes = b"alpha\nneedle beta\nomega\n".to_vec();
    fs::write(&source, &bytes).unwrap();
    fs::write(&grep_target, &bytes).unwrap();

    assert_success(run_alias(
        &cp,
        &[
            "--no-direct",
            source.to_str().unwrap(),
            target.to_str().unwrap(),
        ],
    ));
    assert_eq!(fs::read(&target).unwrap(), bytes);

    assert_success(run_alias(
        &cmp,
        &[source.to_str().unwrap(), target.to_str().unwrap()],
    ));

    let grep = assert_success(run_alias(
        &fgrep,
        &["-n", "1", "needle", grep_target.to_str().unwrap()],
    ));
    assert!(String::from_utf8_lossy(&grep.stdout).contains("needle"));
}

#[test]
fn multicall_cat_tac_and_wc_match_expected_text_behavior() {
    let tmp = unique_temp_dir("fro-coreutils-text");
    let cat = alias_path(&tmp, "cat");
    let tac = alias_path(&tmp, "tac");
    let wc = alias_path(&tmp, "wc");
    let path = tmp.join("text.txt");
    let bytes = b"one two\nthree\n".to_vec();
    fs::write(&path, &bytes).unwrap();

    let cat_out = assert_success(run_alias(&cat, &[path.to_str().unwrap()]));
    assert_eq!(cat_out.stdout, bytes);

    let tac_out = assert_success(run_alias(&tac, &[path.to_str().unwrap()]));
    assert_eq!(tac_out.stdout, b"three\none two\n");

    let wc_out = assert_success(run_alias(&wc, &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&wc_out.stdout).trim(),
        format!("2 3 14 {}", path.to_str().unwrap())
    );
}

#[test]
fn multicall_hash_sums_print_expected_digests() {
    let tmp = unique_temp_dir("fro-coreutils-hash");
    let sha256sum = alias_path(&tmp, "sha256sum");
    let b3sum = alias_path(&tmp, "b3sum");
    let sum = alias_path(&tmp, "sum");
    let cksum = alias_path(&tmp, "cksum");
    let path = tmp.join("hash.bin");
    let bytes = (0..(128 * 1024 + 7))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    let sha_out = assert_success(run_alias(&sha256sum, &[path.to_str().unwrap()]));
    let expected_sha = format!("{:x}  {}", Sha256::digest(&bytes), path.to_str().unwrap());
    assert_eq!(String::from_utf8_lossy(&sha_out.stdout).trim(), expected_sha);

    let mut b3 = Blake3Hasher::new();
    b3.update(&bytes);
    let b3_out = assert_success(run_alias(&b3sum, &[path.to_str().unwrap()]));
    let expected_b3 = format!(
        "{}  {}",
        b3.finalize().to_hex(),
        path.to_str().unwrap()
    );
    assert_eq!(String::from_utf8_lossy(&b3_out.stdout).trim(), expected_b3);

    let sum_out = assert_success(run_alias(&sum, &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&sum_out.stdout).trim(),
        format!(
            "{} {} {}",
            bsd_sum(&bytes),
            (bytes.len() as u64).div_ceil(1024),
            path.to_str().unwrap()
        )
    );

    let cksum_out = assert_success(run_alias(&cksum, &[path.to_str().unwrap()]));
    assert_eq!(
        String::from_utf8_lossy(&cksum_out.stdout).trim(),
        format!("{} {} {}", cksum_crc(&bytes), bytes.len(), path.to_str().unwrap())
    );
}

#[test]
fn multicall_shred_can_zero_and_remove_file() {
    let tmp = unique_temp_dir("fro-coreutils-shred");
    let shred = alias_path(&tmp, "shred");
    let keep = tmp.join("keep.bin");
    let remove = tmp.join("remove.bin");
    fs::write(&keep, vec![0x5a; 64 * 1024]).unwrap();
    fs::write(&remove, vec![0x33; 64 * 1024]).unwrap();

    assert_success(run_alias(
        &shred,
        &["-n", "1", "-z", keep.to_str().unwrap()],
    ));
    assert_eq!(fs::metadata(&keep).unwrap().len(), 64 * 1024);
    assert!(fs::read(&keep).unwrap().iter().all(|&byte| byte == 0));

    assert_success(run_alias(
        &shred,
        &["-n", "1", "-u", remove.to_str().unwrap()],
    ));
    assert!(!remove.exists());
}
