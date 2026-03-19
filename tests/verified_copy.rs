use fro::block_hash::{default_hash_base, verify_file_with_replicas, BlockHashAlgorithm};
use std::fs;
use std::process::Command;

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();

    let p = base.join(format!("{}-{}-{}", prefix, pid, nanos));
    fs::create_dir_all(&p).unwrap();
    p
}

fn run_fro(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro")
}

fn sidecar_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    path.with_extension(format!("bin.fro-hash.{}.json", suffix))
}

#[test]
fn verified_copy_api_is_ephemeral_by_default() {
    let tmp = unique_temp_dir("fro-verified-copy-api");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(2 * 1024 * 1024 + 777))
        .map(|i| ((i * 19) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let report = fro::copy_file_verified_with_options(
        &source,
        &target,
        fro::IOMode::PageCache,
        fro::IOMode::PageCache,
        BlockHashAlgorithm::Sha256,
        false,
        None,
    )
    .unwrap();

    assert_eq!(report.bytes_copied, bytes.len() as u64);
    assert_eq!(report.source_bytes_hashed, bytes.len() as u64);
    assert_eq!(report.repaired_blocks, 0);
    assert!(!report.used_recovery);
    assert_eq!(report.hash_type, BlockHashAlgorithm::Sha256);
    assert!(!report.hashes_persisted);
    assert_eq!(fs::read(&target).unwrap(), bytes);

    for suffix in ["0", "1", "2"] {
        assert!(!sidecar_path(&source, suffix).exists());
        assert!(!sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn verified_copy_api_can_persist_hashes_when_requested() {
    let tmp = unique_temp_dir("fro-verified-copy-api-hash");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 777))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let target_hash_base = default_hash_base(target.to_str().unwrap());
    let report = fro::copy_file_verified_with_options(
        &source,
        &target,
        fro::IOMode::PageCache,
        fro::IOMode::PageCache,
        BlockHashAlgorithm::Sha256,
        false,
        Some(&target_hash_base),
    )
    .unwrap();

    assert!(report.hashes_persisted);
    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&source, suffix).exists());
        assert!(sidecar_path(&target, suffix).exists());
    }

    let verify = verify_file_with_replicas(
        target.to_str().unwrap(),
        Some(&default_hash_base(target.to_str().unwrap())),
        1,
        1024 * 1024,
        1,
        1,
        1024 * 1024,
        1,
        fro::IOMode::PageCache,
    )
    .unwrap();
    assert!(verify.bad_blocks.is_empty());
}

#[test]
fn copy_verify_cli_reports_success_to_stderr_and_leaves_no_sidecars() {
    let tmp = unique_temp_dir("fro-copy-verify-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 513))
        .map(|i| ((i * 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
        "--sha256",
        "--no-direct",
        "-n",
        "1",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!String::from_utf8_lossy(&out.stderr).contains("error"));
    assert!(String::from_utf8_lossy(&out.stderr).contains("copy verify: success"));
    assert_eq!(fs::read(&target).unwrap(), bytes);
    for suffix in ["0", "1", "2"] {
        assert!(!sidecar_path(&source, suffix).exists());
        assert!(!sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn copy_verify_hash_cli_writes_sidecars() {
    let tmp = unique_temp_dir("fro-copy-verify-hash-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 313))
        .map(|i| ((i * 31) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
        "--hash",
        "--sha256",
        "--no-direct",
        "-n",
        "1",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("sidecars_written=true"));
    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&source, suffix).exists());
        assert!(sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn copy_verify_diff_cli_reports_success_to_stderr() {
    let tmp = unique_temp_dir("fro-copy-verify-diff-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 919))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify-diff",
        "--no-direct",
        "-n",
        "1",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("copy verify-diff: success"));
}

#[test]
fn copy_verify_cli_rejects_optimizer_iterations() {
    let tmp = unique_temp_dir("fro-copy-verify-cli-reject");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"hello verified copy").unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
        "-n",
        "2",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("copy verification modes require -n 1"));
}
