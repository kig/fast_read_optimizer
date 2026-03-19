use fro::block_hash::{
    default_hash_base, verify_file_with_replicas, BlockHashAlgorithm, BlockHashManifest,
};
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
fn verified_copy_api_creates_target_sidecars_and_verifies_destination() {
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
    assert_eq!(fs::read(&target).unwrap(), bytes);

    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&target, suffix).exists());
    }

    let manifest_text = fs::read_to_string(sidecar_path(&target, "0")).unwrap();
    let manifest: BlockHashManifest = serde_json::from_str(&manifest_text).unwrap();
    assert_eq!(manifest.hash_type, BlockHashAlgorithm::Sha256);
    assert_eq!(manifest.file_size, bytes.len() as u64);

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
fn verified_copy_cli_reports_success_and_writes_sidecars() {
    let tmp = unique_temp_dir("fro-verified-copy-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 513))
        .map(|i| ((i * 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verified",
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
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("verified-copy:"));
    assert!(stdout.contains("repaired_blocks=0"));
    assert_eq!(fs::read(&target).unwrap(), bytes);
    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn verified_copy_cli_rejects_optimizer_iterations() {
    let tmp = unique_temp_dir("fro-verified-copy-cli-reject");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"hello verified copy").unwrap();

    let out = run_fro(&[
        "copy",
        "--verified",
        "-n",
        "2",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("copy --verified requires -n 1"));
}
