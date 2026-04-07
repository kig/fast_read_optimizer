use fro::config::{AppConfig, ConfigBundleV1, DeviceDbConfig, IOParams, MountOverrides};
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

fn write_config(path: &std::path::Path, defaults: AppConfig) {
    let bundle = ConfigBundleV1 {
        version: 1,
        defaults,
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig::default(),
    };
    fs::write(path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();
}

#[test]
fn diff_still_reports_mismatch_when_config_qd_is_zero() {
    let tmp = unique_temp_dir("fro-diff-zero-qd");
    let cfg = tmp.join("fro.json");
    let left = tmp.join("left.bin");
    let right = tmp.join("right.bin");

    let mut defaults = AppConfig::default();
    defaults.diff.page_cache = IOParams {
        num_threads: 1,
        block_size: 4096,
        qd: 0,
    };
    write_config(&cfg, defaults);

    let left_bytes = vec![b'a'; 8192];
    let mut right_bytes = left_bytes.clone();
    right_bytes[4096] = b'b';
    fs::write(&left, &left_bytes).unwrap();
    fs::write(&right, &right_bytes).unwrap();

    let out = run_fro(&[
        "diff",
        "--no-direct",
        "-n",
        "1",
        "-c",
        cfg.to_str().unwrap(),
        left.to_str().unwrap(),
        right.to_str().unwrap(),
    ]);

    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("Mismatch at offset 4096"),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
