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
fn grep_finds_literal_match_across_block_boundary() {
    let tmp = unique_temp_dir("fro-grep-boundary");
    let cfg = tmp.join("fro.json");
    let target = tmp.join("target.bin");

    let mut defaults = AppConfig::default();
    defaults.grep.page_cache = IOParams {
        num_threads: 1,
        block_size: 4096,
        qd: 2,
    };
    write_config(&cfg, defaults);

    let mut data = vec![b'x'; 8192];
    data[4094..4098].copy_from_slice(b"abcd");
    fs::write(&target, &data).unwrap();

    let out = run_fro(&[
        "grep",
        "--no-direct",
        "-n",
        "1",
        "-c",
        cfg.to_str().unwrap(),
        "abcd",
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("4094:abcd"));
}
