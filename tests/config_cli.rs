use fro::config::{
    load_config, AppConfig, AppConfigPatch, ConfigBundleV1, DeviceDbConfig, IOParams,
    ModeConfigPatch, MountOverrides, RecursiveSmallFileThreads,
};
use fro::CopyAutoMode;
use serde_json::Value;
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

fn sample_bundle(mount_point: &str) -> ConfigBundleV1 {
    let mut defaults = AppConfig::default();
    defaults.read.direct = IOParams {
        num_threads: 11,
        block_size: 2 * 1024 * 1024,
        qd: 5,
    };
    defaults.copy_auto_mode = CopyAutoMode::Heuristic;
    defaults.recursive_small_file_threads = RecursiveSmallFileThreads { hot: 8, cold: 9 };

    let mut by_mountpoint = std::collections::HashMap::new();
    by_mountpoint.insert(
        mount_point.to_string(),
        AppConfigPatch {
            read: Some(ModeConfigPatch {
                direct: Some(IOParams {
                    num_threads: 99,
                    block_size: 4 * 1024,
                    qd: 7,
                }),
                page_cache: None,
            }),
            copy_auto_mode: Some(CopyAutoMode::CopyFileRange),
            recursive_small_file_threads: Some(RecursiveSmallFileThreads { hot: 17, cold: 19 }),
            ..AppConfigPatch::default()
        },
    );

    ConfigBundleV1 {
        version: 1,
        defaults,
        mount_overrides: MountOverrides { by_mountpoint },
        device_db: DeviceDbConfig {
            paths: vec!["/etc/fro.d/fro-device-db.json".to_string()],
            allow_online_update: false,
        },
    }
}

fn mount_point_for(path: &std::path::Path) -> String {
    let loaded = load_config(None);
    loaded
        .mount_info_for_path(path.to_str().unwrap())
        .map(|info| info.mount_point)
        .unwrap_or_else(|| "/".to_string())
}

#[test]
fn config_print_emits_bundle_json() {
    let tmp = unique_temp_dir("fro-config-print");
    let cfg = tmp.join("fro.json");
    let bundle = sample_bundle(&mount_point_for(&tmp));
    fs::write(&cfg, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let out = run_fro(&["config", "print", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let printed: Value = serde_json::from_slice(&out.stdout).unwrap();
    let expected: Value =
        serde_json::from_str(&serde_json::to_string_pretty(&bundle).unwrap()).unwrap();
    assert_eq!(printed, expected);
}

#[test]
fn config_explain_reports_mount_override_and_effective_values() {
    let tmp = unique_temp_dir("fro-config-explain");
    let cfg = tmp.join("fro.json");
    let target = tmp.join("data.bin");
    fs::write(&target, b"hello").unwrap();

    let expected_mount = mount_point_for(&target);
    let bundle = sample_bundle(&expected_mount);
    fs::write(&cfg, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let out = run_fro(&[
        "config",
        "explain",
        "-c",
        cfg.to_str().unwrap(),
        "--for",
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let explain: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(explain["config_format"], "bundle_v1");
    assert_eq!(
        explain["config_path"],
        Value::String(cfg.to_string_lossy().into_owned())
    );
    assert_eq!(
        explain["mount"]["mount_point"],
        Value::String(expected_mount)
    );
    assert_eq!(
        explain["mount_override"]["copy_auto_mode"],
        "copy_file_range"
    );
    assert!(explain.get("device").is_some());
    assert_eq!(explain["effective"]["read"]["direct"]["num_threads"], 99);
    assert_eq!(explain["effective"]["copy_auto_mode"], "copy_file_range");
    assert_eq!(
        explain["effective"]["recursive_small_file_threads"]["hot"],
        17
    );
    assert_eq!(explain["defaults"]["read"]["direct"]["num_threads"], 11);
    assert!(explain["device_db_match"].is_null());
}

#[test]
fn config_explain_reports_device_db_match_and_precedence() {
    let tmp = unique_temp_dir("fro-config-explain-device-db");
    let cfg = tmp.join("fro.json");
    let device_db = tmp.join("device-db.json");
    let target = tmp.join("data.bin");
    fs::write(&target, b"hello").unwrap();

    let mount_info = load_config(None)
        .mount_info_for_path(target.to_str().unwrap())
        .expect("mount info");
    fs::write(
        &device_db,
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 1,
            "profiles": [
                {
                    "id": "fstype-profile",
                    "match": {
                        "fstype": mount_info.fstype,
                    },
                    "params": {
                        "read": {
                            "direct": { "num_threads": 41, "block_size": 1048576, "qd": 4 },
                            "page_cache": { "num_threads": 42, "block_size": 65536, "qd": 2 }
                        },
                        "grep": {
                            "direct": { "num_threads": 43, "block_size": 2097152, "qd": 5 },
                            "page_cache": { "num_threads": 44, "block_size": 32768, "qd": 3 }
                        }
                    }
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();

    let mut bundle = sample_bundle(&mount_point_for(&target));
    bundle.device_db.paths = vec![device_db.to_string_lossy().into_owned()];
    fs::write(&cfg, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let out = run_fro(&[
        "config",
        "explain",
        "-c",
        cfg.to_str().unwrap(),
        "--for",
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let explain: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(explain["device_db_match"]["profile_id"], "fstype-profile");
    assert_eq!(
        explain["device_db_match"]["source_path"],
        Value::String(device_db.to_string_lossy().into_owned())
    );
    assert_eq!(
        explain["device_db_match"]["params"]["read"]["page_cache"]["num_threads"],
        42
    );
    assert_eq!(explain["effective"]["read"]["direct"]["num_threads"], 99);
    assert_eq!(
        explain["effective"]["read"]["page_cache"]["num_threads"],
        42
    );
    assert_eq!(explain["effective"]["grep"]["direct"]["num_threads"], 43);
}

#[test]
fn config_explain_requires_for_path() {
    let tmp = unique_temp_dir("fro-config-explain-missing");
    let cfg = tmp.join("fro.json");
    fs::write(
        &cfg,
        serde_json::to_string_pretty(&sample_bundle(&mount_point_for(&tmp))).unwrap(),
    )
    .unwrap();

    let out = run_fro(&["config", "explain", "-c", cfg.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("config explain requires --for <path>"));
}

#[test]
fn loaded_config_effective_config_applies_mount_patch() {
    let tmp = unique_temp_dir("fro-config-effective");
    let cfg = tmp.join("fro.json");
    let target = tmp.join("nested").join("file.bin");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, b"x").unwrap();

    let bundle = sample_bundle(&mount_point_for(&target));
    fs::write(&cfg, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let loaded = load_config(Some(cfg.to_str().unwrap()));
    let effective = loaded.effective_config_for_path(target.to_str().unwrap());
    assert_eq!(effective.read.direct.num_threads, 99);
    assert_eq!(effective.copy_auto_mode, CopyAutoMode::CopyFileRange);
    assert_eq!(
        effective.recursive_small_file_threads,
        RecursiveSmallFileThreads { hot: 17, cold: 19 }
    );
}

#[test]
fn config_help_lists_subcommands() {
    let out = run_fro(&["config", "--help"]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("config <print|explain>"));
    assert!(text.contains("config print"));
    assert!(text.contains("config explain --for"));
    assert!(text.contains("device signature"));
    assert!(text.contains("device-db profile"));
}
