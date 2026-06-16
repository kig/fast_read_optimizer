use super::cli_utils::parse_size;
use super::inspect::{
    device_db_match, is_disk_backed_mount, parse_zfs_get_props, parse_zpool_status_leaves,
};
use super::*;
use fro::test_sizing::{resolve_test_size, FsSizingStats, TestSizeSource, TestSizingPolicy};
use std::path::Path;
use std::process::Command;

#[test]
fn list_devices_filters_pseudo_and_snap_fs() {
    let entries = vec![
        MountInfoEntry {
            mount_point: "/".into(),
            fstype: "ext4".into(),
            mount_source: "/dev/nvme0n1p2".into(),
            major_minor: "259:2".into(),
            ..Default::default()
        },
        MountInfoEntry {
            mount_point: "/run".into(),
            fstype: "tmpfs".into(),
            mount_source: "tmpfs".into(),
            major_minor: "0:5".into(),
            ..Default::default()
        },
        MountInfoEntry {
            mount_point: "/snap/core".into(),
            fstype: "squashfs".into(),
            mount_source: "/dev/loop0".into(),
            major_minor: "7:0".into(),
            ..Default::default()
        },
        MountInfoEntry {
            mount_point: "/var/lib/data".into(),
            fstype: "xfs".into(),
            mount_source: "/dev/md0".into(),
            major_minor: "9:0".into(),
            ..Default::default()
        },
    ];

    let kept: Vec<_> = entries
        .into_iter()
        .filter(|e| is_disk_backed_mount(e))
        .collect();
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[0].mount_point, "/");
    assert_eq!(kept[1].mount_point, "/var/lib/data");
}

#[test]
fn list_devices_allows_zfs_without_dev_mount_source() {
    let z = MountInfoEntry {
        mount_point: "/tank".into(),
        fstype: "zfs".into(),
        mount_source: "tank/dataset".into(),
        major_minor: "0:0".into(),
        ..Default::default()
    };
    assert!(is_disk_backed_mount(&z));
}

#[test]
fn device_db_matches_ext4_mdraid0() {
    let db: DeviceDb = serde_json::from_str(include_str!("../../../fro-device-db.json")).unwrap();
    let mut e = MountInfoEntry {
        mount_point: "/data".into(),
        fstype: "ext4".into(),
        mount_source: "/dev/md127".into(),
        major_minor: "9:127".into(),
        device_kind: Some("md".into()),
        device_name: Some("md127".into()),
        device_model: Some("unknown".into()),
        md_level: Some("raid0".into()),
        signature: Some("fstype=ext4;dev=md;level=raid0;model=unknown".into()),
        ..Default::default()
    };

    let p = device_db_match(&db, &e).unwrap();
    assert_eq!(p.id, "example-ext4-mdraid0");
    e.device_db_profile = Some(p.id.clone());
    e.device_db_read_direct = Some(p.params.read.direct.clone());
    assert_eq!(e.device_db_read_direct.unwrap().block_size, 3145728);
}

#[test]
fn parse_zfs_get_props_parses_tab_separated_pairs() {
    let out = "recordsize\t128K\ncompression\toff\ncompressratio\t1.00x\n";
    let m = parse_zfs_get_props(out);
    assert_eq!(m.get("recordsize").unwrap(), "128K");
    assert_eq!(m.get("compression").unwrap(), "off");
    assert_eq!(m.get("compressratio").unwrap(), "1.00x");
}

#[test]
fn parse_zpool_status_leaves_extracts_vdev_path_and_was() {
    let out = r#"
  pool: tank
 state: DEGRADED
config:

        NAME                                        STATE     READ WRITE CKSUM
        tank                                        DEGRADED     0     0     0
          mirror-0                                  DEGRADED     0     0     0
            11842916200294856737                    UNAVAIL      0     0     0  was /dev/disk/by-id/nvme-KCD61LUL7T68_6140A14ST4Z8_1-part2
            nvme-KCD61LUL7T68_6150A0DXT4Z8_1-part2  ONLINE       0     0     0

errors: No known data errors
"#;
    let leaves = parse_zpool_status_leaves(out);
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].vdev_path, vec!["mirror-0"]);
    assert_eq!(leaves[0].state.as_deref(), Some("UNAVAIL"));
    assert_eq!(
        leaves[0].was.as_deref(),
        Some("/dev/disk/by-id/nvme-KCD61LUL7T68_6140A14ST4Z8_1-part2")
    );
    assert_eq!(leaves[1].vdev_path, vec!["mirror-0"]);
    assert_eq!(leaves[1].state.as_deref(), Some("ONLINE"));
}

#[test]
fn parse_size_accepts_common_suffixes() {
    assert_eq!(parse_size("1024"), Some(1024));
    assert_eq!(parse_size("1KiB"), Some(1024));
    assert_eq!(parse_size("2MiB"), Some(2 * 1024 * 1024));
    assert_eq!(parse_size("4GiB"), Some(4 * 1024 * 1024 * 1024));
    assert_eq!(parse_size("1g"), Some(1024 * 1024 * 1024));
    assert_eq!(parse_size(""), None);
    assert_eq!(parse_size("nope"), None);
}

#[test]
fn read_pattern_does_not_match_read_to_memory_configs() {
    let cfg = vec![
        "read".to_string(),
        "--to-memory".to_string(),
        "-s".to_string(),
    ];
    let is_to_memory = cfg.iter().any(|arg| arg == "--to-memory");
    let pattern = "read";
    let matches_plain_read = cfg[0] == "read"
        && !is_to_memory
        && (pattern == "read" || pattern == "read-page-cache" || pattern == "read-direct");
    let matches_read_to_memory =
        (pattern == "read-to-memory" || pattern == "read_to_memory") && is_to_memory;
    let matches_other =
        cfg[0].starts_with(pattern) && !(cfg[0] == "read" && is_to_memory && pattern == "read");
    assert!(!(matches_plain_read || matches_read_to_memory || matches_other));
}

#[test]
fn read_to_memory_pattern_matches_read_to_memory_configs() {
    let cfg = vec![
        "read".to_string(),
        "--to-memory".to_string(),
        "-s".to_string(),
    ];
    let is_to_memory = cfg.iter().any(|arg| arg == "--to-memory");
    let pattern = "read-to-memory";
    let matches_plain_read = cfg[0] == "read"
        && !is_to_memory
        && (pattern == "read" || pattern == "read-page-cache" || pattern == "read-direct");
    let matches_read_to_memory =
        (pattern == "read-to-memory" || pattern == "read_to_memory") && is_to_memory;
    let matches_other =
        cfg[0].starts_with(pattern) && !(cfg[0] == "read" && is_to_memory && pattern == "read");
    assert!(matches_plain_read || matches_read_to_memory || matches_other);
}

#[test]
fn resolve_target_selection_uses_target_parent_as_default_benchmark_dir() {
    let tmp = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join("fro-optimize-target-selection");
    std::fs::create_dir_all(&tmp).unwrap();
    let target = tmp.join("data.bin");

    let selection = run::resolve_target_selection(None, Some(target.to_str().unwrap())).unwrap();
    assert_eq!(selection.benchmark_dir, tmp.display().to_string());
    assert_eq!(selection.target_path.as_deref(), target.to_str());
    assert!(selection.target_mount.is_some());
}

#[test]
fn resolve_target_selection_accepts_same_mount_test_dir_and_target() {
    let tmp = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join("fro-optimize-target-selection-same-mount");
    let bench_dir = tmp.join("bench");
    let target_dir = tmp.join("target-dir");
    std::fs::create_dir_all(&bench_dir).unwrap();
    std::fs::create_dir_all(&target_dir).unwrap();
    let target = target_dir.join("data.bin");

    let selection = run::resolve_target_selection(
        Some(bench_dir.to_str().unwrap()),
        Some(target.to_str().unwrap()),
    )
    .unwrap();
    assert_eq!(selection.benchmark_dir, bench_dir.display().to_string());
    assert_eq!(selection.target_path.as_deref(), target.to_str());
    assert!(selection.target_mount.is_some());
}

#[derive(serde::Deserialize)]
struct OptimizeConfigFile {
    defaults: OptimizeAppConfig,
    #[serde(default)]
    mount_overrides: OptimizeMountOverrides,
}

#[derive(Default, serde::Deserialize)]
struct OptimizeMountOverrides {
    #[serde(default)]
    by_mountpoint: std::collections::HashMap<String, OptimizeAppConfigPatch>,
}

#[derive(serde::Deserialize)]
struct OptimizeAppConfig {
    read: OptimizeModeConfig,
}

#[derive(serde::Deserialize)]
struct OptimizeModeConfig {
    direct: IOParams,
    page_cache: IOParams,
}

#[derive(serde::Deserialize)]
struct OptimizeAppConfigPatch {
    #[serde(default)]
    read: Option<OptimizeModeConfigPatch>,
}

#[derive(serde::Deserialize)]
struct OptimizeModeConfigPatch {
    #[serde(default)]
    direct: Option<IOParams>,
    #[serde(default)]
    page_cache: Option<IOParams>,
}

#[test]
fn fro_optimize_for_path_persists_into_mount_override() {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!(
            "fro-optimize-mount-override-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&target_dir).unwrap();
    let config_path = target_dir.join("fro.json");
    let target_path = target_dir.join("data.bin");
    std::fs::write(&target_path, b"x").unwrap();

    let status = Command::new("cargo")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--quiet",
            "--bin",
            "fro-optimize",
            "--",
            "-c",
            config_path.to_str().unwrap(),
            "--for",
            target_path.to_str().unwrap(),
            "--test-size",
            "4096",
            "--iters",
            "1",
            "read",
        ])
        .status()
        .expect("run fro-optimize");
    assert!(status.success());

    let saved: OptimizeConfigFile =
        serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
    let mount = fro::config::mount_info_for_path(target_path.to_str().unwrap()).unwrap();
    let override_entry = saved
        .mount_overrides
        .by_mountpoint
        .get(&mount.mount_point)
        .expect("mount override entry");
    let read_patch = override_entry.read.as_ref().expect("read override");
    assert!(read_patch.direct.is_some());
    assert!(read_patch.page_cache.is_some());
    assert_eq!(
        saved.defaults.read.direct.block_size,
        fro::config::AppConfig::default().read.direct.block_size
    );
    assert_eq!(
        saved.defaults.read.page_cache.block_size,
        fro::config::AppConfig::default().read.page_cache.block_size
    );

    let _ = std::fs::remove_dir_all(target_dir);
}

#[test]
fn fro_optimize_global_for_path_promotes_mount_override_into_defaults() {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!(
            "fro-optimize-global-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&target_dir).unwrap();
    let config_path = target_dir.join("fro.json");
    let target_path = target_dir.join("data.bin");
    std::fs::write(&target_path, b"x").unwrap();

    let status = Command::new("cargo")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--quiet",
            "--bin",
            "fro-optimize",
            "--",
            "-c",
            config_path.to_str().unwrap(),
            "--global",
            "--for",
            target_path.to_str().unwrap(),
            "--test-size",
            "4096",
            "--iters",
            "1",
            "read",
        ])
        .status()
        .expect("run fro-optimize --global");
    assert!(status.success());

    let saved: OptimizeConfigFile =
        serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
    let mount = fro::config::mount_info_for_path(target_path.to_str().unwrap()).unwrap();
    assert!(!saved
        .mount_overrides
        .by_mountpoint
        .contains_key(&mount.mount_point));
    let loaded = fro::config::load_config(Some(config_path.to_str().unwrap()));
    let explained = loaded.explain_for_path(target_path.to_str().unwrap());
    assert!(explained["mount_override"].is_null());
    assert_eq!(
        explained["effective"]["read"]["direct"],
        explained["defaults"]["read"]["direct"]
    );
    assert_eq!(
        explained["effective"]["read"]["page_cache"],
        explained["defaults"]["read"]["page_cache"]
    );

    let _ = std::fs::remove_dir_all(target_dir);
}

#[test]
fn fro_optimize_global_requires_for_path() {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!(
            "fro-optimize-global-requires-target-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&target_dir).unwrap();
    let config_path = target_dir.join("fro.json");

    let status = Command::new("cargo")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--quiet",
            "--bin",
            "fro-optimize",
            "--",
            "-c",
            config_path.to_str().unwrap(),
            "--global",
            "--test-size",
            "4096",
            "--iters",
            "1",
            "read",
        ])
        .status()
        .expect("run fro-optimize --global without target");
    assert!(!status.success());

    let _ = std::fs::remove_dir_all(target_dir);
}

#[test]
fn resolve_test_size_preserves_explicit_override_for_optimizer() {
    let resolved = resolve_test_size(
        Some(FsSizingStats {
            total_bytes: 1024_u64.pow(4),
            avail_bytes: 1024_u64.pow(4),
        }),
        TestSizingPolicy {
            explicit_test_size: Some(12345),
            file_count: 3,
            num_full_writes: 83,
            fixed_write_bytes: 0,
            min_test_size: 256 * 1024 * 1024,
            max_test_size: 1024 * 1024 * 1024,
            max_drive_writes: 0.05,
            fallback_test_size: 4 * 1024 * 1024 * 1024,
        },
    );
    assert_eq!(resolved.size_bytes, 12288);
    assert_eq!(resolved.source, TestSizeSource::Explicit);
}
