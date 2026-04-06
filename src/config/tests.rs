use super::*;
use super::device::device_signature_from_mount_info_with_roots;
use super::storage::{default_system_config_path, default_user_config_path};
use std::path::Path;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!("{}-{}-{}", prefix, pid, nanos));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn set_env_var(key: &str, value: Option<&str>) -> Option<String> {
    let old = std::env::var(key).ok();
    match value {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
    old
}

fn restore_env_var(key: &str, old: Option<String>) {
    match old {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

#[test]
fn resolve_default_config_path_prefers_fro_config_env() {
    let _lock = ENV_LOCK.lock().unwrap();

    let tmp = unique_temp_dir("fro-test");
    let cfg_path = tmp.join("cfg.json");

    let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let resolved = resolve_default_config_path();
    assert_eq!(resolved, cfg_path);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn resolve_default_config_path_defaults_to_user_path() {
    let _lock = ENV_LOCK.lock().unwrap();

    let home = unique_temp_dir("fro-home");
    let old_fro = set_env_var("FRO_CONFIG", None);
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", Some(home.to_str().unwrap()));

    let resolved = resolve_default_config_path();
    assert!(resolved.ends_with(Path::new(".fro/fro.json")));
    assert!(resolved.starts_with(&home));

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn resolve_default_config_path_uses_system_when_user_missing_and_system_exists() {
    let _lock = ENV_LOCK.lock().unwrap();

    let home = unique_temp_dir("fro-home");
    let tmp = unique_temp_dir("fro-sys");
    let sys_cfg = tmp.join("fro.json");
    std::fs::write(&sys_cfg, "{}\n").unwrap();

    let old_fro = set_env_var("FRO_CONFIG", None);
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", Some(sys_cfg.to_str().unwrap()));
    let old_home = set_env_var("HOME", Some(home.to_str().unwrap()));

    let resolved = resolve_default_config_path();
    assert_eq!(resolved, sys_cfg);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn load_config_creates_bundle_and_roundtrips_updates() {
    let _lock = ENV_LOCK.lock().unwrap();

    let tmp = unique_temp_dir("fro-test");
    let cfg_path = tmp.join("fro.json");

    let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let mut loaded = load_config(None);
    match &loaded {
        LoadedConfig::BundleV1 { path, bundle } => {
            assert_eq!(path, &cfg_path);
            assert_eq!(bundle.version, 1);
        }
        _ => panic!("expected bundle v1"),
    }

    assert!(cfg_path.exists());
    let text = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(text.contains("\"version\": 1"));
    assert!(text.contains("\"defaults\""));

    let p0 = loaded.get_params("read", true);
    assert_eq!(p0.num_threads, 16);
    let p_to_mem = loaded.get_params("read_to_memory", true);
    assert_eq!(p_to_mem.num_threads, 16);
    let copy_range0 = loaded.get_copy_range_params();
    assert_eq!(copy_range0.block_size, 512 * 1024);
    assert_eq!(loaded.get_copy_auto_mode(), CopyAutoMode::Heuristic);

    loaded.update_params(
        "read",
        true,
        IOParams {
            num_threads: 99,
            block_size: 4 * 1024,
            qd: 7,
        },
    );
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    let p1 = reloaded.get_params("read", true);
    assert_eq!(p1.num_threads, 99);
    assert_eq!(p1.block_size, 4 * 1024);
    assert_eq!(p1.qd, 7);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn load_config_reads_legacy_appconfig() {
    let _lock = ENV_LOCK.lock().unwrap();

    let tmp = unique_temp_dir("fro-test");
    let cfg_path = tmp.join("legacy.json");

    let old_fro = set_env_var("FRO_CONFIG", None);
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let legacy = AppConfig::default();
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&legacy).unwrap()).unwrap();

    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));
    match loaded {
        LoadedConfig::Legacy { ref path, .. } => assert_eq!(path, &cfg_path),
        _ => panic!("expected legacy config"),
    }

    loaded.update_params(
        "diff",
        false,
        IOParams {
            num_threads: 3,
            block_size: 123,
            qd: 1,
        },
    );
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    let p = reloaded.get_params("diff", false);
    assert_eq!(p.num_threads, 3);
    assert_eq!(p.block_size, 123);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn load_config_reads_legacy_appconfig_without_hash_verify_fields() {
    let _lock = ENV_LOCK.lock().unwrap();

    let tmp = unique_temp_dir("fro-test");
    let cfg_path = tmp.join("legacy-missing-hash-verify.json");

    let old_fro = set_env_var("FRO_CONFIG", None);
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let legacy = AppConfig::default();
    let legacy_text = serde_json::json!({
        "read": legacy.read,
        "write": legacy.write,
        "copy": legacy.copy,
        "grep": legacy.grep,
        "diff": legacy.diff,
        "dual_read_bench": legacy.dual_read_bench,
    });
    std::fs::write(
        &cfg_path,
        serde_json::to_string_pretty(&legacy_text).unwrap(),
    )
    .unwrap();

    let loaded = load_config(Some(cfg_path.to_str().unwrap()));
    let hash = loaded.get_params("hash", false);
    let verify = loaded.get_params("verify", false);
    let compute = loaded.get_params("compute", false);
    let copy_range = loaded.get_copy_range_params();
    assert_eq!(hash.block_size, crate::block_hash::BLOCK_HASH_SIZE);
    assert_eq!(verify.block_size, crate::block_hash::BLOCK_HASH_SIZE);
    assert_eq!(compute.block_size, crate::block_hash::BLOCK_HASH_SIZE);
    assert_eq!(hash.num_threads, 31);
    assert_eq!(compute.num_threads, 32);
    assert_eq!(verify.qd, 1);
    assert_eq!(copy_range.block_size, 512 * 1024);
    assert_eq!(loaded.get_copy_auto_mode(), CopyAutoMode::Heuristic);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
}

#[test]
fn load_config_keeps_malformed_file_on_disk() {
    let _lock = ENV_LOCK.lock().unwrap();

    let tmp = unique_temp_dir("fro-test");
    let cfg_path = tmp.join("broken.json");
    std::fs::write(&cfg_path, "{ definitely not json\n").unwrap();

    let loaded = load_config(Some(cfg_path.to_str().unwrap()));
    match loaded {
        LoadedConfig::BundleV1 {
            ref path,
            ref bundle,
        } => {
            assert_eq!(path, &cfg_path);
            assert_eq!(bundle.version, 1);
        }
        _ => panic!("expected bundle fallback"),
    }

    let text = std::fs::read_to_string(&cfg_path).unwrap();
    assert_eq!(text, "{ definitely not json\n");
}

#[test]
fn bundle_mount_overrides_roundtrip_copy_range_and_auto_mode() {
    let tmp = unique_temp_dir("fro-copy-range-config");
    let cfg_path = tmp.join("fro.json");
    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));

    loaded.update_copy_range_params_for_path(
        tmp.to_str().unwrap(),
        IOParams {
            num_threads: 7,
            block_size: 2 * 1024 * 1024,
            qd: 3,
        },
    );
    loaded.update_copy_auto_mode_for_path(tmp.to_str().unwrap(), CopyAutoMode::CopyFileRange);
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    let copy_range = reloaded.get_copy_range_params_for_path(tmp.to_str().unwrap());
    assert_eq!(copy_range.num_threads, 7);
    assert_eq!(copy_range.block_size, 2 * 1024 * 1024);
    assert_eq!(copy_range.qd, 3);
    assert_eq!(
        reloaded.get_copy_auto_mode_for_path(tmp.to_str().unwrap()),
        CopyAutoMode::CopyFileRange
    );
}

#[test]
fn bundle_mount_overrides_roundtrip_read_auto_strategy() {
    let tmp = unique_temp_dir("fro-read-auto-config");
    let cfg_path = tmp.join("fro.json");
    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));

    let strategy = ReadAutoStrategy {
        hot_large_min_bytes: 64 * 1024 * 1024,
        cold_large_min_bytes: 128 * 1024 * 1024,
        hot_small_path: ReadPathKind::SimplePageCache,
        hot_large_path: ReadPathKind::ThreadedPageCache,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::ThreadedDirect,
    };
    loaded.update_read_auto_strategy_for_path(tmp.to_str().unwrap(), strategy);
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    assert_eq!(
        reloaded.get_read_auto_strategy_for_path(tmp.to_str().unwrap()),
        strategy
    );
}

#[test]
fn bundle_mount_overrides_roundtrip_recursive_small_file_threads() {
    let tmp = unique_temp_dir("fro-recursive-small-file-threads");
    let cfg_path = tmp.join("fro.json");
    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));

    let threads = RecursiveSmallFileThreads { hot: 13, cold: 55 };
    loaded.update_recursive_small_file_threads_for_path(tmp.to_str().unwrap(), threads);
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    assert_eq!(
        reloaded.get_recursive_small_file_threads_for_path(tmp.to_str().unwrap()),
        threads
    );
}

#[test]
fn device_db_match_applies_before_mount_override() {
    let tmp = unique_temp_dir("fro-device-db-precedence");
    let cfg_path = tmp.join("fro.json");
    let device_db_path = tmp.join("device-db.json");
    let target = tmp.join("nested").join("file.bin");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"x").unwrap();

    let mount = mount_info_for_path(target.to_str().unwrap()).expect("mount info");
    let db = serde_json::json!({
        "version": 1,
        "profiles": [
            {
                "id": "test-profile",
                "match": {
                    "fstype": mount.fstype,
                },
                "params": {
                    "read": {
                        "direct": { "num_threads": 21, "block_size": 512 * 1024, "qd": 6 },
                        "page_cache": { "num_threads": 22, "block_size": 64 * 1024, "qd": 2 }
                    },
                    "grep": {
                        "direct": { "num_threads": 23, "block_size": 1024 * 1024, "qd": 4 },
                        "page_cache": { "num_threads": 24, "block_size": 32 * 1024, "qd": 3 }
                    }
                }
            }
        ]
    });
    std::fs::write(&device_db_path, serde_json::to_string_pretty(&db).unwrap()).unwrap();

    let mut defaults = AppConfig::default();
    defaults.read.direct = IOParams {
        num_threads: 11,
        block_size: 2 * 1024 * 1024,
        qd: 5,
    };
    defaults.grep.page_cache = IOParams {
        num_threads: 12,
        block_size: 128 * 1024,
        qd: 1,
    };

    let mut by_mountpoint = std::collections::HashMap::new();
    by_mountpoint.insert(
        mount.mount_point.clone(),
        AppConfigPatch {
            read: Some(ModeConfigPatch {
                direct: Some(IOParams {
                    num_threads: 99,
                    block_size: 4 * 1024,
                    qd: 7,
                }),
                page_cache: None,
            }),
            ..AppConfigPatch::default()
        },
    );

    let bundle = ConfigBundleV1 {
        version: 1,
        defaults,
        mount_overrides: MountOverrides { by_mountpoint },
        device_db: DeviceDbConfig {
            paths: vec![device_db_path.to_string_lossy().into_owned()],
            allow_online_update: false,
        },
    };
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let loaded = load_config(Some(cfg_path.to_str().unwrap()));
    let effective = loaded.effective_config_for_path(target.to_str().unwrap());
    assert_eq!(effective.read.direct.num_threads, 99);
    assert_eq!(effective.read.page_cache.num_threads, 22);
    assert_eq!(effective.grep.direct.num_threads, 23);
    assert_eq!(effective.grep.page_cache.num_threads, 24);
}

#[test]
fn device_db_match_uses_structured_device_fields() {
    let db: DeviceDb = serde_json::from_value(serde_json::json!({
        "version": 1,
        "profiles": [
            {
                "id": "md-raid0-fast-model",
                "match": {
                    "fstype": "ext4",
                    "dev_kind": "md",
                    "md_level": "raid0",
                    "dev_model_contains": "Fast"
                },
                "params": {
                    "read": {
                        "direct": { "num_threads": 8, "block_size": 1048576, "qd": 2 },
                        "page_cache": { "num_threads": 4, "block_size": 131072, "qd": 1 }
                    }
                }
            }
        ]
    }))
    .unwrap();
    let mount = MountInfo {
        mount_point: "/data".into(),
        fstype: "ext4".into(),
        mount_source: "/dev/md0".into(),
    };
    let device = DeviceSignature {
        mount_source: "/dev/md0".into(),
        canonical_source: "/dev/md0".into(),
        match_keys: vec![],
        block_device: Some(BlockDeviceSignature {
            kernel_name: "md0".into(),
            devnode: "/dev/md0".into(),
            by_id: vec![],
            vendor: None,
            model: Some("Very Fast Array".into()),
            rotational: Some(false),
            dm_name: None,
            md_level: Some("raid0".into()),
            slaves: vec![],
        }),
    };

    let matched = device_db_match(&db, &mount, Some(&device)).expect("matched profile");
    assert_eq!(matched.id, "md-raid0-fast-model");
}

#[test]
fn legacy_config_promotes_to_bundle_for_mount_override() {
    let tmp = unique_temp_dir("fro-legacy-promote");
    let cfg_path = tmp.join("fro.json");
    let legacy = AppConfig::default();
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&legacy).unwrap()).unwrap();

    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));
    let strategy = ReadAutoStrategy {
        hot_large_min_bytes: 1,
        cold_large_min_bytes: 1,
        hot_small_path: ReadPathKind::SimpleDirect,
        hot_large_path: ReadPathKind::SimpleDirect,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::SimpleDirect,
    };
    loaded.update_read_auto_strategy_for_path(tmp.to_str().unwrap(), strategy);
    loaded.save();

    let text = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(text.contains("\"version\": 1"));
    assert!(text.contains("\"mount_overrides\""));

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    assert_eq!(
        reloaded.get_read_auto_strategy_for_path(tmp.to_str().unwrap()),
        strategy
    );
}

fn write_trimmed_file(path: &Path, value: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, format!("{value}\n")).unwrap();
}

#[test]
fn device_signature_extracts_leaf_device_metadata_and_by_id() {
    let tmp = unique_temp_dir("fro-device-signature-leaf");
    let dev_root = tmp.join("dev");
    let sys_root = tmp.join("sys/class/block");
    std::fs::create_dir_all(dev_root.join("disk/by-id")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/queue")).unwrap();
    std::fs::write(dev_root.join("nvme0n1"), b"").unwrap();
    std::os::unix::fs::symlink("../../nvme0n1", dev_root.join("disk/by-id/nvme-fastdisk")).unwrap();
    write_trimmed_file(&sys_root.join("nvme0n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme0n1/device/model"), "Turbo");
    write_trimmed_file(&sys_root.join("nvme0n1/queue/rotational"), "0");

    let mount = MountInfo {
        mount_point: "/mnt/data".into(),
        fstype: "ext4".into(),
        mount_source: "/dev/disk/by-id/nvme-fastdisk".into(),
    };
    let roots = DeviceProbeRoots {
        dev_root: dev_root.clone(),
        sys_class_block_root: sys_root.clone(),
    };

    let signature = device_signature_from_mount_info_with_roots(&mount, &roots).unwrap();
    assert_eq!(signature.mount_source, "/dev/disk/by-id/nvme-fastdisk");
    assert_eq!(signature.canonical_source, "/dev/nvme0n1");
    assert!(signature
        .match_keys
        .contains(&"canonical_source=/dev/nvme0n1".to_string()));
    assert!(signature
        .match_keys
        .contains(&"by-id=nvme-fastdisk".to_string()));

    let block = signature.block_device.unwrap();
    assert_eq!(block.kernel_name, "nvme0n1");
    assert_eq!(block.devnode, "/dev/nvme0n1");
    assert_eq!(block.by_id, vec!["nvme-fastdisk".to_string()]);
    assert_eq!(block.vendor.as_deref(), Some("ACME"));
    assert_eq!(block.model.as_deref(), Some("Turbo"));
    assert_eq!(block.rotational, Some(false));
    assert!(block.slaves.is_empty());
}

#[test]
fn device_signature_extracts_composite_dm_and_md_slaves() {
    let tmp = unique_temp_dir("fro-device-signature-composite");
    let dev_root = tmp.join("dev");
    let sys_root = tmp.join("sys/class/block");
    std::fs::create_dir_all(dev_root.join("disk/by-id")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/dm")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/slaves")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/md")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/slaves")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme1n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme1n1/queue")).unwrap();
    std::fs::write(dev_root.join("dm-0"), b"").unwrap();
    std::fs::write(dev_root.join("md0"), b"").unwrap();
    std::fs::write(dev_root.join("nvme0n1"), b"").unwrap();
    std::fs::write(dev_root.join("nvme1n1"), b"").unwrap();
    std::os::unix::fs::symlink("../../dm-0", dev_root.join("disk/by-id/dm-uuid-crypt")).unwrap();
    std::os::unix::fs::symlink("../../md0", dev_root.join("disk/by-id/md-array")).unwrap();
    std::os::unix::fs::symlink("../../nvme0n1", dev_root.join("disk/by-id/nvme-disk-a")).unwrap();
    std::os::unix::fs::symlink("../../nvme1n1", dev_root.join("disk/by-id/nvme-disk-b")).unwrap();
    std::os::unix::fs::symlink("../md0", sys_root.join("dm-0/slaves/md0")).unwrap();
    std::os::unix::fs::symlink("../nvme0n1", sys_root.join("md0/slaves/nvme0n1")).unwrap();
    std::os::unix::fs::symlink("../nvme1n1", sys_root.join("md0/slaves/nvme1n1")).unwrap();
    write_trimmed_file(&sys_root.join("dm-0/dm/name"), "cryptroot");
    write_trimmed_file(&sys_root.join("dm-0/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("md0/md/level"), "raid0");
    write_trimmed_file(&sys_root.join("md0/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("nvme0n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme0n1/device/model"), "Fast A");
    write_trimmed_file(&sys_root.join("nvme0n1/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("nvme1n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme1n1/device/model"), "Fast B");
    write_trimmed_file(&sys_root.join("nvme1n1/queue/rotational"), "0");

    let mount = MountInfo {
        mount_point: "/mnt/crypt".into(),
        fstype: "xfs".into(),
        mount_source: "/dev/mapper/cryptroot".into(),
    };
    let roots = DeviceProbeRoots {
        dev_root: dev_root.clone(),
        sys_class_block_root: sys_root.clone(),
    };

    std::fs::create_dir_all(dev_root.join("mapper")).unwrap();
    std::os::unix::fs::symlink("../dm-0", dev_root.join("mapper/cryptroot")).unwrap();

    let signature = device_signature_from_mount_info_with_roots(&mount, &roots).unwrap();
    assert_eq!(signature.canonical_source, "/dev/dm-0");
    let block = signature.block_device.unwrap();
    assert_eq!(block.kernel_name, "dm-0");
    assert_eq!(block.dm_name.as_deref(), Some("cryptroot"));
    assert_eq!(block.by_id, vec!["dm-uuid-crypt".to_string()]);
    assert_eq!(block.slaves.len(), 1);
    let md = &block.slaves[0];
    assert_eq!(md.kernel_name, "md0");
    assert_eq!(md.md_level.as_deref(), Some("raid0"));
    assert_eq!(md.by_id, vec!["md-array".to_string()]);
    assert_eq!(md.slaves.len(), 2);
    assert!(signature
        .match_keys
        .contains(&"dm_name=cryptroot".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.md_level=raid0".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.slave.by-id=nvme-disk-a".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.slave.by-id=nvme-disk-b".to_string()));
}
