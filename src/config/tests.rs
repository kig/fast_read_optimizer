use super::device::{
    clear_device_signature_cache_for_tests, device_signature_from_mount_info_with_roots,
    mount_info_for_path_from_data,
};
use super::*;
use std::path::Path;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());
static CACHE_LOCK: Mutex<()> = Mutex::new(());

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
    clear_default_config_cache_for_tests();

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
    clear_default_config_cache_for_tests();
}

#[test]
fn load_config_default_cache_tracks_resolved_path_changes() {
    let _lock = ENV_LOCK.lock().unwrap();
    clear_default_config_cache_for_tests();

    let tmp = unique_temp_dir("fro-default-cache-paths");
    let cfg_a = tmp.join("a.json");
    let cfg_b = tmp.join("b.json");

    let old_fro = set_env_var("FRO_CONFIG", Some(cfg_a.to_str().unwrap()));
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let loaded_a = load_config(None);
    assert_eq!(loaded_a.config_path(), cfg_a.as_path());

    set_env_var("FRO_CONFIG", Some(cfg_b.to_str().unwrap()));
    let loaded_b = load_config(None);
    assert_eq!(loaded_b.config_path(), cfg_b.as_path());

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
    clear_default_config_cache_for_tests();
}

#[test]
fn mountinfo_cache_preserves_mount_result_within_process() {
    let _lock = CACHE_LOCK.lock().unwrap();
    clear_mountinfo_cache_for_tests();

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let cold = mount_info_for_path(path.to_str().unwrap()).expect("mount info");
    let warm = mount_info_for_path(path.to_str().unwrap()).expect("mount info");

    assert_eq!(warm, cold);

    clear_mountinfo_cache_for_tests();
}

#[test]
fn device_signature_cache_preserves_signature_within_process() {
    let _lock = CACHE_LOCK.lock().unwrap();
    clear_device_signature_cache_for_tests();

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let cold = load_config(None).device_signature_for_path(path.to_str().unwrap());
    let warm = load_config(None).device_signature_for_path(path.to_str().unwrap());

    assert_eq!(warm, cold);

    clear_device_signature_cache_for_tests();
}

#[test]
fn explicit_config_path_bypasses_default_config_cache() {
    let _lock = ENV_LOCK.lock().unwrap();
    clear_default_config_cache_for_tests();

    let tmp = unique_temp_dir("fro-default-cache-explicit");
    let cfg_path = tmp.join("fro.json");

    let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let cached = load_config(None);
    assert_eq!(cached.get_params("read", true).num_threads, 16);

    let mut replacement = default_bundle_v1();
    replacement.defaults.update_params(
        "read",
        true,
        IOParams {
            num_threads: 77,
            block_size: 8 * 1024,
            qd: 5,
        },
    );
    std::fs::write(
        &cfg_path,
        serde_json::to_string_pretty(&replacement).unwrap(),
    )
    .unwrap();

    let explicit = load_config(Some(cfg_path.to_str().unwrap()));
    assert_eq!(explicit.get_params("read", true).num_threads, 77);
    assert_eq!(load_config(None).get_params("read", true).num_threads, 16);

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
    clear_default_config_cache_for_tests();
}

#[test]
fn load_config_save_refreshes_default_config_cache() {
    let _lock = ENV_LOCK.lock().unwrap();
    clear_default_config_cache_for_tests();

    let tmp = unique_temp_dir("fro-default-cache-save");
    let cfg_path = tmp.join("fro.json");

    let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
    let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
    let old_home = set_env_var("HOME", None);

    let mut loaded = load_config(None);
    loaded.update_params(
        "read",
        true,
        IOParams {
            num_threads: 88,
            block_size: 16 * 1024,
            qd: 6,
        },
    );
    loaded.save();

    let reloaded = load_config(None);
    assert_eq!(reloaded.get_params("read", true).num_threads, 88);
    assert_eq!(reloaded.config_path(), cfg_path.as_path());

    restore_env_var("FRO_CONFIG", old_fro);
    restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
    restore_env_var("HOME", old_home);
    clear_default_config_cache_for_tests();
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
fn copy_auto_mode_config_path_lookup_uses_longest_override_prefix() {
    let mut defaults = AppConfig::default();
    defaults.copy_auto_mode = CopyAutoMode::Heuristic;

    let loaded = LoadedConfig::BundleV1 {
        path: unique_temp_dir("fro-copy-auto-prefix").join("fro.json"),
        bundle: ConfigBundleV1 {
            version: 1,
            defaults,
            mount_overrides: MountOverrides {
                by_mountpoint: std::collections::HashMap::from([
                    (
                        "/data".to_string(),
                        AppConfigPatch {
                            copy_auto_mode: Some(CopyAutoMode::Direct),
                            ..AppConfigPatch::default()
                        },
                    ),
                    (
                        "/data/fro".to_string(),
                        AppConfigPatch {
                            copy_auto_mode: Some(CopyAutoMode::CopyFileRange),
                            ..AppConfigPatch::default()
                        },
                    ),
                ]),
            },
            device_db: DeviceDbConfig::default(),
        },
    };

    assert_eq!(
        loaded.get_copy_auto_mode_for_config_path("/data/fro/run/output.bin"),
        CopyAutoMode::CopyFileRange
    );
    assert_eq!(
        loaded.get_copy_auto_mode_for_config_path("/data/other/output.bin"),
        CopyAutoMode::Direct
    );
    assert_eq!(
        loaded.get_copy_auto_mode_for_config_path("/elsewhere/output.bin"),
        CopyAutoMode::Heuristic
    );
    assert_eq!(
        loaded.get_copy_auto_mode_for_path("/data/fro/run/output.bin"),
        CopyAutoMode::CopyFileRange
    );
}

#[test]
fn cat_dev_null_backend_config_path_lookup_uses_longest_override_prefix() {
    let loaded = LoadedConfig::BundleV1 {
        path: unique_temp_dir("fro-cat-dev-null-prefix").join("fro.json"),
        bundle: ConfigBundleV1 {
            version: 1,
            defaults: AppConfig::default(),
            mount_overrides: MountOverrides {
                by_mountpoint: std::collections::HashMap::from([
                    (
                        "/data".to_string(),
                        AppConfigPatch {
                            cat_dev_null_backend: Some(CatDevNullBackend::BufferedCopy),
                            ..AppConfigPatch::default()
                        },
                    ),
                    (
                        "/data/fro".to_string(),
                        AppConfigPatch {
                            cat_dev_null_backend: Some(CatDevNullBackend::FastCopy),
                            ..AppConfigPatch::default()
                        },
                    ),
                ]),
            },
            device_db: DeviceDbConfig::default(),
        },
    };

    assert_eq!(
        loaded.get_cat_dev_null_backend_for_config_path("/data/fro/run/output.bin"),
        CatDevNullBackend::FastCopy
    );
    assert_eq!(
        loaded.get_cat_dev_null_backend_for_config_path("/data/other/output.bin"),
        CatDevNullBackend::BufferedCopy
    );
    assert_eq!(
        loaded.get_cat_dev_null_backend_for_config_path("/elsewhere/output.bin"),
        CatDevNullBackend::Auto
    );
}

#[test]
fn get_params_for_path_uses_longest_config_prefix_override() {
    let mut defaults = AppConfig::default();
    defaults.update_params(
        "read",
        true,
        IOParams {
            num_threads: 11,
            block_size: 128 * 1024,
            qd: 1,
        },
    );

    let loaded = LoadedConfig::BundleV1 {
        path: unique_temp_dir("fro-config-prefix-params").join("fro.json"),
        bundle: ConfigBundleV1 {
            version: 1,
            defaults,
            mount_overrides: MountOverrides {
                by_mountpoint: std::collections::HashMap::from([
                    (
                        "/data".to_string(),
                        AppConfigPatch {
                            read: Some(ModeConfigPatch {
                                direct: Some(IOParams {
                                    num_threads: 22,
                                    block_size: 256 * 1024,
                                    qd: 2,
                                }),
                                ..ModeConfigPatch::default()
                            }),
                            ..AppConfigPatch::default()
                        },
                    ),
                    (
                        "/data/fro".to_string(),
                        AppConfigPatch {
                            read: Some(ModeConfigPatch {
                                direct: Some(IOParams {
                                    num_threads: 33,
                                    block_size: 512 * 1024,
                                    qd: 3,
                                }),
                                ..ModeConfigPatch::default()
                            }),
                            ..AppConfigPatch::default()
                        },
                    ),
                ]),
            },
            device_db: DeviceDbConfig::default(),
        },
    };

    let nested = loaded.get_params_for_path("read", true, "/data/fro/run/output.bin");
    assert_eq!(nested.num_threads, 33);
    assert_eq!(nested.block_size, 512 * 1024);
    assert_eq!(nested.qd, 3);

    let broader = loaded.get_params_for_path("read", true, "/data/other/output.bin");
    assert_eq!(broader.num_threads, 22);
    assert_eq!(broader.block_size, 256 * 1024);
    assert_eq!(broader.qd, 2);

    let defaulted = loaded.get_params_for_path("read", true, "/elsewhere/output.bin");
    assert_eq!(defaulted.num_threads, 11);
    assert_eq!(defaulted.block_size, 128 * 1024);
    assert_eq!(defaulted.qd, 1);
}

#[test]
fn missing_device_db_paths_are_memoized_after_first_miss() {
    clear_missing_device_db_paths_for_tests();

    let tmp = unique_temp_dir("fro-device-db-miss-cache");
    let target = tmp.join("nested").join("file.bin");
    let missing_db = tmp.join("missing-device-db.json");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"x").unwrap();

    let loaded = LoadedConfig::BundleV1 {
        path: tmp.join("fro.json"),
        bundle: ConfigBundleV1 {
            version: 1,
            defaults: AppConfig::default(),
            mount_overrides: MountOverrides::default(),
            device_db: DeviceDbConfig {
                paths: vec![missing_db.to_string_lossy().into_owned()],
                allow_online_update: false,
            },
        },
    };

    assert!(!missing_device_db_path_is_cached_for_tests(
        missing_db.to_str().unwrap()
    ));
    let _ = loaded.effective_config_for_path(target.to_str().unwrap());
    assert!(missing_device_db_path_is_cached_for_tests(
        missing_db.to_str().unwrap()
    ));
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

mod device_probe;
mod precedence;
