    use super::*;
    use std::path::Path;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("{}-{}-{}", prefix, pid, nanos));
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
        assert_eq!(reloaded.get_read_auto_strategy_for_path(tmp.to_str().unwrap()), strategy);
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
        assert_eq!(reloaded.get_read_auto_strategy_for_path(tmp.to_str().unwrap()), strategy);
    }
