use super::*;

fn precedence_contract_defaults() -> AppConfig {
    let mut defaults = AppConfig::default();
    defaults.read.direct = IOParams {
        num_threads: 11,
        block_size: 2 * 1024 * 1024,
        qd: 5,
    };
    defaults.read.page_cache = IOParams {
        num_threads: 12,
        block_size: 128 * 1024,
        qd: 2,
    };
    defaults.grep.direct = IOParams {
        num_threads: 13,
        block_size: 256 * 1024,
        qd: 3,
    };
    defaults.cat_dev_null_backend = CatDevNullBackend::Auto;
    defaults.copy_auto_mode = CopyAutoMode::Heuristic;
    defaults.recursive_small_file_threads = RecursiveSmallFileThreads { hot: 4, cold: 6 };
    defaults
}

fn write_precedence_device_db(path: &Path, ext4_match: &str, xfs_match: &str) {
    let db = serde_json::json!({
        "version": 1,
        "profiles": [
            {
                "id": ext4_match,
                "match": {
                    "fstype": "ext4",
                },
                "params": {
                    "read": {
                        "direct": { "num_threads": 21, "block_size": 512 * 1024, "qd": 6 },
                        "page_cache": { "num_threads": 22, "block_size": 64 * 1024, "qd": 4 }
                    },
                    "grep": {
                        "direct": { "num_threads": 23, "block_size": 1024 * 1024, "qd": 5 }
                    },
                    "cat_dev_null_backend": "fast_copy",
                    "copy_auto_mode": "direct"
                }
            },
            {
                "id": xfs_match,
                "match": {
                    "fstype": "xfs",
                },
                "params": {
                    "read": {
                        "direct": { "num_threads": 31, "block_size": 1024 * 1024, "qd": 7 }
                    },
                    "recursive_small_file_threads": {
                        "hot": 33,
                        "cold": 34
                    }
                }
            }
        ]
    });
    std::fs::write(path, serde_json::to_string_pretty(&db).unwrap()).unwrap();
}

#[test]
fn config_selection_precedence_contract_is_stable() {
    let tmp = unique_temp_dir("fro-config-selection-contract");
    let device_db_path = tmp.join("device-db.json");
    write_precedence_device_db(&device_db_path, "ext4-defaults", "xfs-defaults");

    let mut by_mountpoint = std::collections::HashMap::new();
    by_mountpoint.insert(
        "/mnt/fast".to_string(),
        AppConfigPatch {
            read: Some(ModeConfigPatch {
                direct: Some(IOParams {
                    num_threads: 91,
                    block_size: 4 * 1024,
                    qd: 9,
                }),
                page_cache: None,
            }),
            cat_dev_null_backend: Some(CatDevNullBackend::BufferedCopy),
            recursive_small_file_threads: Some(RecursiveSmallFileThreads { hot: 17, cold: 19 }),
            ..AppConfigPatch::default()
        },
    );
    by_mountpoint.insert(
        "/mnt/archive".to_string(),
        AppConfigPatch {
            copy_auto_mode: Some(CopyAutoMode::CopyFileRange),
            ..AppConfigPatch::default()
        },
    );

    let bundle = ConfigBundleV1 {
        version: 1,
        defaults: precedence_contract_defaults(),
        mount_overrides: MountOverrides { by_mountpoint },
        device_db: DeviceDbConfig {
            paths: vec![device_db_path.to_string_lossy().into_owned()],
            allow_online_update: false,
        },
    };
    let loaded = LoadedConfig::BundleV1 {
        path: tmp.join("fro.json"),
        bundle,
    };

    struct Case<'a> {
        name: &'a str,
        mount: Option<MountInfo>,
        expected_profile: Option<&'a str>,
        expect_mount_override: bool,
        read_direct_threads: u64,
        read_page_cache_threads: u64,
        grep_direct_threads: u64,
        cat_dev_null_backend: CatDevNullBackend,
        copy_auto_mode: CopyAutoMode,
        recursive_threads: RecursiveSmallFileThreads,
    }

    let cases = [
        Case {
            name: "defaults only when nothing matches",
            mount: Some(MountInfo {
                mount_point: "/mnt/tmpfs".into(),
                fstype: "tmpfs".into(),
                mount_source: "tmpfs".into(),
            }),
            expected_profile: None,
            expect_mount_override: false,
            read_direct_threads: 11,
            read_page_cache_threads: 12,
            grep_direct_threads: 13,
            cat_dev_null_backend: CatDevNullBackend::Auto,
            copy_auto_mode: CopyAutoMode::Heuristic,
            recursive_threads: RecursiveSmallFileThreads { hot: 4, cold: 6 },
        },
        Case {
            name: "device db overrides defaults on matching filesystem",
            mount: Some(MountInfo {
                mount_point: "/mnt/base".into(),
                fstype: "ext4".into(),
                mount_source: "/dev/nvme0n1p1".into(),
            }),
            expected_profile: Some("ext4-defaults"),
            expect_mount_override: false,
            read_direct_threads: 21,
            read_page_cache_threads: 22,
            grep_direct_threads: 23,
            cat_dev_null_backend: CatDevNullBackend::FastCopy,
            copy_auto_mode: CopyAutoMode::Direct,
            recursive_threads: RecursiveSmallFileThreads { hot: 4, cold: 6 },
        },
        Case {
            name: "mount override wins over device db for that mount only",
            mount: Some(MountInfo {
                mount_point: "/mnt/fast".into(),
                fstype: "ext4".into(),
                mount_source: "/dev/nvme1n1p1".into(),
            }),
            expected_profile: Some("ext4-defaults"),
            expect_mount_override: true,
            read_direct_threads: 91,
            read_page_cache_threads: 22,
            grep_direct_threads: 23,
            cat_dev_null_backend: CatDevNullBackend::BufferedCopy,
            copy_auto_mode: CopyAutoMode::Direct,
            recursive_threads: RecursiveSmallFileThreads { hot: 17, cold: 19 },
        },
        Case {
            name: "per-mount override layers on top of a different matched profile",
            mount: Some(MountInfo {
                mount_point: "/mnt/archive".into(),
                fstype: "xfs".into(),
                mount_source: "/dev/sdb1".into(),
            }),
            expected_profile: Some("xfs-defaults"),
            expect_mount_override: true,
            read_direct_threads: 31,
            read_page_cache_threads: 12,
            grep_direct_threads: 13,
            cat_dev_null_backend: CatDevNullBackend::Auto,
            copy_auto_mode: CopyAutoMode::CopyFileRange,
            recursive_threads: RecursiveSmallFileThreads { hot: 33, cold: 34 },
        },
    ];

    for case in cases {
        let device = case.mount.as_ref().map(|mount| DeviceSignature {
            mount_source: mount.mount_source.clone(),
            canonical_source: mount.mount_source.clone(),
            match_keys: vec![],
            block_device: None,
        });
        let selection =
            loaded.device_db_selection_for_context(case.mount.as_ref(), device.as_ref());
        assert_eq!(
            selection
                .as_ref()
                .map(|matched| matched.profile.id.as_str()),
            case.expected_profile,
            "{}: unexpected device-db selection",
            case.name
        );
        assert_eq!(
            loaded.mount_patch_for_mount(case.mount.as_ref()).is_some(),
            case.expect_mount_override,
            "{}: unexpected mount override presence",
            case.name
        );

        let effective = loaded.effective_config_for_context(case.mount.as_ref(), device.as_ref());
        assert_eq!(
            effective.read.direct.num_threads, case.read_direct_threads,
            "{}: read.direct precedence changed",
            case.name
        );
        assert_eq!(
            effective.read.page_cache.num_threads, case.read_page_cache_threads,
            "{}: read.page_cache merge changed",
            case.name
        );
        assert_eq!(
            effective.grep.direct.num_threads, case.grep_direct_threads,
            "{}: grep.direct merge changed",
            case.name
        );
        assert_eq!(
            effective.cat_dev_null_backend, case.cat_dev_null_backend,
            "{}: cat_dev_null_backend precedence changed",
            case.name
        );
        assert_eq!(
            effective.copy_auto_mode, case.copy_auto_mode,
            "{}: copy_auto_mode precedence changed",
            case.name
        );
        assert_eq!(
            effective.recursive_small_file_threads, case.recursive_threads,
            "{}: recursive thread precedence changed",
            case.name
        );
    }
}

#[test]
fn path_explain_and_save_contract_preserves_device_db_then_mount_override() {
    let tmp = unique_temp_dir("fro-config-path-contract");
    let cfg_path = tmp.join("fro.json");
    let device_db_path = tmp.join("device-db.json");
    let target = tmp.join("nested").join("file.bin");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"x").unwrap();

    let mount = mount_info_for_path(target.to_str().unwrap()).expect("mount info");
    let profile_id = format!("{}-profile", mount.fstype);
    let db = serde_json::json!({
        "version": 1,
        "profiles": [
            {
                "id": profile_id,
                "match": {
                    "fstype": mount.fstype,
                },
                "params": {
                    "read": {
                        "direct": { "num_threads": 41, "block_size": 1024 * 1024, "qd": 4 },
                        "page_cache": { "num_threads": 42, "block_size": 64 * 1024, "qd": 2 }
                    },
                    "grep": {
                        "direct": { "num_threads": 43, "block_size": 2 * 1024 * 1024, "qd": 5 }
                    },
                    "copy_auto_mode": "direct"
                }
            }
        ]
    });
    std::fs::write(&device_db_path, serde_json::to_string_pretty(&db).unwrap()).unwrap();

    let bundle = ConfigBundleV1 {
        version: 1,
        defaults: precedence_contract_defaults(),
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig {
            paths: vec![device_db_path.to_string_lossy().into_owned()],
            allow_online_update: false,
        },
    };
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let loaded = load_config(Some(cfg_path.to_str().unwrap()));
    let before = loaded.explain_for_path(target.to_str().unwrap());
    assert_eq!(before["device_db_match"]["profile_id"], profile_id);
    assert!(before["mount_override"].is_null());
    assert_eq!(before["defaults"]["read"]["direct"]["num_threads"], 11);
    assert_eq!(before["effective"]["read"]["direct"]["num_threads"], 41);
    assert_eq!(before["effective"]["read"]["page_cache"]["num_threads"], 42);
    assert_eq!(before["effective"]["grep"]["direct"]["num_threads"], 43);
    assert_eq!(before["effective"]["copy_auto_mode"], "direct");

    let mut updated = load_config(Some(cfg_path.to_str().unwrap()));
    updated.update_params_for_path(
        "read",
        true,
        target.to_str().unwrap(),
        IOParams {
            num_threads: 77,
            block_size: 256 * 1024,
            qd: 9,
        },
    );
    updated.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    let after = reloaded.explain_for_path(target.to_str().unwrap());
    assert_eq!(after["mount"]["mount_point"], mount.mount_point);
    assert_eq!(
        after["device_db_match"]["profile_id"],
        before["device_db_match"]["profile_id"]
    );
    assert_eq!(after["mount_override"]["read"]["direct"]["num_threads"], 77);
    assert_eq!(after["effective"]["read"]["direct"]["num_threads"], 77);
    assert_eq!(after["effective"]["read"]["page_cache"]["num_threads"], 42);
    assert_eq!(after["effective"]["grep"]["direct"]["num_threads"], 43);
    assert_eq!(after["effective"]["copy_auto_mode"], "direct");
}

#[test]
fn promote_mount_override_to_defaults_for_path_merges_and_clears_entry() {
    let tmp = unique_temp_dir("fro-promote-mount-override");
    let cfg_path = tmp.join("fro.json");
    let target = tmp.join("nested").join("file.bin");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"x").unwrap();

    let mount = mount_info_for_path(target.to_str().unwrap()).expect("mount info");
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
                    num_threads: 77,
                    block_size: 256 * 1024,
                    qd: 9,
                }),
                page_cache: None,
            }),
            grep: Some(ModeConfigPatch {
                direct: None,
                page_cache: Some(IOParams {
                    num_threads: 88,
                    block_size: 64 * 1024,
                    qd: 3,
                }),
            }),
            ..AppConfigPatch::default()
        },
    );

    let bundle = ConfigBundleV1 {
        version: 1,
        defaults,
        mount_overrides: MountOverrides { by_mountpoint },
        device_db: DeviceDbConfig::default(),
    };
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));
    assert!(loaded.promote_mount_override_to_defaults_for_path(target.to_str().unwrap()));
    loaded.save();

    let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
    let explained = reloaded.explain_for_path(target.to_str().unwrap());
    assert!(explained["mount_override"].is_null());
    assert_eq!(explained["defaults"]["read"]["direct"]["num_threads"], 77);
    assert_eq!(
        explained["defaults"]["read"]["direct"]["block_size"],
        256 * 1024
    );
    assert_eq!(
        explained["defaults"]["grep"]["page_cache"]["num_threads"],
        88
    );
    assert_eq!(explained["effective"]["read"]["direct"]["num_threads"], 77);
    assert_eq!(
        explained["effective"]["grep"]["page_cache"]["num_threads"],
        88
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
                    },
                    "copy_auto_mode": "direct"
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
            copy_auto_mode: Some(CopyAutoMode::CopyFileRange),
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
    assert_eq!(effective.copy_auto_mode, CopyAutoMode::CopyFileRange);
}

#[test]
fn device_db_match_applies_sparse_profile_fields_over_defaults() {
    let tmp = unique_temp_dir("fro-device-db-sparse-profile");
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
                "id": "sparse-profile",
                "match": {
                    "fstype": mount.fstype,
                },
                "params": {
                    "copy_auto_mode": "direct",
                    "recursive_small_file_threads": {
                        "hot": 27,
                        "cold": 28
                    }
                }
            }
        ]
    });
    std::fs::write(&device_db_path, serde_json::to_string_pretty(&db).unwrap()).unwrap();

    let mut defaults = AppConfig::default();
    defaults.copy_auto_mode = CopyAutoMode::Heuristic;
    defaults.recursive_small_file_threads = RecursiveSmallFileThreads { hot: 8, cold: 9 };

    let bundle = ConfigBundleV1 {
        version: 1,
        defaults,
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig {
            paths: vec![device_db_path.to_string_lossy().into_owned()],
            allow_online_update: false,
        },
    };
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&bundle).unwrap()).unwrap();

    let loaded = load_config(Some(cfg_path.to_str().unwrap()));
    let effective = loaded.effective_config_for_path(target.to_str().unwrap());
    assert_eq!(effective.copy_auto_mode, CopyAutoMode::Direct);
    assert_eq!(
        effective.recursive_small_file_threads,
        RecursiveSmallFileThreads { hot: 27, cold: 28 }
    );
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
