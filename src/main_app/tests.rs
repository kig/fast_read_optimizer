use crate::common::{CopyStrategy, IOMode};
use crate::config;
use crate::main_app::cli;
use crate::main_app::copy_plan::CopyRewriteMode;
use crate::main_app::copy_plan::{
    choose_nonredundant_full_copy_plan, describe_copy_path, parse_zpool_status_leaves,
    should_prefer_cached_diff_overwrite, should_prefer_cached_read_direct_write,
    target_is_similar_size, zfs_storage_redundancy_from_status, HeuristicCopyPlan,
    ResolvedCopyExecution, StorageRedundancy,
};
use crate::main_app::recursive::move_dir;
use crate::main_app::tuning::{
    active_optimizer_param_mask, apply_manual_read_overrides, ManualReadOverrides,
};
use crate::main_app::{RecursiveCopyContext, RelativeCopyMethod};
use crate::writer::{self, RecordedCopyBackend};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static COPY_PATH_TEST_LOCK: Mutex<()> = Mutex::new(());

fn heuristic_plan(
    source_cached: bool,
    target_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
    direct_write_supported: bool,
) -> HeuristicCopyPlan {
    if should_prefer_cached_diff_overwrite(source_cached, target_cached, source_len, target_len) {
        if direct_write_supported {
            HeuristicCopyPlan::DiffOverwrite
        } else {
            HeuristicCopyPlan::CopyFileRangeSingle
        }
    } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len) {
        HeuristicCopyPlan::CachedReadDirectWrite
    } else {
        HeuristicCopyPlan::DirectReadDirectWrite
    }
}

#[test]
fn nonredundant_policy_forces_full_copy_path() {
    let cached = choose_nonredundant_full_copy_plan(true, Some(1024), Some(1024));
    assert!(cached.copy_strategy == CopyStrategy::Threaded);
    assert!(cached.io_mode_read == IOMode::PageCache);
    assert!(cached.io_mode_write == IOMode::Direct);
    assert!(cached.full_rewrite);
    assert!(!cached.diff_overwrite);

    let cold = choose_nonredundant_full_copy_plan(false, Some(1024), Some(1024));
    assert!(cold.copy_strategy == CopyStrategy::Threaded);
    assert!(cold.io_mode_read == IOMode::Direct);
    assert!(cold.io_mode_write == IOMode::Direct);
    assert!(cold.full_rewrite);
    assert!(!cold.diff_overwrite);
}

#[test]
fn parse_zpool_status_tracks_vdev_paths() {
    let leaves = parse_zpool_status_leaves(
        "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          mirror-0                  ONLINE       0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       ONLINE       0     0     0\n\nerrors: No known data errors\n",
    );
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].vdev_path, vec!["mirror-0".to_string()]);
    assert_eq!(leaves[0].state.as_deref(), Some("ONLINE"));
}

#[test]
fn zfs_redundancy_marks_healthy_mirror_redundant() {
    let status = "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          mirror-0                  ONLINE       0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       ONLINE       0     0     0\n\nerrors: No known data errors\n";
    assert_eq!(
        zfs_storage_redundancy_from_status(status),
        StorageRedundancy::Redundant
    );
}

#[test]
fn zfs_redundancy_marks_degraded_mirror_nonredundant() {
    let status = "  pool: tank\n state: DEGRADED\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        DEGRADED     0     0     0\n          mirror-0                  DEGRADED     0     0     0\n            /dev/disk/by-id/a       ONLINE       0     0     0\n            /dev/disk/by-id/b       UNAVAIL      0     0     0  was /dev/disk/by-id/b\n\nerrors: No known data errors\n";
    assert_eq!(
        zfs_storage_redundancy_from_status(status),
        StorageRedundancy::NonRedundant
    );
}

#[test]
fn zfs_redundancy_marks_stripe_nonredundant() {
    let status = "  pool: tank\n state: ONLINE\nconfig:\n\n        NAME                        STATE     READ WRITE CKSUM\n        tank                        ONLINE       0     0     0\n          /dev/disk/by-id/a         ONLINE       0     0     0\n          /dev/disk/by-id/b         ONLINE       0     0     0\n\nerrors: No known data errors\n";
    assert_eq!(
        zfs_storage_redundancy_from_status(status),
        StorageRedundancy::NonRedundant
    );
}

#[test]
fn verify_skips_block_size_mutations() {
    assert_eq!(
        active_optimizer_param_mask(
            "verify",
            IOMode::PageCache,
            IOMode::Auto,
            false,
            CopyStrategy::Threaded
        ),
        vec![true, false, true, false, false, false, false, false, false]
    );
    assert_eq!(
        active_optimizer_param_mask(
            "verify",
            IOMode::Direct,
            IOMode::Auto,
            false,
            CopyStrategy::Threaded
        ),
        vec![false, false, false, true, false, true, false, false, false]
    );
}

#[test]
fn write_only_mutates_write_side_params() {
    assert_eq!(
        active_optimizer_param_mask(
            "write",
            IOMode::Auto,
            IOMode::PageCache,
            false,
            CopyStrategy::Threaded
        ),
        vec![true, true, true, false, false, false, false, false, false]
    );
    assert_eq!(
        active_optimizer_param_mask(
            "copy",
            IOMode::PageCache,
            IOMode::Auto,
            false,
            CopyStrategy::Threaded
        ),
        vec![true, true, true, true, true, true, false, false, false]
    );
    assert_eq!(
        active_optimizer_param_mask(
            "copy",
            IOMode::PageCache,
            IOMode::Auto,
            true,
            CopyStrategy::Threaded
        ),
        vec![false, false, false, false, false, false, false, false, false]
    );
    assert_eq!(
        active_optimizer_param_mask(
            "copy",
            IOMode::PageCache,
            IOMode::Direct,
            true,
            CopyStrategy::Threaded
        ),
        vec![false, false, false, false, false, false, false, false, false]
    );
    assert_eq!(
        active_optimizer_param_mask(
            "copy",
            IOMode::PageCache,
            IOMode::PageCache,
            false,
            CopyStrategy::CopyFileRange
        ),
        vec![false, false, false, false, false, false, true, true, true]
    );
}

#[test]
fn manual_read_overrides_freeze_both_read_param_sets() {
    let mut start_params = vec![8, 16, 2, 12, 32, 4, 1, 64, 1];
    let mut params_steps = vec![1, 4096, 1, 1, 262144, 1, 1, 262144, 1];
    let mut mask = vec![true; 9];
    apply_manual_read_overrides(
        &mut start_params,
        &mut params_steps,
        &mut mask,
        ManualReadOverrides {
            threads: Some(5),
            block_size: Some(131072),
            qd: Some(7),
        },
    );
    assert_eq!(&start_params[..6], &[5, 131072, 7, 5, 131072, 7]);
    assert_eq!(&params_steps[..6], &[1, 1, 1, 1, 1, 1]);
    assert_eq!(&mask[..6], &[false, false, false, false, false, false]);
}

#[test]
fn cached_read_direct_write_requires_hot_close_sized_target() {
    assert!(should_prefer_cached_read_direct_write(
        true,
        Some(1024),
        Some(1024)
    ));
    assert!(should_prefer_cached_read_direct_write(
        true,
        Some(1024),
        Some(900)
    ));
    assert!(should_prefer_cached_read_direct_write(
        true,
        Some(1024),
        Some(2048)
    ));
    assert!(should_prefer_cached_read_direct_write(
        true,
        Some(0),
        Some(0)
    ));
    assert!(!should_prefer_cached_read_direct_write(
        false,
        Some(1024),
        Some(1024)
    ));
    assert!(!should_prefer_cached_read_direct_write(
        true,
        Some(1024),
        Some(716)
    ));
    assert!(!should_prefer_cached_read_direct_write(
        true,
        Some(1024),
        None
    ));
}

#[test]
fn cached_diff_overwrite_requires_hot_similar_sized_files() {
    assert!(should_prefer_cached_diff_overwrite(
        true,
        true,
        Some(1024),
        Some(900)
    ));
    assert!(!should_prefer_cached_diff_overwrite(
        true,
        false,
        Some(1024),
        Some(900)
    ));
    assert!(!should_prefer_cached_diff_overwrite(
        false,
        true,
        Some(1024),
        Some(900)
    ));
    assert!(!should_prefer_cached_diff_overwrite(
        true,
        true,
        Some(1024),
        Some(600)
    ));
}

#[test]
fn zero_length_source_is_only_similar_to_zero_length_target() {
    assert!(target_is_similar_size(Some(0), Some(0)));
    assert!(!target_is_similar_size(Some(0), Some(1)));
}

#[test]
fn heuristic_plan_prefers_diff_overwrite_then_copy_file_range_single_fallback() {
    assert_eq!(
        heuristic_plan(true, true, Some(1024), Some(900), true),
        HeuristicCopyPlan::DiffOverwrite
    );
    assert_eq!(
        heuristic_plan(true, true, Some(1024), Some(900), false),
        HeuristicCopyPlan::CopyFileRangeSingle
    );
    assert_eq!(
        heuristic_plan(true, false, Some(1024), Some(900), true),
        HeuristicCopyPlan::CachedReadDirectWrite
    );
    assert_eq!(
        heuristic_plan(false, false, Some(1024), Some(900), true),
        HeuristicCopyPlan::DirectReadDirectWrite
    );
}

#[test]
fn describe_copy_path_reports_diff_overwrite_details() {
    let path = describe_copy_path(
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: IOMode::PageCache,
            io_mode_write: IOMode::Direct,
            diff_overwrite: true,
            full_rewrite: false,
            path_label: "auto diff-overwrite",
        },
        false,
        false,
    );
    assert!(path.contains("strategy=auto diff-overwrite"));
    assert!(path.contains("read=page-cache"));
    assert!(path.contains("write=direct"));
    assert!(path.contains("delta=changed-chunks"));
}

#[test]
fn describe_copy_path_reports_via_memory_path() {
    let path = describe_copy_path(
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: IOMode::PageCache,
            io_mode_write: IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto cached-read direct-write",
        },
        true,
        false,
    );
    assert_eq!(
        path,
        "copy path: via-memory [read=page-cache, write=direct]"
    );
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("test-tmp");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{}-{}-{}", prefix, std::process::id(), nanos));
    fs::create_dir_all(&path).unwrap();
    path
}

fn set_env_var(key: &str, value: Option<&str>) -> Option<String> {
    let old = std::env::var(key).ok();
    match value {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
    old
}

fn restore_env_var(key: &str, old: Option<String>) {
    match old {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
}

fn set_path_mtime(path: &std::path::Path, seconds: i64) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let times = [
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(rc, 0, "failed to set timestamps for {}", path.display());
}

#[test]
fn cp_path_preserving_single_file_flags_keep_threaded_copy_backend() {
    let _lock = COPY_PATH_TEST_LOCK.lock().unwrap();
    struct Case {
        name: &'static str,
        configure: fn(&mut cli::TestCopyRunOptions, &std::path::Path, &std::path::Path),
    }

    let cases = [
        Case {
            name: "plain",
            configure: |_args, _source, _target| {},
        },
        Case {
            name: "verbose",
            configure: |args, _source, _target| args.verbose = true,
        },
        Case {
            name: "preserve",
            configure: |args, source, _target| {
                args.cp_preserve = true;
                set_path_mtime(source, 1_234_567_890);
            },
        },
        Case {
            name: "no-clobber-copy",
            configure: |args, _source, _target| args.cp_no_clobber = true,
        },
        Case {
            name: "update-copy",
            configure: |args, source, target| {
                args.cp_update = true;
                fs::write(target, b"stale").unwrap();
                set_path_mtime(target, 1_234_567_880);
                set_path_mtime(source, 1_234_567_890);
            },
        },
        Case {
            name: "no-target-directory",
            configure: |args, _source, _target| args.cp_no_target_directory = true,
        },
    ];

    for case in cases {
        let root = unique_temp_dir(&format!("fro-copy-path-single-{}", case.name));
        let source = root.join("source.txt");
        let target = root.join("target.txt");
        fs::write(&source, format!("payload-{}", case.name)).unwrap();

        let mut args = cli::TestCopyRunOptions {
            source: source.to_string_lossy().into_owned(),
            target: target.to_string_lossy().into_owned(),
            recursive: false,
            verbose: false,
            cp_no_clobber: false,
            cp_no_target_directory: false,
            cp_update: false,
            cp_preserve: false,
            cp_no_dereference: false,
        };
        (case.configure)(&mut args, &source, &target);

        writer::begin_copy_backend_trace(root.to_string_lossy().into_owned());
        let exit_code = cli::run_test_copy(args).unwrap();
        let backends = writer::finish_copy_backend_trace();

        assert_eq!(exit_code, 0, "case {}", case.name);
        assert_eq!(
            fs::read(&target).unwrap(),
            fs::read(&source).unwrap(),
            "case {}",
            case.name
        );
        assert_eq!(
            backends,
            vec![RecordedCopyBackend::Threaded],
            "case {}",
            case.name
        );

        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn cp_recursive_preserve_and_verbose_keep_threaded_copy_backend() {
    let _lock = COPY_PATH_TEST_LOCK.lock().unwrap();
    let root = unique_temp_dir("fro-copy-path-recursive");
    let source_root = root.join("source");
    let source_dir = source_root.join("dir");
    let target_root = root.join("target");
    fs::create_dir_all(&source_dir).unwrap();
    let source_file = source_dir.join("payload.bin");
    fs::write(&source_file, b"recursive-copy-payload").unwrap();
    set_path_mtime(&source_file, 1_234_567_890);

    let threshold = set_env_var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD", Some("1"));

    let args = cli::TestCopyRunOptions {
        source: source_root.to_string_lossy().into_owned(),
        target: target_root.to_string_lossy().into_owned(),
        recursive: true,
        verbose: true,
        cp_no_clobber: false,
        cp_no_target_directory: false,
        cp_update: false,
        cp_preserve: true,
        cp_no_dereference: false,
    };

    writer::begin_copy_backend_trace(root.to_string_lossy().into_owned());
    let exit_code = cli::run_test_copy(args).unwrap();
    let backends = writer::finish_copy_backend_trace();
    restore_env_var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD", threshold);

    let copied = target_root.join("dir/payload.bin");
    assert_eq!(exit_code, 0);
    assert_eq!(fs::read(&copied).unwrap(), fs::read(&source_file).unwrap());
    assert_eq!(backends, vec![RecordedCopyBackend::Threaded]);

    let source_mtime = fs::metadata(&source_file).unwrap().mtime();
    let copied_mtime = fs::metadata(&copied).unwrap().mtime();
    assert_eq!(copied_mtime, source_mtime);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn cp_archive_recursive_keeps_threaded_copy_backend() {
    let _lock = COPY_PATH_TEST_LOCK.lock().unwrap();
    let root = unique_temp_dir("fro-copy-path-recursive-archive");
    let source_root = root.join("source");
    let source_dir = source_root.join("dir");
    let target_root = root.join("target");
    fs::create_dir_all(&source_dir).unwrap();
    let source_file = source_dir.join("payload.bin");
    fs::write(&source_file, b"recursive-archive-payload").unwrap();
    set_path_mtime(&source_file, 1_234_567_890);
    std::os::unix::fs::symlink("../dir/payload.bin", source_root.join("payload-link")).unwrap();

    let threshold = set_env_var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD", Some("1"));

    let args = cli::TestCopyRunOptions {
        source: source_root.to_string_lossy().into_owned(),
        target: target_root.to_string_lossy().into_owned(),
        recursive: true,
        verbose: true,
        cp_no_clobber: false,
        cp_no_target_directory: false,
        cp_update: false,
        cp_preserve: true,
        cp_no_dereference: true,
    };

    writer::begin_copy_backend_trace(root.to_string_lossy().into_owned());
    let exit_code = cli::run_test_copy(args).unwrap();
    let backends = writer::finish_copy_backend_trace();
    restore_env_var("FRO_RECURSIVE_COPY_THREADED_THRESHOLD", threshold);

    let copied = target_root.join("dir/payload.bin");
    let copied_link = target_root.join("payload-link");
    assert_eq!(exit_code, 0);
    assert_eq!(fs::read(&copied).unwrap(), fs::read(&source_file).unwrap());
    assert_eq!(backends, vec![RecordedCopyBackend::Threaded]);
    assert_eq!(
        fs::read_link(&copied_link).unwrap(),
        PathBuf::from("../dir/payload.bin")
    );

    let source_mtime = fs::metadata(&source_file).unwrap().mtime();
    let copied_mtime = fs::metadata(&copied).unwrap().mtime();
    assert_eq!(copied_mtime, source_mtime);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn recursive_move_keeps_pending_state_until_large_children_finish() {
    let root = unique_temp_dir("fro-main-app-recursive-move");
    let source_root = root.join("src");
    let target_root = root.join("dst");
    fs::create_dir_all(source_root.join("000/000")).unwrap();
    fs::create_dir_all(source_root.join("000/001")).unwrap();

    let large_a = vec![0x41; (17 << 20) + 123];
    let large_b = vec![0x42; (18 << 20) + 321];
    fs::write(source_root.join("000/000/a.bin"), &large_a).unwrap();
    fs::write(source_root.join("000/001/b.bin"), &large_b).unwrap();

    let config = config::load_config(None);
    let target_str = target_root.to_string_lossy();
    let params_page_cache = config.get_params_for_path("copy", false, target_str.as_ref());
    let params_direct = config.get_params_for_path("copy", true, target_str.as_ref());
    let params_copy_range = config.get_copy_range_params_for_path(target_str.as_ref());
    let ctx = RecursiveCopyContext {
        config,
        source_root: source_root.clone(),
        target_root: target_root.clone(),
        optimizer_params: [
            params_page_cache.num_threads,
            params_page_cache.block_size,
            params_page_cache.qd as u64,
            params_direct.num_threads,
            params_direct.block_size,
            params_direct.qd as u64,
            params_copy_range.num_threads,
            params_copy_range.block_size,
            params_copy_range.qd as u64,
        ],
        requested_strategy: CopyStrategy::Auto,
        rewrite_mode: CopyRewriteMode::Auto,
        io_mode_read: IOMode::Auto,
        io_mode_write: IOMode::Auto,
        keep_target_size: false,
        use_lock: true,
        relative_copy_method: RelativeCopyMethod::CopyFileRange,
        verbose: false,
        cp_compat: false,
        cp_no_clobber: false,
        preserve_timestamps: false,
    };

    let moved = move_dir::run_recursive_move(ctx, false).unwrap();
    assert_eq!(moved, (large_a.len() + large_b.len()) as u64);
    assert!(!source_root.exists());
    assert_eq!(
        fs::read(target_root.join("000/000/a.bin")).unwrap(),
        large_a
    );
    assert_eq!(
        fs::read(target_root.join("000/001/b.bin")).unwrap(),
        large_b
    );

    let _ = fs::remove_dir_all(root);
}
