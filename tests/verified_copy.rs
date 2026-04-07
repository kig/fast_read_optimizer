use fro::block_hash::{default_hash_base, verify_file_with_replicas, BlockHashAlgorithm};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::symlink;
use std::os::unix::io::AsRawFd;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

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

fn set_file_mtime(path: &std::path::Path, secs: i64, nsecs: i64) {
    let times = [
        libc::timespec {
            tv_sec: secs,
            tv_nsec: nsecs,
        },
        libc::timespec {
            tv_sec: secs,
            tv_nsec: nsecs,
        },
    ];
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(
        rc,
        0,
        "utimensat failed: {}",
        std::io::Error::last_os_error()
    );
}

fn sidecar_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    path.with_extension(format!("bin.fro-hash.{}.json", suffix))
}

fn lock_exclusive(path: &std::path::Path) -> File {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(rc, 0, "failed to lock {}", path.display());
    file
}

#[test]
fn verified_copy_api_is_ephemeral_by_default() {
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
    assert!(!report.hashes_persisted);
    assert_eq!(fs::read(&target).unwrap(), bytes);

    for suffix in ["0", "1", "2"] {
        assert!(!sidecar_path(&source, suffix).exists());
        assert!(!sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn verified_copy_api_can_persist_hashes_when_requested() {
    let tmp = unique_temp_dir("fro-verified-copy-api-hash");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 777))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let target_hash_base = default_hash_base(target.to_str().unwrap());
    let report = fro::copy_file_verified_with_options(
        &source,
        &target,
        fro::IOMode::PageCache,
        fro::IOMode::PageCache,
        BlockHashAlgorithm::Sha256,
        false,
        Some(&target_hash_base),
    )
    .unwrap();

    assert!(report.hashes_persisted);
    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&source, suffix).exists());
        assert!(sidecar_path(&target, suffix).exists());
    }

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
fn copy_verify_cli_reports_success_to_stderr_and_leaves_no_sidecars() {
    let tmp = unique_temp_dir("fro-copy-verify-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 513))
        .map(|i| ((i * 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
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
    assert!(!String::from_utf8_lossy(&out.stderr).contains("error"));
    assert!(String::from_utf8_lossy(&out.stderr).contains("copy verify: success"));
    assert_eq!(fs::read(&target).unwrap(), bytes);
    for suffix in ["0", "1", "2"] {
        assert!(!sidecar_path(&source, suffix).exists());
        assert!(!sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn copy_verify_hash_cli_writes_sidecars() {
    let tmp = unique_temp_dir("fro-copy-verify-hash-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 313))
        .map(|i| ((i * 31) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
        "--hash",
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
    assert!(String::from_utf8_lossy(&out.stderr).contains("sidecars_written=true"));
    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&source, suffix).exists());
        assert!(sidecar_path(&target, suffix).exists());
    }
}

#[test]
fn verified_copy_replaces_existing_target_without_leaking_temp_files() {
    let tmp = unique_temp_dir("fro-verified-copy-atomic-swap");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 211))
        .map(|i| ((i * 43) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();
    fs::write(&target, b"stale-target").unwrap();

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
    assert_eq!(fs::read(&target).unwrap(), bytes);
    let leftovers = fs::read_dir(&tmp)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".fro-verified-copy-tmp-"))
        .collect::<Vec<_>>();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
}

#[test]
fn copy_verify_diff_cli_reports_success_to_stderr() {
    let tmp = unique_temp_dir("fro-copy-verify-diff-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 919))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--verify-diff",
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
    assert!(String::from_utf8_lossy(&out.stderr).contains("copy verify-diff: success"));
}

#[test]
fn copy_verify_cli_rejects_optimizer_iterations() {
    let tmp = unique_temp_dir("fro-copy-verify-cli-reject");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"hello verified copy").unwrap();

    let out = run_fro(&[
        "copy",
        "--verify",
        "-n",
        "2",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("copy verification modes require -n 1"));
}

#[test]
fn copy_file_range_cli_copies_file() {
    let tmp = unique_temp_dir("fro-copy-file-range-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 777))
        .map(|i| ((i * 13) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--copy-file-range",
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
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[test]
fn copy_verify_cli_supports_copy_file_range() {
    let tmp = unique_temp_dir("fro-copy-file-range-verify");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 321))
        .map(|i| ((i * 7) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--copy-file-range",
        "--verify",
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
    assert!(String::from_utf8_lossy(&out.stderr).contains("copy verify: success"));
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[test]
fn copy_file_range_cli_rejects_direct_modes() {
    let tmp = unique_temp_dir("fro-copy-file-range-direct-reject");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"copy-file-range").unwrap();

    let out = run_fro(&[
        "copy",
        "--copy-file-range",
        "--direct",
        "-n",
        "1",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stdout)
            .contains("copy --copy-file-range and --copy-file-range-single do not support direct read/write modes")
    );
}

#[test]
fn copy_file_range_single_cli_copies_file() {
    let tmp = unique_temp_dir("fro-copy-file-range-single-cli");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 129))
        .map(|i| ((i * 5) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--copy-file-range-single",
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
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[test]
fn copy_recursive_cli_copies_tree_contents_and_symlinks() {
    let tmp = unique_temp_dir("fro-copy-recursive-cli");
    let source_root = tmp.join("src-tree");
    let nested = source_root.join("nested/deeper");
    let fro_dest_parent = tmp.join("fro-dest");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&fro_dest_parent).unwrap();

    fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
    fs::write(
        nested.join("large.bin"),
        (0..(2 * 1024 * 1024 + 333))
            .map(|i| ((i * 13) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    symlink("../small.txt", source_root.join("nested/link-small")).unwrap();

    let out = run_fro(&[
        "copy",
        "--recursive",
        "--no-direct",
        "-n",
        "1",
        source_root.to_str().unwrap(),
        fro_dest_parent.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let copied_root = fro_dest_parent.join("src-tree");
    assert_eq!(
        fs::read(copied_root.join("small.txt")).unwrap(),
        b"alpha\nbeta\n"
    );
    assert_eq!(
        fs::read(copied_root.join("nested/deeper/large.bin")).unwrap(),
        fs::read(source_root.join("nested/deeper/large.bin")).unwrap()
    );
    assert_eq!(
        fs::read_link(copied_root.join("nested/link-small")).unwrap(),
        std::path::PathBuf::from("../small.txt")
    );
}

#[test]
fn copy_recursive_cli_rejects_target_inside_source() {
    let tmp = unique_temp_dir("fro-copy-recursive-inside-source");
    let source_root = tmp.join("src-tree");
    let nested = source_root.join("subdir");
    fs::create_dir_all(&nested).unwrap();
    fs::write(source_root.join("small.txt"), b"alpha\n").unwrap();
    let target = source_root.join("nested-copy");

    let out = run_fro(&[
        "copy",
        "--recursive",
        "--no-direct",
        "-n",
        "1",
        source_root.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("refusing to copy directory"));
}

#[test]
fn copy_recursive_cli_preserves_timestamps_with_cp_preserve() {
    use std::os::unix::fs::MetadataExt;

    let tmp = unique_temp_dir("fro-copy-recursive-preserve-times");
    let source_root = tmp.join("src-tree");
    let nested = source_root.join("nested/deeper");
    let fro_dest_parent = tmp.join("fro-dest");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&fro_dest_parent).unwrap();

    let file = source_root.join("small.txt");
    let nested_dir = source_root.join("nested");
    let link = nested_dir.join("link-small");
    fs::write(&file, b"alpha\nbeta\n").unwrap();
    fs::write(nested.join("large.bin"), b"payload").unwrap();
    symlink("../small.txt", &link).unwrap();

    set_file_mtime(&file, 1_700_123_456, 123_456_789);
    set_file_mtime(&nested.join("large.bin"), 1_700_123_460, 987_654_321);
    set_file_mtime(&nested_dir, 1_700_123_470, 222_333_444);
    set_file_mtime(&source_root, 1_700_123_480, 555_666_777);
    let link_times = [1_700_123_490_i64, 111_222_333_i64];
    {
        let c_path = std::ffi::CString::new(link.as_os_str().as_encoded_bytes()).unwrap();
        let times = [
            libc::timespec {
                tv_sec: link_times[0],
                tv_nsec: link_times[1],
            },
            libc::timespec {
                tv_sec: link_times[0],
                tv_nsec: link_times[1],
            },
        ];
        let rc = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                c_path.as_ptr(),
                times.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        assert_eq!(
            rc,
            0,
            "symlink utimensat failed: {}",
            std::io::Error::last_os_error()
        );
    }

    let out = run_fro(&[
        "copy",
        "--recursive",
        "--no-direct",
        "-n",
        "1",
        "--cp-preserve",
        source_root.to_str().unwrap(),
        fro_dest_parent.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let copied_root = fro_dest_parent.join("src-tree");
    let copied_file_meta = fs::metadata(copied_root.join("small.txt")).unwrap();
    let source_file_meta = fs::metadata(&file).unwrap();
    assert_eq!(copied_file_meta.mtime(), source_file_meta.mtime());
    assert_eq!(copied_file_meta.mtime_nsec(), source_file_meta.mtime_nsec());

    let copied_nested_meta = fs::metadata(copied_root.join("nested")).unwrap();
    let source_nested_meta = fs::metadata(&nested_dir).unwrap();
    assert_eq!(copied_nested_meta.mtime(), source_nested_meta.mtime());
    assert_eq!(
        copied_nested_meta.mtime_nsec(),
        source_nested_meta.mtime_nsec()
    );

    let copied_root_meta = fs::metadata(&copied_root).unwrap();
    let source_root_meta = fs::metadata(&source_root).unwrap();
    assert_eq!(copied_root_meta.mtime(), source_root_meta.mtime());
    assert_eq!(copied_root_meta.mtime_nsec(), source_root_meta.mtime_nsec());

    let copied_link_meta = fs::symlink_metadata(copied_root.join("nested/link-small")).unwrap();
    let source_link_meta = fs::symlink_metadata(&link).unwrap();
    assert_eq!(copied_link_meta.mtime(), source_link_meta.mtime());
    assert_eq!(copied_link_meta.mtime_nsec(), source_link_meta.mtime_nsec());
}

#[test]
fn reflink_cli_rejects_direct_modes() {
    let tmp = unique_temp_dir("fro-reflink-direct-reject");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"reflink").unwrap();

    let out = run_fro(&[
        "copy",
        "--reflink",
        "--direct",
        "-n",
        "1",
        source.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout)
        .contains("copy --reflink does not support direct read/write modes"));
}

#[test]
fn copy_cli_keep_target_size_preserves_existing_suffix() {
    let tmp = unique_temp_dir("fro-copy-keep-target-size");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    fs::write(&source, b"abcdef").unwrap();
    fs::write(&target, b"0123456789").unwrap();

    let out = run_fro(&[
        "copy",
        "--keep-target-size",
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
    assert_eq!(fs::read(&target).unwrap(), b"abcdef6789");
}

#[test]
fn copy_diff_cli_creates_missing_target_even_without_locks() {
    let tmp = unique_temp_dir("fro-copy-diff-missing-target");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = (0..(1024 * 1024 + 17))
        .map(|i| ((i * 11) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &bytes).unwrap();

    let out = run_fro(&[
        "copy",
        "--diff",
        "--no-lock",
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
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[test]
fn copy_cli_blocks_on_existing_source_lock_by_default() {
    let tmp = unique_temp_dir("fro-copy-lock-default");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = vec![7u8; 1024 * 1024];
    fs::write(&source, &bytes).unwrap();

    let locked = lock_exclusive(&source);
    let (tx, rx) = mpsc::channel();
    let source_owned = source.clone();
    let target_owned = target.clone();
    thread::spawn(move || {
        let out = run_fro(&[
            "copy",
            "--no-direct",
            "-n",
            "1",
            source_owned.to_str().unwrap(),
            target_owned.to_str().unwrap(),
        ]);
        tx.send(out.status.success()).unwrap();
    });

    thread::sleep(Duration::from_millis(150));
    assert!(
        rx.try_recv().is_err(),
        "copy should still be waiting on the source lock"
    );

    drop(locked);
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), true);
    assert_eq!(fs::read(&target).unwrap(), bytes);
}

#[test]
fn copy_cli_no_lock_skips_advisory_source_locking() {
    let tmp = unique_temp_dir("fro-copy-no-lock");
    let source = tmp.join("source.bin");
    let target = tmp.join("target.bin");
    let bytes = vec![9u8; 1024 * 1024];
    fs::write(&source, &bytes).unwrap();

    let _locked = lock_exclusive(&source);
    let (tx, rx) = mpsc::channel();
    let source_owned = source.clone();
    let target_owned = target.clone();
    thread::spawn(move || {
        let out = run_fro(&[
            "copy",
            "--no-lock",
            "--no-direct",
            "-n",
            "1",
            source_owned.to_str().unwrap(),
            target_owned.to_str().unwrap(),
        ]);
        tx.send(out).unwrap();
    });

    let out = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(fs::read(&target).unwrap(), bytes);
}
