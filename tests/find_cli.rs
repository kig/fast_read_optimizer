#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn run_fro(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro find")
}

fn run_system(args: &[&str]) -> Output {
    Command::new("find")
        .args(args)
        .output()
        .expect("failed to run system find")
}

fn make_fifo(path: &std::path::Path) {
    let fifo = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let rc = unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
}

fn assert_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn sorted_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines = String::from_utf8_lossy(bytes)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

#[test]
fn find_matches_system_for_nested_tree_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-tree");
    let root = tmp.join("root");
    let nested = root.join("a").join("b");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(root.join("a").join("mid.txt"), b"mid").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_matches_system_for_multiple_roots_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-multi");
    let dir = tmp.join("dir");
    let nested = dir.join("nested");
    let standalone = tmp.join("standalone.txt");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("child.txt"), b"child").unwrap();
    fs::write(&standalone, b"standalone").unwrap();

    let fro = assert_success(run_fro(&[
        "find",
        dir.to_str().unwrap(),
        standalone.to_str().unwrap(),
    ]));
    let system = assert_success(run_system(&[
        dir.to_str().unwrap(),
        standalone.to_str().unwrap(),
    ]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_matches_system_for_wide_tree_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-wide");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    for i in 0..24 {
        let branch = root.join(format!("branch-{i}"));
        let nested = branch.join("nested").join("leaf");
        fs::create_dir_all(&nested).unwrap();
        fs::write(branch.join("root.txt"), format!("root-{i}\n")).unwrap();
        fs::write(nested.join("leaf.txt"), format!("leaf-{i}\n")).unwrap();
    }

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_warns_and_continues_on_permission_denied_directory() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }

    let tmp = unique_temp_dir("fro-find-perms");
    let root = tmp.join("root");
    let blocked = root.join("blocked");
    fs::create_dir_all(&blocked).unwrap();
    fs::write(root.join("visible.txt"), b"visible").unwrap();
    fs::write(blocked.join("hidden.txt"), b"hidden").unwrap();

    let mut perms = fs::metadata(&blocked).unwrap().permissions();
    perms.set_mode(0);
    fs::set_permissions(&blocked, perms).unwrap();

    let fro = run_fro(&["find", root.to_str().unwrap()]);
    let system = run_system(&[root.to_str().unwrap()]);

    let mut restore = fs::metadata(&blocked).unwrap().permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&blocked, restore).unwrap();

    assert_eq!(fro.status.code(), system.status.code());
    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert!(String::from_utf8_lossy(&fro.stderr).contains("Permission denied"));
    assert!(String::from_utf8_lossy(&system.stderr).contains("Permission denied"));
}

#[test]
fn find_matches_system_for_symlinks_broken_symlinks_and_fifos() {
    let tmp = unique_temp_dir("fro-find-special");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let regular = root.join("regular.txt");
    let symlink_path = root.join("regular-link");
    let broken_symlink = root.join("broken-link");
    let fifo = root.join("events.fifo");
    fs::write(&regular, b"regular").unwrap();
    std::os::unix::fs::symlink(&regular, &symlink_path).unwrap();
    std::os::unix::fs::symlink(root.join("missing-target"), &broken_symlink).unwrap();
    make_fifo(&fifo);
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}
