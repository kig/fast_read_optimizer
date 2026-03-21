#![cfg(unix)]

use std::fs;
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
