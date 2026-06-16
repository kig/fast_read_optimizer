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

fn run_fro(command: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .output()
        .expect("failed to run fro command")
}

fn assert_gbps_report(stderr: &[u8], command: &str) {
    let text = String::from_utf8_lossy(stderr);
    assert!(
        text.contains(command) && text.contains(" bytes in ") && text.contains(" GB/s"),
        "missing throughput report for {command}: {text}"
    );
}

#[test]
fn cat_report_gbps_keeps_stdout_and_writes_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-cat-report-gbps");
    let path = tmp.join("input.txt");
    fs::write(&path, b"alpha\nbeta\ngamma\n").unwrap();

    let plain = run_fro("cat", &[path.to_str().unwrap()]);
    let reported = run_fro("cat", &["--report-gbps", path.to_str().unwrap()]);

    assert!(plain.status.success());
    assert!(reported.status.success());
    assert_eq!(reported.stdout, plain.stdout);
    assert_gbps_report(&reported.stderr, "cat");
}

#[test]
fn fgrep_report_gbps_keeps_stdout_and_writes_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-report-gbps");
    let path = tmp.join("input.txt");
    fs::write(&path, b"alpha\nbeta\nalpha beta\n").unwrap();

    let plain = run_fro("fgrep", &["alpha", path.to_str().unwrap()]);
    let reported = run_fro("fgrep", &["--report-gbps", "alpha", path.to_str().unwrap()]);

    assert!(plain.status.success());
    assert!(reported.status.success());
    assert_eq!(reported.stdout, plain.stdout);
    assert_gbps_report(&reported.stderr, "fgrep");
}

#[test]
fn wc_report_gbps_keeps_stdout_and_writes_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-wc-report-gbps");
    let path = tmp.join("input.txt");
    fs::write(&path, b"one\ntwo\nthree\n").unwrap();

    let plain = run_fro("wc", &["-l", path.to_str().unwrap()]);
    let reported = run_fro("wc", &["-l", "--report-gbps", path.to_str().unwrap()]);

    assert!(plain.status.success());
    assert!(reported.status.success());
    assert_eq!(reported.stdout, plain.stdout);
    assert_gbps_report(&reported.stderr, "wc");
}

#[test]
fn hash_tools_report_gbps_keep_stdout_and_write_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-hash-report-gbps");
    let path = tmp.join("input.bin");
    fs::write(
        &path,
        (0..8192)
            .map(|i| ((i * 17 + 3) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for command in ["cksum", "sha256sum"] {
        let plain = run_fro(command, &[path.to_str().unwrap()]);
        let reported = run_fro(command, &["--report-gbps", path.to_str().unwrap()]);

        assert!(plain.status.success(), "{command} plain failed");
        assert!(reported.status.success(), "{command} reported failed");
        assert_eq!(reported.stdout, plain.stdout, "{command} stdout changed");
        assert_gbps_report(&reported.stderr, command);
    }
}

#[test]
fn base64_report_gbps_keeps_stdout_and_writes_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-base64-report-gbps");
    let path = tmp.join("input.bin");
    fs::write(
        &path,
        (0..4096)
            .map(|i| ((i * 19 + 5) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let plain = run_fro("base64", &[path.to_str().unwrap()]);
    let reported = run_fro("base64", &["--report-gbps", path.to_str().unwrap()]);

    assert!(plain.status.success());
    assert!(reported.status.success());
    assert_eq!(reported.stdout, plain.stdout);
    assert_gbps_report(&reported.stderr, "base64");
}

#[test]
fn cmp_report_gbps_keeps_success_output_and_writes_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-report-gbps");
    let left = tmp.join("left.bin");
    let right = tmp.join("right.bin");
    let bytes = (0..8192)
        .map(|i| ((i * 23 + 11) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&left, &bytes).unwrap();
    fs::write(&right, &bytes).unwrap();

    let plain = run_fro("cmp", &[left.to_str().unwrap(), right.to_str().unwrap()]);
    let reported = run_fro(
        "cmp",
        &[
            "--report-gbps",
            left.to_str().unwrap(),
            right.to_str().unwrap(),
        ],
    );

    assert!(plain.status.success());
    assert!(reported.status.success());
    assert_eq!(reported.stdout, plain.stdout);
    assert_gbps_report(&reported.stderr, "cmp");
}

#[test]
fn head_and_tail_report_gbps_keep_stdout_and_write_stderr() {
    let tmp = unique_temp_dir("fro-coreutils-head-tail-report-gbps");
    let path = tmp.join("input.bin");
    fs::write(
        &path,
        (0..16384)
            .map(|i| ((i * 29 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (command, args) in [
        ("head", vec!["-c", "1MiB", path.to_str().unwrap()]),
        ("tail", vec!["-c", "1MiB", path.to_str().unwrap()]),
    ] {
        let plain = run_fro(command, &args);
        let mut reported_args = vec!["--report-gbps"];
        reported_args.extend(args.iter().copied());
        let reported = run_fro(command, &reported_args);

        assert!(plain.status.success(), "{command} plain failed");
        assert!(reported.status.success(), "{command} reported failed");
        assert_eq!(reported.stdout, plain.stdout, "{command} stdout changed");
        assert_gbps_report(&reported.stderr, command);
    }
}
