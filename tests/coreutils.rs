#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
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
        .expect("failed to run fro subcommand")
}

fn run_system(program: &str, args: &[&str]) -> Output {
    Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"))
}

fn run_fro_with_stdin(command: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro command");
    child
        .stdin
        .take()
        .expect("missing fro stdin")
        .write_all(stdin_bytes)
        .expect("failed to write fro stdin");
    child
        .wait_with_output()
        .expect("failed to collect fro output")
}

fn run_system_with_stdin(program: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn {program}: {err}"));
    child
        .stdin
        .take()
        .expect("missing system stdin")
        .write_all(stdin_bytes)
        .expect("failed to write system stdin");
    child
        .wait_with_output()
        .unwrap_or_else(|err| panic!("failed to collect {program} output: {err}"))
}

fn assert_same_result(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
    assert_eq!(
        fro.stdout, system.stdout,
        "{label}: stdout mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
    assert_eq!(
        fro.stderr, system.stderr,
        "{label}: stderr mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
}

fn io_flag_sets() -> Vec<Vec<&'static str>> {
    vec![vec![], vec!["--no-direct"], vec!["--direct"]]
}

#[path = "coreutils/cat.rs"]
mod cat;
