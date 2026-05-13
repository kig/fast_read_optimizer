// ─── -z/--null-data: NUL-delimited records ───────────────────────────────────

use super::*;
use std::io::Write;
use std::process::{Command, Stdio};

fn spawn_fro_fgrep(args: &[&str]) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg("fgrep")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro fgrep")
}

fn spawn_system_grep(args: &[&str]) -> std::process::Child {
    system_command("grep")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn system grep")
}

fn run_fro_fgrep_null_data(args: &[&str], stdin_bytes: &[u8]) -> Vec<u8> {
    let mut child = spawn_fro_fgrep(args);
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin_bytes)
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "fro fgrep -z failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn run_sys_grep_null_data(args: &[&str], stdin_bytes: &[u8]) -> Vec<u8> {
    let mut child = spawn_system_grep(args);
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin_bytes)
        .unwrap();
    let out = child.wait_with_output().unwrap();
    // exit 1 = no match, exit 0 = match, both are fine
    assert!(
        out.status.code().map_or(false, |c| c <= 1),
        "grep -z failed unexpectedly: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

// Helper: byte stream of NUL-delimited records
fn nul_records(records: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::new();
    for r in records {
        v.extend_from_slice(r);
        v.push(b'\0');
    }
    v
}

#[test]
fn fgrep_null_data_basic_match() {
    let input = nul_records(&[b"alpha", b"needle", b"beta"]);
    let fro = run_fro_fgrep_null_data(&["-z", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "needle", "-"], &input);
    assert_eq!(fro, sys, "basic -z match output mismatch");
}

#[test]
fn fgrep_null_data_no_match() {
    let input = nul_records(&[b"alpha", b"beta", b"gamma"]);
    let fro_out = {
        let mut child = spawn_fro_fgrep(&["-z", "needle", "-"]);
        child.stdin.take().unwrap().write_all(&input).unwrap();
        child.wait_with_output().unwrap()
    };
    let sys_out = {
        let mut child = spawn_system_grep(&["-F", "-z", "needle", "-"]);
        child.stdin.take().unwrap().write_all(&input).unwrap();
        child.wait_with_output().unwrap()
    };
    assert_eq!(fro_out.status.code(), sys_out.status.code(), "-z no-match exit code");
    assert_eq!(fro_out.stdout, sys_out.stdout, "-z no-match stdout");
}

#[test]
fn fgrep_null_data_embedded_newlines_in_record() {
    // A record that contains newlines — with -z newlines are NOT separators.
    let input = nul_records(&[b"foo\nbar", b"needle\nxyz", b"baz"]);
    let fro = run_fro_fgrep_null_data(&["-z", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "needle", "-"], &input);
    assert_eq!(fro, sys, "-z embedded-newline record mismatch");
}

#[test]
fn fgrep_null_data_count() {
    let input = nul_records(&[b"alpha", b"needle", b"alpha", b"needle"]);
    let fro = run_fro_fgrep_null_data(&["-z", "-c", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "-c", "needle", "-"], &input);
    assert_eq!(fro, sys, "-z -c count mismatch");
}

#[test]
fn fgrep_null_data_invert_match() {
    let input = nul_records(&[b"alpha", b"needle", b"beta"]);
    let fro = run_fro_fgrep_null_data(&["-z", "-v", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "-v", "needle", "-"], &input);
    assert_eq!(fro, sys, "-z -v invert mismatch");
}

#[test]
fn fgrep_null_data_ignore_case() {
    let input = nul_records(&[b"Alpha", b"NEEDLE", b"beta"]);
    let fro = run_fro_fgrep_null_data(&["-z", "-i", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "-i", "needle", "-"], &input);
    assert_eq!(fro, sys, "-z -i case-insensitive mismatch");
}

#[test]
fn fgrep_null_data_line_regexp() {
    let input = nul_records(&[b"alpha", b"alpha beta", b"needle", b"needleX"]);
    let fro = run_fro_fgrep_null_data(&["-z", "-x", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "-z", "-x", "needle", "-"], &input);
    assert_eq!(fro, sys, "-z -x line-regexp mismatch");
}

#[test]
fn fgrep_null_data_long_flag_alias() {
    let input = nul_records(&[b"alpha", b"needle", b"beta"]);
    let fro = run_fro_fgrep_null_data(&["--null-data", "needle", "-"], &input);
    let sys = run_sys_grep_null_data(&["-F", "--null-data", "needle", "-"], &input);
    assert_eq!(fro, sys, "--null-data alias mismatch");
}

#[test]
fn fgrep_null_data_nul_is_not_binary() {
    // With -z, NUL bytes are record separators, not a binary indicator.
    // fro should not suppress output or bail out with binary detection.
    let input = nul_records(&[b"alpha", b"needle", b"beta"]);
    let fro = run_fro_fgrep_null_data(&["-z", "needle", "-"], &input);
    // Output must contain the matching record terminated by NUL.
    assert_eq!(fro, b"needle\0", "-z should not treat NUL as binary signal");
}
