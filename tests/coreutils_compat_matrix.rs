#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::thread;

static FIFO_TEST_LOCK: Mutex<()> = Mutex::new(());

#[path = "helpers/coreutils_parity.rs"]
mod helpers;
use helpers::{stream_surfaces, unique_temp_dir, CoreutilsParityFixture};

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

fn make_fifo(path: &Path) {
    let fifo = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let rc = unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
}

fn with_fifo_input<F, T>(path: &Path, data: &[u8], run: F) -> T
where
    F: FnOnce(&str) -> T,
{
    make_fifo(path);
    let fifo_path = path.to_string_lossy().into_owned();
    let data = data.to_vec();
    let writer = thread::spawn({
        let fifo_path = fifo_path.clone();
        move || {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&fifo_path)
                .expect("failed to open fifo for writing");
            file.write_all(&data).expect("failed to write fifo data");
        }
    });
    let output = run(&fifo_path);
    writer.join().expect("fifo writer panicked");
    fs::remove_file(path).unwrap();
    output
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

fn assert_same_wc(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: wc status mismatch"
    );
    let fro_tokens = String::from_utf8_lossy(&fro.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let sys_tokens = String::from_utf8_lossy(&system.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(fro_tokens, sys_tokens, "{label}: wc token mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: wc stderr mismatch");
}

fn assert_same_sorted_lines(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    let mut fro_lines = String::from_utf8_lossy(&fro.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut sys_lines = String::from_utf8_lossy(&system.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    fro_lines.sort();
    sys_lines.sort();
    assert_eq!(fro_lines, sys_lines, "{label}: sorted stdout mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
}

fn snapshot_tree(root: &Path) -> Vec<(String, u8, Vec<u8>)> {
    fn walk(root: &Path, rel: &Path, out: &mut Vec<(String, u8, Vec<u8>)>) {
        let dir = if rel.as_os_str().is_empty() {
            root.to_path_buf()
        } else {
            root.join(rel)
        };
        let mut entries = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name();
            let rel_path = if rel.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                rel.join(&name)
            };
            let metadata = fs::symlink_metadata(entry.path()).unwrap();
            let rel_string = rel_path.to_string_lossy().into_owned();
            if metadata.file_type().is_dir() {
                out.push((rel_string.clone(), 0, Vec::new()));
                walk(root, &rel_path, out);
            } else if metadata.file_type().is_symlink() {
                out.push((
                    rel_string,
                    2,
                    fs::read_link(entry.path())
                        .unwrap()
                        .as_os_str()
                        .as_bytes()
                        .to_vec(),
                ));
            } else {
                out.push((rel_string, 1, fs::read(entry.path()).unwrap()));
            }
        }
    }

    let mut out = Vec::new();
    walk(root, Path::new(""), &mut out);
    out
}

fn io_flag_sets() -> Vec<Vec<&'static str>> {
    vec![vec![], vec!["--no-direct"], vec!["--direct"]]
}

fn set_file_mtime(path: &Path, secs: i64) {
    let times = [
        libc::timespec {
            tv_sec: secs,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: secs,
            tv_nsec: 0,
        },
    ];
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(
        rc,
        0,
        "utimensat failed: {}",
        std::io::Error::last_os_error()
    );
}

fn set_symlink_mtime(path: &Path, secs: i64) {
    let times = [
        libc::timespec {
            tv_sec: secs,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: secs,
            tv_nsec: 0,
        },
    ];
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
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
        "utimensat symlink failed: {}",
        std::io::Error::last_os_error()
    );
}

fn wc_flag_sets() -> Vec<Vec<&'static str>> {
    vec![
        vec![],
        vec!["-l"],
        vec!["-w"],
        vec!["-m"],
        vec!["-c"],
        vec!["-L"],
        vec!["-m", "-c"],
        vec!["-m", "-L"],
        vec!["-l", "-w", "-c"],
        vec!["-l", "-m"],
        vec!["-l", "-L"],
        vec!["-l", "-m", "-c"],
        vec!["-l", "-m", "-L"],
    ]
}

fn assert_same_wc_exact(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: wc status mismatch"
    );
    let fro_tokens = String::from_utf8_lossy(&fro.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let sys_tokens = String::from_utf8_lossy(&system.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(fro_tokens, sys_tokens, "{label}: wc stdout token mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: wc stderr mismatch");
}

#[path = "coreutils_compat_matrix/base64.rs"]
mod base64;
#[path = "coreutils_compat_matrix/cat_flags.rs"]
mod cat_flags;
#[path = "coreutils_compat_matrix/cat_tac.rs"]
mod cat_tac;
#[path = "coreutils_compat_matrix/cmp_fgrep.rs"]
mod cmp_fgrep;
#[path = "coreutils_compat_matrix/cp_shred.rs"]
mod cp_shred;
#[path = "coreutils_compat_matrix/du.rs"]
mod du;
#[path = "coreutils_compat_matrix/fifo.rs"]
mod fifo;
#[path = "coreutils_compat_matrix/hash.rs"]
mod hash;
#[path = "coreutils_compat_matrix/rm_mv.rs"]
mod rm_mv;
#[path = "coreutils_compat_matrix/stream_coreutils.rs"]
mod stream_coreutils;
#[path = "coreutils_compat_matrix/tar.rs"]
mod tar;
#[path = "coreutils_compat_matrix/wc.rs"]
mod wc;
