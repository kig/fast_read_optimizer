#![cfg(unix)]

use memchr::memchr_iter;
use std::cell::RefCell;
use std::fs;
use std::io::{self, BufReader, Cursor, Write};
use std::process::{Command, Stdio};

use fro::IOMode;

thread_local! {
    static CAPTURED_STDOUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static MOCK_STDIN: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Debug)]
enum StreamInput {
    File(String),
    Stdin,
}

fn parse_stream_inputs(files: Vec<String>) -> Vec<StreamInput> {
    if files.is_empty() {
        return vec![StreamInput::Stdin];
    }
    files
        .into_iter()
        .map(|file| {
            if file == "-" {
                StreamInput::Stdin
            } else {
                StreamInput::File(file)
            }
        })
        .collect()
}

struct CaptureWriter {
    bytes: Vec<u8>,
}

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CaptureWriter {
    fn into_inner(self) -> io::Result<()> {
        CAPTURED_STDOUT.with(|captured| {
            *captured.borrow_mut() = self.bytes;
        });
        Ok(())
    }
}

fn stdout_buf_writer() -> io::Result<CaptureWriter> {
    CAPTURED_STDOUT.with(|captured| captured.borrow_mut().clear());
    Ok(CaptureWriter { bytes: Vec::new() })
}

fn is_regular_input_path(path: &str) -> io::Result<bool> {
    Ok(fs::metadata(path)?.file_type().is_file())
}

fn fd_is_regular(_fd: libc::c_int) -> io::Result<bool> {
    Ok(false)
}

fn copy_fd_to_fd_splice_limited_counted<F>(
    _src_fd: libc::c_int,
    _dst_fd: libc::c_int,
    _limit: u64,
    _progress: &mut F,
) -> io::Result<Option<u64>>
where
    F: FnMut(u64) -> io::Result<()>,
{
    Ok(None)
}

fn loaded_or_stream_bytes(input: &StreamInput, _io_mode: IOMode) -> io::Result<Vec<u8>> {
    match input {
        StreamInput::File(path) => fs::read(path),
        StreamInput::Stdin => Ok(MOCK_STDIN.with(|stdin| stdin.borrow().clone())),
    }
}

fn visit_ordered_input<F>(input: &StreamInput, io_mode: IOMode, mut on_block: F) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let bytes = loaded_or_stream_bytes(input, io_mode)?;
    on_block(&bytes)
}

fn stdin_buf_reader() -> io::Result<BufReader<Cursor<Vec<u8>>>> {
    let bytes = MOCK_STDIN.with(|stdin| stdin.borrow().clone());
    Ok(BufReader::new(Cursor::new(bytes)))
}

#[path = "../src/coreutils/head.rs"]
mod head;

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[derive(Debug)]
struct CapturedRun {
    ok: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_head_impl(args: &[&str], stdin_bytes: &[u8]) -> CapturedRun {
    MOCK_STDIN.with(|stdin| {
        *stdin.borrow_mut() = stdin_bytes.to_vec();
    });
    let argv = std::iter::once("head".to_string())
        .chain(args.iter().map(|arg| (*arg).to_string()))
        .collect::<Vec<_>>();
    let status = head::run_head(&argv);
    let stdout = CAPTURED_STDOUT.with(|captured| captured.borrow().clone());
    CapturedRun {
        ok: status.is_ok(),
        stdout,
        stderr: Vec::new(),
    }
}

fn run_system(program: &str, args: &[&str]) -> std::process::Output {
    Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"))
}

fn run_system_with_stdin(program: &str, args: &[&str], stdin_bytes: &[u8]) -> std::process::Output {
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

fn assert_same_result(actual: CapturedRun, expected: std::process::Output, label: &str) {
    assert_eq!(
        actual.ok,
        expected.status.success(),
        "{label}: status mismatch\nactual stdout:\n{}\nexpected stdout:\n{}\nactual stderr:\n{}\nexpected stderr:\n{}",
        String::from_utf8_lossy(&actual.stdout),
        String::from_utf8_lossy(&expected.stdout),
        String::from_utf8_lossy(&actual.stderr),
        String::from_utf8_lossy(&expected.stderr),
    );
    assert_eq!(
        actual.stdout, expected.stdout,
        "{label}: stdout mismatch\nactual stdout:\n{}\nexpected stdout:\n{}\nactual stderr:\n{}\nexpected stderr:\n{}",
        String::from_utf8_lossy(&actual.stdout),
        String::from_utf8_lossy(&expected.stdout),
        String::from_utf8_lossy(&actual.stderr),
        String::from_utf8_lossy(&expected.stderr),
    );
    assert_eq!(
        actual.stderr, expected.stderr,
        "{label}: stderr mismatch\nactual stdout:\n{}\nexpected stdout:\n{}\nactual stderr:\n{}\nexpected stderr:\n{}",
        String::from_utf8_lossy(&actual.stdout),
        String::from_utf8_lossy(&expected.stdout),
        String::from_utf8_lossy(&actual.stderr),
        String::from_utf8_lossy(&expected.stderr),
    );
}

#[test]
fn head_impl_matches_system_for_negative_counts_and_headers() {
    let tmp = unique_temp_dir("fro-head-impl");
    let file_a = tmp.join("a.txt");
    let file_b = tmp.join("b.txt");
    fs::write(&file_a, b"zero\none\ntwo\nthree\n").unwrap();
    fs::write(&file_b, b"apple\nbanana\ncarrot\n").unwrap();

    let a = file_a.to_str().unwrap();
    let b = file_b.to_str().unwrap();

    for args in [
        vec!["-n", "-1", a],
        vec!["-n", "-2", a],
        vec!["--lines=-2", a],
        vec!["--lines", "2", a],
        vec!["-n-2", a],
        vec!["-2", a],
        vec!["-12", a],
        vec!["-c", "-1", a],
        vec!["-c", "-4", a],
        vec!["--bytes=-4", a],
        vec!["--bytes", "4", a],
        vec!["-c-2", a],
        vec!["-2c", a],
        vec!["-2b", a],
        vec!["-2k", a],
        vec!["-v", a],
        vec!["-q", a],
        vec!["--verbose", a, b],
        vec!["--quiet", a, b],
        vec!["--silent", a, b],
        vec!["-q", a, b],
        vec!["-v", a, b],
        vec!["-qv", a, b],
        vec!["-vq", a, b],
        vec!["-q", "-v", a],
        vec!["-v", "-q", a],
        vec!["-2q", a, b],
        vec!["-2v", a, b],
        vec!["-2qv", a],
        vec!["-2vq", a],
    ] {
        assert_same_result(
            run_head_impl(&args, b""),
            run_system("head", &args),
            &format!("head impl file parity {:?}", args),
        );
    }

    let stdin_text = b"alpha\nbeta\ngamma\ndelta\n";
    for args in [
        vec!["-n", "-1"],
        vec!["-n", "-2"],
        vec!["--lines=-2"],
        vec!["--lines", "2"],
        vec!["-n-2"],
        vec!["-2"],
        vec!["-c", "-1"],
        vec!["-c", "-5"],
        vec!["--bytes=-5"],
        vec!["--bytes", "5"],
        vec!["-c-2"],
        vec!["-2c"],
        vec!["-v"],
        vec!["-q"],
        vec!["--verbose"],
        vec!["--quiet"],
        vec!["-v", "-"],
        vec!["-q", "-"],
        vec!["-2q"],
        vec!["-2v"],
    ] {
        assert_same_result(
            run_head_impl(&args, stdin_text),
            run_system_with_stdin("head", &args, stdin_text),
            &format!("head impl stdin parity {:?}", args),
        );
    }
}
