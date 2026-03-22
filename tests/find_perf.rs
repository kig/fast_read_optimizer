#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Instant;

const FORMALANSWER_ROOT: &str = "/data/repos/formalanswer";

fn run_fro_find(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro find")
}

fn run_system_find(args: &[&str]) -> Output {
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

fn sorted_lines(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = bytes
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.to_vec())
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn time_command(mut command: Command) -> f64 {
    let start = Instant::now();
    let status = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to run timed command");
    assert!(status.success(), "timed command failed with {status}");
    start.elapsed().as_secs_f64()
}

fn assert_same_sorted_lines(fro_output: &[u8], system_output: &[u8]) {
    let fro_lines = sorted_lines(fro_output);
    let sys_lines = sorted_lines(system_output);
    if fro_lines != sys_lines {
        eprintln!("fro exe: {}", env!("CARGO_BIN_EXE_fro"));
        eprintln!("fro line count: {}", fro_lines.len());
        eprintln!("sys line count: {}", sys_lines.len());
        for (idx, pair) in fro_lines.iter().zip(sys_lines.iter()).enumerate() {
            if pair.0 != pair.1 {
                eprintln!("first mismatch at index {idx}");
                eprintln!("fro: {:?}", String::from_utf8_lossy(pair.0));
                eprintln!("sys: {:?}", String::from_utf8_lossy(pair.1));
                panic!("formalanswer find output mismatch");
            }
        }
        panic!("formalanswer find output length mismatch");
    }
}

#[test]
#[ignore = "requires /data/repos/formalanswer and is intended for local perf validation"]
fn find_matches_system_and_reports_timings_on_formalanswer() {
    if !Path::new(FORMALANSWER_ROOT).is_dir() {
        eprintln!("skipping: {FORMALANSWER_ROOT} is not available");
        return;
    }

    let fro = assert_success(run_fro_find(&["find", FORMALANSWER_ROOT]));
    let system = assert_success(run_system_find(&[FORMALANSWER_ROOT]));
    assert_same_sorted_lines(&fro.stdout, &system.stdout);

    let fro_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
                cmd.args(["find", FORMALANSWER_ROOT]);
                cmd
            })
        })
        .collect::<Vec<_>>();
    let sys_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new("find");
                cmd.arg(FORMALANSWER_ROOT);
                cmd
            })
        })
        .collect::<Vec<_>>();

    eprintln!("fro find timings (s): {fro_times:?}");
    eprintln!("sys find timings (s): {sys_times:?}");
}
