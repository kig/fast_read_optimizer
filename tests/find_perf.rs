#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn run_fro_du(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro du")
}

fn run_system_du(args: &[&str]) -> Output {
    Command::new("du")
        .args(args)
        .output()
        .expect("failed to run system du")
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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

fn build_synthetic_dirwalk_tree(root: &Path) -> usize {
    let mut paths = 1usize;
    for branch in 0..128 {
        let branch_dir = root.join(format!("branch-{branch:03}"));
        fs::create_dir_all(&branch_dir).unwrap();
        paths += 1;
        for child in 0..32 {
            let leaf = branch_dir.join(format!("child-{child:03}")).join("leaf");
            fs::create_dir_all(&leaf).unwrap();
            fs::write(leaf.parent().unwrap().join("node.txt"), b"node").unwrap();
            fs::write(leaf.join("leaf.txt"), b"leaf").unwrap();
            paths += 4;
        }
    }
    paths
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

#[test]
#[ignore = "intended for local synthetic dirwalk perf validation"]
fn find_and_du_report_timings_on_synthetic_tree() {
    let tmp = unique_temp_dir("fro-dirwalk-perf");
    let root = tmp.join("tree");
    let path_count = build_synthetic_dirwalk_tree(&root);

    let fro_find = assert_success(run_fro_find(&["find", root.to_str().unwrap()]));
    let system_find = assert_success(run_system_find(&[root.to_str().unwrap()]));
    assert_same_sorted_lines(&fro_find.stdout, &system_find.stdout);

    let fro_du = assert_success(run_fro_du(&["du", root.to_str().unwrap()]));
    let system_du = assert_success(run_system_du(&[root.to_str().unwrap()]));
    assert_same_sorted_lines(&fro_du.stdout, &system_du.stdout);

    let fro_find_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
                cmd.args(["find", root.to_str().unwrap()]);
                cmd
            })
        })
        .collect::<Vec<_>>();
    let system_find_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new("find");
                cmd.arg(root.to_str().unwrap());
                cmd
            })
        })
        .collect::<Vec<_>>();
    let fro_du_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
                cmd.args(["du", root.to_str().unwrap()]);
                cmd
            })
        })
        .collect::<Vec<_>>();
    let system_du_times = (0..5)
        .map(|_| {
            time_command({
                let mut cmd = Command::new("du");
                cmd.arg(root.to_str().unwrap());
                cmd
            })
        })
        .collect::<Vec<_>>();

    eprintln!("synthetic dirwalk tree paths: {path_count}");
    eprintln!("fro find timings (s): {fro_find_times:?}");
    eprintln!("sys find timings (s): {system_find_times:?}");
    eprintln!("fro du timings (s): {fro_du_times:?}");
    eprintln!("sys du timings (s): {system_du_times:?}");
}
