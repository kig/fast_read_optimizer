use super::*;

fn run_system_sort(args: &[&str]) -> Output {
    Command::new("sort")
        .env("LC_ALL", "C")
        .args(args)
        .output()
        .expect("failed to run system sort")
}

fn run_fro_env(command: &str, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
    cmd.arg(command).args(args);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.output().expect("failed to run fro coreutils command")
}

fn run_fro_capture_env(command: &str, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
    cmd.arg(command)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.output().expect("failed to run fro coreutils command")
}

fn run_fro_with_stdin_env(
    command: &str,
    args: &[&str],
    stdin_bytes: &[u8],
    envs: &[(&str, &str)],
) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fro"));
    cmd.arg(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let mut child = cmd.spawn().expect("failed to spawn fro coreutils command");
    let mut stdin = child.stdin.take().expect("missing child stdin");
    let input = stdin_bytes.to_vec();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&input).or_else(|err| match err.kind() {
            std::io::ErrorKind::BrokenPipe => Ok(()),
            _ => Err(err),
        })
    });
    let output = child
        .wait_with_output()
        .expect("failed to read child output");
    writer
        .join()
        .expect("stdin writer thread panicked")
        .expect("failed to write child stdin");
    output
}

fn run_system_sort_with_stdin(args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new("sort")
        .env("LC_ALL", "C")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn system sort");
    child
        .stdin
        .take()
        .expect("missing system sort stdin")
        .write_all(stdin_bytes)
        .expect("failed to write system sort stdin");
    child
        .wait_with_output()
        .expect("failed to collect system sort output")
}

#[path = "sort/advanced.rs"]
mod advanced;
#[path = "sort/backends.rs"]
mod backends;
#[path = "sort/basic.rs"]
mod basic;
#[path = "sort/modes.rs"]
mod modes;
#[path = "sort/spill_output.rs"]
mod spill_output;
