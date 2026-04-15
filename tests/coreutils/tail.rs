use super::*;
use std::fs;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum TailProgram {
    Fro,
    System,
}

fn spawn_tail(program: TailProgram, args: &[String]) -> Child {
    let mut command = match program {
        TailProgram::Fro => {
            let mut command = Command::new(env!("CARGO_BIN_EXE_fro"));
            command.arg("tail");
            command
        }
        TailProgram::System => Command::new("tail"),
    };
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn tail follow process")
}

fn terminate_tail(child: Child) -> Output {
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    child
        .wait_with_output()
        .expect("failed to collect tail follow output")
}

fn wait_for_exit(mut child: Child, timeout: Duration) -> Output {
    let start = Instant::now();
    loop {
        if child.try_wait().expect("failed to poll child").is_some() {
            return child
                .wait_with_output()
                .expect("failed to collect completed child output");
        }
        if start.elapsed() >= timeout {
            panic!("tail child did not exit within {:?}", timeout);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn run_follow_case(program: TailProgram, args: &[String], drive: impl FnOnce()) -> Output {
    let child = spawn_tail(program, args);
    thread::sleep(Duration::from_millis(250));
    drive();
    thread::sleep(Duration::from_millis(450));
    terminate_tail(child)
}

#[test]
fn tail_help_mentions_long_counts_help_and_version() {
    let output = run_fro("tail", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("+N start offsets"));
    assert!(stdout.contains("--lines"));
    assert!(stdout.contains("--lines="));
    assert!(stdout.contains("--bytes"));
    assert!(stdout.contains("--bytes="));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("--quiet"));
    assert!(stdout.contains("--silent"));
    assert!(stdout.contains("--verbose"));
    assert!(stdout.contains("-f"));
    assert!(stdout.contains("--follow"));
    assert!(stdout.contains("--follow=name"));
    assert!(stdout.contains("-F"));
    assert!(stdout.contains("--retry"));
    assert!(stdout.contains("-s"));
    assert!(stdout.contains("--sleep-interval"));
    assert!(stdout.contains("--pid"));
    assert!(stdout.contains("--max-unchanged-stats"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro tail version string and exits."));
}

#[test]
fn tail_version_prints_version_string() {
    let output = run_fro("tail", &["--version"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tail (fro coreutils)"));
}

#[test]
fn tail_long_count_forms_match_system_for_file_and_stdin() {
    let tmp = unique_temp_dir("fro-tail-long-counts");
    let file = tmp.join("input.txt");
    let input = b"alpha\nbeta\ngamma\ndelta\n";
    std::fs::write(&file, input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["--lines", "2", path],
        vec!["--lines=+2", path],
        vec!["--bytes", "7", path],
        vec!["--bytes=+4", path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail long-count file {:?}", args),
        );
    }

    for args in [
        vec!["--lines", "2"],
        vec!["--lines=+2"],
        vec!["--bytes", "7"],
        vec!["--bytes=+4"],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, input),
            run_system_with_stdin("tail", &args, input),
            &format!("tail long-count stdin {:?}", args),
        );
    }
}

#[test]
fn tail_long_header_forms_match_system_for_multi_input_modes() {
    let tmp = unique_temp_dir("fro-tail-long-headers");
    let first = tmp.join("first.txt");
    let second = tmp.join("second.txt");
    std::fs::write(&first, b"first-a\nfirst-b\n").unwrap();
    std::fs::write(&second, b"second-a\nsecond-b\n").unwrap();
    let first_path = first.to_str().unwrap();
    let second_path = second.to_str().unwrap();

    for args in [
        vec!["--quiet", first_path, second_path],
        vec!["--silent", first_path, second_path],
        vec!["--verbose", first_path, second_path],
        vec!["--verbose", "--quiet", first_path, second_path],
        vec!["--quiet", "--verbose", first_path, second_path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail long headers file {:?}", args),
        );
    }

    let stdin = b"stdin-a\nstdin-b\n";
    for args in [
        vec!["--quiet", "-", second_path],
        vec!["--silent", "-", second_path],
        vec!["--verbose", "-", second_path],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, stdin),
            run_system_with_stdin("tail", &args, stdin),
            &format!("tail long headers stdin {:?}", args),
        );
    }
}

#[test]
fn tail_byte_mode_matches_system_for_small_stdin_suffix() {
    let input = (0..(512 * 1024))
        .map(|idx| b'a' + (idx % 23) as u8)
        .collect::<Vec<_>>();

    for args in [
        vec!["-c", "64"],
        vec!["-c", "4096"],
        vec!["-c", "65536"],
        vec!["--no-direct", "-c", "64"],
        vec!["--no-direct", "-c", "4096"],
        vec!["--no-direct", "-c", "65536"],
    ] {
        let count = *args.last().unwrap();
        assert_same_result(
            run_fro_with_stdin("tail", &args, &input),
            run_system_with_stdin("tail", &["-c", count], &input),
            &format!("tail stdin bytes {:?}", args),
        );
    }
}

#[test]
fn tail_byte_mode_matches_system_for_large_stdin_suffix() {
    let input = (0..(3 * 1024 * 1024 + 131_072))
        .map(|idx| (idx % 251) as u8)
        .collect::<Vec<_>>();

    for args in [vec!["-c", "1572865"], vec!["--no-direct", "-c", "1572865"]] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, &input),
            run_system_with_stdin("tail", &["-c", "1572865"], &input),
            &format!("tail stdin large bytes {:?}", args),
        );
    }
}

#[test]
fn tail_zero_terminated_matches_system_for_file_and_stdin() {
    let tmp = unique_temp_dir("fro-tail-zero-terminated");
    let file = tmp.join("records.bin");
    let input = b"alpha\0beta\0gamma\0delta";
    std::fs::write(&file, input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["-z", "-n", "2", path],
        vec!["--zero-terminated", "-n", "+2", path],
        vec!["-z", "-n", "1", path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail zero-terminated file {:?}", args),
        );
    }

    for args in [
        vec!["-z", "-n", "2"],
        vec!["--zero-terminated", "-n", "+2"],
        vec!["-z", "-n", "1", "-"],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, input),
            run_system_with_stdin("tail", &args, input),
            &format!("tail zero-terminated stdin {:?}", args),
        );
    }
}

#[test]
fn tail_follow_descriptor_matches_system_for_appends() {
    let tmp = unique_temp_dir("fro-tail-follow-descriptor");
    let file = tmp.join("follow.log");
    fs::write(&file, b"seed\n").unwrap();
    let path = file.to_str().unwrap();
    let args = vec![
        "-n".to_string(),
        "0".to_string(),
        "--follow=descriptor".to_string(),
        "-s".to_string(),
        "0.1".to_string(),
        path.to_string(),
    ];

    let fro = run_follow_case(TailProgram::Fro, &args, || {
        let mut handle = fs::OpenOptions::new().append(true).open(&file).unwrap();
        use std::io::Write;
        handle.write_all(b"first\n").unwrap();
    });
    fs::write(&file, b"seed\n").unwrap();
    let system = run_follow_case(TailProgram::System, &args, || {
        let mut handle = fs::OpenOptions::new().append(true).open(&file).unwrap();
        use std::io::Write;
        handle.write_all(b"first\n").unwrap();
    });

    assert_same_result(fro, system, "tail follow descriptor append");
}

#[test]
fn tail_follow_name_retry_matches_system_when_file_appears() {
    let tmp = unique_temp_dir("fro-tail-follow-name-retry");
    let file = tmp.join("appears.log");
    let path = file.to_str().unwrap();
    let args = vec![
        "-n".to_string(),
        "0".to_string(),
        "-F".to_string(),
        "-s".to_string(),
        "0.1".to_string(),
        path.to_string(),
    ];

    let fro = run_follow_case(TailProgram::Fro, &args, || {
        fs::write(&file, b"hello\n").unwrap();
    });
    let _ = fs::remove_file(&file);
    let system = run_follow_case(TailProgram::System, &args, || {
        fs::write(&file, b"hello\n").unwrap();
    });

    assert_same_result(fro, system, "tail follow name retry create");
}

#[test]
fn tail_follow_name_rotation_matches_system() {
    let tmp = unique_temp_dir("fro-tail-follow-name-rotate");
    let file = tmp.join("rotate.log");
    let rotated = tmp.join("rotate.log.1");
    fs::write(&file, b"old\n").unwrap();
    let path = file.to_str().unwrap();
    let args = vec![
        "-n".to_string(),
        "0".to_string(),
        "--follow=name".to_string(),
        "--max-unchanged-stats=1".to_string(),
        "-s".to_string(),
        "0.1".to_string(),
        path.to_string(),
    ];

    let rotate = || {
        if rotated.exists() {
            fs::remove_file(&rotated).unwrap();
        }
        fs::rename(&file, &rotated).unwrap();
        thread::sleep(Duration::from_millis(220));
        fs::write(&file, b"new\n").unwrap();
    };

    let fro = run_follow_case(TailProgram::Fro, &args, rotate);
    if file.exists() {
        fs::remove_file(&file).unwrap();
    }
    if rotated.exists() {
        fs::remove_file(&rotated).unwrap();
    }
    fs::write(&file, b"old\n").unwrap();
    let system = run_follow_case(TailProgram::System, &args, || {
        fs::rename(&file, &rotated).unwrap();
        thread::sleep(Duration::from_millis(220));
        fs::write(&file, b"new\n").unwrap();
    });

    assert_same_result(fro, system, "tail follow name rotation");
}

#[test]
fn tail_follow_pid_matches_system_for_natural_exit() {
    let tmp = unique_temp_dir("fro-tail-follow-pid");
    let file = tmp.join("pid.log");
    fs::write(&file, b"seed\n").unwrap();
    let path = file.to_str().unwrap();

    let run_with = |program: TailProgram| {
        let pid_target = Command::new("python")
            .args(["-c", "import time; time.sleep(0.35)"])
            .spawn()
            .expect("failed to spawn pid target");
        let args = vec![
            "-n".to_string(),
            "0".to_string(),
            "-f".to_string(),
            "--pid".to_string(),
            pid_target.id().to_string(),
            "-s".to_string(),
            "0.1".to_string(),
            path.to_string(),
        ];
        let child = spawn_tail(program, &args);
        thread::sleep(Duration::from_millis(150));
        fs::write(&file, b"seed\npid-line\n").unwrap();
        let _ = Command::new("python").args(["-c", "pass"]).status();
        let _ = pid_target
            .wait_with_output()
            .expect("failed to reap pid target");
        wait_for_exit(child, Duration::from_secs(3))
    };

    let fro = run_with(TailProgram::Fro);
    fs::write(&file, b"seed\n").unwrap();
    let system = run_with(TailProgram::System);

    assert_same_result(fro, system, "tail follow pid exit");
}
