use super::*;

fn run_system_sort(args: &[&str]) -> Output {
    Command::new("sort")
        .env("LC_ALL", "C")
        .args(args)
        .output()
        .expect("failed to run system sort")
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

#[test]
fn sort_help_mentions_bounded_bytewise_slice() {
    let output = run_fro("sort", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("newline-delimited"));
    assert!(stdout.contains("bytewise"));
    assert!(stdout.contains("Unsupported GNU sort features"));
}

#[test]
fn sort_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"beta\nalpha\n\nzeta\n").unwrap();
    fs::write(&b, b"alpha\ngamma").unwrap();

    for io_flags in io_flag_sets() {
        for files in [
            vec![a.to_str().unwrap()],
            vec![a.to_str().unwrap(), b.to_str().unwrap()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(files.iter().copied());
            assert_same_result(
                run_fro("sort", &fro_args),
                run_system_sort(&files),
                &format!("sort {:?}", fro_args),
            );
        }

        let mut fro_stdin_args = io_flags.clone();
        fro_stdin_args.push("-");
        assert_same_result(
            run_fro_with_stdin("sort", &fro_stdin_args, b"bbb\na\nab\n\na"),
            run_system_sort_with_stdin(&["-"], b"bbb\na\nab\n\na"),
            &format!("sort stdin {:?}", io_flags),
        );
    }
}

#[test]
fn sort_rejects_unsupported_flags_with_help_hint() {
    let output = run_fro("sort", &["-r"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported option '-r'"));
    assert!(stderr.contains("Try 'sort --help' for more information."));
}
