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
    assert!(stdout.contains("byte"));
    assert!(stdout.contains("numeric"));
    assert!(stdout.contains("--reverse"));
    assert!(stdout.contains("--unique"));
    assert!(stdout.contains("--numeric-sort"));
    assert!(stdout.contains("--output=FILE"));
    assert!(stdout.contains("Unsupported GNU sort features"));
    assert!(stdout.contains("month/version/human"));
}

#[test]
fn sort_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"beta\nalpha\n\nzeta\n").unwrap();
    fs::write(&b, b"alpha\ngamma").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec![],
            vec!["-r"],
            vec!["-u"],
            vec!["-ru"],
            vec!["--reverse", "--unique"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(sort_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = sort_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("sort", &fro_args),
                    run_system_sort(&sys_args),
                    &format!("sort {:?}", fro_args),
                );
            }
        }

        for sort_flags in [vec![], vec!["-r"], vec!["-u"], vec!["-ru"]] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            let mut sys_stdin_args = sort_flags.clone();
            sys_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin("sort", &fro_stdin_args, b"bbb\na\nab\n\na\nbbb"),
                run_system_sort_with_stdin(&sys_stdin_args, b"bbb\na\nab\n\na\nbbb"),
                &format!("sort stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_numeric_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-numeric");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"10\n2\n-3\n.5\n02\nx\n").unwrap();
    fs::write(&b, b"1.0\n1\n1.00\n+2\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-n"],
            vec!["--numeric-sort"],
            vec!["-nr"],
            vec!["-nu"],
            vec!["--numeric-sort", "--reverse", "--unique"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(sort_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("sort", &fro_args),
                    run_system_sort(
                        &sort_flags
                            .iter()
                            .copied()
                            .chain(files.iter().copied())
                            .collect::<Vec<_>>(),
                    ),
                    &format!("sort numeric {:?}", fro_args),
                );
            }
        }

        for sort_flags in [
            vec!["-n"],
            vec!["-nr"],
            vec!["-nu"],
            vec!["--numeric-sort", "--reverse", "--unique"],
        ] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin(
                    "sort",
                    &fro_stdin_args,
                    b"1.0\n1\n1.00\n10\n2\n-3\n.5\n+2\nx\n",
                ),
                run_system_sort_with_stdin(
                    sort_flags
                        .iter()
                        .copied()
                        .chain(["-"])
                        .collect::<Vec<_>>()
                        .as_slice(),
                    b"1.0\n1\n1.00\n10\n2\n-3\n.5\n+2\nx\n",
                ),
                &format!("sort numeric stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_numeric_output_file_matches_system_and_suppresses_stdout() {
    let tmp = unique_temp_dir("fro-coreutils-sort-numeric-output");
    let input = tmp.join("input.txt");
    fs::write(&input, b"1.0\n1\n1.00\n10\n2\n-3\n.5\n+2\nx\n").unwrap();

    let fro_output = tmp.join("fro-output.txt");
    let sys_output = tmp.join("sys-output.txt");
    let fro = run_fro(
        "sort",
        &[
            "--numeric-sort",
            "--unique",
            "-o",
            fro_output.to_str().unwrap(),
            input.to_str().unwrap(),
        ],
    );
    let system = run_system_sort(&[
        "--numeric-sort",
        "--unique",
        "-o",
        sys_output.to_str().unwrap(),
        input.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert!(system.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );
}

#[test]
fn sort_rejects_unsupported_flags_with_help_hint() {
    let output = run_fro("sort", &["-M"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported option '-M'"));
    assert!(stderr.contains("Try 'sort --help' for more information."));
}

#[test]
fn sort_output_file_matches_system_and_suppresses_stdout() {
    let tmp = unique_temp_dir("fro-coreutils-sort-output");
    let input = tmp.join("input.txt");
    fs::write(&input, b"beta\nalpha\nbeta\n").unwrap();

    for io_flags in io_flag_sets() {
        for form in ["split", "long", "attached"] {
            let fro_output = tmp.join(format!("fro-output-{form}.txt"));
            let sys_output = tmp.join(format!("sys-output-{form}.txt"));
            let mut fro_args = io_flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect::<Vec<_>>();
            fro_args.push("-u".to_string());
            match form {
                "split" => {
                    fro_args.push("-o".to_string());
                    fro_args.push(fro_output.to_string_lossy().into_owned());
                }
                "long" => {
                    fro_args.push(format!("--output={}", fro_output.display()));
                }
                "attached" => {
                    fro_args.push(format!("-o{}", fro_output.display()));
                }
                _ => unreachable!(),
            }
            fro_args.push(input.to_string_lossy().into_owned());
            let fro_args_refs = fro_args.iter().map(String::as_str).collect::<Vec<_>>();

            let fro = run_fro("sort", &fro_args_refs);
            let system = match form {
                "split" => run_system_sort(&[
                    "-u",
                    "-o",
                    sys_output.to_str().unwrap(),
                    input.to_str().unwrap(),
                ]),
                "long" => {
                    let output_flag = format!("--output={}", sys_output.display());
                    run_system_sort(&["-u", output_flag.as_str(), input.to_str().unwrap()])
                }
                "attached" => {
                    let output_flag = format!("-o{}", sys_output.display());
                    run_system_sort(&["-u", output_flag.as_str(), input.to_str().unwrap()])
                }
                _ => unreachable!(),
            };

            assert_eq!(fro.status.code(), system.status.code());
            assert!(fro.stdout.is_empty(), "fro unexpectedly wrote to stdout");
            assert!(
                system.stdout.is_empty(),
                "system sort unexpectedly wrote to stdout"
            );
            assert_eq!(fro.stderr, system.stderr);
            assert_eq!(
                fs::read(&fro_output).unwrap(),
                fs::read(&sys_output).unwrap()
            );
            let _ = fs::remove_file(&fro_output);
            let _ = fs::remove_file(&sys_output);
        }
    }
}

#[test]
fn sort_output_file_supports_in_place_rewrite_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-output-in-place");

    let fro_in_place = tmp.join("fro-in-place.txt");
    let sys_in_place = tmp.join("sys-in-place.txt");
    fs::write(&fro_in_place, b"bbb\na\nbbb\n").unwrap();
    fs::write(&sys_in_place, b"bbb\na\nbbb\n").unwrap();
    let fro = run_fro(
        "sort",
        &[
            "-u",
            "-o",
            fro_in_place.to_str().unwrap(),
            fro_in_place.to_str().unwrap(),
        ],
    );
    let system = run_system_sort(&[
        "-u",
        "-o",
        sys_in_place.to_str().unwrap(),
        sys_in_place.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_in_place).unwrap(),
        fs::read(&sys_in_place).unwrap()
    );

    let fro_stdin = tmp.join("fro-stdin.txt");
    let sys_stdin = tmp.join("sys-stdin.txt");
    let fro = run_fro_with_stdin(
        "sort",
        &["-r", "-o", fro_stdin.to_str().unwrap(), "-"],
        b"bbb\na\nab\n",
    );
    let system = run_system_sort_with_stdin(
        &["-r", "-o", sys_stdin.to_str().unwrap(), "-"],
        b"bbb\na\nab\n",
    );
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(fs::read(&fro_stdin).unwrap(), fs::read(&sys_stdin).unwrap());
}
