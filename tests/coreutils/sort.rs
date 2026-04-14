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

#[test]
fn sort_help_mentions_bounded_bytewise_slice() {
    let output = run_fro("sort", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("newline-delimited"));
    assert!(stdout.contains("byte"));
    assert!(stdout.contains("general-numeric"));
    assert!(stdout.contains("human-numeric"));
    assert!(stdout.contains("numeric"));
    assert!(stdout.contains("--check"));
    assert!(stdout.contains("--check=diagnose-first"));
    assert!(stdout.contains("--check=quiet"));
    assert!(stdout.contains("--check=silent"));
    assert!(stdout.contains("-C"));
    assert!(stdout.contains("--general-numeric-sort"));
    assert!(stdout.contains("--human-numeric-sort"));
    assert!(stdout.contains("--merge"));
    assert!(stdout.contains("--month-sort"));
    assert!(stdout.contains("--reverse"));
    assert!(stdout.contains("--unique"));
    assert!(stdout.contains("--version-sort"));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("--numeric-sort"));
    assert!(stdout.contains("--key=KEY"));
    assert!(stdout.contains("--output=FILE"));
    assert!(stdout.contains("--temporary-directory"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro sort version string and exits."));
    assert!(stdout.contains("Unsupported GNU sort features"));
    assert!(stdout.contains("locale collation"));
    assert!(stdout.contains("per-key modifiers"));
    assert!(!stdout.contains("--field-separator"));
    assert!(!stdout.contains("--ignore-leading-blanks"));
    assert!(!stdout.contains("--ignore-case"));
    assert!(!stdout.contains("--ignore-nonprinting"));
    assert!(!stdout.contains("--parallel"));
    assert!(!stdout.contains("--files0-from"));
    assert!(!stdout.contains("zero-terminated records and locale collation"));
    assert!(!stdout.contains("temp-file controls"));
    assert!(!stdout.contains("merge/check modes"));
}

#[test]
fn sort_version_prints_version_string() {
    let output = run_fro("sort", &["--version"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout,
        format!("sort (fro coreutils) {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
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
fn sort_general_numeric_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-general");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"x\nNaN\n-inf\n-3\n.5\n02\n0x10\n1e2\ninf\n").unwrap();
    fs::write(&b, b"1E+02\n+inf\n-nan\n0\n-0\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-g"],
            vec!["--general-numeric-sort"],
            vec!["-gr"],
            vec!["-gu"],
            vec!["--general-numeric-sort", "--reverse", "--unique"],
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
                    &format!("sort general numeric {:?}", fro_args),
                );
            }
        }

        for sort_flags in [
            vec!["-g"],
            vec!["-gr"],
            vec!["-gu"],
            vec!["--general-numeric-sort", "--reverse", "--unique"],
        ] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin(
                    "sort",
                    &fro_stdin_args,
                    b"x\nNaN\n-inf\n-3\n.5\n02\n0x10\n1e2\n1E+02\n+inf\ninf\n",
                ),
                run_system_sort_with_stdin(
                    sort_flags
                        .iter()
                        .copied()
                        .chain(["-"])
                        .collect::<Vec<_>>()
                        .as_slice(),
                    b"x\nNaN\n-inf\n-3\n.5\n02\n0x10\n1e2\n1E+02\n+inf\ninf\n",
                ),
                &format!("sort general numeric stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_human_numeric_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-human");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"2K\n1G\n512M\n100\n1.5K\n-2K\n1KiB\n0\nfoo\n").unwrap();
    fs::write(&b, b"1000\n1024\n1K\n1.0K\n1024K\n1M\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-h"],
            vec!["--human-numeric-sort"],
            vec!["-hr"],
            vec!["-hu"],
            vec!["--human-numeric-sort", "--reverse", "--unique"],
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
                    &format!("sort human numeric {:?}", fro_args),
                );
            }
        }

        for sort_flags in [
            vec!["-h"],
            vec!["-hr"],
            vec!["-hu"],
            vec!["--human-numeric-sort", "--reverse", "--unique"],
        ] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin(
                    "sort",
                    &fro_stdin_args,
                    b"2K\n1G\n512M\n100\n1.5K\n-2K\n1KiB\n1000\n1024\n1K\n1.0K\n1024K\n1M\nfoo\n",
                ),
                run_system_sort_with_stdin(
                    sort_flags
                        .iter()
                        .copied()
                        .chain(["-"])
                        .collect::<Vec<_>>()
                        .as_slice(),
                    b"2K\n1G\n512M\n100\n1.5K\n-2K\n1KiB\n1000\n1024\n1K\n1.0K\n1024K\n1M\nfoo\n",
                ),
                &format!("sort human numeric stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_month_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-month");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"jan\nFeb\nmar\nfoo\nDec\nAug 1\nsept\nSep\n").unwrap();
    fs::write(&b, b"JAN\njan\nJaN\n  feb\nMarx\nMarch\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-M"],
            vec!["--month-sort"],
            vec!["-Mr"],
            vec!["-Mu"],
            vec!["--month-sort", "--reverse", "--unique"],
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
                    &format!("sort month {:?}", fro_args),
                );
            }
        }

        for sort_flags in [
            vec!["-M"],
            vec!["-Mr"],
            vec!["-Mu"],
            vec!["--month-sort", "--reverse", "--unique"],
        ] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin(
                    "sort",
                    &fro_stdin_args,
                    b"jan\nFeb\nmar\nfoo\nDec\nAug 1\nsept\nSep\nJAN\njan\nJaN\n  feb\n",
                ),
                run_system_sort_with_stdin(
                    sort_flags
                        .iter()
                        .copied()
                        .chain(["-"])
                        .collect::<Vec<_>>()
                        .as_slice(),
                    b"jan\nFeb\nmar\nfoo\nDec\nAug 1\nsept\nSep\nJAN\njan\nJaN\n  feb\n",
                ),
                &format!("sort month stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_version_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-version");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(
        &a,
        b"v1\nv01\nv1.0\nv1.0.2\nv1.0.10\nv1.0.02\nv1a\nv1~\nv1-1\n",
    )
    .unwrap();
    fs::write(&b, b"Foo\nbar\nbar002\nbar02\nbar2\nbar2a\nbar10\nfoo\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-V"],
            vec!["--version-sort"],
            vec!["-Vr"],
            vec!["-Vu"],
            vec!["--version-sort", "--reverse", "--unique"],
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
                    &format!("sort version {:?}", fro_args),
                );
            }
        }

        for sort_flags in [
            vec!["-V"],
            vec!["-Vr"],
            vec!["-Vu"],
            vec!["--version-sort", "--reverse", "--unique"],
        ] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin(
                    "sort",
                    &fro_stdin_args,
                    b"v1\nv01\nv1.0\nv1.0.2\nv1.0.10\nv1.0.02\nv1a\nv1~\nv1-1\nbar\nbar002\nbar02\nbar2\nbar2a\nbar10\n",
                ),
                run_system_sort_with_stdin(
                    sort_flags
                        .iter()
                        .copied()
                        .chain(["-"])
                        .collect::<Vec<_>>()
                        .as_slice(),
                    b"v1\nv01\nv1.0\nv1.0.2\nv1.0.10\nv1.0.02\nv1a\nv1~\nv1-1\nbar\nbar002\nbar02\nbar2\nbar2a\nbar10\n",
                ),
                &format!("sort version stdin {:?}", fro_stdin_args),
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
fn sort_tiny_page_cache_fast_path_skips_config_creation() {
    let tmp = unique_temp_dir("fro-coreutils-sort-tiny-fast-path");
    let input = tmp.join("tiny.txt");
    let config = tmp.join("would-be-created.json");
    fs::write(&input, b"beta\nalpha\n").unwrap();

    let output = run_fro_capture_env(
        "sort",
        &["--no-direct", input.to_str().unwrap()],
        &[("FRO_CONFIG", config.to_str().unwrap())],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"alpha\nbeta\n");
    assert!(
        !config.exists(),
        "tiny sort fast path should not create a config file"
    );
}

#[test]
fn sort_zero_terminated_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-zero");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    fs::write(&a, b"beta\0alpha\0\0zeta\0").unwrap();
    fs::write(&b, b"alpha\0gamma").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-z"],
            vec!["-zr"],
            vec!["-zu"],
            vec!["--zero-terminated", "--reverse", "--unique"],
            vec!["-zn"],
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
                    &format!("sort zero {:?}", fro_args),
                );
            }
        }

        for sort_flags in [vec!["-z"], vec!["-zr"], vec!["-zu"], vec!["-zn"]] {
            let mut fro_stdin_args = io_flags.clone();
            fro_stdin_args.extend(sort_flags.iter().copied());
            fro_stdin_args.push("-");
            let mut sys_stdin_args = sort_flags.clone();
            sys_stdin_args.push("-");
            assert_same_result(
                run_fro_with_stdin("sort", &fro_stdin_args, b"bbb\0a\0ab\0\0a\0bbb"),
                run_system_sort_with_stdin(&sys_stdin_args, b"bbb\0a\0ab\0\0a\0bbb"),
                &format!("sort zero stdin {:?}", fro_stdin_args),
            );
        }
    }
}

#[test]
fn sort_zero_terminated_output_merge_and_check_match_system() {
    let tmp = unique_temp_dir("fro-coreutils-sort-zero-io");
    let left = tmp.join("left.bin");
    let right = tmp.join("right.bin");
    let input = tmp.join("input.bin");
    let unsorted = tmp.join("unsorted.bin");
    fs::write(&left, b"alpha\0charlie\0").unwrap();
    fs::write(&right, b"beta\0delta").unwrap();
    fs::write(&input, b"beta\0alpha\0beta").unwrap();
    fs::write(&unsorted, b"beta\0alpha").unwrap();

    let fro_output = tmp.join("fro-output.bin");
    let sys_output = tmp.join("sys-output.bin");
    let fro = run_fro(
        "sort",
        &[
            "-z",
            "-u",
            "-o",
            fro_output.to_str().unwrap(),
            input.to_str().unwrap(),
        ],
    );
    let system = run_system_sort(&[
        "-z",
        "-u",
        "-o",
        sys_output.to_str().unwrap(),
        input.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );

    assert_same_result(
        run_fro(
            "sort",
            &["-z", "-m", left.to_str().unwrap(), right.to_str().unwrap()],
        ),
        run_system_sort(&["-z", "-m", left.to_str().unwrap(), right.to_str().unwrap()]),
        "sort zero merge",
    );

    assert_same_result(
        run_fro("sort", &["-z", "-c", unsorted.to_str().unwrap()]),
        run_system_sort(&["-z", "-c", unsorted.to_str().unwrap()]),
        "sort zero check",
    );
}

#[test]
fn sort_key_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-key");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"beta 2 b\nalpha 10 z\nalpha 2 a\nbeta 10 y\n").unwrap();
    fs::write(&b, b"alpha 2 z\nalpha 02 y\nbeta 2 a\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-k", "1,1"],
            vec!["-k2,2"],
            vec!["--key=2.2,2.2"],
            vec!["-k", "2,2", "-k", "3,3"],
            vec!["-n", "-k", "2,2"],
            vec!["-nu", "-k2,2"],
            vec!["-r", "--key=2,2", "--key=3,3"],
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
                    &format!("sort key {:?}", fro_args),
                );
            }
        }
    }

    for sort_flags in [
        vec!["-k", "2,2"],
        vec!["-k2,2", "-k3,3"],
        vec!["-n", "-k", "2,2"],
        vec!["-u", "--key=2,2"],
    ] {
        let mut fro_stdin_args = sort_flags.clone();
        fro_stdin_args.push("-");
        assert_same_result(
            run_fro_with_stdin(
                "sort",
                &fro_stdin_args,
                b"beta 2 b\nalpha 10 z\nalpha 2 a\nbeta 10 y\nalpha 2 z\n",
            ),
            run_system_sort_with_stdin(
                sort_flags
                    .iter()
                    .copied()
                    .chain(["-"])
                    .collect::<Vec<_>>()
                    .as_slice(),
                b"beta 2 b\nalpha 10 z\nalpha 2 a\nbeta 10 y\nalpha 2 z\n",
            ),
            &format!("sort key stdin {:?}", fro_stdin_args),
        );
    }
}

#[test]
fn sort_merge_matches_system_for_files_and_stdin() {
    let tmp = unique_temp_dir("fro-coreutils-sort-merge");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let c = tmp.join("c.txt");
    fs::write(&a, b"alpha\ncharlie\n").unwrap();
    fs::write(&b, b"beta\ndelta\n").unwrap();
    fs::write(&c, b"1\n10\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-m"],
            vec!["--merge"],
            vec!["-mu"],
            vec!["-mr"],
            vec!["-mn"],
            vec!["-mnr"],
        ] {
            let files = if sort_flags.iter().any(|flag| flag.contains('n')) {
                vec![c.to_str().unwrap(), c.to_str().unwrap()]
            } else {
                vec![a.to_str().unwrap(), b.to_str().unwrap()]
            };
            let mut fro_args = io_flags.clone();
            fro_args.extend(sort_flags.iter().copied());
            fro_args.extend(files.iter().copied());
            let mut sys_args = sort_flags.clone();
            sys_args.extend(files.iter().copied());
            assert_same_result(
                run_fro("sort", &fro_args),
                run_system_sort(&sys_args),
                &format!("sort merge {:?}", fro_args),
            );
        }
    }

    assert_same_result(
        run_fro_with_stdin("sort", &["-m", "-"], b"alpha\nbeta\n"),
        run_system_sort_with_stdin(&["-m", "-"], b"alpha\nbeta\n"),
        "sort merge stdin",
    );
}

#[test]
fn sort_merge_output_file_supports_in_place_rewrite() {
    let tmp = unique_temp_dir("fro-coreutils-sort-merge-output");
    let fro_in_place = tmp.join("fro-in-place.txt");
    let sys_in_place = tmp.join("sys-in-place.txt");
    fs::write(&fro_in_place, b"alpha\nbeta\n").unwrap();
    fs::write(&sys_in_place, b"alpha\nbeta\n").unwrap();

    let fro = run_fro(
        "sort",
        &[
            "-m",
            "-o",
            fro_in_place.to_str().unwrap(),
            fro_in_place.to_str().unwrap(),
        ],
    );
    let system = run_system_sort(&[
        "-m",
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
}

#[test]
fn sort_check_matches_system_status_and_diagnostics() {
    let tmp = unique_temp_dir("fro-coreutils-sort-check");
    let sorted = tmp.join("sorted.txt");
    let unsorted = tmp.join("unsorted.txt");
    let numeric = tmp.join("numeric.txt");
    let numeric_dup = tmp.join("numeric-dup.txt");
    fs::write(&sorted, b"alpha\nbeta\n").unwrap();
    fs::write(&unsorted, b"beta\nalpha\n").unwrap();
    fs::write(&numeric, b"2\n10\n").unwrap();
    fs::write(&numeric_dup, b"1\n1.0\n").unwrap();

    for io_flags in io_flag_sets() {
        for sort_flags in [
            vec!["-c", sorted.to_str().unwrap()],
            vec!["--check", unsorted.to_str().unwrap()],
            vec!["--check=diagnose-first", unsorted.to_str().unwrap()],
            vec!["-C", unsorted.to_str().unwrap()],
            vec!["--check=quiet", unsorted.to_str().unwrap()],
            vec!["--check=silent", unsorted.to_str().unwrap()],
            vec!["-cr", unsorted.to_str().unwrap()],
            vec!["-Cn", numeric.to_str().unwrap()],
            vec!["-cn", numeric.to_str().unwrap()],
            vec!["-cnu", numeric_dup.to_str().unwrap()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(sort_flags.iter().copied());
            assert_same_result(
                run_fro("sort", &fro_args),
                run_system_sort(&sort_flags),
                &format!("sort check {:?}", fro_args),
            );
        }
    }

    assert_same_result(
        run_fro_with_stdin("sort", &["-c", "-"], b"beta\nalpha\n"),
        run_system_sort_with_stdin(&["-c", "-"], b"beta\nalpha\n"),
        "sort check stdin",
    );
    assert_same_result(
        run_fro_with_stdin("sort", &["-C", "-"], b"beta\nalpha\n"),
        run_system_sort_with_stdin(&["-C", "-"], b"beta\nalpha\n"),
        "sort check silent stdin",
    );
}

#[test]
fn sort_general_and_human_merge_check_and_spill_match_system() {
    let tmp = unique_temp_dir("fro-coreutils-sort-general-human-backends");
    let general_left = tmp.join("general-left.txt");
    let general_right = tmp.join("general-right.txt");
    let general_unsorted = tmp.join("general-unsorted.txt");
    let human_left = tmp.join("human-left.txt");
    let human_right = tmp.join("human-right.txt");
    let human_unsorted = tmp.join("human-unsorted.txt");
    let spill_input = tmp.join("spill-input.txt");
    let fro_output = tmp.join("fro-spill.txt");
    let sys_output = tmp.join("sys-spill.txt");

    fs::write(&general_left, b"NaN\n-inf\n-3\n").unwrap();
    fs::write(&general_right, b".5\n10\ninf\n").unwrap();
    fs::write(&general_unsorted, b"10\n-inf\n").unwrap();
    fs::write(&human_left, b"-2K\n1000\n1K\n").unwrap();
    fs::write(&human_right, b"1024K\n1M\n1G\n").unwrap();
    fs::write(&human_unsorted, b"1K\n1000\n").unwrap();

    let mut spill_bytes = Vec::new();
    for idx in 0..120000 {
        spill_bytes.extend_from_slice(format!("{}e1\n", 120000 - idx).as_bytes());
    }
    fs::write(&spill_input, &spill_bytes).unwrap();

    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mg",
                general_left.to_str().unwrap(),
                general_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mg",
            general_left.to_str().unwrap(),
            general_right.to_str().unwrap(),
        ]),
        "sort merge general numeric",
    );
    assert_same_result(
        run_fro("sort", &["-cg", general_unsorted.to_str().unwrap()]),
        run_system_sort(&["-cg", general_unsorted.to_str().unwrap()]),
        "sort check general numeric",
    );
    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mh",
                human_left.to_str().unwrap(),
                human_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mh",
            human_left.to_str().unwrap(),
            human_right.to_str().unwrap(),
        ]),
        "sort merge human numeric",
    );
    assert_same_result(
        run_fro("sort", &["-ch", human_unsorted.to_str().unwrap()]),
        run_system_sort(&["-ch", human_unsorted.to_str().unwrap()]),
        "sort check human numeric",
    );

    let fro = run_fro_env(
        "sort",
        &[
            "-g",
            "--no-direct",
            "-o",
            fro_output.to_str().unwrap(),
            spill_input.to_str().unwrap(),
        ],
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "65536")],
    );
    let system = run_system_sort(&[
        "-g",
        "-o",
        sys_output.to_str().unwrap(),
        spill_input.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );
}

#[test]
fn sort_month_and_version_merge_check_and_spill_match_system() {
    let tmp = unique_temp_dir("fro-coreutils-sort-month-version-backends");
    let month_left = tmp.join("month-left.txt");
    let month_right = tmp.join("month-right.txt");
    let month_unsorted = tmp.join("month-unsorted.txt");
    let version_left = tmp.join("version-left.txt");
    let version_right = tmp.join("version-right.txt");
    let version_unsorted = tmp.join("version-unsorted.txt");
    let spill_input = tmp.join("spill-input.txt");
    let fro_output = tmp.join("fro-spill.txt");
    let sys_output = tmp.join("sys-spill.txt");

    fs::write(&month_left, b"Jan\nMarx\n").unwrap();
    fs::write(&month_right, b"Apr\nDec\n").unwrap();
    fs::write(&month_unsorted, b"Feb\nJan\n").unwrap();
    fs::write(&version_left, b"v01\nv1\nv1.0\n").unwrap();
    fs::write(&version_right, b"v1.0.2\nv1.0.10\n").unwrap();
    fs::write(&version_unsorted, b"v1.0.10\nv1.0.2\n").unwrap();

    let mut spill_bytes = Vec::new();
    for idx in 0..120000 {
        spill_bytes.extend_from_slice(format!("pkg-{}.tar\n", 120000 - idx).as_bytes());
    }
    fs::write(&spill_input, &spill_bytes).unwrap();

    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mM",
                month_left.to_str().unwrap(),
                month_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mM",
            month_left.to_str().unwrap(),
            month_right.to_str().unwrap(),
        ]),
        "sort merge month",
    );
    assert_same_result(
        run_fro("sort", &["-cM", month_unsorted.to_str().unwrap()]),
        run_system_sort(&["-cM", month_unsorted.to_str().unwrap()]),
        "sort check month",
    );
    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mV",
                version_left.to_str().unwrap(),
                version_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mV",
            version_left.to_str().unwrap(),
            version_right.to_str().unwrap(),
        ]),
        "sort merge version",
    );
    assert_same_result(
        run_fro("sort", &["-cV", version_unsorted.to_str().unwrap()]),
        run_system_sort(&["-cV", version_unsorted.to_str().unwrap()]),
        "sort check version",
    );

    let fro = run_fro_env(
        "sort",
        &[
            "-V",
            "--no-direct",
            "-o",
            fro_output.to_str().unwrap(),
            spill_input.to_str().unwrap(),
        ],
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "65536")],
    );
    let system = run_system_sort(&[
        "-V",
        "-o",
        sys_output.to_str().unwrap(),
        spill_input.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );
}

#[path = "sort/spill_output.rs"]
mod spill_output;
