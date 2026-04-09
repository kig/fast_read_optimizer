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
    assert!(stdout.contains("numeric"));
    assert!(stdout.contains("--check"));
    assert!(stdout.contains("--merge"));
    assert!(stdout.contains("--reverse"));
    assert!(stdout.contains("--unique"));
    assert!(stdout.contains("--numeric-sort"));
    assert!(stdout.contains("--output=FILE"));
    assert!(stdout.contains("--temporary-directory"));
    assert!(stdout.contains("Unsupported GNU sort features"));
    assert!(stdout.contains("month/version/human"));
    assert!(!stdout.contains("temp-file controls"));
    assert!(!stdout.contains("merge/check modes"));
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
            vec!["-cr", unsorted.to_str().unwrap()],
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
}

#[test]
fn sort_check_rejects_extra_operands_and_output_flag() {
    let tmp = unique_temp_dir("fro-coreutils-sort-check-invalid");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"alpha\n").unwrap();
    fs::write(&b, b"beta\n").unwrap();

    let fro = run_fro("sort", &["-c", a.to_str().unwrap(), b.to_str().unwrap()]);
    let system = run_system_sort(&["-c", a.to_str().unwrap(), b.to_str().unwrap()]);
    assert_same_result(fro, system, "sort check extra operand");

    let fro = run_fro("sort", &["--check", "-o", "out.txt", a.to_str().unwrap()]);
    let system = run_system_sort(&["--check", "-o", "out.txt", a.to_str().unwrap()]);
    assert_same_result(fro, system, "sort check incompatible output");
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

#[test]
fn sort_spills_regular_files_when_memory_budget_is_low() {
    let tmp = unique_temp_dir("fro-coreutils-sort-spill-file");
    let input = tmp.join("input.txt");
    let mut bytes = Vec::new();
    for idx in 0..120000 {
        bytes.extend_from_slice(format!("line-{idx:05}\n").as_bytes());
    }
    fs::write(&input, &bytes).unwrap();

    let fro_output = tmp.join("fro-spill.txt");
    let sys_output = tmp.join("sys-spill.txt");
    let fro = run_fro_env(
        "sort",
        &[
            "--no-direct",
            "-r",
            "-o",
            fro_output.to_str().unwrap(),
            input.to_str().unwrap(),
        ],
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "65536")],
    );
    let system = run_system_sort(&[
        "-r",
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
}

#[test]
fn sort_temporary_directory_matches_system_when_spilling() {
    let tmp = unique_temp_dir("fro-coreutils-sort-tempdir");
    let input = tmp.join("input.txt");
    let spill_dir = tmp.join("spill");
    fs::create_dir(&spill_dir).unwrap();
    let mut bytes = Vec::new();
    for idx in 0..240000 {
        bytes.extend_from_slice(format!("line-{:06}\n", 240000 - idx).as_bytes());
    }
    fs::write(&input, &bytes).unwrap();

    for (label, temp_flags) in [
        (
            "split",
            vec!["-T".to_string(), spill_dir.to_string_lossy().into_owned()],
        ),
        ("attached", vec![format!("-T{}", spill_dir.display())]),
        (
            "long",
            vec![format!("--temporary-directory={}", spill_dir.display())],
        ),
    ] {
        let fro_output = tmp.join(format!("fro-tempdir-{label}.txt"));
        let sys_output = tmp.join(format!("sys-tempdir-{label}.txt"));

        let mut fro_args = vec!["--no-direct".to_string()];
        fro_args.extend(temp_flags.iter().cloned());
        fro_args.push("-o".to_string());
        fro_args.push(fro_output.to_string_lossy().into_owned());
        fro_args.push(input.to_string_lossy().into_owned());
        let fro_args_refs = fro_args.iter().map(String::as_str).collect::<Vec<_>>();

        let mut sys_args = temp_flags.clone();
        sys_args.push("-o".to_string());
        sys_args.push(sys_output.to_string_lossy().into_owned());
        sys_args.push(input.to_string_lossy().into_owned());
        let sys_args_refs = sys_args.iter().map(String::as_str).collect::<Vec<_>>();

        let fro = run_fro_env(
            "sort",
            &fro_args_refs,
            &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "65536")],
        );
        let system = run_system_sort(&sys_args_refs);
        assert_eq!(fro.status.code(), system.status.code(), "{label}");
        assert!(fro.stdout.is_empty(), "{label}");
        assert_eq!(fro.stderr, system.stderr, "{label}");
        assert_eq!(
            fs::read(&fro_output).unwrap(),
            fs::read(&sys_output).unwrap(),
            "{label}"
        );
        assert!(
            fs::read_dir(&spill_dir).unwrap().next().is_none(),
            "{label}: custom spill directory should be cleaned after sort finishes"
        );
    }
}

#[test]
fn sort_temporary_directory_is_observed_for_spill_files_only() {
    let tmp = unique_temp_dir("fro-coreutils-sort-tempdir-observe");
    let small_input = tmp.join("small.txt");
    let large_input = tmp.join("large.txt");
    let spill_dir = tmp.join("spill");
    let invalid_spill_dir = tmp.join("not-a-dir");
    let output_path = tmp.join("output.txt");

    fs::create_dir(&spill_dir).unwrap();
    fs::write(&small_input, b"beta\nalpha\n").unwrap();
    fs::write(&invalid_spill_dir, b"sentinel").unwrap();

    let small = run_fro(
        "sort",
        &[
            "-T",
            invalid_spill_dir.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            small_input.to_str().unwrap(),
        ],
    );
    assert!(
        small.status.success(),
        "in-memory sort should ignore -T until spill files are needed: {}",
        String::from_utf8_lossy(&small.stderr)
    );

    let mut bytes = Vec::new();
    for idx in 0..1_200_000u32 {
        bytes.extend_from_slice(format!("line-{:07}\n", 1_200_000 - idx).as_bytes());
    }
    fs::write(&large_input, &bytes).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg("sort")
        .args([
            "-T",
            spill_dir.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            large_input.to_str().unwrap(),
        ])
        .env("FRO_SORT_MAX_IN_MEMORY_BYTES", "32768")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro sort with custom temp dir");

    let mut observed_spill_dir = false;
    for _ in 0..400 {
        if fs::read_dir(&spill_dir)
            .unwrap()
            .any(|entry| entry.unwrap().path().is_dir())
        {
            observed_spill_dir = true;
            break;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "spilling sort failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        observed_spill_dir,
        "did not observe spill files under custom temporary directory"
    );
    assert!(
        fs::read_dir(&spill_dir).unwrap().next().is_none(),
        "custom spill directory should be empty after cleanup"
    );

    let spill_failure = run_fro_env(
        "sort",
        &[
            "-T",
            invalid_spill_dir.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            large_input.to_str().unwrap(),
        ],
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "32768")],
    );
    assert_eq!(spill_failure.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&spill_failure.stderr);
    assert!(stderr.contains("cannot create temporary file in"));
    assert!(stderr.contains(invalid_spill_dir.to_str().unwrap()));
}

#[test]
fn sort_spills_stream_input_when_memory_budget_is_low() {
    let tmp = unique_temp_dir("fro-coreutils-sort-spill-stdin");
    let fro_output = tmp.join("fro-spill-stdin.txt");
    let sys_output = tmp.join("sys-spill-stdin.txt");
    let mut input = Vec::new();
    for idx in 0..220000 {
        input.extend_from_slice(format!("{:06}\n", 220000 - idx).as_bytes());
    }

    let fro = run_fro_with_stdin_env(
        "sort",
        &["-o", fro_output.to_str().unwrap(), "-"],
        &input,
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "32768")],
    );
    let system = run_system_sort_with_stdin(&["-o", sys_output.to_str().unwrap(), "-"], &input);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );
}
