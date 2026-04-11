use super::*;

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
