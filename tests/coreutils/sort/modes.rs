use super::*;

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
