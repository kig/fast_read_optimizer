use super::*;

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
    assert!(stdout.contains("--ignore-case"));
    assert!(stdout.contains("--ignore-leading-blanks"));
    assert!(stdout.contains("--ignore-nonprinting"));
    assert!(stdout.contains("--human-numeric-sort"));
    assert!(stdout.contains("--merge"));
    assert!(stdout.contains("--month-sort"));
    assert!(stdout.contains("--parallel"));
    assert!(stdout.contains("--reverse"));
    assert!(stdout.contains("--unique"));
    assert!(stdout.contains("--version-sort"));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("--numeric-sort"));
    assert!(stdout.contains("--files0-from=FILE"));
    assert!(stdout.contains("--field-separator=SEP"));
    assert!(stdout.contains("--key=KEY"));
    assert!(stdout.contains("--output=FILE"));
    assert!(stdout.contains("--temporary-directory"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro sort version string and exits."));
    assert!(stdout.contains("Unsupported GNU sort features"));
    assert!(stdout.contains("locale collation"));
    assert!(stdout.contains("per-key modifiers"));
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
