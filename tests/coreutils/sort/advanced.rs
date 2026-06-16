use super::*;

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
