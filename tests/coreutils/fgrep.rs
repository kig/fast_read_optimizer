use super::*;

#[test]
fn fgrep_fixed_strings_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-fixed-strings");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");

    fs::write(&a, b"alpha[1]\nbeta\nalpha[1]\n").unwrap();
    fs::write(&b, b"gamma\nalpha[1]\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-F", "alpha[1]"],
            vec!["--fixed-strings", "alpha[1]"],
            vec!["-F", "-n", "alpha[1]"],
            vec!["-n", "-F", "alpha[1]"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("fgrep", &sys_args),
                    &format!("fgrep {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-F", "alpha[1]"],
            vec!["--fixed-strings", "alpha[1]"],
            vec!["-F", "-n", "alpha[1]"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"alpha[1]\nfoo\n"),
                run_system_with_stdin("fgrep", &sys_args, b"alpha[1]\nfoo\n"),
                &format!("fgrep stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_line_number_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-line-number");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");

    fs::write(&a, b"alpha\nfoo\nalpha\n").unwrap();
    fs::write(&b, b"beta\nalpha\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-n", "alpha"],
            vec!["--line-number", "alpha"],
            vec!["-F", "-n", "alpha"],
            vec!["-F", "--line-number", "alpha"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("fgrep", &sys_args),
                    &format!("fgrep {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [vec!["-n", "alpha"], vec!["--line-number", "alpha"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"alpha\nfoo\n"),
                run_system_with_stdin("fgrep", &sys_args, b"alpha\nfoo\n"),
                &format!("fgrep stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_stdin_matches_pattern_crossing_block_boundary() {
    let prefix = vec![b'a'; (1 << 20) - 3];
    let mut input = prefix;
    input.extend_from_slice(b"foo");
    input.extend_from_slice(b"bar\n");

    assert_same_result(
        run_fro_with_stdin("fgrep", &["foobar"], &input),
        run_system_with_stdin("fgrep", &["foobar"], &input),
        "fgrep stdin boundary match",
    );
    assert_same_result(
        run_fro_with_stdin("fgrep", &["-n", "foobar"], &input),
        run_system_with_stdin("fgrep", &["-n", "foobar"], &input),
        "fgrep stdin boundary match -n",
    );
}

#[test]
fn fgrep_line_regexp_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-line-regexp");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");

    fs::write(&a, b"alpha\nalpha beta\nbeta alpha\nalpha\n").unwrap();
    fs::write(&b, b"gamma\nalpha\nalphA\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-x", "alpha"],
            vec!["--line-regexp", "alpha"],
            vec!["-x", "-n", "alpha"],
            vec!["-n", "--line-regexp", "alpha"],
            vec!["-F", "-x", "alpha"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("fgrep", &sys_args),
                    &format!("fgrep -x {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-x", "alpha"],
            vec!["--line-regexp", "alpha"],
            vec!["-x", "-n", "alpha"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"alpha\nalpha beta\nalpha"),
                run_system_with_stdin("fgrep", &sys_args, b"alpha\nalpha beta\nalpha"),
                &format!("fgrep -x stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_ignore_case_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-ignore-case");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");

    fs::write(&a, b"Alpha\nalpha beta\nGAMMA\n").unwrap();
    fs::write(&b, b"beta\nALPHA\nalpha[1]\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-i", "alpha"],
            vec!["--ignore-case", "alpha"],
            vec!["-F", "-i", "alpha"],
            vec!["-i", "-n", "alpha"],
            vec!["-i", "-x", "alpha"],
            vec!["--ignore-case", "--line-number", "alpha"],
            vec!["-i", "--no-ignore-case", "alpha"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("fgrep", &sys_args),
                    &format!("fgrep -i {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-i", "alpha"],
            vec!["--ignore-case", "alpha"],
            vec!["-i", "-n", "alpha"],
            vec!["-i", "-x", "alpha"],
            vec!["-i", "--no-ignore-case", "alpha"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"Alpha\nalpha beta\nALPHA\n"),
                run_system_with_stdin("fgrep", &sys_args, b"Alpha\nalpha beta\nALPHA\n"),
                &format!("fgrep -i stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_count_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-count");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let c = tmp.join("c.txt");
    let patterns = tmp.join("patterns.txt");
    let empty_patterns = tmp.join("empty-patterns.txt");

    fs::write(&a, b"alpha\nfoo\nalpha\n").unwrap();
    fs::write(&b, b"beta\nALPHA\nalpha beta\n").unwrap();
    fs::write(&c, b"gamma\ndelta\n").unwrap();
    fs::write(&patterns, b"alpha\nbeta\n").unwrap();
    fs::write(&empty_patterns, b"").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-c", "alpha"],
            vec!["--count", "alpha"],
            vec!["-c", "-n", "alpha"],
            vec!["-c", "-i", "alpha"],
            vec!["-c", "-x", "alpha"],
            vec!["-c", "-f", patterns.to_str().unwrap()],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
                vec![c.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("fgrep", &sys_args),
                    &format!("fgrep -c {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-c", "alpha"],
            vec!["--count", "alpha"],
            vec!["-c", "-i", "alpha"],
            vec!["-c", "-x", "alpha"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"Alpha\nalpha\nbeta\n"),
                run_system_with_stdin("fgrep", &sys_args, b"Alpha\nalpha\nbeta\n"),
                &format!("fgrep -c stdin {:?} {:?}", io_flags, compat_flags),
            );
        }

        let mut fro_args = io_flags.clone();
        fro_args.extend([
            "-c",
            "-f",
            empty_patterns.to_str().unwrap(),
            a.to_str().unwrap(),
        ]);
        assert_same_result(
            run_fro("fgrep", &fro_args),
            run_system(
                "fgrep",
                &[
                    "-c",
                    "-f",
                    empty_patterns.to_str().unwrap(),
                    a.to_str().unwrap(),
                ],
            ),
            &format!("fgrep -c empty pattern file {:?}", io_flags),
        );
    }
}
