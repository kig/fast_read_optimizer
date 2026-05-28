use super::*;

#[test]
fn fgrep_help_and_version_surface_stay_wired() {
    let output = run_fro("fgrep", &["--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(0));
    assert!(stdout.contains(
        "fgrep - Bounded literal line-oriented grep slice on top of fro's fast substring scanner."
    ));
    assert!(stdout.contains(
        "fgrep [-n] [-b] [-i] [-x] [-w] [-v] [-z] [-r|-R] [-A NUM] [-B NUM] [-C NUM] [-NUM] [-c|-q|-l|-L] [-H|-h] [-Z] [-s] [-T] [-D ACTION] [-d ACTION] [--label LABEL] [--line-buffered] [-a|-I|-U|--binary-files=TYPE] [-m NUM] [--color[=WHEN]|--colour[=WHEN]] [-e PATTERN | -f FILE]... [-V|--version] [--no-ignore-case] [--auto|--no-direct|--direct] [--report-gbps] [pattern] <file> [file ...]"
    ));
    assert!(
        stdout.contains("This is a bounded literal-search compatibility slice, not full GNU grep.")
    );
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro fgrep version string and exits."));
    assert!(stdout
        .contains("Tracked GNU/coreutils flags implemented in this bounded literal-search slice:"));
    assert!(stdout
        .contains("GNU grep tokens intentionally omitted from the in-process literal-search path"));
    assert!(stdout.contains("-A/--after-context=NUM"));
    assert!(stdout.contains("-B/--before-context=NUM"));
    assert!(stdout.contains("-C/--context=NUM"));
    assert!(stdout.contains("-NUM"));
    assert!(stdout.contains("-b/--byte-offset"));
    assert!(stdout.contains("-w/--word-regexp"));
    assert!(stdout.contains("-m/--max-count"));
    assert!(stdout.contains("-D/--devices"));
    assert!(stdout.contains("-d/--directories"));
    assert!(stdout.contains("--label"));
    assert!(stdout.contains("-T/--initial-tab"));
    assert!(stdout.contains("--line-buffered"));
    assert!(stdout.contains("--binary-files=TYPE"));
    assert!(stdout.contains("-I/--binary-files=without-match"));
    assert!(stdout.contains("-a/--text"));
    assert!(stdout.contains("-U/--binary"));
    assert!(stdout.contains("-E/--extended-regexp"));
    assert!(stdout.contains("-r/--recursive"));
    assert!(stdout.contains("-s/--no-messages"));
    assert!(stdout.contains("-Z/--null"));
    assert!(stdout.contains("-V"));
    assert!(output.stderr.is_empty());

    let version = run_fro("fgrep", &["--version"]);
    assert_eq!(version.status.code(), Some(0));
    assert!(
        version.stderr.is_empty(),
        "fgrep --version unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&version.stderr)
    );
    let version_stdout = String::from_utf8_lossy(&version.stdout);
    assert!(version_stdout.starts_with("fgrep (fro coreutils) "));
    assert!(version_stdout.contains(env!("CARGO_PKG_VERSION")));

    let short_version = run_fro("fgrep", &["-V"]);
    assert_eq!(short_version.status.code(), Some(0));
    assert!(
        short_version.stderr.is_empty(),
        "fgrep -V unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&short_version.stderr)
    );
    let short_version_stdout = String::from_utf8_lossy(&short_version.stdout);
    assert!(short_version_stdout.starts_with("fgrep (fro coreutils) "));
    assert!(short_version_stdout.contains(env!("CARGO_PKG_VERSION")));
}

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

#[test]
fn fgrep_invert_match_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-fgrep-invert-match");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let patterns = tmp.join("patterns.txt");

    fs::write(&a, b"alpha\nfoo\nalpha beta\n").unwrap();
    fs::write(&b, b"beta\nALPHA\nalpha\n").unwrap();
    fs::write(&patterns, b"alpha\nbeta\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-v", "alpha"],
            vec!["--invert-match", "alpha"],
            vec!["-v", "-n", "alpha"],
            vec!["-v", "-i", "alpha"],
            vec!["-v", "-x", "alpha"],
            vec!["-c", "-v", "alpha"],
            vec!["-v", "-f", patterns.to_str().unwrap()],
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
                    &format!("fgrep -v {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-v", "alpha"],
            vec!["--invert-match", "alpha"],
            vec!["-v", "-n", "alpha"],
            vec!["-v", "-i", "alpha"],
            vec!["-c", "-v", "alpha"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, b"Alpha\nbeta\nalpha\n"),
                run_system_with_stdin("fgrep", &sys_args, b"Alpha\nbeta\nalpha\n"),
                &format!("fgrep -v stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
