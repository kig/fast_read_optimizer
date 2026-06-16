use super::*;

#[test]
fn cartesian_cmp_and_fgrep_match_system_output() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-cmp-grep-matrix");
    let tmp = &fixture.root;
    let equal_a = tmp.join("equal-a.txt");
    let equal_b = tmp.join("equal-b.txt");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let eof_a = tmp.join("eof-a.txt");
    let eof_b = tmp.join("eof-b.txt");
    let grep_a = fixture.text_file.clone();
    let grep_b = fixture.nested_text_file.clone();

    fs::write(&equal_a, b"same\nbytes\n").unwrap();
    fs::write(&equal_b, b"same\nbytes\n").unwrap();
    fs::write(&diff_a, b"same\nbytes\n").unwrap();
    fs::write(&diff_b, b"same\nbytex\n").unwrap();
    fs::write(&eof_a, b"short\n").unwrap();
    fs::write(&eof_b, b"short\nextra\n").unwrap();

    for flags in io_flag_sets() {
        for files in [
            vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
            vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
            vec![eof_a.to_str().unwrap(), eof_b.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(files.iter().copied());
            assert_same_result(
                run_fro("cmp", &args),
                run_system("cmp", &files),
                &format!("cmp {:?}", args),
            );
        }

        for quiet_flags in [vec!["-s"], vec!["--quiet"], vec!["--silent"]] {
            for files in [
                vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![eof_a.to_str().unwrap(), eof_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(quiet_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = quiet_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, quiet_flags),
                );
            }
        }

        for grep_args in [
            vec!["needle", grep_a.to_str().unwrap()],
            vec!["-n", "needle", grep_a.to_str().unwrap()],
            vec!["-v", "needle", grep_a.to_str().unwrap()],
            vec!["needle", grep_a.to_str().unwrap(), grep_b.to_str().unwrap()],
            vec!["missing", grep_a.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(grep_args.iter().copied());
            let system_program = "grep";
            let mut system_args = vec!["-F"];
            system_args.extend(grep_args.iter().copied());
            assert_same_result(
                run_fro("fgrep", &args),
                run_system(system_program, &system_args),
                &format!("fgrep {:?}", args),
            );
        }
    }
}

#[test]
fn fgrep_line_regexp_matches_system_on_nested_fixture_paths() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-fgrep-dir");

    for (kind, path) in fixture.text_path_inputs() {
        let file = path.to_str().unwrap();
        assert_same_result(
            run_fro("fgrep", &["-x", "needle beta", file]),
            run_system("grep", &["-F", "-x", "needle beta", file]),
            &format!("fgrep line-regexp {kind}"),
        );
    }
}

#[test]
fn fgrep_pattern_sources_match_system_output() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-fgrep-pattern-sources");
    let patterns = fixture.root.join("patterns.txt");
    let empty_patterns = fixture.root.join("empty-patterns.txt");
    fs::write(&patterns, b"needle beta\nomega\n").unwrap();
    fs::write(&empty_patterns, b"").unwrap();

    let text = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();
    let text_link = fixture.text_symlink.to_str().unwrap();
    let pattern_file = patterns.to_str().unwrap();
    let empty_pattern_file = empty_patterns.to_str().unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-e", "needle beta", text],
            vec!["--regexp=needle beta", text],
            vec!["-e", "needle beta", "-e", "omega", text],
            vec!["-eneedle beta", "-eomega", text],
            vec!["-f", pattern_file, text],
            vec!["--file", pattern_file, text],
            vec!["--file=".into(), "--regexp=omega".into()],
        ] {
            let compat_flags = compat_flags
                .into_iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>();
            let mut fro_args = flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect::<Vec<_>>();
            fro_args.extend(compat_flags.clone());
            let mut sys_args = compat_flags.clone();
            if sys_args.first().map(String::as_str) == Some("--file=") {
                sys_args[0] = format!("--file={pattern_file}");
                fro_args[flags.len()] = format!("--file={pattern_file}");
            }
            let fro_refs = fro_args.iter().map(String::as_str).collect::<Vec<_>>();
            let sys_refs = sys_args.iter().map(String::as_str).collect::<Vec<_>>();
            assert_same_result(
                run_fro("fgrep", &fro_refs),
                run_system("fgrep", &sys_refs),
                &format!("fgrep pattern source {:?}", fro_refs),
            );
        }

        for compat_flags in [
            vec!["-f", pattern_file, text, nested],
            vec!["-e", "needle beta", "-f", pattern_file, text, nested],
            vec!["-f", pattern_file, text_link],
            vec!["-f", empty_pattern_file, text],
        ] {
            let mut fro_args = flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("fgrep", &compat_flags),
                &format!("fgrep pattern file {:?}", fro_args),
            );
        }
    }
}

#[test]
fn cmp_end_of_options_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-end-of-options");
    let dash_a = tmp.join("--left.bin");
    let dash_b = tmp.join("--right.bin");
    let flag_named = tmp.join("-s");
    let normal = tmp.join("normal.bin");

    fs::write(&dash_a, b"same\nbytes\n").unwrap();
    fs::write(&dash_b, b"same\nbytex\n").unwrap();
    fs::write(&flag_named, b"abc").unwrap();
    fs::write(&normal, b"abc").unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["--"],
            vec!["-s", "--"],
            vec!["-b", "--"],
            vec!["-i", "2", "--"],
        ] {
            for files in [
                vec![dash_a.to_str().unwrap(), dash_b.to_str().unwrap()],
                vec![flag_named.to_str().unwrap(), normal.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_bytes_flag_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-bytes");
    let equal_a = tmp.join("equal-a.txt");
    let equal_b = tmp.join("equal-b.txt");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let short = tmp.join("short.txt");
    let long = tmp.join("long.txt");

    fs::write(&equal_a, b"same-bytes\n").unwrap();
    fs::write(&equal_b, b"same-bytes\n").unwrap();
    fs::write(&diff_a, b"abcde\n").unwrap();
    fs::write(&diff_b, b"abXde\n").unwrap();
    fs::write(&short, b"abc").unwrap();
    fs::write(&long, b"abcXYZ").unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-n", "0"],
            vec!["-n", "2"],
            vec!["-n", "3"],
            vec!["-n3"],
            vec!["--bytes", "3"],
            vec!["--bytes=3"],
            vec!["--bytes", "99"],
        ] {
            for files in [
                vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![short.to_str().unwrap(), long.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_suffix_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-suffixes");
    let prefix = vec![b'a'; 2_100_000];
    let mut left = prefix.clone();
    left.push(b'X');
    let mut right = prefix;
    right.push(b'Y');
    let equal = vec![b'z'; 4096];
    let equal_a = tmp.join("equal-a.bin");
    let equal_b = tmp.join("equal-b.bin");
    let diff_a = tmp.join("diff-a.bin");
    let diff_b = tmp.join("diff-b.bin");

    fs::write(&equal_a, &equal).unwrap();
    fs::write(&equal_b, &equal).unwrap();
    fs::write(&diff_a, &left).unwrap();
    fs::write(&diff_b, &right).unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-n", "1k"],
            vec!["-n", "1KB"],
            vec!["--bytes=1MiB"],
            vec!["-i", "1K"],
            vec!["-i", "1KB:1K"],
            vec!["--ignore-initial=1MiB", "--bytes=1K"],
        ] {
            for files in [
                vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_ignore_initial_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-ignore-initial");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let short = tmp.join("short.txt");
    let long = tmp.join("long.txt");
    let multiline_a = tmp.join("multiline-a.txt");
    let multiline_b = tmp.join("multiline-b.txt");

    fs::write(&diff_a, b"0123456789\n").unwrap();
    fs::write(&diff_b, b"012x456789\n").unwrap();
    fs::write(&short, b"abcXYZ").unwrap();
    fs::write(&long, b"abcUVWXYZ").unwrap();
    fs::write(&multiline_a, b"aa\nbb\ncc\n").unwrap();
    fs::write(&multiline_b, b"aa\nxb\ncc\n").unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-i", "0"],
            vec!["-i1"],
            vec!["-i", "1"],
            vec!["-i", "4"],
            vec!["--ignore-initial=4"],
            vec!["-i", "3:4"],
            vec!["--ignore-initial=3:4"],
            vec!["-i", "3:3", "-n", "2"],
            vec!["-i", "6"],
            vec!["-i", "7"],
            vec!["-i", "4:6"],
        ] {
            for files in [
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![short.to_str().unwrap(), long.to_str().unwrap()],
                vec![multiline_a.to_str().unwrap(), multiline_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_verbose_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-verbose");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let short = tmp.join("short.txt");
    let long = tmp.join("long.txt");
    let wide_a = tmp.join("wide-a.txt");
    let wide_b = tmp.join("wide-b.txt");

    fs::write(&diff_a, b"abc\nxyz\n").unwrap();
    fs::write(&diff_b, b"abQ\nxyZ\n").unwrap();
    fs::write(&short, b"abc").unwrap();
    fs::write(&long, b"abQXYZ").unwrap();
    fs::write(&wide_a, b"0123456789\n").unwrap();
    fs::write(&wide_b, b"012x45y789\n").unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-l"],
            vec!["--verbose"],
            vec!["-l", "-n", "4"],
            vec!["-l", "-i", "3"],
            vec!["-l", "-i", "3:4"],
            vec!["-l", "-i", "3:4", "-n", "7"],
            vec!["-l", "-s"],
            vec!["--verbose", "--quiet"],
        ] {
            for files in [
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![short.to_str().unwrap(), long.to_str().unwrap()],
                vec![wide_a.to_str().unwrap(), wide_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_print_bytes_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-print-bytes");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let short = tmp.join("short.txt");
    let long = tmp.join("long.txt");
    let binary_a = tmp.join("binary-a.bin");
    let binary_b = tmp.join("binary-b.bin");

    fs::write(&diff_a, b"abc\nxyz\n").unwrap();
    fs::write(&diff_b, b"abQ\nxyZ\n").unwrap();
    fs::write(&short, b"abc").unwrap();
    fs::write(&long, b"abQXYZ").unwrap();
    fs::write(&binary_a, [0, b'\n', b' ', b'A', 126, 127, 255]).unwrap();
    fs::write(&binary_b, [1, b'\n', b'\t', b'B', 126, 127, 254]).unwrap();

    for flags in io_flag_sets() {
        for compat_flags in [
            vec!["-b"],
            vec!["--print-bytes"],
            vec!["-b", "-n", "4"],
            vec!["-b", "-i", "3"],
            vec!["-b", "-i", "3:4"],
            vec!["-b", "-l"],
            vec!["--print-bytes", "--verbose"],
            vec!["-b", "-s"],
        ] {
            for files in [
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![short.to_str().unwrap(), long.to_str().unwrap()],
                vec![binary_a.to_str().unwrap(), binary_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn cmp_recursive_reports_tree_differences_in_sorted_order() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-recursive-diff");
    let left = tmp.join("left");
    let right = tmp.join("right");
    fs::create_dir_all(left.join("sub")).unwrap();
    fs::create_dir_all(right.join("sub")).unwrap();
    fs::write(left.join("common.txt"), b"same\n").unwrap();
    fs::write(right.join("common.txt"), b"same\n").unwrap();
    fs::write(left.join("diff.txt"), b"alpha\nbeta\n").unwrap();
    fs::write(right.join("diff.txt"), b"alpha\nzeta\n").unwrap();
    fs::write(left.join("only-left.txt"), b"left\n").unwrap();
    fs::write(right.join("only-right.txt"), b"right\n").unwrap();
    fs::write(left.join("sub").join("nested.txt"), b"nested\n").unwrap();
    fs::write(right.join("sub").join("nested.txt"), b"changed\n").unwrap();

    let output = run_fro(
        "cmp",
        &["-r", left.to_str().unwrap(), right.to_str().unwrap()],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stderr.is_empty(),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stdout,
        format!(
            "{} {} differ: byte 7, line 2\nOnly in {}: only-left.txt\nOnly in {}: only-right.txt\n{} {} differ: byte 1, line 1\n",
            left.join("diff.txt").display(),
            right.join("diff.txt").display(),
            left.display(),
            right.display(),
            left.join("sub").join("nested.txt").display(),
            right.join("sub").join("nested.txt").display(),
        )
    );
}

#[test]
fn cmp_recursive_quiet_stops_after_first_mismatch() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-recursive-quiet");
    let left = tmp.join("left");
    let right = tmp.join("right");
    fs::create_dir_all(&left).unwrap();
    fs::create_dir_all(&right).unwrap();
    fs::write(left.join("a.txt"), b"left\n").unwrap();
    fs::write(right.join("a.txt"), b"right\n").unwrap();
    fs::write(left.join("z.txt"), b"extra-left\n").unwrap();
    fs::write(right.join("z.txt"), b"extra-right\n").unwrap();

    let output = run_fro(
        "cmp",
        &[
            "--recursive",
            "--quiet",
            left.to_str().unwrap(),
            right.to_str().unwrap(),
        ],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn cmp_recursive_compares_symlink_targets_without_following_them() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-recursive-symlink");
    let left = tmp.join("left");
    let right = tmp.join("right");
    fs::create_dir_all(&left).unwrap();
    fs::create_dir_all(&right).unwrap();
    symlink("../target-a", left.join("link")).unwrap();
    symlink("../target-b", right.join("link")).unwrap();

    let output = run_fro(
        "cmp",
        &["-r", left.to_str().unwrap(), right.to_str().unwrap()],
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "cmp: symbolic links {} and {} differ\n",
            left.join("link").display(),
            right.join("link").display()
        )
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn cmp_help_and_version_surface_stay_wired() {
    let help = run_fro("cmp", &["--help"]);
    assert_eq!(
        help.status.code(),
        Some(0),
        "cmp --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&help.stdout),
        String::from_utf8_lossy(&help.stderr),
    );
    assert!(
        help.stderr.is_empty(),
        "cmp --help unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&help.stderr),
    );
    let help_stdout = String::from_utf8(help.stdout).expect("cmp help should be UTF-8");
    for token in ["--help", "--version", "-v"] {
        assert!(
            help_stdout.contains(token),
            "cmp --help output should mention {token}:\n{help_stdout}"
        );
    }

    let version = run_fro("cmp", &["--version"]);
    assert_eq!(
        version.status.code(),
        Some(0),
        "cmp --version failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr),
    );
    assert!(
        version.stderr.is_empty(),
        "cmp --version unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&version.stderr),
    );

    let short_version = run_fro("cmp", &["-v"]);
    assert_eq!(
        short_version.status.code(),
        Some(0),
        "cmp -v failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&short_version.stdout),
        String::from_utf8_lossy(&short_version.stderr),
    );
    assert!(
        short_version.stderr.is_empty(),
        "cmp -v unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&short_version.stderr),
    );
    assert_eq!(
        short_version.stdout, version.stdout,
        "cmp -v should match cmp --version output"
    );
}
