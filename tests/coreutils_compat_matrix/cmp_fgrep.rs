use super::*;

#[test]
fn cartesian_cmp_and_fgrep_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-grep-matrix");
    let equal_a = tmp.join("equal-a.txt");
    let equal_b = tmp.join("equal-b.txt");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let eof_a = tmp.join("eof-a.txt");
    let eof_b = tmp.join("eof-b.txt");
    let grep_a = tmp.join("grep-a.txt");
    let grep_b = tmp.join("grep-b.txt");

    fs::write(&equal_a, b"same\nbytes\n").unwrap();
    fs::write(&equal_b, b"same\nbytes\n").unwrap();
    fs::write(&diff_a, b"same\nbytes\n").unwrap();
    fs::write(&diff_b, b"same\nbytex\n").unwrap();
    fs::write(&eof_a, b"short\n").unwrap();
    fs::write(&eof_b, b"short\nextra\n").unwrap();
    fs::write(&grep_a, b"alpha\nneedle beta\nomega\n").unwrap();
    fs::write(&grep_b, b"needle gamma\nzeta\n").unwrap();

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
