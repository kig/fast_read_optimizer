use super::*;

#[test]
fn cat_number_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-number");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"alpha\n\n beta\n").unwrap();
    fs::write(&b, b"uno\ndos\n").unwrap();
    fs::write(&blank, b"\n\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec!["-n"], vec!["--number"]] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![blank.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cat", &fro_args),
                    run_system("cat", &sys_args),
                    &format!("cat {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [vec!["-n"], vec!["--number"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\ny\n"),
                run_system_with_stdin("cat", &sys_args, b"x\ny\n"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_squeeze_blank_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-squeeze");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"alpha\n\n\n beta\n").unwrap();
    fs::write(&b, b"\n\nuno\n").unwrap();
    fs::write(&blank, b"\n\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec!["-s"], vec!["--squeeze-blank"]] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![blank.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cat", &fro_args),
                    run_system("cat", &sys_args),
                    &format!("cat {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [vec!["-s"], vec!["--squeeze-blank"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\n\n\n\ny\n"),
                run_system_with_stdin("cat", &sys_args, b"x\n\n\n\ny\n"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_number_nonblank_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-number-nonblank");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"alpha\n\n beta\n").unwrap();
    fs::write(&b, b"\n\nuno\n").unwrap();
    fs::write(&blank, b"\n\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-b"],
            vec!["--number-nonblank"],
            vec!["-b", "-n"],
            vec!["-n", "-b"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![blank.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cat", &fro_args),
                    run_system("cat", &sys_args),
                    &format!("cat {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [
            vec!["-b"],
            vec!["--number-nonblank"],
            vec!["-b", "-n"],
            vec!["-n", "-b"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\n\n\ny\n"),
                run_system_with_stdin("cat", &sys_args, b"x\n\n\ny\n"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_show_ends_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-ends");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"alpha\n\n beta\ntrail").unwrap();
    fs::write(&b, b"\nuno\n").unwrap();
    fs::write(&blank, b"\n\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-E"],
            vec!["--show-ends"],
            vec!["-E", "-n"],
            vec!["-E", "-b"],
        ] {
            for files in [
                vec![a.to_str().unwrap()],
                vec![blank.to_str().unwrap()],
                vec![a.to_str().unwrap(), b.to_str().unwrap()],
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = compat_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cat", &fro_args),
                    run_system("cat", &sys_args),
                    &format!("cat {:?} {:?}", io_flags, compat_flags),
                );
            }
        }

        for compat_flags in [vec!["-E"], vec!["--show-ends"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\n\ny"),
                run_system_with_stdin("cat", &sys_args, b"x\n\ny"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
