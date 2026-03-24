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

#[test]
fn cat_show_tabs_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-tabs");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"a\tb\n\tlead\nend\t").unwrap();
    fs::write(&b, b"\t\nmid\t\n").unwrap();
    fs::write(&blank, b"\t\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-T"],
            vec!["--show-tabs"],
            vec!["-T", "-n"],
            vec!["-T", "-b"],
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

        for compat_flags in [vec!["-T"], vec!["--show-tabs"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\t\n\ty"),
                run_system_with_stdin("cat", &sys_args, b"x\t\n\ty"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_show_tab_nonprinting_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-tab-nonprinting");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    let blank = tmp.join("blank.bin");

    fs::write(
        &a,
        [b'a', b'\t', b'b', b'\n', 0x01, b'\n', 0x7f, b'\n', 0x80],
    )
    .unwrap();
    fs::write(&b, [b'\t', b'\n', 0x1f, b'\n', 0xff, b'\n']).unwrap();
    fs::write(&blank, [b'\t', b'\n']).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-t"],
            vec!["-t", "-n"],
            vec!["-t", "-b"],
            vec!["-t", "-E"],
            vec!["--show-tabs", "--show-nonprinting"],
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

        for compat_flags in [vec!["-t"], vec!["--show-tabs", "--show-nonprinting"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\t\n\x01\n\xff"),
                run_system_with_stdin("cat", &sys_args, b"x\t\n\x01\n\xff"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_show_nonprinting_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-nonprinting");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    let blank = tmp.join("blank.bin");

    fs::write(
        &a,
        [b'a', b'\t', b'b', b'\n', 0x01, b'\n', 0x7f, b'\n', 0x80],
    )
    .unwrap();
    fs::write(&b, [b'\n', 0x1f, b'\n', 0xff, b'\n']).unwrap();
    fs::write(&blank, [b'\n']).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-v"],
            vec!["--show-nonprinting"],
            vec!["-v", "-n"],
            vec!["-v", "-b"],
            vec!["-v", "-E"],
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

        for compat_flags in [vec!["-v"], vec!["--show-nonprinting"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\t\n\x01\n\xff"),
                run_system_with_stdin("cat", &sys_args, b"x\t\n\x01\n\xff"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn cat_unbuffered_flag_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-unbuffered");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    let blank = tmp.join("blank.txt");

    fs::write(&a, b"alpha\n\n beta\n").unwrap();
    fs::write(&b, b"uno\ndos\n").unwrap();
    fs::write(&blank, b"\n").unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-u"],
            vec!["-u", "-n"],
            vec!["-u", "-b"],
            vec!["-u", "-E"],
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

        let mut fro_args = io_flags.clone();
        fro_args.push("-u");
        fro_args.push("-");
        assert_same_result(
            run_fro_with_stdin("cat", &fro_args, b"x\ny\n"),
            run_system_with_stdin("cat", &["-u", "-"], b"x\ny\n"),
            &format!("cat stdin {:?} {:?}", io_flags, fro_args),
        );
    }
}

#[test]
fn cat_show_ends_nonprinting_flag_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-ends-nonprinting");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    let blank = tmp.join("blank.bin");

    fs::write(
        &a,
        [b'a', b'\t', b'b', b'\n', 0x01, b'\n', 0x7f, b'\n', 0x80],
    )
    .unwrap();
    fs::write(&b, [b'\n', 0x1f, b'\n', 0xff, b'\n']).unwrap();
    fs::write(&blank, [b'\n']).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec!["-e"], vec!["-e", "-n"], vec!["-e", "-b"]] {
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

        let mut fro_args = io_flags.clone();
        fro_args.push("-e");
        fro_args.push("-");
        assert_same_result(
            run_fro_with_stdin("cat", &fro_args, b"x\t\n\x01\n\xff"),
            run_system_with_stdin("cat", &["-e", "-"], b"x\t\n\x01\n\xff"),
            &format!("cat stdin {:?} {:?}", io_flags, fro_args),
        );
    }
}

#[test]
fn cat_show_all_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-show-all");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    let blank = tmp.join("blank.bin");

    fs::write(
        &a,
        [b'a', b'\t', b'b', b'\n', 0x01, b'\n', 0x7f, b'\n', 0x80],
    )
    .unwrap();
    fs::write(&b, [b'\t', b'\n', 0x1f, b'\n', 0xff, b'\n']).unwrap();
    fs::write(&blank, [b'\n']).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-A"],
            vec!["--show-all"],
            vec!["-A", "-n"],
            vec!["-A", "-b"],
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

        for compat_flags in [vec!["-A"], vec!["--show-all"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push("-");
            let mut sys_args = compat_flags.clone();
            sys_args.push("-");
            assert_same_result(
                run_fro_with_stdin("cat", &fro_args, b"x\t\n\x01\n\xff"),
                run_system_with_stdin("cat", &sys_args, b"x\t\n\x01\n\xff"),
                &format!("cat stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
