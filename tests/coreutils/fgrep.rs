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
