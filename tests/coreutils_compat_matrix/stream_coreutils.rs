use super::*;

#[test]
fn cartesian_stream_coreutils_match_system_for_input_kinds() {
    let tmp = unique_temp_dir("fro-coreutils-stream-matrix");
    let text = b"alpha\nneedle beta\nomega\n".to_vec();
    let binary = (0..65557)
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();

    let text_path = tmp.join("text.txt");
    let text_symlink = tmp.join("text-link.txt");
    let binary_path = tmp.join("binary.bin");
    let binary_symlink = tmp.join("binary-link.bin");
    fs::write(&text_path, &text).unwrap();
    fs::write(&binary_path, &binary).unwrap();
    symlink(&text_path, &text_symlink).unwrap();
    symlink(&binary_path, &binary_symlink).unwrap();

    for flags in io_flag_sets() {
        for path in [&text_path, &text_symlink] {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cat", &args),
                run_system("cat", &[file]),
                &format!("cat path {:?}", args),
            );
            assert_same_result(
                run_fro("tac", &args),
                run_system("tac", &[file]),
                &format!("tac path {:?}", args),
            );
            for head_args in [
                vec![],
                vec!["-n", "2"],
                vec!["-c", "5"],
                vec!["-c", "1KiB"],
                vec!["-c", "1MiB"],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(head_args.iter().copied());
                fro_args.push(file);
                let mut sys_args = head_args;
                sys_args.push(file);
                assert_same_result(
                    run_fro("head", &fro_args),
                    run_system("head", &sys_args),
                    &format!("head path {:?}", fro_args),
                );
            }
            for wc_flags in wc_flag_sets() {
                let mut fro_args = flags.clone();
                fro_args.extend(wc_flags.iter().copied());
                fro_args.push(file);
                let mut sys_args = wc_flags;
                sys_args.push(file);
                assert_same_wc(
                    run_fro("wc", &fro_args),
                    run_system("wc", &sys_args),
                    &format!("wc path {:?}", fro_args),
                );
            }
            for grep_args in [vec!["needle", file], vec!["-n", "needle", file]] {
                let mut fro_args = flags.clone();
                fro_args.extend(grep_args.iter().copied());
                let mut sys_args = vec!["-F"];
                sys_args.extend(grep_args.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep path {:?}", fro_args),
                );
            }
        }

        for path in [&binary_path, &binary_symlink] {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cksum", &args),
                run_system("cksum", &[file]),
                &format!("cksum path {:?}", args),
            );
            assert_same_result(
                run_fro("sha256sum", &args),
                run_system("sha256sum", &[file]),
                &format!("sha256sum path {:?}", args),
            );
        }
    }

    assert_same_result(
        run_fro_with_stdin("cat", &[], &text),
        run_system_with_stdin("cat", &[], &text),
        "cat stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("cat", &["-"], &text),
        run_system_with_stdin("cat", &["-"], &text),
        "cat dash",
    );
    assert_same_result(
        run_fro_with_stdin("tac", &[], &text),
        run_system_with_stdin("tac", &[], &text),
        "tac stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("tac", &["-"], &text),
        run_system_with_stdin("tac", &["-"], &text),
        "tac dash",
    );
    for head_args in [
        vec![],
        vec!["-n", "2"],
        vec!["-c", "5"],
        vec!["-c", "1KiB"],
        vec!["-c", "1MiB"],
    ] {
        assert_same_result(
            run_fro_with_stdin("head", &head_args, &text),
            run_system_with_stdin("head", &head_args, &text),
            &format!("head stdin {:?}", head_args),
        );
        let mut dash_args = head_args.clone();
        dash_args.push("-");
        assert_same_result(
            run_fro_with_stdin("head", &dash_args, &text),
            run_system_with_stdin("head", &dash_args, &text),
            &format!("head dash {:?}", dash_args),
        );
    }

    for wc_flags in wc_flag_sets() {
        assert_same_wc(
            run_fro_with_stdin("wc", &wc_flags, &text),
            run_system_with_stdin("wc", &wc_flags, &text),
            &format!("wc stdin {:?}", wc_flags),
        );
        let mut dash_args = wc_flags.clone();
        dash_args.push("-");
        assert_same_wc(
            run_fro_with_stdin("wc", &dash_args, &text),
            run_system_with_stdin("wc", &dash_args, &text),
            &format!("wc dash {:?}", dash_args),
        );
    }

    for grep_args in [vec!["needle"], vec!["-n", "needle"]] {
        let mut sys_args = vec!["-F"];
        sys_args.extend(grep_args.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &grep_args, &text),
            run_system_with_stdin("grep", &sys_args, &text),
            &format!("fgrep stdin {:?}", grep_args),
        );

        let mut dash_args = grep_args.clone();
        dash_args.push("-");
        let mut sys_dash_args = vec!["-F"];
        sys_dash_args.extend(grep_args.iter().copied());
        sys_dash_args.push("-");
        assert_same_result(
            run_fro_with_stdin("fgrep", &dash_args, &text),
            run_system_with_stdin("grep", &sys_dash_args, &text),
            &format!("fgrep dash {:?}", dash_args),
        );
    }

    assert_same_result(
        run_fro_with_stdin("cksum", &[], &binary),
        run_system_with_stdin("cksum", &[], &binary),
        "cksum stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("cksum", &["-"], &binary),
        run_system_with_stdin("cksum", &["-"], &binary),
        "cksum dash",
    );
    assert_same_result(
        run_fro_with_stdin("sha256sum", &[], &binary),
        run_system_with_stdin("sha256sum", &[], &binary),
        "sha256sum stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("sha256sum", &["-"], &binary),
        run_system_with_stdin("sha256sum", &["-"], &binary),
        "sha256sum dash",
    );
}
