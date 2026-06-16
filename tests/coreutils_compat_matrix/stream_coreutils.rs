use super::helpers::StreamSurface;
use super::*;

fn wc_stream_flag_sets() -> Vec<Vec<&'static str>> {
    wc_flag_sets()
        .into_iter()
        .filter(|flags| !flags.iter().any(|flag| *flag == "-L"))
        .collect()
}

#[test]
fn cartesian_stream_coreutils_match_system_for_input_kinds() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-stream-matrix");
    let text = fixture.text_bytes.clone();
    let binary = fixture.binary_bytes.clone();

    for flags in io_flag_sets() {
        for (kind, path) in fixture.text_path_inputs() {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cat", &args),
                run_system("cat", &[file]),
                &format!("cat {kind} path {:?}", args),
            );
            assert_same_result(
                run_fro("tac", &args),
                run_system("tac", &[file]),
                &format!("tac {kind} path {:?}", args),
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
                    &format!("head {kind} path {:?}", fro_args),
                );
            }
            for tail_args in [
                vec![],
                vec!["-n", "2"],
                vec!["-c", "5"],
                vec!["-c", "1KiB"],
                vec!["-c", "1MiB"],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(tail_args.iter().copied());
                fro_args.push(file);
                let mut sys_args = tail_args;
                sys_args.push(file);
                assert_same_result(
                    run_fro("tail", &fro_args),
                    run_system("tail", &sys_args),
                    &format!("tail {kind} path {:?}", fro_args),
                );
            }
            for wc_flags in wc_stream_flag_sets() {
                let mut fro_args = flags.clone();
                fro_args.extend(wc_flags.iter().copied());
                fro_args.push(file);
                let mut sys_args = wc_flags;
                sys_args.push(file);
                assert_same_wc(
                    run_fro("wc", &fro_args),
                    run_system("wc", &sys_args),
                    &format!("wc {kind} path {:?}", fro_args),
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
                    &format!("fgrep {kind} path {:?}", fro_args),
                );
            }
        }

        for (kind, path) in fixture.binary_path_inputs() {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cksum", &args),
                run_system("cksum", &[file]),
                &format!("cksum {kind} path {:?}", args),
            );
            assert_same_result(
                run_fro("sha256sum", &args),
                run_system("sha256sum", &[file]),
                &format!("sha256sum {kind} path {:?}", args),
            );
        }
    }

    for surface in stream_surfaces() {
        let args = surface.args(&[]);
        assert_same_result(
            run_fro_with_stdin("cat", &args, &text),
            run_system_with_stdin("cat", &args, &text),
            &format!("cat {}", surface.label()),
        );
        assert_same_result(
            run_fro_with_stdin("tac", &args, &text),
            run_system_with_stdin("tac", &args, &text),
            &format!("tac {}", surface.label()),
        );
    }
    for head_args in [
        vec![],
        vec!["-n", "2"],
        vec!["-c", "5"],
        vec!["-c", "1KiB"],
        vec!["-c", "1MiB"],
    ] {
        for surface in stream_surfaces() {
            let args = surface.args(&head_args);
            assert_same_result(
                run_fro_with_stdin("head", &args, &text),
                run_system_with_stdin("head", &args, &text),
                &format!("head {} {:?}", surface.label(), args),
            );
        }
    }
    for tail_args in [
        vec![],
        vec!["-n", "2"],
        vec!["-c", "5"],
        vec!["-c", "1KiB"],
        vec!["-c", "1MiB"],
    ] {
        for surface in stream_surfaces() {
            let args = surface.args(&tail_args);
            assert_same_result(
                run_fro_with_stdin("tail", &args, &text),
                run_system_with_stdin("tail", &args, &text),
                &format!("tail {} {:?}", surface.label(), args),
            );
        }
    }

    for wc_flags in wc_stream_flag_sets() {
        for surface in stream_surfaces() {
            let args = surface.args(&wc_flags);
            assert_same_wc(
                run_fro_with_stdin("wc", &args, &text),
                run_system_with_stdin("wc", &args, &text),
                &format!("wc {} {:?}", surface.label(), args),
            );
        }
    }

    for grep_args in [vec!["needle"], vec!["-n", "needle"]] {
        for surface in stream_surfaces() {
            let fro_args = surface.args(&grep_args);
            let mut sys_args = vec!["-F"];
            sys_args.extend(grep_args.iter().copied());
            if matches!(surface, StreamSurface::Dash) {
                sys_args.push("-");
            }
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, &text),
                run_system_with_stdin("grep", &sys_args, &text),
                &format!("fgrep {} {:?}", surface.label(), fro_args),
            );
        }
    }

    for surface in stream_surfaces() {
        let args = surface.args(&[]);
        assert_same_result(
            run_fro_with_stdin("cksum", &args, &binary),
            run_system_with_stdin("cksum", &args, &binary),
            &format!("cksum {}", surface.label()),
        );
        assert_same_result(
            run_fro_with_stdin("sha256sum", &args, &binary),
            run_system_with_stdin("sha256sum", &args, &binary),
            &format!("sha256sum {}", surface.label()),
        );
    }
}

#[test]
fn head_matches_system_for_negative_counts_and_header_controls() {
    let tmp = unique_temp_dir("fro-head-gnu-slice");
    let file_a = tmp.join("a.txt");
    let file_b = tmp.join("b.txt");
    fs::write(&file_a, b"zero\none\ntwo\nthree\n").unwrap();
    fs::write(&file_b, b"apple\nbanana\ncarrot\n").unwrap();

    let a = file_a.to_str().unwrap();
    let b = file_b.to_str().unwrap();

    for args in [
        vec!["-n", "-1", a],
        vec!["-n", "-2", a],
        vec!["--lines=-2", a],
        vec!["--lines", "2", a],
        vec!["-n-2", a],
        vec!["-2", a],
        vec!["-12", a],
        vec!["-c", "-1", a],
        vec!["-c", "-4", a],
        vec!["--bytes=-4", a],
        vec!["--bytes", "4", a],
        vec!["-c-2", a],
        vec!["-2c", a],
        vec!["-2b", a],
        vec!["-2k", a],
        vec!["-v", a],
        vec!["-q", a],
        vec!["--verbose", a, b],
        vec!["--quiet", a, b],
        vec!["--silent", a, b],
        vec!["-q", a, b],
        vec!["-v", a, b],
        vec!["-qv", a, b],
        vec!["-vq", a, b],
        vec!["-q", "-v", a],
        vec!["-v", "-q", a],
        vec!["-2q", a, b],
        vec!["-2v", a, b],
        vec!["-2qv", a],
        vec!["-2vq", a],
    ] {
        assert_same_result(
            run_fro("head", &args),
            run_system("head", &args),
            &format!("head parity {:?}", args),
        );
    }

    let stdin_text = b"alpha\nbeta\ngamma\ndelta\n";
    for args in [
        vec!["-n", "-1"],
        vec!["-n", "-2"],
        vec!["--lines=-2"],
        vec!["--lines", "2"],
        vec!["-n-2"],
        vec!["-2"],
        vec!["-c", "-1"],
        vec!["-c", "-5"],
        vec!["--bytes=-5"],
        vec!["--bytes", "5"],
        vec!["-c-2"],
        vec!["-2c"],
        vec!["-v"],
        vec!["-q"],
        vec!["--verbose"],
        vec!["--quiet"],
        vec!["-v", "-"],
        vec!["-q", "-"],
        vec!["-2q"],
        vec!["-2v"],
    ] {
        assert_same_result(
            run_fro_with_stdin("head", &args, stdin_text),
            run_system_with_stdin("head", &args, stdin_text),
            &format!("head stdin parity {:?}", args),
        );
    }
}

#[test]
fn tail_matches_system_for_positive_counts_and_header_controls() {
    let tmp = unique_temp_dir("fro-tail-gnu-slice");
    let file_a = tmp.join("a.txt");
    let file_b = tmp.join("b.txt");
    fs::write(&file_a, b"zero\none\ntwo\nthree\n").unwrap();
    fs::write(&file_b, b"apple\nbanana\ncarrot\n").unwrap();

    let a = file_a.to_str().unwrap();
    let b = file_b.to_str().unwrap();

    for args in [
        vec!["-n", "+1", a],
        vec!["-n", "+3", a],
        vec!["-n+2", a],
        vec!["-c", "+1", a],
        vec!["-c", "+4", a],
        vec!["-c+2", a],
        vec!["-v", a],
        vec!["-q", a],
        vec!["-q", a, b],
        vec!["-v", a, b],
        vec!["-qv", a, b],
        vec!["-vq", a, b],
        vec!["-q", "-v", a],
        vec!["-v", "-q", a],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail parity {:?}", args),
        );
    }

    let stdin_text = b"alpha\nbeta\ngamma\ndelta\n";
    for args in [
        vec!["-n", "+1"],
        vec!["-n", "+3"],
        vec!["-n+2"],
        vec!["-c", "+1"],
        vec!["-c", "+5"],
        vec!["-c+2"],
        vec!["-v"],
        vec!["-q"],
        vec!["-v", "-"],
        vec!["-q", "-"],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, stdin_text),
            run_system_with_stdin("tail", &args, stdin_text),
            &format!("tail stdin parity {:?}", args),
        );
    }
}

#[test]
fn tail_matches_system_for_large_stream_windows() {
    let line = "0123456789abcdef".repeat(256);
    let mut text = String::new();
    for idx in 0..8192 {
        text.push_str(&line);
        text.push_str(&format!("-{idx:04}\n"));
    }

    for args in [vec!["-n", "4096"], vec!["-c", "65536"]] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, text.as_bytes()),
            run_system_with_stdin("tail", &args, text.as_bytes()),
            &format!("tail large stdin parity {:?}", args),
        );
    }
}

#[test]
fn head_and_tail_zero_terminated_match_system_across_stream_surfaces() {
    let fixture = CoreutilsParityFixture::new("fro-head-tail-zero-terminated");
    let input = b"zero\0one\0two\0three".to_vec();
    let file = fixture.root.join("records.bin");
    fs::write(&file, &input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["-z", "-n", "2", path],
        vec!["--zero-terminated", "-n", "-1", path],
    ] {
        assert_same_result(
            run_fro("head", &args),
            run_system("head", &args),
            &format!("head zero-terminated path {:?}", args),
        );
    }

    for args in [
        vec!["-z", "-n", "2", path],
        vec!["--zero-terminated", "-n", "+2", path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail zero-terminated path {:?}", args),
        );
    }

    for surface in stream_surfaces() {
        for args in [vec!["-z", "-n", "2"], vec!["--zero-terminated", "-n", "-1"]] {
            let fro_args = surface.args(&args);
            let sys_args = surface.args(&args);
            assert_same_result(
                run_fro_with_stdin("head", &fro_args, &input),
                run_system_with_stdin("head", &sys_args, &input),
                &format!("head zero-terminated {} {:?}", surface.label(), fro_args),
            );
        }
        for args in [vec!["-z", "-n", "2"], vec!["--zero-terminated", "-n", "+2"]] {
            let fro_args = surface.args(&args);
            let sys_args = surface.args(&args);
            assert_same_result(
                run_fro_with_stdin("tail", &fro_args, &input),
                run_system_with_stdin("tail", &sys_args, &input),
                &format!("tail zero-terminated {} {:?}", surface.label(), fro_args),
            );
        }
    }
}
