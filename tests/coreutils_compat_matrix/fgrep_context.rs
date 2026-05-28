use super::*;

fn write_fgrep_context_fixture(fixture: &CoreutilsParityFixture) -> (String, String) {
    let primary = fixture.root.join("context-primary.txt");
    let secondary = fixture.root.join("context-secondary.txt");
    fs::write(
        &primary,
        b"one\nneedle alpha\nthree\nfour\nfive\nneedle beta\nseven\neight\nnine\nneedle gamma\nend\n",
    )
    .unwrap();
    fs::write(&secondary, b"alpha\nmissing\nneedle delta\nomega\n").unwrap();
    (
        primary.to_string_lossy().into_owned(),
        secondary.to_string_lossy().into_owned(),
    )
}

#[test]
fn fgrep_context_window_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-context");
    let (primary, secondary) = write_fgrep_context_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-A", "1", "-n", "needle", primary.as_str()],
            vec!["--after-context=1", "-n", "needle", primary.as_str()],
            vec!["-B1", "-b", "needle", primary.as_str()],
            vec!["--before-context=1", "-H", "needle", primary.as_str()],
            vec!["-C", "1", "-n", "needle", primary.as_str()],
            vec!["-C1", "-b", "-n", "needle", primary.as_str()],
            vec!["-1", "-n", "needle", primary.as_str()],
            vec!["-A1", "-H", "needle", primary.as_str(), secondary.as_str()],
            vec!["-h", "-C1", "needle", primary.as_str(), secondary.as_str()],
            vec!["-m", "1", "-A1", "-n", "needle", primary.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep context {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_context_count_quiet_and_listing_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-context-count");
    let (primary, secondary) = write_fgrep_context_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-A1", "-c", "needle", primary.as_str()],
            vec!["-B1", "-c", "needle", primary.as_str()],
            vec!["-C1", "-c", "needle", primary.as_str()],
            vec!["-1", "-c", "needle", primary.as_str()],
            vec!["-A1", "-q", "needle", primary.as_str()],
            vec!["-C1", "-l", "needle", primary.as_str(), secondary.as_str()],
            vec![
                "-C1",
                "-L",
                "missing-xyz",
                primary.as_str(),
                secondary.as_str(),
            ],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep context count {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_context_flags_match_system_on_stdin() {
    let stdin_payload =
        b"one\nneedle alpha\ntwo\nthree\nneedle beta\nfour\nfive\nneedle gamma\nsix\n";

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-A1", "-n", "needle", "-"],
            vec!["-B1", "--line-buffered", "needle", "-"],
            vec!["-C1", "-m", "2", "needle", "-"],
            vec!["-1", "-q", "needle", "-"],
            vec!["-A", "1", "-b", "needle", "-"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!("fgrep context stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_group_separator_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-group-separator");
    let (primary, secondary) = write_fgrep_context_fixture(&fixture);
    let stdin_payload =
        b"one\nneedle alpha\ntwo\nthree\nneedle beta\nfour\nfive\nneedle gamma\nsix\n";

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec![
                "-A1",
                "-n",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
            ],
            vec![
                "-C1",
                "-b",
                "-H",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
                secondary.as_str(),
            ],
            vec![
                "-B1",
                "-h",
                "--no-group-separator",
                "needle",
                primary.as_str(),
                secondary.as_str(),
            ],
            vec![
                "-C1",
                "-c",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
            ],
            vec![
                "-C1",
                "-q",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
            ],
            vec![
                "-C1",
                "-l",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
                secondary.as_str(),
            ],
            vec![
                "-C1",
                "-L",
                "--no-group-separator",
                "missing-xyz",
                primary.as_str(),
                secondary.as_str(),
            ],
            vec![
                "-m",
                "1",
                "-C1",
                "--group-separator=SEP",
                "needle",
                primary.as_str(),
            ],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep group-separator {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec![
                "--line-buffered",
                "-C1",
                "-n",
                "--group-separator=SEP",
                "needle",
                "-",
            ],
            vec![
                "--line-buffered",
                "-C1",
                "-h",
                "--no-group-separator",
                "needle",
                "-",
            ],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!(
                    "fgrep group-separator stdin {:?} {:?}",
                    io_flags, compat_flags
                ),
            );
        }
    }
}
