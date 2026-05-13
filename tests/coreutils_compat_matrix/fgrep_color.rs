use super::*;

fn write_fgrep_color_fixture(fixture: &CoreutilsParityFixture) -> (String, String, String) {
    let primary = fixture.root.join("color-primary.txt");
    let secondary = fixture.root.join("color-secondary.txt");
    let binary = fixture.root.join("color-binary.bin");
    fs::write(
        &primary,
        b"one\nneedle alpha needle\ntwo\nneedle beta\nfive\n",
    )
    .unwrap();
    fs::write(&secondary, b"alpha\nneedle gamma\nomega\n").unwrap();
    fs::write(&binary, b"alpha\0needle\nomega\n").unwrap();
    (
        primary.to_string_lossy().into_owned(),
        secondary.to_string_lossy().into_owned(),
        binary.to_string_lossy().into_owned(),
    )
}

#[test]
fn fgrep_color_flags_match_system_for_files() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-color-files");
    let (primary, secondary, binary) = write_fgrep_color_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["--color=always", "needle", primary.as_str()],
            vec!["--color=never", "needle", primary.as_str()],
            vec!["--color=auto", "needle", primary.as_str()],
            vec!["--color", "needle", primary.as_str()],
            vec!["--colour=always", "needle", primary.as_str()],
            vec!["--colour=never", "needle", primary.as_str()],
            vec!["--color=always", "-n", "needle", primary.as_str()],
            vec!["--color=always", "-b", "needle", primary.as_str()],
            vec!["--color=always", "-o", "needle", primary.as_str()],
            vec!["--color=always", "-o", "-n", "needle", primary.as_str()],
            vec!["--color=always", "-A1", "-n", "needle", primary.as_str()],
            vec!["--color=always", "-B1", "-n", "needle", primary.as_str()],
            vec!["--color=always", "-C1", "-n", "needle", primary.as_str()],
            vec!["--color=always", "-H", "needle", primary.as_str(), secondary.as_str()],
            vec!["--color=always", "-h", "needle", primary.as_str(), secondary.as_str()],
            vec!["--color=always", "-c", "needle", primary.as_str()],
            vec!["--color=always", "-q", "needle", primary.as_str()],
            vec!["--color=always", "-l", "needle", primary.as_str(), secondary.as_str()],
            vec!["--color=always", "-L", "missing-xyz", primary.as_str(), secondary.as_str()],
            vec!["--color=always", "-m", "1", "needle", primary.as_str()],
            vec!["--color=always", "-v", "needle", primary.as_str()],
            vec!["--color=always", "-I", "needle", binary.as_str()],
            vec!["--color=always", "-U", "needle", binary.as_str()],
            vec!["--color=always", "-s", "needle", binary.as_str()],
            vec!["--color=always", "--binary-files=text", "needle", binary.as_str()],
            vec!["--color=always", "--binary-files=without-match", "needle", binary.as_str()],
            vec!["--colour=always", "-n", "needle", secondary.as_str()],
            vec!["--colour=always", "-o", "needle", secondary.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep color files {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_color_flags_match_system_for_stdin() {
    let stdin_payload = b"one\nneedle alpha needle\ntwo\nneedle beta\nfive\n";

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["--color=always", "-n", "needle", "-"],
            vec!["--color=always", "-o", "needle", "-"],
            vec!["--color=always", "-o", "-n", "needle", "-"],
            vec!["--color=always", "-C1", "-n", "needle", "-"],
            vec!["--color=always", "--label=stdin-label", "-H", "needle", "-"],
            vec!["--color=always", "--label=stdin-label", "-h", "needle", "-"],
            vec!["--color=always", "-q", "needle", "-"],
            vec!["--color=never", "-n", "needle", "-"],
            vec!["--color=auto", "-n", "needle", "-"],
            vec!["--colour=always", "-n", "needle", "-"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!("fgrep color stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
