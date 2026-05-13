use super::*;

fn write_fgrep_only_matching_fixture(fixture: &CoreutilsParityFixture) -> (String, String, String) {
    let primary = fixture.root.join("only-matching-primary.txt");
    let secondary = fixture.root.join("only-matching-secondary.txt");
    let binary = fixture.root.join("only-matching-binary.bin");
    fs::write(
        &primary,
        b"one\nneedle alpha needle\ntwo\nthree\nneedle beta\nfour\n",
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
fn fgrep_only_matching_prefix_and_listing_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-only-matching-prefix");
    let (primary, secondary, _) = write_fgrep_only_matching_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-o", "needle", primary.as_str()],
            vec!["-o", "-n", "needle", primary.as_str()],
            vec!["-o", "-b", "needle", primary.as_str()],
            vec!["-o", "-H", "needle", primary.as_str(), secondary.as_str()],
            vec!["-o", "-h", "needle", primary.as_str(), secondary.as_str()],
            vec!["-o", "-Z", "needle", primary.as_str(), secondary.as_str()],
            vec!["-o", "-c", "needle", primary.as_str()],
            vec!["-o", "-l", "needle", primary.as_str(), secondary.as_str()],
            vec!["-o", "-L", "missing-xyz", primary.as_str(), secondary.as_str()],
            vec!["-o", "-q", "needle", primary.as_str()],
            vec!["-o", "-m", "1", "needle", primary.as_str()],
            vec!["-o", "-m", "1", "-n", "needle", primary.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep only-matching prefix {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_only_matching_context_and_invert_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-only-matching-context");
    let (primary, secondary, _) = write_fgrep_only_matching_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-o", "-A1", "needle", primary.as_str()],
            vec!["-o", "-B1", "needle", primary.as_str()],
            vec!["-o", "-C1", "needle", primary.as_str()],
            vec!["-o", "-A1", "-n", "needle", primary.as_str()],
            vec!["-o", "-C1", "-H", "needle", primary.as_str(), secondary.as_str()],
            vec!["-o", "-v", "needle", primary.as_str()],
            vec!["-o", "-v", "-A1", "needle", primary.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep only-matching context {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_only_matching_case_and_binary_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-only-matching-case");
    let (primary, _, binary) = write_fgrep_only_matching_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-o", "-i", "NEEDLE", primary.as_str()],
            vec!["-o", "-x", "needle alpha needle", primary.as_str()],
            vec!["-o", "-a", "needle", binary.as_str()],
            vec!["-o", "--binary-files=text", "needle", binary.as_str()],
            vec!["-o", "-I", "needle", binary.as_str()],
            vec!["-o", "--binary-files=without-match", "needle", binary.as_str()],
            vec!["-o", "-U", "needle", binary.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep only-matching binary {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_only_matching_stream_and_stdin_match_system() {
    let stdin_payload = b"needle alpha needle\nomega\n";

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-o", "needle", "-"],
            vec!["--line-buffered", "-o", "needle", "-"],
            vec!["-o", "-n", "needle", "-"],
            vec!["-o", "-b", "needle", "-"],
            vec!["-o", "-H", "needle", "-"],
            vec!["-o", "-Z", "needle", "-"],
            vec!["-o", "-m", "1", "needle", "-"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!("fgrep only-matching stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
