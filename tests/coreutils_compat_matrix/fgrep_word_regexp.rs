use super::*;

fn write_fgrep_word_regexp_fixture(fixture: &CoreutilsParityFixture) -> String {
    let file = fixture.root.join("word-regexp.txt");
    fs::write(
        &file,
        b"needle alpha needle\nalpha-needle\nalpha_needle\nneedle beta\n\n---\n",
    )
    .unwrap();
    file.to_string_lossy().into_owned()
}

#[test]
fn fgrep_word_regexp_flags_match_system_for_files() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-word-regexp-files");
    let file = write_fgrep_word_regexp_fixture(&fixture);

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-w", "needle", file.as_str()],
            vec!["--word-regexp", "needle", file.as_str()],
            vec!["-w", "-n", "needle", file.as_str()],
            vec!["-w", "-i", "NEEDLE", file.as_str()],
            vec!["-w", "-c", "needle", file.as_str()],
            vec!["-w", "", file.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep word-regexp files {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_word_regexp_only_matching_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-word-regexp-only-matching");
    let file = write_fgrep_word_regexp_fixture(&fixture);
    let stdin_payload = b"needle alpha needle\nalpha-needle\nalpha_needle\nneedle beta\n\n---\n";

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-w", "-o", "needle", file.as_str()],
            vec!["--word-regexp", "-o", "needle", file.as_str()],
            vec!["-w", "-o", "-n", "needle", file.as_str()],
            vec!["-w", "-o", "", file.as_str()],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep word-regexp only-matching {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [
            vec!["-w", "-o", "needle", "-"],
            vec!["--word-regexp", "-o", "needle", "-"],
            vec!["-w", "-o", "-n", "needle", "-"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!("fgrep word-regexp stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
