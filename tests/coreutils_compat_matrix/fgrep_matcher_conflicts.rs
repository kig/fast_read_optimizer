#![cfg(unix)]

use super::*;

#[test]
fn fgrep_conflicting_matcher_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-matcher-conflicts");
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in [
            "-E",
            "--extended-regexp",
            "-G",
            "--basic-regexp",
            "-P",
            "--perl-regexp",
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend([compat_flag, "needle", file]);
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &["-F", compat_flag, "needle", file]),
                &format!("fgrep matcher conflict {compat_flag} {:?}", io_flags),
            );

            let mut fro_no_fallback_args = vec!["--no-fallback"];
            fro_no_fallback_args.extend(io_flags.iter().copied());
            fro_no_fallback_args.extend([compat_flag, "needle", file]);
            assert_same_result(
                run_fro("fgrep", &fro_no_fallback_args),
                run_system("grep", &["-F", compat_flag, "needle", file]),
                &format!(
                    "fgrep matcher conflict --no-fallback {compat_flag} {:?}",
                    io_flags
                ),
            );
        }
    }
}
