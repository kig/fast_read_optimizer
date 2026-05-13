use super::*;
use std::io::{BufRead, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

// ════════════════════════════════════════════════════════════════════
// §1  fgrep: covered flags not exercised by earlier test files
//
// All comparisons use `grep -F` as the system oracle so the tests are
// robust on hosts where `/usr/bin/fgrep` has been deprecated.
// ════════════════════════════════════════════════════════════════════

fn spawn_fro_fgrep(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg("fgrep")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro fgrep")
}

fn spawn_system_grep(args: &[&str]) -> Child {
    system_command("grep")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn system grep")
}

fn write_fgrep_binary_fixture(fixture: &CoreutilsParityFixture) -> (String, String, String) {
    let binary_match = fixture.root.join("binary-match.bin");
    let binary_nomatch = fixture.root.join("binary-nomatch.bin");
    let text_match = fixture.root.join("binary-text.txt");
    fs::write(&binary_match, b"alpha\0needle\nomega\n").unwrap();
    fs::write(&binary_nomatch, b"alpha\0beta\nomega\n").unwrap();
    fs::write(&text_match, b"needle\nplain text\n").unwrap();
    (
        binary_match.to_string_lossy().into_owned(),
        binary_nomatch.to_string_lossy().into_owned(),
        text_match.to_string_lossy().into_owned(),
    )
}

fn observe_line_buffered_stream_output(
    spawn: impl FnOnce() -> Child,
    stdin_bytes: &[u8],
) -> (Option<Vec<u8>>, Output) {
    let mut child = spawn();
    let mut stdin = child.stdin.take().expect("missing child stdin");
    let stdout = child.stdout.take().expect("missing child stdout");
    let mut stderr = child.stderr.take().expect("missing child stderr");
    let (tx, rx) = mpsc::channel();
    let stdout_reader = std::thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut reader = std::io::BufReader::new(stdout);
        let mut first_line = Vec::new();
        let read = reader.read_until(b'\n', &mut first_line)?;
        let _ = tx.send((read != 0).then_some(first_line.clone()));
        let mut output = first_line;
        reader.read_to_end(&mut output)?;
        Ok(output)
    });

    stdin
        .write_all(stdin_bytes)
        .expect("failed to write child stdin");
    let early_line = rx.recv_timeout(Duration::from_millis(400)).ok().flatten();
    drop(stdin);

    let mut stderr_bytes = Vec::new();
    stderr
        .read_to_end(&mut stderr_bytes)
        .expect("failed to read child stderr");
    let status = child.wait().expect("failed to wait for child");
    let stdout_bytes = stdout_reader
        .join()
        .expect("stdout reader panicked")
        .unwrap();
    (
        early_line,
        Output {
            status,
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        },
    )
}

#[test]
fn fgrep_fixed_strings_flag_matches_system() {
    // -F/--fixed-strings is the defining flag of fgrep; passing it explicitly
    // must still yield byte-identical output to `grep -F`.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-fixed-strings");
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for (fgrep_flag, needle) in [
            ("-F", "needle beta"),
            ("--fixed-strings", "needle beta"),
            ("-F", "missing-xyz"),
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend([fgrep_flag, needle, file]);
            let sys_args = ["-F", needle, file];
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep {fgrep_flag:?} {needle:?} {:?}", io_flags),
            );
        }
    }
}

#[test]
fn fgrep_ignore_case_flags_match_system() {
    // -i/--ignore-case and its explicit negation --no-ignore-case.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-icase");
    let mixed = fixture.root.join("mixed.txt");
    fs::write(&mixed, b"Alpha\nBETA\nneedle beta\nOMEGA\n").unwrap();
    let path = mixed.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-i", "--ignore-case"] {
            for (needle, files) in [
                ("alpha", vec![path]),
                ("NEEDLE", vec![path]),
                ("beta", vec![path, nested]),
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.push(compat_flag);
                fro_args.push(needle);
                fro_args.extend(files.iter().copied());

                let mut sys_args = vec!["-F", compat_flag, needle];
                sys_args.extend(files.iter().copied());

                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep {compat_flag} {needle:?} {:?}", io_flags),
                );
            }
        }

        // --no-ignore-case resets case-sensitivity (GNU grep extension)
        for compat_flag in ["--no-ignore-case"] {
            let mut fro_args = io_flags.clone();
            fro_args.extend([compat_flag, "alpha", path]);
            let sys_args = ["-F", compat_flag, "alpha", path];
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep {compat_flag} {:?}", io_flags),
            );
        }

        // -i followed by --no-ignore-case cancels case-insensitivity
        {
            let mut fro_args = io_flags.clone();
            fro_args.extend(["-i", "--no-ignore-case", "alpha", path]);
            let sys_args = ["-F", "-i", "--no-ignore-case", "alpha", path];
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep -i --no-ignore-case {:?}", io_flags),
            );
        }
    }
}

#[test]
fn fgrep_count_flag_matches_system() {
    // -c/--count: print only the count of matching lines per file.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-count");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-c", "--count"] {
            for (needle, files) in [
                ("needle", vec![file]),
                ("missing-xyz", vec![file]),
                ("needle", vec![file, nested]),
                ("alpha", vec![file, nested]),
            ] {
                let mut fro_args = io_flags.clone();
                fro_args.push(compat_flag);
                fro_args.push(needle);
                fro_args.extend(files.iter().copied());

                let mut sys_args = vec!["-F", compat_flag, needle];
                sys_args.extend(files.iter().copied());

                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep {compat_flag} {needle:?} {:?}", io_flags),
                );
            }
        }
    }
}

#[test]
fn fgrep_quiet_flag_matches_system() {
    // -q/--quiet/--silent: suppress output; exit 0 if any match, 1 otherwise.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-quiet");
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-q", "--quiet", "--silent"] {
            for needle in ["needle", "missing-xyz"] {
                let mut fro_args = io_flags.clone();
                fro_args.extend([compat_flag, needle, file]);
                let sys_args = ["-F", compat_flag, needle, file];
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep {compat_flag} {needle:?} {:?}", io_flags),
                );
            }
        }
    }
}

#[test]
fn fgrep_files_with_matches_flag_matches_system() {
    // -l/--files-with-matches: print only names of files containing a match.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-files-with");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-l", "--files-with-matches"] {
            for needle in ["needle", "missing-xyz"] {
                let mut fro_args = io_flags.clone();
                fro_args.extend([compat_flag, needle, file, nested]);
                let sys_args = ["-F", compat_flag, needle, file, nested];
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep {compat_flag} {needle:?} {:?}", io_flags),
                );
            }
        }
    }
}

#[test]
fn fgrep_files_without_match_flag_matches_system() {
    // -L/--files-without-match: print only names of files with NO match.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-files-without");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-L", "--files-without-match"] {
            for needle in ["needle", "missing-xyz"] {
                let mut fro_args = io_flags.clone();
                fro_args.extend([compat_flag, needle, file, nested]);
                let sys_args = ["-F", compat_flag, needle, file, nested];
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep {compat_flag} {needle:?} {:?}", io_flags),
                );
            }
        }
    }
}

#[test]
fn fgrep_with_filename_flag_matches_system() {
    // -H/--with-filename: always prefix matched lines with the filename, even
    // for a single-file search where it is normally suppressed.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-with-filename");
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-H", "--with-filename"] {
            let mut fro_args = io_flags.clone();
            fro_args.extend([compat_flag, "needle", file]);
            let sys_args = ["-F", compat_flag, "needle", file];
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep {compat_flag} {:?}", io_flags),
            );
        }
    }
}

#[test]
fn fgrep_no_filename_flag_matches_system() {
    // -h/--no-filename: suppress filename prefix in multi-file output.
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-no-filename");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flag in ["-h", "--no-filename"] {
            let mut fro_args = io_flags.clone();
            fro_args.extend([compat_flag, "needle", file, nested]);
            let sys_args = ["-F", compat_flag, "needle", file, nested];
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep {compat_flag} {:?}", io_flags),
            );
        }
    }
}

#[test]
fn fgrep_help_and_version_surface_stay_wired() {
    let help = run_fro("fgrep", &["--help"]);
    assert_eq!(
        help.status.code(),
        Some(0),
        "fgrep --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&help.stdout),
        String::from_utf8_lossy(&help.stderr),
    );
    assert!(
        help.stderr.is_empty(),
        "fgrep --help wrote to stderr:\n{}",
        String::from_utf8_lossy(&help.stderr),
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("--help"),
        "fgrep --help output should mention --help:\n{stdout}"
    );
    assert!(
        stdout.contains("--version"),
        "fgrep --help output should mention --version:\n{stdout}"
    );

    let version = run_fro("fgrep", &["--version"]);
    assert_eq!(
        version.status.code(),
        Some(0),
        "fgrep --version failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr),
    );
    assert!(
        version.stderr.is_empty(),
        "fgrep --version wrote to stderr:\n{}",
        String::from_utf8_lossy(&version.stderr),
    );
    let ver_stdout = String::from_utf8_lossy(&version.stdout);
    // fro multicall prints something like "fgrep (fro) X.Y.Z" or "grep (fro) X.Y.Z"
    assert!(
        ver_stdout.contains("fgrep") || ver_stdout.contains("grep"),
        "fgrep --version output should mention the command name:\n{ver_stdout}"
    );
}

#[test]
fn fgrep_max_count_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-max-count");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-m", "1", "needle", file],
            vec!["-m1", "needle", file],
            vec!["--max-count=2", "needle", file],
            vec!["-m", "2", "-n", "needle", file],
            vec!["-m", "2", "-c", "needle", file],
            vec!["-m", "1", "-v", "needle", file],
            vec!["-m", "1", "-l", "needle", file, nested],
            vec!["-m", "1", "-L", "needle", file, nested],
            vec!["-m", "1", "-q", "needle", file, nested],
            vec!["-m", "0", "needle", file],
            vec!["-m", "0", "-c", "needle", file],
            vec!["-m", "1", "-H", "needle", file, nested],
            vec!["-m", "1", "-h", "needle", file, nested],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep max-count {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_max_count_stream_inputs_match_system() {
    let stdin_payload = b"needle\nneedle beta\nomega\n";
    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-m", "1", "needle", "-"],
            vec!["-m", "1", "-n", "needle", "-"],
            vec!["-m", "1", "-v", "needle", "-"],
            vec!["-m", "1", "-q", "needle", "-"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("fgrep", &fro_args, stdin_payload),
                run_system_with_stdin("grep", &sys_args, stdin_payload),
                &format!("fgrep max-count stdin {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-max-count-fifo");
    let fifo = fixture.root.join("input.fifo");
    let payload = b"alpha\nneedle\nneedle\nomega\n";
    for io_flags in io_flag_sets() {
        let fro = with_fifo_input(&fifo, payload, |fifo_path| {
            let mut args = io_flags.clone();
            args.extend(["-m", "1", "needle", fifo_path]);
            run_fro("fgrep", &args)
        });
        let sys = with_fifo_input(&fifo, payload, |fifo_path| {
            run_system("grep", &["-F", "-m", "1", "needle", fifo_path])
        });
        assert_same_result(fro, sys, &format!("fgrep max-count fifo {:?}", io_flags));
    }
}

#[test]
fn fgrep_no_messages_matches_system_for_input_errors() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-no-messages");
    let readable = fixture.text_file.to_str().unwrap();
    let missing = fixture.root.join("missing.txt");
    let unreadable = fixture.root.join("unreadable.txt");
    fs::write(&unreadable, b"needle beta\n").unwrap();
    let original_mode = fs::metadata(&unreadable).unwrap().permissions().mode();
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0)).unwrap();
    let unreadable_path = unreadable.to_str().unwrap();
    let missing_path = missing.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-s", "needle", missing_path],
            vec!["-s", "needle", unreadable_path],
            vec!["-s", "needle", missing_path, readable],
            vec!["-s", "-m", "1", "needle", missing_path, readable],
            vec!["-s", "-c", "needle", missing_path, readable],
            vec!["-q", "-s", "needle", missing_path, readable],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep no-messages {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    fs::set_permissions(&unreadable, fs::Permissions::from_mode(original_mode)).unwrap();
}

#[test]
fn fgrep_byte_offset_flag_matches_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-byte-offset");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-b", "needle", file],
            vec!["--byte-offset", "needle", file],
            vec!["-b", "-n", "needle", file],
            vec!["-b", "needle", file, nested],
            vec!["-b", "-H", "needle", file],
            vec!["-b", "-h", "needle", file, nested],
            vec!["-b", "-l", "needle", file, nested],
            vec!["-b", "-L", "missing-xyz", file, nested],
            vec!["-b", "-c", "needle", file, nested],
            vec!["-b", "-q", "needle", file, nested],
            vec!["-b", "-m", "1", "needle", file],
            vec!["-b", "-i", "alpha", file],
            vec!["-b", "-x", "needle beta", file],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep byte-offset {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    for surface in stream_surfaces() {
        let compat_flags = surface.args(&["-b", "-H", "needle"]);
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &compat_flags, b"needle beta\nomega\n"),
            run_system_with_stdin("grep", &sys_args, b"needle beta\nomega\n"),
            &format!("fgrep byte-offset {} -H", surface.label()),
        );
    }
}

#[test]
fn fgrep_null_flag_matches_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-null");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-Z", "needle", file, nested],
            vec!["--null", "needle", file, nested],
            vec!["-Z", "-H", "needle", file],
            vec!["-Z", "-h", "needle", file, nested],
            vec!["-Z", "-n", "needle", file, nested],
            vec!["-Z", "-b", "-n", "needle", file, nested],
            vec!["-Z", "-c", "needle", file, nested],
            vec!["-Z", "-l", "needle", file, nested],
            vec!["-Z", "-L", "missing-xyz", file, nested],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep null {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    for surface in stream_surfaces() {
        let compat_flags = surface.args(&["-Z", "-H", "needle"]);
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &compat_flags, b"needle\nomega\n"),
            run_system_with_stdin("grep", &sys_args, b"needle\nomega\n"),
            &format!("fgrep null {} -H", surface.label()),
        );
    }
}

#[test]
fn fgrep_initial_tab_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-initial-tab");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-T", "needle", file],
            vec!["--initial-tab", "-n", "needle", file],
            vec!["-T", "-H", "needle", file],
            vec!["-T", "-H", "-n", "needle", file],
            vec!["-T", "-H", "-b", "-n", "needle", file],
            vec!["-T", "-h", "-n", "needle", file, nested],
            vec!["-T", "-Z", "-H", "needle", file],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep initial-tab {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    for surface in stream_surfaces() {
        let compat_flags = surface.args(&["-T", "-H", "-n", "needle"]);
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &compat_flags, b"needle beta\nomega\n"),
            run_system_with_stdin("grep", &sys_args, b"needle beta\nomega\n"),
            &format!("fgrep initial-tab {} -H -n", surface.label()),
        );
    }
}

#[test]
fn fgrep_line_buffered_flag_matches_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-line-buffered");
    let file = fixture.text_file.to_str().unwrap();
    let nested = fixture.nested_text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["--line-buffered", "needle", file],
            vec!["--line-buffered", "-n", "needle", file],
            vec!["--line-buffered", "-T", "-H", "-n", "needle", file],
            vec!["--line-buffered", "-c", "needle", file, nested],
            vec!["--line-buffered", "-l", "needle", file, nested],
            vec!["--line-buffered", "-L", "missing-xyz", file, nested],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep line-buffered {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_line_buffered_flushes_like_system_for_streaming_output() {
    for compat_flags in [
        vec!["--line-buffered", "needle", "-"],
        vec!["--line-buffered", "-l", "needle", "-"],
    ] {
        let (fro_early, fro_output) =
            observe_line_buffered_stream_output(|| spawn_fro_fgrep(&compat_flags), b"needle\n");
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        let (sys_early, sys_output) =
            observe_line_buffered_stream_output(|| spawn_system_grep(&sys_args), b"needle\n");
        assert_eq!(
            fro_early, sys_early,
            "fgrep line-buffered streaming flush mismatch for {:?}",
            compat_flags
        );
        assert_same_result(
            fro_output,
            sys_output,
            &format!("fgrep line-buffered streaming {:?}", compat_flags),
        );
    }
}

#[test]
fn fgrep_binary_mode_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-binary-flags");
    let (binary_match, binary_nomatch, text_match) = write_fgrep_binary_fixture(&fixture);
    let binary_match = binary_match.as_str();
    let binary_nomatch = binary_nomatch.as_str();
    let text_match = text_match.as_str();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["needle", binary_match],
            vec!["--binary-files=binary", "needle", binary_match],
            vec!["-U", "needle", binary_match],
            vec!["-a", "needle", binary_match],
            vec!["--text", "needle", binary_match],
            vec!["--binary-files=text", "needle", binary_match],
            vec!["-I", "needle", binary_match],
            vec!["--binary-files=without-match", "needle", binary_match],
            vec!["-s", "needle", binary_match],
            vec!["-c", "needle", binary_match],
            vec!["-q", "needle", binary_match],
            vec!["-v", "needle", binary_match],
            vec!["-H", "needle", binary_match, text_match],
            vec!["-l", "needle", binary_match, text_match],
            vec!["-L", "needle", binary_nomatch, text_match],
            vec!["-l", "-I", "needle", binary_match, text_match],
            vec!["-L", "-I", "needle", binary_match, text_match],
            vec!["-c", "-I", "needle", binary_match, text_match],
            vec!["--binary-files=without-match", "-a", "needle", binary_match],
            vec!["-a", "-I", "needle", binary_match],
            vec!["-U", "-a", "needle", binary_match],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep binary {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_binary_mode_flags_match_system_on_stdin() {
    let stdin_payload = b"alpha\0needle\nomega\n";
    for compat_flags in [
        vec!["needle", "-"],
        vec!["-a", "needle", "-"],
        vec!["--binary-files=text", "needle", "-"],
        vec!["-I", "needle", "-"],
        vec!["--binary-files=without-match", "needle", "-"],
        vec!["-U", "needle", "-"],
        vec!["-H", "needle", "-"],
        vec!["-c", "needle", "-"],
        vec!["-q", "-I", "needle", "-"],
    ] {
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &compat_flags, stdin_payload),
            run_system_with_stdin("grep", &sys_args, stdin_payload),
            &format!("fgrep binary stdin {:?}", compat_flags),
        );
    }
}

#[test]
fn fgrep_devices_flags_match_system() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-devices");
    let fifo = fixture.root.join("input.fifo");
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for (compat_flags, payload) in [
            (vec!["needle", file], b"needle\n".as_slice()),
            (vec!["-D", "read", "needle", file], b"needle\n".as_slice()),
            (vec!["--devices=read", "-c", "needle", file], b"needle\n".as_slice()),
            (vec!["-Dread", "-l", "needle", file], b"needle\n".as_slice()),
            (vec!["-D", "read", "-L", "needle", file], b"miss\n".as_slice()),
            (vec!["-q", "-D", "read", "needle", file], b"needle\n".as_slice()),
        ] {
            let fro = with_fifo_input(&fifo, payload, |fifo_path| {
                let mut args = io_flags.clone();
                args.extend(compat_flags.iter().copied());
                args.insert(args.len() - 1, fifo_path);
                run_fro("fgrep", &args)
            });
            let sys = with_fifo_input(&fifo, payload, |fifo_path| {
                let mut args = vec!["-F"];
                args.extend(compat_flags.iter().copied());
                args.insert(args.len() - 1, fifo_path);
                run_system("grep", &args)
            });
            assert_same_result(
                fro,
                sys,
                &format!("fgrep devices read {:?} {:?}", io_flags, compat_flags),
            );
        }

        make_fifo(&fifo);
        for compat_flags in [
            vec!["-D", "skip", "needle", file],
            vec!["--devices=skip", "-c", "needle", file],
            vec!["-Dskip", "-l", "needle", file],
            vec!["-D", "skip", "-L", "needle", file],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.insert(fro_args.len() - 1, fifo.to_str().unwrap());

            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            sys_args.insert(sys_args.len() - 1, fifo.to_str().unwrap());

            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep devices skip {:?} {:?}", io_flags, compat_flags),
            );
        }
        fs::remove_file(&fifo).unwrap();
    }
}

#[test]
fn fgrep_directories_flags_match_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-directories");
    let dir = fixture.tree_root.to_str().unwrap();
    let nested_dir = fixture.nested_dir.to_str().unwrap();
    let file = fixture.text_file.to_str().unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["needle", dir, file],
            vec!["-d", "read", "-s", "needle", dir, file],
            vec!["--directories=read", "-c", "needle", dir, file],
            vec!["-dread", "-L", "needle", dir, file],
            vec!["-q", "needle", dir, file],
            vec!["-d", "skip", "needle", dir, file],
            vec!["--directories=skip", "-c", "needle", dir, file],
            vec!["-dskip", "-l", "needle", dir, file],
            vec!["-d", "skip", "-L", "needle", dir, file],
            vec!["-d", "skip", "-H", "needle", nested_dir, file],
            vec!["-d", "skip", "-h", "needle", dir, file],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            let mut sys_args = vec!["-F"];
            sys_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro("fgrep", &fro_args),
                run_system("grep", &sys_args),
                &format!("fgrep directories {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn fgrep_label_flag_matches_system() {
    let fixture = CoreutilsParityFixture::new("fro-fp-fgrep-label");
    let file = fixture.text_file.to_str().unwrap();

    for compat_flags in [
        vec!["--label=stdin-label", "needle"],
        vec!["--label=stdin-label", "-H", "needle"],
        vec!["--label=stdin-label", "-h", "needle", "-", file],
        vec!["--label=stdin-label", "needle", "-", file],
        vec!["--label=stdin-label", "-c", "needle", "-", file],
        vec!["--label=stdin-label", "-l", "needle", "-", file],
        vec!["--label=stdin-label", "-L", "needle", "-", file],
        vec!["--label=stdin-label", "-q", "needle", "-", file],
        vec!["--label=stdin-label", "-H", "-c", "needle"],
        vec!["--label=stdin-label", "-H", "needle", file],
    ] {
        let mut sys_args = vec!["-F"];
        sys_args.extend(compat_flags.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &compat_flags, b"needle\n"),
            run_system_with_stdin("grep", &sys_args, b"needle\n"),
            &format!("fgrep label {:?}", compat_flags),
        );
    }

    let mut sys_no_match = vec!["-F"];
    let compat_no_match = vec!["--label=stdin-label", "-L", "needle"];
    sys_no_match.extend(compat_no_match.iter().copied());
    assert_same_result(
        run_fro_with_stdin("fgrep", &compat_no_match, b"miss\n"),
        run_system_with_stdin("grep", &sys_no_match, b"miss\n"),
        "fgrep label no-match -L",
    );
}

#[test]
fn fgrep_short_version_flag_matches_system_exit_surface() {
    let fro = run_fro("fgrep", &["-V"]);
    let system = run_system("grep", &["-F", "-V"]);
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "fgrep -V status mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
    assert!(
        fro.stderr.is_empty() && system.stderr.is_empty(),
        "fgrep -V should keep stderr empty\nfro stderr:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stderr),
    );
    assert!(
        !fro.stdout.is_empty() && !system.stdout.is_empty(),
        "fgrep -V should print a version surface"
    );
}

// ════════════════════════════════════════════════════════════════════
// §3  fgrep: TODO stubs for remaining system-only flags (16 items)
//
// Each stub corresponds to one entry in the `remaining` slice of the
// fgrep CoverageRow in src/help_compat.rs.  When a flag is natively
// implemented, move its test to §1 or cmp_fgrep.rs and remove the stub.
// ════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "TODO: fgrep --directories=recurse not yet implemented in fro literal-search slice"]
fn fgrep_directories_recurse_todo() {}

#[test]
#[ignore = "TODO: fgrep -E/--extended-regexp not yet implemented in fro literal-search slice (regex is out of scope for fgrep)"]
fn fgrep_extended_regexp_todo() {}

#[test]
#[ignore = "TODO: fgrep -G/--basic-regexp not yet implemented in fro literal-search slice (regex is out of scope for fgrep)"]
fn fgrep_basic_regexp_todo() {}

#[test]
#[ignore = "TODO: fgrep -P/--perl-regexp not yet implemented in fro literal-search slice (regex is out of scope for fgrep)"]
fn fgrep_perl_regexp_todo() {}

#[test]
#[ignore = "TODO: fgrep -r/--recursive directory search not yet implemented in fro literal-search slice"]
fn fgrep_recursive_todo() {}

#[test]
#[ignore = "TODO: fgrep -R/--dereference-recursive not yet implemented in fro literal-search slice"]
fn fgrep_dereference_recursive_todo() {}

#[test]
#[ignore = "TODO: fgrep --include=GLOB not yet implemented in fro literal-search slice"]
fn fgrep_include_glob_todo() {}

#[test]
#[ignore = "TODO: fgrep --exclude=GLOB not yet implemented in fro literal-search slice"]
fn fgrep_exclude_glob_todo() {}

#[test]
#[ignore = "TODO: fgrep --exclude-dir=GLOB not yet implemented in fro literal-search slice"]
fn fgrep_exclude_dir_todo() {}

#[test]
#[ignore = "TODO: fgrep --exclude-from=FILE not yet implemented in fro literal-search slice"]
fn fgrep_exclude_from_todo() {}

