//! Per-flag real-behaviour tests for all currently tracked/native flags that were not
//! already exercised by earlier compat-matrix files, plus `#[ignore]` TODO stubs for
//! every remaining system-only flag in `fgrep` and `find`.
//!
//! Layout
//! ──────
//! 1. fgrep covered flags – real fro-vs-system comparisons (flags not in cmp_fgrep.rs)
//! 2. find `-print` explicit test (covered but previously only implicit)
//! 3. `#[ignore = "TODO"]` stubs for every remaining fgrep system-only flag (36 items)
//! 4. `#[ignore = "TODO"]` stubs for every remaining find system-only flag  (69 items)

use super::*;
use std::ffi::CStr;
use std::os::unix::fs::MetadataExt;

fn assert_find_same_sorted(root: &Path, extra_args: &[&str], label: &str) {
    let root_arg = root.to_str().unwrap();
    let mut fro_args = vec![root_arg];
    fro_args.extend_from_slice(extra_args);
    let mut sys_args = vec![root_arg];
    sys_args.extend_from_slice(extra_args);
    assert_same_sorted_lines(
        run_fro("find", &fro_args),
        run_system("find", &sys_args),
        label,
    );
}

fn set_file_times(path: &Path, atime_sec: i64, mtime_sec: i64) {
    let path_c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let times = [
        libc::timespec {
            tv_sec: atime_sec,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: mtime_sec,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(
        rc,
        0,
        "utimensat failed for {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

fn current_user_name() -> String {
    let uid = unsafe { libc::getuid() };
    let mut buf_len = 1024usize;
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getpwuid_r(
                uid,
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        assert_eq!(
            rc,
            0,
            "getpwuid_r failed: {}",
            std::io::Error::from_raw_os_error(rc)
        );
        let result = result.cast_const();
        assert!(!result.is_null(), "current uid {uid} has no passwd entry");
        return unsafe { CStr::from_ptr((*result).pw_name) }
            .to_str()
            .unwrap()
            .to_string();
    }
}

fn current_group_name() -> String {
    let gid = unsafe { libc::getgid() };
    let mut buf_len = 1024usize;
    loop {
        let mut grp = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getgrgid_r(
                gid,
                grp.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        assert_eq!(
            rc,
            0,
            "getgrgid_r failed: {}",
            std::io::Error::from_raw_os_error(rc)
        );
        let result = result.cast_const();
        assert!(!result.is_null(), "current gid {gid} has no group entry");
        return unsafe { CStr::from_ptr((*result).gr_name) }
            .to_str()
            .unwrap()
            .to_string();
    }
}

// ════════════════════════════════════════════════════════════════════
// §1  fgrep: covered flags not exercised by earlier test files
//
// All comparisons use `grep -F` as the system oracle so the tests are
// robust on hosts where `/usr/bin/fgrep` has been deprecated.
// ════════════════════════════════════════════════════════════════════

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

// ════════════════════════════════════════════════════════════════════
// §2  find: covered flags not exercised by earlier test files
// ════════════════════════════════════════════════════════════════════

#[test]
fn find_print_flag_explicit_matches_system() {
    // `-print` is the default action but must also be accepted as an explicit
    // predicate and produce identical output to the implicit form.
    let tmp = unique_temp_dir("fro-fp-find-print");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = run_fro("find", &[root.to_str().unwrap(), "-print"]);
    let system = run_system("find", &[root.to_str().unwrap(), "-print"]);
    assert_same_sorted_lines(fro, system, "find -print explicit");

    // -type f -print combined
    let fro2 = run_fro("find", &[root.to_str().unwrap(), "-type", "f", "-print"]);
    let system2 = run_system("find", &[root.to_str().unwrap(), "-type", "f", "-print"]);
    assert_same_sorted_lines(fro2, system2, "find -type f -print");
}

// ════════════════════════════════════════════════════════════════════
// §3  fgrep: TODO stubs for remaining system-only flags (36 items)
//
// Each stub corresponds to one entry in the `remaining` slice of the
// fgrep CoverageRow in src/help_compat.rs.  When a flag is natively
// implemented, move its test to §1 or cmp_fgrep.rs and remove the stub.
// ════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "TODO: fgrep -A/--after-context not yet implemented in fro literal-search slice"]
fn fgrep_after_context_todo() {}

#[test]
#[ignore = "TODO: fgrep -B/--before-context not yet implemented in fro literal-search slice"]
fn fgrep_before_context_todo() {}

#[test]
#[ignore = "TODO: fgrep -C/--context not yet implemented in fro literal-search slice"]
fn fgrep_context_todo() {}

#[test]
#[ignore = "TODO: fgrep -NUM (shorthand context count) not yet implemented in fro literal-search slice"]
fn fgrep_num_shorthand_context_todo() {}

#[test]
#[ignore = "TODO: fgrep -b/--byte-offset not yet implemented in fro literal-search slice"]
fn fgrep_byte_offset_todo() {}

#[test]
#[ignore = "TODO: fgrep --color/--colour not yet implemented in fro literal-search slice"]
fn fgrep_color_todo() {}

#[test]
#[ignore = "TODO: fgrep --colour alias not yet implemented in fro literal-search slice"]
fn fgrep_colour_alias_todo() {}

#[test]
#[ignore = "TODO: fgrep -D/--devices not yet implemented in fro literal-search slice"]
fn fgrep_devices_todo() {}

#[test]
#[ignore = "TODO: fgrep -d/--directories not yet implemented in fro literal-search slice"]
fn fgrep_directories_todo() {}

#[test]
#[ignore = "TODO: fgrep --directories=recurse not yet implemented in fro literal-search slice"]
fn fgrep_directories_recurse_todo() {}

#[test]
#[ignore = "TODO: fgrep --binary-files=TYPE not yet implemented in fro literal-search slice"]
fn fgrep_binary_files_type_todo() {}

#[test]
#[ignore = "TODO: fgrep --binary-files=text not yet implemented in fro literal-search slice"]
fn fgrep_binary_files_text_todo() {}

#[test]
#[ignore = "TODO: fgrep -I/--binary-files=without-match not yet implemented in fro literal-search slice"]
fn fgrep_binary_files_without_match_todo() {}

#[test]
#[ignore = "TODO: fgrep -U/--binary not yet implemented in fro literal-search slice"]
fn fgrep_binary_todo() {}

#[test]
#[ignore = "TODO: fgrep -a/--text not yet implemented in fro literal-search slice"]
fn fgrep_text_todo() {}

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
#[ignore = "TODO: fgrep --label=LABEL not yet implemented in fro literal-search slice"]
fn fgrep_label_todo() {}

#[test]
#[ignore = "TODO: fgrep --line-buffered not yet implemented in fro literal-search slice"]
fn fgrep_line_buffered_todo() {}

#[test]
#[ignore = "TODO: fgrep -m/--max-count not yet implemented in fro literal-search slice"]
fn fgrep_max_count_todo() {}

#[test]
#[ignore = "TODO: fgrep --group-separator not yet implemented in fro literal-search slice"]
fn fgrep_group_separator_todo() {}

#[test]
#[ignore = "TODO: fgrep --no-group-separator not yet implemented in fro literal-search slice"]
fn fgrep_no_group_separator_todo() {}

#[test]
#[ignore = "TODO: fgrep -o/--only-matching not yet implemented in fro literal-search slice"]
fn fgrep_only_matching_todo() {}

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

#[test]
#[ignore = "TODO: fgrep -T/--initial-tab not yet implemented in fro literal-search slice"]
fn fgrep_initial_tab_todo() {}

#[test]
#[ignore = "TODO: fgrep -V (version short flag) not yet implemented in fro literal-search slice"]
fn fgrep_version_short_todo() {}

#[test]
#[ignore = "TODO: fgrep -Z/--null (NUL-terminate filenames) not yet implemented in fro literal-search slice"]
fn fgrep_null_terminate_todo() {}

#[test]
#[ignore = "TODO: fgrep -w/--word-regexp not yet implemented in fro literal-search slice (word boundaries require regex)"]
fn fgrep_word_regexp_todo() {}

#[test]
#[ignore = "TODO: fgrep -z/--null-data (NUL-delimited records) not yet implemented in fro literal-search slice"]
fn fgrep_null_data_todo() {}

#[test]
#[ignore = "TODO: fgrep -s/--no-messages (suppress error messages) not yet implemented in fro literal-search slice"]
fn fgrep_no_messages_todo() {}

// ════════════════════════════════════════════════════════════════════
// §4  find: TODO stubs for remaining system-only flags (69 items)
//
// Each stub corresponds to one entry in the `remaining` slice of the
// find CoverageRow in src/help_compat.rs.  When a predicate is natively
// supported, move its test to §2 or find_cli.rs and remove the stub.
// ════════════════════════════════════════════════════════════════════

// ── Debug / optimisation level ──────────────────────────────────────

#[test]
#[ignore = "TODO: find -D (debug options) not yet implemented in fro bounded find slice"]
fn find_debug_todo() {}

#[test]
#[ignore = "TODO: find -Olevel (optimisation level) not yet implemented in fro bounded find slice"]
fn find_optimise_level_todo() {}

// ── Symlink-traversal top-level flags ───────────────────────────────

#[test]
#[ignore = "TODO: find -H (follow symlinks only for command-line args) not yet implemented in fro bounded find slice"]
fn find_h_symlink_todo() {}

#[test]
#[ignore = "TODO: find -L (follow all symlinks) not yet implemented in fro bounded find slice"]
fn find_l_symlink_todo() {}

#[test]
#[ignore = "TODO: find -N (ignore read errors on symlinks) not yet implemented in fro bounded find slice"]
fn find_n_symlink_todo() {}

#[test]
#[ignore = "TODO: find -P (never follow symlinks, default) not yet implemented as explicit flag in fro bounded find slice"]
fn find_p_symlink_todo() {}

// ── Boolean combinators ─────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -a / -and (explicit AND operator) not yet implemented in fro bounded find slice"]
fn find_and_todo() {}

#[test]
#[ignore = "TODO: find -o (OR operator) not yet implemented in fro bounded find slice"]
fn find_or_short_todo() {}

#[test]
#[ignore = "TODO: find -or (long OR operator) not yet implemented in fro bounded find slice"]
fn find_or_long_todo() {}

#[test]
#[ignore = "TODO: find -not (NOT operator) not yet implemented in fro bounded find slice"]
fn find_not_todo() {}

// ── Time-based predicates ───────────────────────────────────────────

#[test]
fn find_atime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-atime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-atime", "0"], "find -atime 0");
}

#[test]
fn find_amin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-amin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-amin", "-1"], "find -amin -1");
}

#[test]
fn find_anewer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-anewer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-anewer", reference.to_str().unwrap()],
        "find -anewer ref",
    );
}

#[test]
fn find_ctime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ctime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-ctime", "0"], "find -ctime 0");
}

#[test]
fn find_cmin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-cmin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-cmin", "-1"], "find -cmin -1");
}

#[test]
fn find_cnewer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-cnewer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-cnewer", reference.to_str().unwrap()],
        "find -cnewer ref",
    );
}

#[test]
fn find_mtime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mtime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-mtime", "0"], "find -mtime 0");
}

#[test]
fn find_mmin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mmin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-mmin", "-1"], "find -mmin -1");
}

#[test]
fn find_newer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-newer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-newer", reference.to_str().unwrap()],
        "find -newer ref",
    );
}

#[test]
#[ignore = "TODO: find -daystart not yet implemented in fro bounded find slice"]
fn find_daystart_todo() {}

#[test]
#[ignore = "TODO: find -used not yet implemented in fro bounded find slice"]
fn find_used_todo() {}

// ── Size / count predicates ─────────────────────────────────────────

#[test]
fn find_size_todo() {
    let tmp = unique_temp_dir("fro-fp-find-size");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("empty.txt"), b"").unwrap();
    fs::write(root.join("five.txt"), b"12345").unwrap();
    fs::write(root.join("six.txt"), b"123456").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-size", "+0c"], "find -size +0c");
    assert_find_same_sorted(&root, &["-type", "f", "-size", "5c"], "find -size 5c");
}

#[test]
fn find_links_todo() {
    let tmp = unique_temp_dir("fro-fp-find-links");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("single.txt"), b"single").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-links", "1"], "find -links 1");
}

#[test]
fn find_inum_todo() {
    let tmp = unique_temp_dir("fro-fp-find-inum");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let target = root.join("target.txt");
    fs::write(&target, b"target").unwrap();
    let inum = fs::metadata(&target).unwrap().ino().to_string();
    assert_find_same_sorted(&root, &["-inum", &inum], "find -inum");
}

// ── Ownership / permission predicates ───────────────────────────────

#[test]
fn find_perm_todo() {
    let tmp = unique_temp_dir("fro-fp-find-perm");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let exact = root.join("exact.txt");
    let other = root.join("other.txt");
    fs::write(&exact, b"exact").unwrap();
    fs::write(&other, b"other").unwrap();
    fs::set_permissions(&exact, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(&other, fs::Permissions::from_mode(0o755)).unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-perm", "0644"], "find -perm 0644");
}

#[test]
fn find_uid_todo() {
    let tmp = unique_temp_dir("fro-fp-find-uid");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let uid = unsafe { libc::getuid() }.to_string();
    assert_find_same_sorted(&root, &["-type", "f", "-uid", &uid], "find -uid");
}

#[test]
fn find_gid_todo() {
    let tmp = unique_temp_dir("fro-fp-find-gid");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let gid = unsafe { libc::getgid() }.to_string();
    assert_find_same_sorted(&root, &["-type", "f", "-gid", &gid], "find -gid");
}

#[test]
fn find_user_todo() {
    let tmp = unique_temp_dir("fro-fp-find-user");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let user = current_user_name();
    assert_find_same_sorted(&root, &["-type", "f", "-user", &user], "find -user");
}

#[test]
fn find_group_todo() {
    let tmp = unique_temp_dir("fro-fp-find-group");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let group = current_group_name();
    assert_find_same_sorted(&root, &["-type", "f", "-group", &group], "find -group");
}

#[test]
fn find_nouser_todo() {
    let tmp = unique_temp_dir("fro-fp-find-nouser");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-nouser"], "find -nouser");
}

#[test]
fn find_nogroup_todo() {
    let tmp = unique_temp_dir("fro-fp-find-nogroup");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-nogroup"], "find -nogroup");
}

#[test]
fn find_readable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-readable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("readable.txt"), b"readable").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-readable"], "find -readable");
}

#[test]
fn find_writable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-writable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("writable.txt"), b"writable").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-writable"], "find -writable");
}

#[test]
fn find_executable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-executable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let exec = root.join("exec.sh");
    let plain = root.join("plain.txt");
    fs::write(&exec, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::write(&plain, b"plain").unwrap();
    fs::set_permissions(&exec, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&plain, fs::Permissions::from_mode(0o644)).unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-executable"], "find -executable");
}

#[test]
fn find_empty_todo() {
    let tmp = unique_temp_dir("fro-fp-find-empty");
    let root = tmp.join("root");
    let empty_dir = root.join("empty-dir");
    let full_dir = root.join("full-dir");
    fs::create_dir_all(&empty_dir).unwrap();
    fs::create_dir_all(&full_dir).unwrap();
    fs::write(root.join("empty.txt"), b"").unwrap();
    fs::write(root.join("full.txt"), b"full").unwrap();
    fs::write(full_dir.join("nested.txt"), b"nested").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-empty"], "find -empty -type f");
    assert_find_same_sorted(&root, &["-type", "d", "-empty"], "find -empty -type d");
}

// ── Regex predicates ────────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -regex not yet implemented in fro bounded find slice"]
fn find_regex_todo() {}

#[test]
#[ignore = "TODO: find -iregex not yet implemented in fro bounded find slice"]
fn find_iregex_todo() {}

#[test]
#[ignore = "TODO: find -regextype not yet implemented in fro bounded find slice"]
fn find_regextype_todo() {}

#[test]
#[ignore = "TODO: find -lname not yet implemented in fro bounded find slice"]
fn find_lname_todo() {}

#[test]
#[ignore = "TODO: find -ilname not yet implemented in fro bounded find slice"]
fn find_ilname_todo() {}

#[test]
#[ignore = "TODO: find -wholename not yet implemented in fro bounded find slice"]
fn find_wholename_todo() {}

#[test]
#[ignore = "TODO: find -iwholename not yet implemented in fro bounded find slice"]
fn find_iwholename_todo() {}

// ── Filesystem / traversal control ──────────────────────────────────

#[test]
fn find_mindepth_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mindepth");
    let root = tmp.join("root");
    let level1 = root.join("a");
    let level2 = level1.join("b");
    fs::create_dir_all(&level2).unwrap();
    fs::write(level2.join("file.txt"), b"leaf").unwrap();
    assert_find_same_sorted(&root, &["-mindepth", "2"], "find -mindepth 2");
}

#[test]
#[ignore = "TODO: find -depth not yet implemented in fro bounded find slice"]
fn find_depth_todo() {}

#[test]
#[ignore = "TODO: find -mount / -xdev not yet implemented in fro bounded find slice"]
fn find_mount_xdev_todo() {}

#[test]
#[ignore = "TODO: find -xdev not yet implemented in fro bounded find slice"]
fn find_xdev_todo() {}

#[test]
#[ignore = "TODO: find -noleaf not yet implemented in fro bounded find slice"]
fn find_noleaf_todo() {}

#[test]
#[ignore = "TODO: find -follow not yet implemented in fro bounded find slice"]
fn find_follow_todo() {}

#[test]
#[ignore = "TODO: find -ignore_readdir_race not yet implemented in fro bounded find slice"]
fn find_ignore_readdir_race_todo() {}

#[test]
#[ignore = "TODO: find -noignore_readdir_race not yet implemented in fro bounded find slice"]
fn find_noignore_readdir_race_todo() {}

#[test]
#[ignore = "TODO: find -fstype not yet implemented in fro bounded find slice"]
fn find_fstype_todo() {}

#[test]
#[ignore = "TODO: find -context (SELinux) not yet implemented in fro bounded find slice"]
fn find_context_todo() {}

#[test]
#[ignore = "TODO: find -xtype not yet implemented in fro bounded find slice"]
fn find_xtype_todo() {}

// ── Action predicates ────────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -exec not yet implemented in fro bounded find slice"]
fn find_exec_todo() {}

#[test]
#[ignore = "TODO: find -execdir not yet implemented in fro bounded find slice"]
fn find_execdir_todo() {}

#[test]
#[ignore = "TODO: find -ok not yet implemented in fro bounded find slice"]
fn find_ok_todo() {}

#[test]
#[ignore = "TODO: find -okdir not yet implemented in fro bounded find slice"]
fn find_okdir_todo() {}

#[test]
#[ignore = "TODO: find -delete not yet implemented in fro bounded find slice"]
fn find_delete_todo() {}

#[test]
#[ignore = "TODO: find -ls not yet implemented in fro bounded find slice"]
fn find_ls_todo() {}

#[test]
#[ignore = "TODO: find -fls not yet implemented in fro bounded find slice"]
fn find_fls_todo() {}

#[test]
#[ignore = "TODO: find -printf not yet implemented in fro bounded find slice"]
fn find_printf_todo() {}

#[test]
#[ignore = "TODO: find -fprintf not yet implemented in fro bounded find slice"]
fn find_fprintf_todo() {}

#[test]
#[ignore = "TODO: find -fprint not yet implemented in fro bounded find slice"]
fn find_fprint_todo() {}

#[test]
#[ignore = "TODO: find -fprint0 not yet implemented in fro bounded find slice"]
fn find_fprint0_todo() {}

#[test]
#[ignore = "TODO: find -prune not yet implemented in fro bounded find slice"]
fn find_prune_todo() {}

#[test]
#[ignore = "TODO: find -quit not yet implemented in fro bounded find slice"]
fn find_quit_todo() {}

// ── Constant predicates ──────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -true not yet implemented in fro bounded find slice"]
fn find_true_todo() {}

#[test]
#[ignore = "TODO: find -false not yet implemented in fro bounded find slice"]
fn find_false_todo() {}
