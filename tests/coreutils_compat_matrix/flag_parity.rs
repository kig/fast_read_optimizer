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
    let fro2 = run_fro(
        "find",
        &[root.to_str().unwrap(), "-type", "f", "-print"],
    );
    let system2 = run_system(
        "find",
        &[root.to_str().unwrap(), "-type", "f", "-print"],
    );
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
#[ignore = "TODO: find -atime not yet implemented in fro bounded find slice"]
fn find_atime_todo() {}

#[test]
#[ignore = "TODO: find -amin not yet implemented in fro bounded find slice"]
fn find_amin_todo() {}

#[test]
#[ignore = "TODO: find -anewer not yet implemented in fro bounded find slice"]
fn find_anewer_todo() {}

#[test]
#[ignore = "TODO: find -ctime not yet implemented in fro bounded find slice"]
fn find_ctime_todo() {}

#[test]
#[ignore = "TODO: find -cmin not yet implemented in fro bounded find slice"]
fn find_cmin_todo() {}

#[test]
#[ignore = "TODO: find -cnewer not yet implemented in fro bounded find slice"]
fn find_cnewer_todo() {}

#[test]
#[ignore = "TODO: find -mtime not yet implemented in fro bounded find slice"]
fn find_mtime_todo() {}

#[test]
#[ignore = "TODO: find -mmin not yet implemented in fro bounded find slice"]
fn find_mmin_todo() {}

#[test]
#[ignore = "TODO: find -newer not yet implemented in fro bounded find slice"]
fn find_newer_todo() {}

#[test]
#[ignore = "TODO: find -daystart not yet implemented in fro bounded find slice"]
fn find_daystart_todo() {}

#[test]
#[ignore = "TODO: find -used not yet implemented in fro bounded find slice"]
fn find_used_todo() {}

// ── Size / count predicates ─────────────────────────────────────────

#[test]
#[ignore = "TODO: find -size not yet implemented in fro bounded find slice"]
fn find_size_todo() {}

#[test]
#[ignore = "TODO: find -links not yet implemented in fro bounded find slice"]
fn find_links_todo() {}

#[test]
#[ignore = "TODO: find -inum not yet implemented in fro bounded find slice"]
fn find_inum_todo() {}

// ── Ownership / permission predicates ───────────────────────────────

#[test]
#[ignore = "TODO: find -perm not yet implemented in fro bounded find slice"]
fn find_perm_todo() {}

#[test]
#[ignore = "TODO: find -uid not yet implemented in fro bounded find slice"]
fn find_uid_todo() {}

#[test]
#[ignore = "TODO: find -gid not yet implemented in fro bounded find slice"]
fn find_gid_todo() {}

#[test]
#[ignore = "TODO: find -user not yet implemented in fro bounded find slice"]
fn find_user_todo() {}

#[test]
#[ignore = "TODO: find -group not yet implemented in fro bounded find slice"]
fn find_group_todo() {}

#[test]
#[ignore = "TODO: find -nouser not yet implemented in fro bounded find slice"]
fn find_nouser_todo() {}

#[test]
#[ignore = "TODO: find -nogroup not yet implemented in fro bounded find slice"]
fn find_nogroup_todo() {}

#[test]
#[ignore = "TODO: find -readable not yet implemented in fro bounded find slice"]
fn find_readable_todo() {}

#[test]
#[ignore = "TODO: find -writable not yet implemented in fro bounded find slice"]
fn find_writable_todo() {}

#[test]
#[ignore = "TODO: find -executable not yet implemented in fro bounded find slice"]
fn find_executable_todo() {}

#[test]
#[ignore = "TODO: find -empty not yet implemented in fro bounded find slice"]
fn find_empty_todo() {}

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
#[ignore = "TODO: find -mindepth not yet implemented in fro bounded find slice"]
fn find_mindepth_todo() {}

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
