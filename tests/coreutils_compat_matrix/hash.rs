use super::*;

#[test]
fn cartesian_hash_tools_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-hash-matrix");
    let path_a = tmp.join("a.bin");
    let path_b = tmp.join("b.bin");
    fs::write(
        &path_a,
        (0..65599).map(|i| (i % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_b,
        (0..32791)
            .map(|i| ((i * 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("cksum", "cksum"),
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for flags in io_flag_sets() {
            for files in [
                vec![path_a.to_str().unwrap()],
                vec![path_a.to_str().unwrap(), path_b.to_str().unwrap()],
            ] {
                let mut args = flags.clone();
                args.extend(files.iter().copied());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &files),
                    &format!("{name} {:?}", args),
                );
            }
        }
    }
}

#[test]
fn cksum_special_filenames_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cksum-special-filenames");
    let backslash_path = tmp.join("slash\\name.txt");
    let newline_path = tmp.join("line\nname.txt");
    fs::write(
        &backslash_path,
        (0..4097)
            .map(|i| ((i * 97 + 31) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &newline_path,
        (0..4097)
            .map(|i| ((i * 101 + 37) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for path in [backslash_path.as_path(), newline_path.as_path()] {
        for io_flags in io_flag_sets() {
            let mut fro_args = io_flags.clone();
            fro_args.push(path.to_str().unwrap());
            let sys_args = [path.to_str().unwrap()];
            assert_same_result(
                run_fro("cksum", &fro_args),
                run_system("cksum", &sys_args),
                &format!("cksum {:?} {:?}", io_flags, path),
            );
        }
    }
}

#[test]
fn digest_family_format_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-flags");
    let path = tmp.join("hash file.txt");
    fs::write(
        &path,
        (0..65599)
            .map(|i| ((i * 11 + 3) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for compat_flags in [
            vec!["-b"],
            vec!["-t"],
            vec!["--tag"],
            vec!["-z"],
            vec!["-b", "-z"],
        ] {
            for io_flags in io_flag_sets() {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.push(path.to_str().unwrap());
                let mut sys_args = compat_flags.clone();
                sys_args.push(path.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &fro_args),
                    run_system(system_name, &sys_args),
                    &format!("{name} {:?} {:?}", io_flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn b2sum_length_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-b2sum-length");
    let path = tmp.join("hash file.txt");
    fs::write(
        &path,
        (0..65599)
            .map(|i| ((i * 17 + 5) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for compat_flags in [
        vec!["--length=8"],
        vec!["-l8"],
        vec!["-l", "72"],
        vec!["--length", "0"],
        vec!["--tag", "--length=72"],
        vec!["-z", "--length=16"],
    ] {
        for io_flags in io_flag_sets() {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(path.to_str().unwrap());
            assert_same_result(
                run_fro("b2sum", &fro_args),
                run_system("b2sum", &sys_args),
                &format!("b2sum {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn b2sum_truncated_manifests_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-b2sum-length-check");
    let path = tmp.join("hash file.txt");
    fs::write(
        &path,
        (0..8193)
            .map(|i| ((i * 61 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for compat_flags in [vec!["--length=72"], vec!["--tag", "--length=72"]] {
        let manifest = tmp.join(format!("{}.txt", compat_flags.join("_").replace('-', "")));
        let mut sys_args = compat_flags.clone();
        sys_args.push(path.to_str().unwrap());
        let generated = run_system("b2sum", &sys_args);
        assert!(
            generated.status.success(),
            "{}",
            String::from_utf8_lossy(&generated.stderr)
        );
        fs::write(&manifest, &generated.stdout).unwrap();

        let args = ["-c", manifest.to_str().unwrap()];
        assert_same_result(
            run_fro("b2sum", &args),
            run_system("b2sum", &args),
            &format!("b2sum truncated check {:?}", compat_flags),
        );
    }
}

#[test]
fn digest_family_double_dash_treats_following_operands_as_files() {
    let tmp = unique_temp_dir("fro-coreutils-digest-double-dash");
    let dash_path = tmp.join("-leading-dash.txt");
    fs::write(
        &dash_path,
        (0..4097)
            .map(|i| ((i * 13 + 5) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for io_flags in io_flag_sets() {
            let mut fro_args = io_flags.clone();
            fro_args.push("--");
            fro_args.push(dash_path.to_str().unwrap());
            let sys_args = ["--", dash_path.to_str().unwrap()];
            assert_same_result(
                run_fro(name, &fro_args),
                run_system(system_name, &sys_args),
                &format!("{name} {:?} -- {:?}", io_flags, dash_path),
            );
        }
    }
}

#[test]
fn digest_family_escapes_special_filenames_like_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-escape-filenames");
    let backslash_path = tmp.join("slash\\name.txt");
    let newline_path = tmp.join("line\nname.txt");
    fs::write(
        &backslash_path,
        (0..4097)
            .map(|i| ((i * 73 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &newline_path,
        (0..4097)
            .map(|i| ((i * 79 + 19) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for compat_flags in [vec![], vec!["-b"], vec!["--tag"], vec!["-z"]] {
            for path in [backslash_path.as_path(), newline_path.as_path()] {
                let mut fro_args = compat_flags.clone();
                fro_args.push(path.to_str().unwrap());
                let mut sys_args = compat_flags.clone();
                sys_args.push(path.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &fro_args),
                    run_system(system_name, &sys_args),
                    &format!("{name} {:?} {:?}", compat_flags, path),
                );
            }
        }
    }
}

#[test]
fn digest_family_tagged_check_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-tagged");
    let path_ok = tmp.join("ok file).bin");
    let path_bad = tmp.join("bad file).bin");
    fs::write(
        &path_ok,
        (0..8193)
            .map(|i| ((i * 59 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..8193)
            .map(|i| ((i * 61 + 11) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-tagged-ok.txt"));
        let manifest_bad = tmp.join(format!("{name}-tagged-bad.txt"));
        let ok_manifest = run_system(system_name, &["--tag", path_ok.to_str().unwrap()]);
        assert!(
            ok_manifest.status.success(),
            "{}",
            String::from_utf8_lossy(&ok_manifest.stderr)
        );
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let bad_manifest = String::from_utf8(ok_manifest.stdout.clone())
            .unwrap()
            .replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for manifest in [manifest_ok.as_path(), manifest_bad.as_path()] {
            let args = ["-c", manifest.to_str().unwrap()];
            assert_same_result(
                run_fro(name, &args),
                run_system(system_name, &args),
                &format!("{name} tagged check {:?}", manifest),
            );
        }
    }
}

#[test]
fn digest_family_check_rejects_check_plus_tag_flag_like_system() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-tag-flag");
    let path = tmp.join("ok.bin");
    fs::write(
        &path,
        (0..4097)
            .map(|i| ((i * 67 + 13) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest = tmp.join(format!("{name}-tagged.txt"));
        let tagged = run_system(system_name, &["--tag", path.to_str().unwrap()]);
        assert!(tagged.status.success());
        fs::write(&manifest, tagged.stdout).unwrap();

        let args = ["--check", "--tag", manifest.to_str().unwrap()];
        assert_same_result(
            run_fro(name, &args),
            run_system(system_name, &args),
            &format!("{name} check+tag {:?}", manifest),
        );
    }
}

#[test]
fn digest_family_check_flag_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    fs::write(
        &path_ok,
        (0..8193)
            .map(|i| ((i * 19 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..8193)
            .map(|i| ((i * 23 + 5) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-ok.txt"));
        let manifest_bad = tmp.join(format!("{name}-bad.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(
            ok_manifest.status.success(),
            "{}",
            String::from_utf8_lossy(&ok_manifest.stderr)
        );
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let mut bad_manifest = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        bad_manifest = bad_manifest.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for manifest in [manifest_ok.as_path(), manifest_bad.as_path()] {
            let args = ["-c", manifest.to_str().unwrap()];
            assert_same_result(
                run_fro(name, &args),
                run_system(system_name, &args),
                &format!("{name} check {:?}", manifest),
            );
        }
    }
}

#[test]
fn digest_family_check_quiet_and_status_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-flags");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    fs::write(
        &path_ok,
        (0..4097)
            .map(|i| ((i * 29 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..4097)
            .map(|i| ((i * 31 + 9) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-ok-flags.txt"));
        let manifest_bad = tmp.join(format!("{name}-bad-flags.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(ok_manifest.status.success());
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let mut bad_manifest = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        bad_manifest = bad_manifest.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for extra_flags in [vec!["--quiet", "-c"], vec!["--status", "-c"]] {
            for manifest in [manifest_ok.as_path(), manifest_bad.as_path()] {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
                );
            }
        }
    }
}

#[test]
fn digest_family_check_warn_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-warn");
    let path_ok = tmp.join("ok.bin");
    fs::write(
        &path_ok,
        (0..4097)
            .map(|i| ((i * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-ok.txt"));
        let manifest_mixed = tmp.join(format!("{name}-warn-mixed.txt"));
        let manifest_only_bad = tmp.join(format!("{name}-warn-only-bad.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(ok_manifest.status.success());
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let ok_line = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        fs::write(&manifest_mixed, format!("bogus-line\n{ok_line}")).unwrap();
        fs::write(&manifest_only_bad, "bogus-line\n").unwrap();

        for extra_flags in [vec!["--warn", "-c"], vec!["--warn", "--status", "-c"]] {
            for manifest in [manifest_mixed.as_path(), manifest_only_bad.as_path()] {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
                );
            }
        }
    }
}

#[test]
fn digest_family_check_strict_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-strict");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    fs::write(
        &path_ok,
        (0..4097)
            .map(|i| ((i * 41 + 13) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..4097)
            .map(|i| ((i * 43 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_malformed = tmp.join(format!("{name}-strict-malformed.txt"));
        let manifest_bad = tmp.join(format!("{name}-strict-bad.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(ok_manifest.status.success());
        let ok_line = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        fs::write(&manifest_malformed, format!("bogus-line\n{ok_line}")).unwrap();

        let mut bad_manifest = ok_line.clone();
        bad_manifest = bad_manifest.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for extra_flags in [vec!["--strict", "-c"], vec!["--strict", "--status", "-c"]] {
            for manifest in [manifest_malformed.as_path(), manifest_bad.as_path()] {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
                );
            }
        }
    }
}

#[test]
fn digest_family_check_missing_file_modes_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-missing");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    let path_missing = tmp.join("missing.bin");
    fs::write(
        &path_ok,
        (0..4097)
            .map(|i| ((i * 47 + 19) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..4097)
            .map(|i| ((i * 53 + 23) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_mixed = tmp.join(format!("{name}-missing-mixed.txt"));
        let manifest_only_missing = tmp.join(format!("{name}-missing-only.txt"));
        let manifest_mixed_bad = tmp.join(format!("{name}-missing-bad.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(ok_manifest.status.success());
        let ok_line = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        let missing_line =
            ok_line.replace(path_ok.to_str().unwrap(), path_missing.to_str().unwrap());
        let bad_line = ok_line.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_mixed, format!("{missing_line}{ok_line}")).unwrap();
        fs::write(&manifest_only_missing, &missing_line).unwrap();
        fs::write(&manifest_mixed_bad, format!("{missing_line}{bad_line}")).unwrap();

        for (extra_flags, manifests) in [
            (
                vec!["-c"],
                vec![manifest_mixed.as_path(), manifest_only_missing.as_path()],
            ),
            (
                vec!["--status", "-c"],
                vec![manifest_mixed.as_path(), manifest_only_missing.as_path()],
            ),
            (
                vec!["--ignore-missing", "-c"],
                vec![
                    manifest_mixed.as_path(),
                    manifest_only_missing.as_path(),
                    manifest_mixed_bad.as_path(),
                ],
            ),
            (
                vec!["--ignore-missing", "--status", "-c"],
                vec![
                    manifest_mixed.as_path(),
                    manifest_only_missing.as_path(),
                    manifest_mixed_bad.as_path(),
                ],
            ),
        ] {
            for manifest in manifests {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
                );
            }
        }
    }
}

#[test]
fn digest_family_check_escaped_manifest_paths_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-escaped-paths");
    let backslash_path = tmp.join("slash\\name.txt");
    let newline_path = tmp.join("line\nname.txt");
    fs::write(
        &backslash_path,
        (0..4097)
            .map(|i| ((i * 83 + 23) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &newline_path,
        (0..4097)
            .map(|i| ((i * 89 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for path in [backslash_path.as_path(), newline_path.as_path()] {
            let manifest = tmp.join(format!(
                "{name}-{}.txt",
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .replace('\n', "_nl_")
            ));
            let generated = run_system(system_name, &[path.to_str().unwrap()]);
            assert!(
                generated.status.success(),
                "{}",
                String::from_utf8_lossy(&generated.stderr)
            );
            fs::write(&manifest, &generated.stdout).unwrap();

            for extra_flags in [vec!["-c"], vec!["--quiet", "-c"], vec!["--status", "-c"]] {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
                );
            }
        }
    }
}
