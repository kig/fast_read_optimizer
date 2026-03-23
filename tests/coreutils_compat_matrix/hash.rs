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
