use super::*;

#[test]
fn rm_force_matches_system_for_missing_operands_and_missing_files() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-force");

    let fro_missing = run_fro("rm", &["-f"]);
    let sys_missing = run_system("rm", &["-f"]);
    assert_same_result(fro_missing, sys_missing, "rm -f with no operands");

    let fro_root = tmp.join("fro");
    let sys_root = tmp.join("sys");
    fs::create_dir_all(&fro_root).unwrap();
    fs::create_dir_all(&sys_root).unwrap();

    let missing_name = "ghost.txt";
    let fro_existing = fro_root.join("keep.txt");
    let sys_existing = sys_root.join("keep.txt");
    fs::write(&fro_existing, b"keep\n").unwrap();
    fs::write(&sys_existing, b"keep\n").unwrap();

    let fro_out = Command::new(env!("CARGO_BIN_EXE_fro"))
        .current_dir(&fro_root)
        .arg("rm")
        .args(["-fv", missing_name, "keep.txt"])
        .output()
        .expect("failed to run fro rm in fixture dir");
    let sys_out = system_command("rm")
        .current_dir(&sys_root)
        .args(["-fv", missing_name, "keep.txt"])
        .output()
        .expect("failed to run system rm in fixture dir");
    assert_same_result(fro_out, sys_out, "rm -fv missing and existing");
    assert!(!fro_existing.exists());
    assert!(!sys_existing.exists());
    assert_eq!(
        fro_root.join(missing_name).exists(),
        sys_root.join(missing_name).exists()
    );
}

#[test]
fn cartesian_rm_recursive_matches_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-matrix");

    for recursive_flag in [["-r"], ["-R"], ["--recursive"]] {
        let fro_root = tmp.join(format!("rm-fro-{}", recursive_flag[0].replace('-', "_")));
        let sys_root = tmp.join(format!("rm-sys-{}", recursive_flag[0].replace('-', "_")));
        for root in [&fro_root, &sys_root] {
            let nested = root.join("nested/deeper");
            fs::create_dir_all(&nested).unwrap();
            fs::write(root.join("small.txt"), b"alpha\nbeta\n").unwrap();
            fs::write(
                nested.join("large.bin"),
                (0..(2 * 1024 * 1024 + 333))
                    .map(|i| ((i * 13) % 251) as u8)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            symlink("../small.txt", root.join("nested/link-small")).unwrap();
        }

        assert_same_result(
            run_fro("rm", &[recursive_flag[0], fro_root.to_str().unwrap()]),
            run_system("rm", &[recursive_flag[0], sys_root.to_str().unwrap()]),
            &format!("rm {}", recursive_flag[0]),
        );
        assert_eq!(fro_root.exists(), sys_root.exists());
    }
}

#[test]
fn rm_preserve_root_policy_flags_match_system_for_non_root_recursive_paths() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-preserve-root");

    for flags in [
        vec!["-r", "--preserve-root", "tree"],
        vec!["-r", "--no-preserve-root", "tree"],
        vec!["--recursive", "--preserve-root", "tree"],
        vec!["--recursive", "--no-preserve-root", "tree"],
    ] {
        let suffix = flags.join("_").replace('-', "");
        let fro_root = tmp.join(format!("rm-preserve-fro-{suffix}"));
        let sys_root = tmp.join(format!("rm-preserve-sys-{suffix}"));
        for root in [&fro_root, &sys_root] {
            let nested = root.join("tree/nested");
            fs::create_dir_all(&nested).unwrap();
            fs::write(root.join("tree/root.txt"), b"alpha\n").unwrap();
            fs::write(nested.join("leaf.txt"), b"beta\n").unwrap();
        }

        let fro_out = Command::new(env!("CARGO_BIN_EXE_fro"))
            .current_dir(&fro_root)
            .arg("rm")
            .args(&flags)
            .output()
            .expect("failed to run fro rm preserve-root fixture");
        let sys_out = system_command("rm")
            .current_dir(&sys_root)
            .args(&flags)
            .output()
            .expect("failed to run system rm preserve-root fixture");
        assert_same_result(
            fro_out,
            sys_out,
            &format!("rm {:?} non-root recursive removal", flags),
        );
        assert_eq!(
            fro_root.join("tree").exists(),
            sys_root.join("tree").exists()
        );
    }
}

#[test]
fn rm_one_file_system_matches_system_on_same_device_recursive_paths() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-one-file-system");

    let fro_root = tmp.join("fro");
    let sys_root = tmp.join("sys");
    for root in [&fro_root, &sys_root] {
        let nested = root.join("tree/nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(root.join("tree/root.txt"), b"alpha\n").unwrap();
        fs::write(nested.join("leaf.txt"), b"beta\n").unwrap();
    }

    let fro_out = Command::new(env!("CARGO_BIN_EXE_fro"))
        .current_dir(&fro_root)
        .arg("rm")
        .args(["-r", "--one-file-system", "tree"])
        .output()
        .expect("failed to run fro rm --one-file-system fixture");
    let sys_out = system_command("rm")
        .current_dir(&sys_root)
        .args(["-r", "--one-file-system", "tree"])
        .output()
        .expect("failed to run system rm --one-file-system fixture");
    assert_same_result(fro_out, sys_out, "rm --one-file-system same-device removal");
    assert_eq!(
        fro_root.join("tree").exists(),
        sys_root.join("tree").exists()
    );
}

#[test]
fn rm_dir_matches_system_for_empty_and_non_empty_directories() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-dir");

    for flags in [vec!["-d"], vec!["-d", "-v"], vec!["--dir"]] {
        let suffix = flags.join("_").replace('-', "");
        let fro_root = tmp.join(format!("rm-dir-fro-{suffix}"));
        let sys_root = tmp.join(format!("rm-dir-sys-{suffix}"));
        fs::create_dir_all(fro_root.join("empty")).unwrap();
        fs::create_dir_all(sys_root.join("empty")).unwrap();
        fs::create_dir_all(fro_root.join("nonempty/child")).unwrap();
        fs::create_dir_all(sys_root.join("nonempty/child")).unwrap();

        let mut fro_args = flags.clone();
        fro_args.push("empty");
        fro_args.push("nonempty");
        let fro_out = Command::new(env!("CARGO_BIN_EXE_fro"))
            .current_dir(&fro_root)
            .arg("rm")
            .args(&fro_args)
            .output()
            .expect("failed to run fro rm -d fixture");

        let mut sys_args = flags.clone();
        sys_args.push("empty");
        sys_args.push("nonempty");
        let sys_out = system_command("rm")
            .current_dir(&sys_root)
            .args(&sys_args)
            .output()
            .expect("failed to run system rm -d fixture");

        assert_same_result(
            fro_out,
            sys_out,
            &format!("rm {:?} empty/non-empty dir", flags),
        );
        assert_eq!(
            fro_root.join("empty").exists(),
            sys_root.join("empty").exists()
        );
        assert_eq!(
            fro_root.join("nonempty").exists(),
            sys_root.join("nonempty").exists()
        );
    }
}

#[test]
fn rm_interactive_prompt_modes_match_system_for_representative_cases() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-rm-interactive");

    let fro_root = tmp.join("always-fro");
    let sys_root = tmp.join("always-sys");
    fs::create_dir_all(&fro_root).unwrap();
    fs::create_dir_all(&sys_root).unwrap();
    fs::write(fro_root.join("file.txt"), b"alpha\n").unwrap();
    fs::write(sys_root.join("file.txt"), b"alpha\n").unwrap();
    let fro_always = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["rm", "--interactive", "file.txt"],
        &fro_root,
        Some(b"n\n"),
    );
    let sys_always = run_command_in_dir(
        "rm",
        &["--interactive", "file.txt"],
        &sys_root,
        Some(b"n\n"),
    );
    assert_same_result(fro_always, sys_always, "rm --interactive file decline");
    assert_eq!(
        fro_root.join("file.txt").exists(),
        sys_root.join("file.txt").exists()
    );

    let fro_once = tmp.join("once-fro");
    let sys_once = tmp.join("once-sys");
    for root in [&fro_once, &sys_once] {
        fs::create_dir_all(root.join("dir/sub")).unwrap();
        fs::write(root.join("dir/sub/file.txt"), b"beta\n").unwrap();
    }
    let fro_once_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["rm", "--interactive=once", "-r", "dir"],
        &fro_once,
        Some(b"y\n"),
    );
    let sys_once_out = run_command_in_dir(
        "rm",
        &["--interactive=once", "-r", "dir"],
        &sys_once,
        Some(b"y\n"),
    );
    assert_same_result(
        fro_once_out,
        sys_once_out,
        "rm --interactive=once -r accepted",
    );
    assert_eq!(fro_once.join("dir").exists(), sys_once.join("dir").exists());

    for (flags, label, expect_exists, prompt_stdin) in [
        (
            vec!["-if", "file.txt"],
            "rm -if last flag wins",
            false,
            None,
        ),
        (
            vec!["-fi", "file.txt"],
            "rm -fi last flag wins",
            true,
            Some(b"n\n".as_slice()),
        ),
        (
            vec!["-i", "--interactive=never", "file.txt"],
            "rm --interactive=never last flag wins",
            false,
            None,
        ),
    ] {
        let fro_case = tmp.join(format!("conflict-fro-{}", label.replace(' ', "-")));
        let sys_case = tmp.join(format!("conflict-sys-{}", label.replace(' ', "-")));
        fs::create_dir_all(&fro_case).unwrap();
        fs::create_dir_all(&sys_case).unwrap();
        fs::write(fro_case.join("file.txt"), b"gamma\n").unwrap();
        fs::write(sys_case.join("file.txt"), b"gamma\n").unwrap();
        let mut fro_args = vec!["rm"];
        fro_args.extend(flags.iter().copied());
        let fro_out = run_command_in_dir(
            env!("CARGO_BIN_EXE_fro"),
            &fro_args,
            &fro_case,
            prompt_stdin,
        );
        let sys_out = run_command_in_dir("rm", &flags, &sys_case, prompt_stdin);
        assert_same_result(fro_out, sys_out, label);
        assert_eq!(
            fro_case.join("file.txt").exists(),
            sys_case.join("file.txt").exists(),
            "{label}: side effects mismatch"
        );
        assert_eq!(
            fro_case.join("file.txt").exists(),
            expect_exists,
            "{label}: expected file state"
        );
    }
}
