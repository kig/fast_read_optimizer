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

#[test]
fn cartesian_mv_file_and_recursive_directory_match_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-mv-matrix");

    for verbose_flags in [Vec::<&str>::new(), vec!["-v"]] {
        let tag = if verbose_flags.is_empty() {
            "plain"
        } else {
            "verbose"
        };

        let fro_src_file = tmp.join(format!("mv-file-fro-src-{tag}.bin"));
        let fro_dst_file = tmp.join(format!("mv-file-fro-dst-{tag}.bin"));
        let sys_src_file = tmp.join(format!("mv-file-sys-src-{tag}.bin"));
        let sys_dst_file = tmp.join(format!("mv-file-sys-dst-{tag}.bin"));
        fs::write(&fro_src_file, b"fro file payload").unwrap();
        fs::write(&sys_src_file, b"fro file payload").unwrap();
        fs::write(&fro_dst_file, b"old").unwrap();
        fs::write(&sys_dst_file, b"old").unwrap();

        let mut fro_file_args = verbose_flags.clone();
        fro_file_args.push(fro_src_file.to_str().unwrap());
        fro_file_args.push(fro_dst_file.to_str().unwrap());
        let mut sys_file_args = verbose_flags.clone();
        sys_file_args.push(sys_src_file.to_str().unwrap());
        sys_file_args.push(sys_dst_file.to_str().unwrap());
        let fro_file_out = run_fro("mv", &fro_file_args);
        let sys_file_out = run_system("mv", &sys_file_args);
        assert_eq!(
            fro_file_out.status.code(),
            sys_file_out.status.code(),
            "mv file status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_file_args,
            String::from_utf8_lossy(&fro_file_out.stdout),
            String::from_utf8_lossy(&fro_file_out.stderr),
            String::from_utf8_lossy(&sys_file_out.stdout),
            String::from_utf8_lossy(&sys_file_out.stderr),
        );
        assert_eq!(fro_file_out.stderr, sys_file_out.stderr);
        assert_eq!(
            fs::read(&fro_dst_file).unwrap(),
            fs::read(&sys_dst_file).unwrap()
        );
        assert_eq!(fro_src_file.exists(), sys_src_file.exists());
        if !verbose_flags.is_empty() {
            assert!(
                String::from_utf8_lossy(&fro_file_out.stdout).contains("renamed"),
                "expected mv -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_file_out.stdout).contains("renamed"),
                "expected mv -v to mention rename in system stdout"
            );
        } else {
            assert_eq!(fro_file_out.stdout, sys_file_out.stdout);
        }

        let fro_source_root = tmp.join(format!("mv-tree-fro-src-{tag}"));
        let sys_source_root = tmp.join(format!("mv-tree-sys-src-{tag}"));
        let fro_dest_parent = tmp.join(format!("mv-tree-fro-parent-{tag}"));
        let sys_dest_parent = tmp.join(format!("mv-tree-sys-parent-{tag}"));
        for (source_root, dest_parent) in [
            (&fro_source_root, &fro_dest_parent),
            (&sys_source_root, &sys_dest_parent),
        ] {
            let nested = source_root.join("nested/deeper");
            fs::create_dir_all(&nested).unwrap();
            fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
            fs::write(
                nested.join("large.bin"),
                (0..(2 * 1024 * 1024 + 333))
                    .map(|i| ((i * 17) % 251) as u8)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            symlink("../small.txt", source_root.join("nested/link-small")).unwrap();
            fs::create_dir_all(dest_parent).unwrap();
        }

        let mut fro_dir_args = verbose_flags.clone();
        fro_dir_args.push(fro_source_root.to_str().unwrap());
        fro_dir_args.push(fro_dest_parent.to_str().unwrap());
        let mut sys_dir_args = verbose_flags.clone();
        sys_dir_args.push(sys_source_root.to_str().unwrap());
        sys_dir_args.push(sys_dest_parent.to_str().unwrap());
        let fro_dir_out = run_fro("mv", &fro_dir_args);
        let sys_dir_out = run_system("mv", &sys_dir_args);
        assert_eq!(
            fro_dir_out.status.code(),
            sys_dir_out.status.code(),
            "mv dir status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_dir_args,
            String::from_utf8_lossy(&fro_dir_out.stdout),
            String::from_utf8_lossy(&fro_dir_out.stderr),
            String::from_utf8_lossy(&sys_dir_out.stdout),
            String::from_utf8_lossy(&sys_dir_out.stderr),
        );
        assert_eq!(fro_dir_out.stderr, sys_dir_out.stderr);
        if !verbose_flags.is_empty() {
            assert!(
                String::from_utf8_lossy(&fro_dir_out.stdout).contains("renamed"),
                "expected mv -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_dir_out.stdout).contains("renamed"),
                "expected mv -v to mention rename in system stdout"
            );
        } else {
            assert_eq!(fro_dir_out.stdout, sys_dir_out.stdout);
        }

        let fro_moved_name = fro_source_root.file_name().unwrap();
        let sys_moved_name = sys_source_root.file_name().unwrap();
        let fro_tree = snapshot_tree(&fro_dest_parent.join(fro_moved_name));
        let sys_tree = snapshot_tree(&sys_dest_parent.join(sys_moved_name));
        assert_eq!(fro_tree, sys_tree, "recursive move tree mismatch for {tag}");
        assert_eq!(fro_source_root.exists(), sys_source_root.exists());
    }
}

#[test]
fn mv_target_directory_matches_system_for_files_and_directories() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-target-directory");

    for flags in [Vec::<&str>::new(), vec!["-v"]] {
        let suffix = if flags.is_empty() { "plain" } else { "verbose" };

        let fro_src_root = tmp.join(format!("mv-target-dir-fro-src-root-{suffix}"));
        let sys_src_root = tmp.join(format!("mv-target-dir-sys-src-root-{suffix}"));
        fs::create_dir_all(&fro_src_root).unwrap();
        fs::create_dir_all(&sys_src_root).unwrap();
        let src_a = fro_src_root.join("mv-source-a.txt");
        let src_b = fro_src_root.join("mv-source-b.txt");
        let sys_src_a = sys_src_root.join("mv-source-a.txt");
        let sys_src_b = sys_src_root.join("mv-source-b.txt");
        let fro_dest = tmp.join(format!("mv-dest-fro-{suffix}"));
        let sys_dest = tmp.join(format!("mv-dest-sys-{suffix}"));
        fs::write(&src_a, b"alpha-target-dir").unwrap();
        fs::write(&src_b, b"beta-target-dir").unwrap();
        fs::write(&sys_src_a, b"alpha-target-dir").unwrap();
        fs::write(&sys_src_b, b"beta-target-dir").unwrap();
        fs::create_dir_all(&fro_dest).unwrap();
        fs::create_dir_all(&sys_dest).unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend([
            "-t",
            fro_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
            src_b.to_str().unwrap(),
        ]);
        let mut sys_args = flags.clone();
        sys_args.extend([
            "-t",
            sys_dest.to_str().unwrap(),
            sys_src_a.to_str().unwrap(),
            sys_src_b.to_str().unwrap(),
        ]);
        let fro_out = run_fro("mv", &fro_args);
        let sys_out = run_system("mv", &sys_args);
        assert_eq!(
            fro_out.status.code(),
            sys_out.status.code(),
            "mv -t multi-source status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_args,
            String::from_utf8_lossy(&fro_out.stdout),
            String::from_utf8_lossy(&fro_out.stderr),
            String::from_utf8_lossy(&sys_out.stdout),
            String::from_utf8_lossy(&sys_out.stderr),
        );
        assert_eq!(fro_out.stderr, sys_out.stderr);
        if flags.is_empty() {
            assert_eq!(fro_out.stdout, sys_out.stdout);
        } else {
            assert!(
                String::from_utf8_lossy(&fro_out.stdout).contains("renamed"),
                "expected mv -t -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_out.stdout).contains("renamed"),
                "expected mv -t -v to mention rename in system stdout"
            );
        }
        assert_eq!(snapshot_tree(&fro_dest), snapshot_tree(&sys_dest));

        let long_src = fro_src_root.join("mv-source-long.txt");
        let long_sys_src = sys_src_root.join("mv-source-long.txt");
        fs::write(&long_src, b"gamma-target-dir").unwrap();
        fs::write(&long_sys_src, b"gamma-target-dir").unwrap();
        let long_fro_dest = tmp.join(format!("mv-long-dest-fro-{suffix}"));
        let long_sys_dest = tmp.join(format!("mv-long-dest-sys-{suffix}"));
        fs::create_dir_all(&long_fro_dest).unwrap();
        fs::create_dir_all(&long_sys_dest).unwrap();
        let fro_long_target = format!("--target-directory={}", long_fro_dest.display());
        let sys_long_target = format!("--target-directory={}", long_sys_dest.display());
        let mut fro_long_args = flags.clone();
        fro_long_args.extend([fro_long_target.as_str(), long_src.to_str().unwrap()]);
        let mut sys_long_args = flags.clone();
        sys_long_args.extend([sys_long_target.as_str(), long_sys_src.to_str().unwrap()]);
        let fro_long_out = run_fro("mv", &fro_long_args);
        let sys_long_out = run_system("mv", &sys_long_args);
        assert_eq!(
            fro_long_out.status.code(),
            sys_long_out.status.code(),
            "mv --target-directory status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_long_args,
            String::from_utf8_lossy(&fro_long_out.stdout),
            String::from_utf8_lossy(&fro_long_out.stderr),
            String::from_utf8_lossy(&sys_long_out.stdout),
            String::from_utf8_lossy(&sys_long_out.stderr),
        );
        assert_eq!(fro_long_out.stderr, sys_long_out.stderr);
        if flags.is_empty() {
            assert_eq!(fro_long_out.stdout, sys_long_out.stdout);
        } else {
            assert!(
                String::from_utf8_lossy(&fro_long_out.stdout).contains("renamed"),
                "expected mv --target-directory -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_long_out.stdout).contains("renamed"),
                "expected mv --target-directory -v to mention rename in system stdout"
            );
        }
        assert_eq!(snapshot_tree(&long_fro_dest), snapshot_tree(&long_sys_dest));

        let sep_src = fro_src_root.join("mv-source-sep.txt");
        let sep_sys_src = sys_src_root.join("mv-source-sep.txt");
        fs::write(&sep_src, b"delta-target-dir").unwrap();
        fs::write(&sep_sys_src, b"delta-target-dir").unwrap();
        let sep_fro_dest = tmp.join(format!("mv-sep-dest-fro-{suffix}"));
        let sep_sys_dest = tmp.join(format!("mv-sep-dest-sys-{suffix}"));
        fs::create_dir_all(&sep_fro_dest).unwrap();
        fs::create_dir_all(&sep_sys_dest).unwrap();
        let mut fro_sep_args = flags.clone();
        fro_sep_args.extend([
            "--target-directory",
            sep_fro_dest.to_str().unwrap(),
            sep_src.to_str().unwrap(),
        ]);
        let mut sys_sep_args = flags.clone();
        sys_sep_args.extend([
            "--target-directory",
            sep_sys_dest.to_str().unwrap(),
            sep_sys_src.to_str().unwrap(),
        ]);
        let fro_sep_out = run_fro("mv", &fro_sep_args);
        let sys_sep_out = run_system("mv", &sys_sep_args);
        assert_eq!(
            fro_sep_out.status.code(),
            sys_sep_out.status.code(),
            "mv --target-directory separated status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_sep_args,
            String::from_utf8_lossy(&fro_sep_out.stdout),
            String::from_utf8_lossy(&fro_sep_out.stderr),
            String::from_utf8_lossy(&sys_sep_out.stdout),
            String::from_utf8_lossy(&sys_sep_out.stderr),
        );
        assert_eq!(fro_sep_out.stderr, sys_sep_out.stderr);
        if flags.is_empty() {
            assert_eq!(fro_sep_out.stdout, sys_sep_out.stdout);
        } else {
            assert!(
                String::from_utf8_lossy(&fro_sep_out.stdout).contains("renamed"),
                "expected mv --target-directory separated -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_sep_out.stdout).contains("renamed"),
                "expected mv --target-directory separated -v to mention rename in system stdout"
            );
        }
        assert_eq!(snapshot_tree(&sep_fro_dest), snapshot_tree(&sep_sys_dest));

        let fro_dir_src = fro_src_root.join("mv-tree-src");
        let sys_dir_src = sys_src_root.join("mv-tree-src");
        fs::create_dir_all(fro_dir_src.join("nested")).unwrap();
        fs::create_dir_all(sys_dir_src.join("nested")).unwrap();
        fs::write(fro_dir_src.join("nested/file.txt"), b"recursive-target-dir").unwrap();
        fs::write(sys_dir_src.join("nested/file.txt"), b"recursive-target-dir").unwrap();
        let fro_recursive_dest = tmp.join(format!("mv-tree-dest-fro-{suffix}"));
        let sys_recursive_dest = tmp.join(format!("mv-tree-dest-sys-{suffix}"));
        fs::create_dir_all(&fro_recursive_dest).unwrap();
        fs::create_dir_all(&sys_recursive_dest).unwrap();

        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-t",
            fro_recursive_dest.to_str().unwrap(),
            fro_dir_src.to_str().unwrap(),
        ]);
        let mut sys_recursive_args = flags.clone();
        sys_recursive_args.extend([
            "-t",
            sys_recursive_dest.to_str().unwrap(),
            sys_dir_src.to_str().unwrap(),
        ]);
        let fro_recursive_out = run_fro("mv", &fro_recursive_args);
        let sys_recursive_out = run_system("mv", &sys_recursive_args);
        assert_eq!(
            fro_recursive_out.status.code(),
            sys_recursive_out.status.code(),
            "mv -t recursive status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_recursive_args,
            String::from_utf8_lossy(&fro_recursive_out.stdout),
            String::from_utf8_lossy(&fro_recursive_out.stderr),
            String::from_utf8_lossy(&sys_recursive_out.stdout),
            String::from_utf8_lossy(&sys_recursive_out.stderr),
        );
        assert_eq!(fro_recursive_out.stderr, sys_recursive_out.stderr);
        if flags.is_empty() {
            assert_eq!(fro_recursive_out.stdout, sys_recursive_out.stdout);
        } else {
            assert!(
                String::from_utf8_lossy(&fro_recursive_out.stdout).contains("renamed"),
                "expected mv -t recursive -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_recursive_out.stdout).contains("renamed"),
                "expected mv -t recursive -v to mention rename in system stdout"
            );
        }
        assert_eq!(
            snapshot_tree(&fro_recursive_dest),
            snapshot_tree(&sys_recursive_dest)
        );
    }
}

#[test]
fn mv_no_clobber_and_update_match_system_for_existing_destinations() {
    let tmp = unique_temp_dir("fro-coreutils-mv-no-clobber-update");

    for (label, flags, source_mtime, target_mtime, expect_source_exists, expected_target) in [
        (
            "no-clobber skip",
            vec!["-n"],
            1_700_000_100,
            1_700_000_000,
            true,
            b"keep-target".as_slice(),
        ),
        (
            "update skip newer destination",
            vec!["--update"],
            1_700_000_000,
            1_700_000_100,
            true,
            b"keep-target".as_slice(),
        ),
        (
            "update move older destination",
            vec!["-u"],
            1_700_000_100,
            1_700_000_000,
            false,
            b"fresh-source".as_slice(),
        ),
    ] {
        let suffix = label.replace(' ', "-");
        let fro_source = tmp.join(format!("mv-policy-fro-src-{suffix}.txt"));
        let sys_source = tmp.join(format!("mv-policy-sys-src-{suffix}.txt"));
        let fro_target = tmp.join(format!("mv-policy-fro-dst-{suffix}.txt"));
        let sys_target = tmp.join(format!("mv-policy-sys-dst-{suffix}.txt"));
        fs::write(&fro_source, b"fresh-source").unwrap();
        fs::write(&sys_source, b"fresh-source").unwrap();
        fs::write(&fro_target, b"keep-target").unwrap();
        fs::write(&sys_target, b"keep-target").unwrap();
        set_file_mtime(&fro_source, source_mtime);
        set_file_mtime(&sys_source, source_mtime);
        set_file_mtime(&fro_target, target_mtime);
        set_file_mtime(&sys_target, target_mtime);

        let mut fro_args = flags.clone();
        fro_args.extend([fro_source.to_str().unwrap(), fro_target.to_str().unwrap()]);
        let mut sys_args = flags.clone();
        sys_args.extend([sys_source.to_str().unwrap(), sys_target.to_str().unwrap()]);
        assert_same_result(run_fro("mv", &fro_args), run_system("mv", &sys_args), label);
        assert_eq!(
            fro_source.exists(),
            expect_source_exists,
            "{label}: unexpected fro source state"
        );
        assert_eq!(
            fro_source.exists(),
            sys_source.exists(),
            "{label}: source existence mismatch"
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            fs::read(&sys_target).unwrap(),
            "{label}: target contents mismatch"
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            expected_target,
            "{label}: unexpected target contents"
        );
    }

    for (label, flags, source_mtime, child_mtime, expect_source_exists, expected_child) in [
        (
            "target-directory no-clobber skip",
            vec!["--no-clobber"],
            1_700_000_100,
            1_700_000_000,
            true,
            b"keep-child".as_slice(),
        ),
        (
            "target-directory update move",
            vec!["--update"],
            1_700_000_100,
            1_700_000_000,
            false,
            b"fresh-child".as_slice(),
        ),
    ] {
        let suffix = label.replace(' ', "-");
        let fro_root = tmp.join(format!("mv-target-policy-fro-{suffix}"));
        let sys_root = tmp.join(format!("mv-target-policy-sys-{suffix}"));
        let fro_dest = fro_root.join("dest");
        let sys_dest = sys_root.join("dest");
        fs::create_dir_all(&fro_dest).unwrap();
        fs::create_dir_all(&sys_dest).unwrap();
        let fro_source = fro_root.join("entry.txt");
        let sys_source = sys_root.join("entry.txt");
        let fro_child = fro_dest.join("entry.txt");
        let sys_child = sys_dest.join("entry.txt");
        fs::write(&fro_source, b"fresh-child").unwrap();
        fs::write(&sys_source, b"fresh-child").unwrap();
        fs::write(&fro_child, b"keep-child").unwrap();
        fs::write(&sys_child, b"keep-child").unwrap();
        set_file_mtime(&fro_source, source_mtime);
        set_file_mtime(&sys_source, source_mtime);
        set_file_mtime(&fro_child, child_mtime);
        set_file_mtime(&sys_child, child_mtime);

        let mut fro_args = flags.clone();
        fro_args.extend([
            "-t",
            fro_dest.to_str().unwrap(),
            fro_source.to_str().unwrap(),
        ]);
        let mut sys_args = flags.clone();
        sys_args.extend([
            "-t",
            sys_dest.to_str().unwrap(),
            sys_source.to_str().unwrap(),
        ]);
        assert_same_result(run_fro("mv", &fro_args), run_system("mv", &sys_args), label);
        assert_eq!(
            fro_source.exists(),
            expect_source_exists,
            "{label}: unexpected fro source state"
        );
        assert_eq!(
            fro_source.exists(),
            sys_source.exists(),
            "{label}: source existence mismatch"
        );
        assert_eq!(
            snapshot_tree(&fro_dest),
            snapshot_tree(&sys_dest),
            "{label}: target-directory tree mismatch"
        );
        assert_eq!(
            fs::read(&fro_child).unwrap(),
            expected_child,
            "{label}: unexpected moved child contents"
        );
    }
}

#[test]
fn mv_force_and_interactive_match_system_for_existing_destinations() {
    let tmp = unique_temp_dir("fro-coreutils-mv-force-interactive");

    let fro_decline = tmp.join("decline-fro");
    let sys_decline = tmp.join("decline-sys");
    fs::create_dir_all(&fro_decline).unwrap();
    fs::create_dir_all(&sys_decline).unwrap();
    fs::write(fro_decline.join("src.txt"), b"fresh-source").unwrap();
    fs::write(sys_decline.join("src.txt"), b"fresh-source").unwrap();
    fs::write(fro_decline.join("dst.txt"), b"keep-target").unwrap();
    fs::write(sys_decline.join("dst.txt"), b"keep-target").unwrap();
    let fro_decline_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["mv", "--interactive", "src.txt", "dst.txt"],
        &fro_decline,
        Some(b"n\n"),
    );
    let sys_decline_out = run_command_in_dir(
        "mv",
        &["--interactive", "src.txt", "dst.txt"],
        &sys_decline,
        Some(b"n\n"),
    );
    assert_same_result(
        fro_decline_out,
        sys_decline_out,
        "mv --interactive decline existing destination",
    );
    assert_eq!(
        snapshot_tree(&fro_decline),
        snapshot_tree(&sys_decline),
        "mv --interactive decline tree mismatch",
    );

    let fro_target_dir = tmp.join("target-directory-fro");
    let sys_target_dir = tmp.join("target-directory-sys");
    for root in [&fro_target_dir, &sys_target_dir] {
        fs::create_dir_all(root.join("dest")).unwrap();
        fs::write(root.join("entry.txt"), b"fresh-child").unwrap();
        fs::write(root.join("dest/entry.txt"), b"keep-child").unwrap();
    }
    let fro_target_dir_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["mv", "-i", "-t", "dest", "entry.txt"],
        &fro_target_dir,
        Some(b"y\n"),
    );
    let sys_target_dir_out = run_command_in_dir(
        "mv",
        &["-i", "-t", "dest", "entry.txt"],
        &sys_target_dir,
        Some(b"y\n"),
    );
    assert_same_result(
        fro_target_dir_out,
        sys_target_dir_out,
        "mv -i -t accepted child overwrite",
    );
    assert_eq!(
        snapshot_tree(&fro_target_dir),
        snapshot_tree(&sys_target_dir),
        "mv -i -t tree mismatch",
    );

    for (flags, label, prompt_stdin, expect_source_exists, expected_target) in [
        (
            vec!["-if", "src.txt", "dst.txt"],
            "mv -if last flag wins",
            None,
            false,
            b"fresh-source".as_slice(),
        ),
        (
            vec!["-fi", "src.txt", "dst.txt"],
            "mv -fi last flag wins",
            Some(b"n\n".as_slice()),
            true,
            b"keep-target".as_slice(),
        ),
        (
            vec!["-i", "--force", "src.txt", "dst.txt"],
            "mv --force last flag wins",
            None,
            false,
            b"fresh-source".as_slice(),
        ),
        (
            vec!["--force", "-i", "src.txt", "dst.txt"],
            "mv -i last flag wins",
            Some(b"n\n".as_slice()),
            true,
            b"keep-target".as_slice(),
        ),
    ] {
        let fro_case = tmp.join(format!("force-interactive-fro-{}", label.replace(' ', "-")));
        let sys_case = tmp.join(format!("force-interactive-sys-{}", label.replace(' ', "-")));
        fs::create_dir_all(&fro_case).unwrap();
        fs::create_dir_all(&sys_case).unwrap();
        fs::write(fro_case.join("src.txt"), b"fresh-source").unwrap();
        fs::write(sys_case.join("src.txt"), b"fresh-source").unwrap();
        fs::write(fro_case.join("dst.txt"), b"keep-target").unwrap();
        fs::write(sys_case.join("dst.txt"), b"keep-target").unwrap();

        let mut fro_args = vec!["mv"];
        fro_args.extend(flags.iter().copied());
        let fro_out = run_command_in_dir(
            env!("CARGO_BIN_EXE_fro"),
            &fro_args,
            &fro_case,
            prompt_stdin,
        );
        let sys_out = run_command_in_dir("mv", &flags, &sys_case, prompt_stdin);
        assert_same_result(fro_out, sys_out, label);
        assert_eq!(
            fro_case.join("src.txt").exists(),
            sys_case.join("src.txt").exists(),
            "{label}: source existence mismatch",
        );
        assert_eq!(
            fro_case.join("src.txt").exists(),
            expect_source_exists,
            "{label}: unexpected source state",
        );
        assert_eq!(
            fs::read(fro_case.join("dst.txt")).unwrap(),
            fs::read(sys_case.join("dst.txt")).unwrap(),
            "{label}: target contents mismatch",
        );
        assert_eq!(
            fs::read(fro_case.join("dst.txt")).unwrap(),
            expected_target,
            "{label}: unexpected target contents",
        );
    }
}

#[test]
fn mv_backup_and_suffix_flags_match_system_for_representative_paths() {
    let tmp = unique_temp_dir("fro-coreutils-mv-backup-suffix");

    for (flags, label, expected_backup_name) in [
        (vec!["-b"], "simple backup", "dst.txt~"),
        (
            vec!["--suffix=.bak"],
            "suffix implies backup",
            "dst.txt.bak",
        ),
        (vec!["-bS.bak"], "short suffix cluster", "dst.txt.bak"),
    ] {
        let fro_case = tmp.join(format!("backup-fro-{}", label.replace(' ', "-")));
        let sys_case = tmp.join(format!("backup-sys-{}", label.replace(' ', "-")));
        fs::create_dir_all(&fro_case).unwrap();
        fs::create_dir_all(&sys_case).unwrap();
        fs::write(fro_case.join("src.txt"), b"fresh-source").unwrap();
        fs::write(sys_case.join("src.txt"), b"fresh-source").unwrap();
        fs::write(fro_case.join("dst.txt"), b"keep-target").unwrap();
        fs::write(sys_case.join("dst.txt"), b"keep-target").unwrap();

        let mut fro_args = vec!["mv"];
        fro_args.extend(flags.iter().copied());
        fro_args.extend(["src.txt", "dst.txt"]);
        let fro_out = run_command_in_dir(env!("CARGO_BIN_EXE_fro"), &fro_args, &fro_case, None);
        let sys_out = run_command_in_dir("mv", &fro_args[1..], &sys_case, None);
        assert_same_result(fro_out, sys_out, label);
        assert_eq!(
            snapshot_tree(&fro_case),
            snapshot_tree(&sys_case),
            "{label}: tree mismatch"
        );
        assert!(
            fro_case.join(expected_backup_name).exists(),
            "{label}: expected backup file {expected_backup_name}"
        );
    }

    let fro_dir = tmp.join("backup-target-dir-fro");
    let sys_dir = tmp.join("backup-target-dir-sys");
    for root in [&fro_dir, &sys_dir] {
        fs::create_dir_all(root.join("destdir")).unwrap();
        fs::write(root.join("src.txt"), b"fresh-source").unwrap();
        fs::write(root.join("destdir/src.txt"), b"keep-target").unwrap();
    }
    let fro_dir_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["mv", "-bS.bak", "src.txt", "destdir"],
        &fro_dir,
        None,
    );
    let sys_dir_out = run_command_in_dir("mv", &["-bS.bak", "src.txt", "destdir"], &sys_dir, None);
    assert_same_result(fro_dir_out, sys_dir_out, "backup inside target directory");
    assert_eq!(snapshot_tree(&fro_dir), snapshot_tree(&sys_dir));
}

#[test]
fn mv_backup_conflicts_with_no_clobber_like_system() {
    let tmp = unique_temp_dir("fro-coreutils-mv-backup-no-clobber");
    let fro_case = tmp.join("fro");
    let sys_case = tmp.join("sys");
    fs::create_dir_all(&fro_case).unwrap();
    fs::create_dir_all(&sys_case).unwrap();
    fs::write(fro_case.join("src.txt"), b"fresh-source").unwrap();
    fs::write(sys_case.join("src.txt"), b"fresh-source").unwrap();
    fs::write(fro_case.join("dst.txt"), b"keep-target").unwrap();
    fs::write(sys_case.join("dst.txt"), b"keep-target").unwrap();

    assert_same_result(
        run_command_in_dir(
            env!("CARGO_BIN_EXE_fro"),
            &["mv", "-bn", "src.txt", "dst.txt"],
            &fro_case,
            None,
        ),
        run_command_in_dir("mv", &["-bn", "src.txt", "dst.txt"], &sys_case, None),
        "mv -bn mutual exclusion",
    );
    assert_eq!(snapshot_tree(&fro_case), snapshot_tree(&sys_case));
}

#[test]
fn mv_strip_trailing_slashes_matches_system_for_representative_sources() {
    let tmp = unique_temp_dir("fro-coreutils-mv-strip-trailing-slashes");

    let fro_symlink = tmp.join("symlink-fro");
    let sys_symlink = tmp.join("symlink-sys");
    for root in [&fro_symlink, &sys_symlink] {
        fs::create_dir_all(root.join("real/sub")).unwrap();
        fs::create_dir_all(root.join("dest")).unwrap();
        fs::write(root.join("real/sub/file.txt"), b"payload").unwrap();
        symlink("real", root.join("linkdir")).unwrap();
    }
    let fro_symlink_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["mv", "--strip-trailing-slashes", "linkdir/", "dest/"],
        &fro_symlink,
        None,
    );
    let sys_symlink_out = run_command_in_dir(
        "mv",
        &["--strip-trailing-slashes", "linkdir/", "dest/"],
        &sys_symlink,
        None,
    );
    assert_same_result(
        fro_symlink_out,
        sys_symlink_out,
        "mv --strip-trailing-slashes symlink source",
    );
    assert_eq!(snapshot_tree(&fro_symlink), snapshot_tree(&sys_symlink));
    assert!(
        fs::symlink_metadata(fro_symlink.join("dest/linkdir"))
            .unwrap()
            .file_type()
            .is_symlink(),
        "strip-trailing-slashes should preserve the symlink move path"
    );

    let fro_dir = tmp.join("dir-fro");
    let sys_dir = tmp.join("dir-sys");
    for root in [&fro_dir, &sys_dir] {
        fs::create_dir_all(root.join("src/nested")).unwrap();
        fs::create_dir_all(root.join("dest")).unwrap();
        fs::write(root.join("src/nested/file.txt"), b"dir-payload").unwrap();
    }
    let fro_dir_out = run_command_in_dir(
        env!("CARGO_BIN_EXE_fro"),
        &["mv", "--strip-trailing-slashes", "-t", "dest/", "src/"],
        &fro_dir,
        None,
    );
    let sys_dir_out = run_command_in_dir(
        "mv",
        &["--strip-trailing-slashes", "-t", "dest/", "src/"],
        &sys_dir,
        None,
    );
    assert_same_result(
        fro_dir_out,
        sys_dir_out,
        "mv --strip-trailing-slashes directory source",
    );
    assert_eq!(snapshot_tree(&fro_dir), snapshot_tree(&sys_dir));
}

#[test]
fn mv_help_and_version_surface_stay_wired() {
    let help = run_fro("mv", &["--help"]);
    assert_eq!(
        help.status.code(),
        Some(0),
        "mv --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&help.stdout),
        String::from_utf8_lossy(&help.stderr),
    );
    assert!(
        help.stderr.is_empty(),
        "mv --help unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&help.stderr),
    );
    let help_stdout = String::from_utf8(help.stdout).expect("mv help should be UTF-8");
    for token in [
        "--backup",
        "--suffix",
        "--strip-trailing-slashes",
        "--force",
        "--interactive",
        "--help",
        "--version",
    ] {
        assert!(
            help_stdout.contains(token),
            "mv --help output should mention {token}:\n{help_stdout}"
        );
    }

    let version = run_fro("mv", &["--version"]);
    assert_eq!(
        version.status.code(),
        Some(0),
        "mv --version failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr),
    );
    assert!(
        version.stderr.is_empty(),
        "mv --version unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&version.stderr),
    );
    let version_stdout = String::from_utf8(version.stdout).expect("mv version should be UTF-8");
    assert!(
        version_stdout.starts_with("mv "),
        "mv --version should start with the mv command name:\n{version_stdout}"
    );
}

#[test]
fn mv_no_target_directory_matches_system_for_common_cases() {
    let tmp = unique_temp_dir("fro-coreutils-mv-no-target-directory");

    for flags in [Vec::<&str>::new(), vec!["-v"]] {
        let suffix = if flags.is_empty() { "plain" } else { "verbose" };

        let fro_file_src = tmp.join(format!("mv-no-target-file-fro-src-{suffix}.txt"));
        let sys_file_src = tmp.join(format!("mv-no-target-file-sys-src-{suffix}.txt"));
        let fro_file_dest = tmp.join(format!("mv-no-target-file-fro-dest-{suffix}"));
        let sys_file_dest = tmp.join(format!("mv-no-target-file-sys-dest-{suffix}"));
        fs::write(&fro_file_src, b"file-no-target-directory").unwrap();
        fs::write(&sys_file_src, b"file-no-target-directory").unwrap();
        fs::create_dir_all(&fro_file_dest).unwrap();
        fs::create_dir_all(&sys_file_dest).unwrap();

        let mut fro_file_args = flags.clone();
        fro_file_args.extend([
            "-T",
            fro_file_src.to_str().unwrap(),
            fro_file_dest.to_str().unwrap(),
        ]);
        let mut sys_file_args = flags.clone();
        sys_file_args.extend([
            "-T",
            sys_file_src.to_str().unwrap(),
            sys_file_dest.to_str().unwrap(),
        ]);
        let fro_file_out = run_fro("mv", &fro_file_args);
        let sys_file_out = run_system("mv", &sys_file_args);
        assert_eq!(
            fro_file_out.status.code(),
            sys_file_out.status.code(),
            "mv -T file into existing dir status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_file_args,
            String::from_utf8_lossy(&fro_file_out.stdout),
            String::from_utf8_lossy(&fro_file_out.stderr),
            String::from_utf8_lossy(&sys_file_out.stdout),
            String::from_utf8_lossy(&sys_file_out.stderr),
        );
        assert_eq!(fro_file_src.exists(), sys_file_src.exists());
        assert_eq!(snapshot_tree(&fro_file_dest), snapshot_tree(&sys_file_dest));
        assert!(
            String::from_utf8_lossy(&fro_file_out.stderr).contains("cannot overwrite directory"),
            "expected fro mv -T file error to mention directory overwrite"
        );
        assert!(
            String::from_utf8_lossy(&sys_file_out.stderr).contains("cannot overwrite directory"),
            "expected system mv -T file error to mention directory overwrite"
        );

        let fro_dir_src = tmp.join(format!("mv-no-target-dir-fro-src-{suffix}"));
        let sys_dir_src = tmp.join(format!("mv-no-target-dir-sys-src-{suffix}"));
        let fro_dir_dest = tmp.join(format!("mv-no-target-dir-fro-dest-{suffix}"));
        let sys_dir_dest = tmp.join(format!("mv-no-target-dir-sys-dest-{suffix}"));
        fs::create_dir_all(fro_dir_src.join("nested")).unwrap();
        fs::create_dir_all(sys_dir_src.join("nested")).unwrap();
        fs::write(
            fro_dir_src.join("nested/file.txt"),
            b"dir-no-target-directory",
        )
        .unwrap();
        fs::write(
            sys_dir_src.join("nested/file.txt"),
            b"dir-no-target-directory",
        )
        .unwrap();
        fs::create_dir_all(&fro_dir_dest).unwrap();
        fs::create_dir_all(&sys_dir_dest).unwrap();

        let mut fro_dir_args = flags.clone();
        fro_dir_args.extend([
            "-T",
            fro_dir_src.to_str().unwrap(),
            fro_dir_dest.to_str().unwrap(),
        ]);
        let mut sys_dir_args = flags.clone();
        sys_dir_args.extend([
            "-T",
            sys_dir_src.to_str().unwrap(),
            sys_dir_dest.to_str().unwrap(),
        ]);
        let fro_dir_out = run_fro("mv", &fro_dir_args);
        let sys_dir_out = run_system("mv", &sys_dir_args);
        assert_eq!(
            fro_dir_out.status.code(),
            sys_dir_out.status.code(),
            "mv -T directory status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_dir_args,
            String::from_utf8_lossy(&fro_dir_out.stdout),
            String::from_utf8_lossy(&fro_dir_out.stderr),
            String::from_utf8_lossy(&sys_dir_out.stdout),
            String::from_utf8_lossy(&sys_dir_out.stderr),
        );
        assert_eq!(snapshot_tree(&fro_dir_dest), snapshot_tree(&sys_dir_dest));
        assert_eq!(fro_dir_src.exists(), sys_dir_src.exists());
        if flags.is_empty() {
            assert_eq!(fro_dir_out.stdout, sys_dir_out.stdout);
        } else {
            assert!(
                String::from_utf8_lossy(&fro_dir_out.stdout).contains("renamed"),
                "expected mv -T -v to mention rename in fro stdout"
            );
            assert!(
                String::from_utf8_lossy(&sys_dir_out.stdout).contains("renamed"),
                "expected mv -T -v to mention rename in system stdout"
            );
        }
        assert_eq!(fro_dir_out.stderr, sys_dir_out.stderr);

        let fro_conflict_src = tmp.join(format!("mv-no-target-conflict-fro-src-{suffix}.txt"));
        let sys_conflict_src = tmp.join(format!("mv-no-target-conflict-sys-src-{suffix}.txt"));
        let fro_conflict_dest = tmp.join(format!("mv-no-target-conflict-fro-dest-{suffix}"));
        let sys_conflict_dest = tmp.join(format!("mv-no-target-conflict-sys-dest-{suffix}"));
        fs::write(&fro_conflict_src, b"conflict").unwrap();
        fs::write(&sys_conflict_src, b"conflict").unwrap();
        fs::create_dir_all(&fro_conflict_dest).unwrap();
        fs::create_dir_all(&sys_conflict_dest).unwrap();

        let mut fro_conflict_args = flags.clone();
        fro_conflict_args.extend([
            "-T",
            "-t",
            fro_conflict_dest.to_str().unwrap(),
            fro_conflict_src.to_str().unwrap(),
        ]);
        let mut sys_conflict_args = flags.clone();
        sys_conflict_args.extend([
            "-T",
            "-t",
            sys_conflict_dest.to_str().unwrap(),
            sys_conflict_src.to_str().unwrap(),
        ]);
        let fro_conflict_out = run_fro("mv", &fro_conflict_args);
        let sys_conflict_out = run_system("mv", &sys_conflict_args);
        assert_eq!(
            fro_conflict_out.status.code(),
            sys_conflict_out.status.code(),
            "mv -T -t conflict status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_conflict_args,
            String::from_utf8_lossy(&fro_conflict_out.stdout),
            String::from_utf8_lossy(&fro_conflict_out.stderr),
            String::from_utf8_lossy(&sys_conflict_out.stdout),
            String::from_utf8_lossy(&sys_conflict_out.stderr),
        );
        assert!(
            String::from_utf8_lossy(&fro_conflict_out.stderr)
                .contains("cannot combine --target-directory (-t) and --no-target-directory (-T)"),
            "expected fro mv -T/-t conflict message"
        );
        assert!(
            String::from_utf8_lossy(&sys_conflict_out.stderr)
                .contains("cannot combine --target-directory (-t) and --no-target-directory (-T)"),
            "expected system mv -T/-t conflict message"
        );

        let fro_extra_a = tmp.join(format!("mv-no-target-extra-fro-a-{suffix}.txt"));
        let fro_extra_b = tmp.join(format!("mv-no-target-extra-fro-b-{suffix}.txt"));
        let sys_extra_a = tmp.join(format!("mv-no-target-extra-sys-a-{suffix}.txt"));
        let sys_extra_b = tmp.join(format!("mv-no-target-extra-sys-b-{suffix}.txt"));
        let fro_extra_c = tmp.join(format!("mv-no-target-extra-fro-c-{suffix}.txt"));
        let sys_extra_c = tmp.join(format!("mv-no-target-extra-sys-c-{suffix}.txt"));
        fs::write(&fro_extra_a, b"a").unwrap();
        fs::write(&fro_extra_b, b"b").unwrap();
        fs::write(&sys_extra_a, b"a").unwrap();
        fs::write(&sys_extra_b, b"b").unwrap();

        let mut fro_extra_args = flags.clone();
        fro_extra_args.extend([
            "-T",
            fro_extra_a.to_str().unwrap(),
            fro_extra_b.to_str().unwrap(),
            fro_extra_c.to_str().unwrap(),
        ]);
        let mut sys_extra_args = flags.clone();
        sys_extra_args.extend([
            "-T",
            sys_extra_a.to_str().unwrap(),
            sys_extra_b.to_str().unwrap(),
            sys_extra_c.to_str().unwrap(),
        ]);
        let fro_extra_out = run_fro("mv", &fro_extra_args);
        let sys_extra_out = run_system("mv", &sys_extra_args);
        assert_eq!(
            fro_extra_out.status.code(),
            sys_extra_out.status.code(),
            "mv -T extra operand status mismatch for {:?}\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
            fro_extra_args,
            String::from_utf8_lossy(&fro_extra_out.stdout),
            String::from_utf8_lossy(&fro_extra_out.stderr),
            String::from_utf8_lossy(&sys_extra_out.stdout),
            String::from_utf8_lossy(&sys_extra_out.stderr),
        );
        assert!(
            String::from_utf8_lossy(&fro_extra_out.stderr).contains("extra operand"),
            "expected fro mv -T extra operand error"
        );
        assert!(
            String::from_utf8_lossy(&sys_extra_out.stderr).contains("extra operand"),
            "expected system mv -T extra operand error"
        );
    }
}
