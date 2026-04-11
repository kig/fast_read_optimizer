use super::*;

#[path = "cp_shred/shred.rs"]
mod shred;

#[test]
fn cartesian_cp_recursive_matches_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-cp-recursive-matrix");

    for flags in io_flag_sets() {
        let source_root = tmp.join(format!("cp-tree-src-{}", flags.join("_")));
        let fro_dest_parent = tmp.join(format!("cp-tree-fro-{}", flags.join("_")));
        let sys_dest_parent = tmp.join(format!("cp-tree-sys-{}", flags.join("_")));
        let nested = source_root.join("nested/deeper");
        fs::create_dir_all(&nested).unwrap();
        fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
        fs::write(
            nested.join("large.bin"),
            (0..(2 * 1024 * 1024 + 333))
                .map(|i| ((i * 13) % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        symlink("../small.txt", source_root.join("nested/link-small")).unwrap();
        fs::create_dir_all(&fro_dest_parent).unwrap();
        fs::create_dir_all(&sys_dest_parent).unwrap();

        let mut fro_args = flags.clone();
        fro_args.push("-r");
        fro_args.push(source_root.to_str().unwrap());
        fro_args.push(fro_dest_parent.to_str().unwrap());
        let sys_args = [
            "-r",
            source_root.to_str().unwrap(),
            sys_dest_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp recursive {:?}", fro_args),
        );

        let copied_name = source_root.file_name().unwrap();
        let fro_tree = snapshot_tree(&fro_dest_parent.join(copied_name));
        let sys_tree = snapshot_tree(&sys_dest_parent.join(copied_name));
        assert_eq!(
            fro_tree, sys_tree,
            "recursive tree mismatch for {:?}",
            fro_args
        );
    }
}

#[test]
fn cp_no_clobber_matches_system_for_single_file_and_recursive_copy() {
    let tmp = unique_temp_dir("fro-coreutils-cp-no-clobber");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source = tmp.join(format!("src-{suffix}.txt"));
        let fro_target = tmp.join(format!("fro-target-{suffix}.txt"));
        let sys_target = tmp.join(format!("sys-target-{suffix}.txt"));
        fs::write(&source, b"new-data").unwrap();
        fs::write(&fro_target, b"old-data").unwrap();
        fs::write(&sys_target, b"old-data").unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend(["-n", source.to_str().unwrap(), fro_target.to_str().unwrap()]);
        let sys_args = ["-n", source.to_str().unwrap(), sys_target.to_str().unwrap()];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp no-clobber {:?}", fro_args),
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            fs::read(&sys_target).unwrap()
        );

        let recursive_source = tmp.join(format!("tree-src-{suffix}"));
        let fro_dest = tmp.join(format!("tree-fro-{suffix}"));
        let sys_dest = tmp.join(format!("tree-sys-{suffix}"));
        fs::create_dir_all(recursive_source.join("sub")).unwrap();
        fs::create_dir_all(fro_dest.join("sub")).unwrap();
        fs::create_dir_all(sys_dest.join("sub")).unwrap();
        fs::write(recursive_source.join("sub/existing.txt"), b"fresh").unwrap();
        fs::write(recursive_source.join("sub/new.txt"), b"brand-new").unwrap();
        fs::write(fro_dest.join("sub/existing.txt"), b"keep-me").unwrap();
        fs::write(sys_dest.join("sub/existing.txt"), b"keep-me").unwrap();

        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-rn",
            recursive_source.to_str().unwrap(),
            fro_dest.to_str().unwrap(),
        ]);
        let sys_recursive_args = [
            "-rn",
            recursive_source.to_str().unwrap(),
            sys_dest.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_args),
            run_system("cp", &sys_recursive_args),
            &format!("cp recursive no-clobber {:?}", fro_recursive_args),
        );

        let copied_name = recursive_source.file_name().unwrap();
        let fro_tree = snapshot_tree(&fro_dest.join(copied_name));
        let sys_tree = snapshot_tree(&sys_dest.join(copied_name));
        assert_eq!(
            fro_tree, sys_tree,
            "recursive -n tree mismatch for {:?}",
            fro_recursive_args
        );
    }
}

#[test]
fn cp_recursive_no_clobber_matches_system_when_merging_into_existing_tree() {
    let tmp = unique_temp_dir("fro-coreutils-cp-recursive-no-clobber-merge");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source_root = tmp.join(format!("merge-src-{suffix}"));
        let source_nested = source_root.join("nested");
        fs::create_dir_all(&source_nested).unwrap();
        fs::write(source_nested.join("existing.txt"), b"fresh-data").unwrap();
        fs::write(source_nested.join("new.txt"), b"brand-new").unwrap();
        symlink("nested/new.txt", source_root.join("link")).unwrap();

        let fro_dest = tmp.join(format!("merge-fro-{suffix}"));
        let sys_dest = tmp.join(format!("merge-sys-{suffix}"));
        fs::create_dir_all(fro_dest.join("nested")).unwrap();
        fs::create_dir_all(sys_dest.join("nested")).unwrap();
        fs::write(fro_dest.join("nested/existing.txt"), b"keep-me").unwrap();
        fs::write(sys_dest.join("nested/existing.txt"), b"keep-me").unwrap();
        fs::write(fro_dest.join("link"), b"existing-link-placeholder").unwrap();
        fs::write(sys_dest.join("link"), b"existing-link-placeholder").unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend([
            "-rnT",
            source_root.to_str().unwrap(),
            fro_dest.to_str().unwrap(),
        ]);
        let sys_args = [
            "-rnT",
            source_root.to_str().unwrap(),
            sys_dest.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp recursive no-clobber merge {:?}", fro_args),
        );

        assert_eq!(
            snapshot_tree(&fro_dest),
            snapshot_tree(&sys_dest),
            "recursive -n -T merge tree mismatch for {:?}",
            fro_args
        );
    }
}

#[test]
fn cp_update_matches_system_for_older_and_newer_destinations() {
    let tmp = unique_temp_dir("fro-coreutils-cp-update");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let newer_source = tmp.join(format!("newer-source-{suffix}.txt"));
        let older_fro_target = tmp.join(format!("older-fro-target-{suffix}.txt"));
        let older_sys_target = tmp.join(format!("older-sys-target-{suffix}.txt"));
        fs::write(&newer_source, b"replace-dst").unwrap();
        fs::write(&older_fro_target, b"stale").unwrap();
        fs::write(&older_sys_target, b"stale").unwrap();
        set_file_mtime(&newer_source, 1_700_000_100);
        set_file_mtime(&older_fro_target, 1_700_000_000);
        set_file_mtime(&older_sys_target, 1_700_000_000);

        let mut fro_copy_args = flags.clone();
        fro_copy_args.extend([
            "-u",
            newer_source.to_str().unwrap(),
            older_fro_target.to_str().unwrap(),
        ]);
        let sys_copy_args = [
            "-u",
            newer_source.to_str().unwrap(),
            older_sys_target.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_copy_args),
            run_system("cp", &sys_copy_args),
            &format!("cp update copy {:?}", fro_copy_args),
        );
        assert_eq!(
            fs::read(&older_fro_target).unwrap(),
            fs::read(&older_sys_target).unwrap()
        );

        let older_source = tmp.join(format!("older-source-{suffix}.txt"));
        let newer_fro_target = tmp.join(format!("newer-fro-target-{suffix}.txt"));
        let newer_sys_target = tmp.join(format!("newer-sys-target-{suffix}.txt"));
        fs::write(&older_source, b"source-should-skip").unwrap();
        fs::write(&newer_fro_target, b"stay-put").unwrap();
        fs::write(&newer_sys_target, b"stay-put").unwrap();
        set_file_mtime(&older_source, 1_700_000_000);
        set_file_mtime(&newer_fro_target, 1_700_000_100);
        set_file_mtime(&newer_sys_target, 1_700_000_100);

        let mut fro_skip_args = flags.clone();
        fro_skip_args.extend([
            "-u",
            older_source.to_str().unwrap(),
            newer_fro_target.to_str().unwrap(),
        ]);
        let sys_skip_args = [
            "-u",
            older_source.to_str().unwrap(),
            newer_sys_target.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_skip_args),
            run_system("cp", &sys_skip_args),
            &format!("cp update skip {:?}", fro_skip_args),
        );
        assert_eq!(
            fs::read(&newer_fro_target).unwrap(),
            fs::read(&newer_sys_target).unwrap()
        );
    }
}

#[test]
fn cp_preserve_matches_system_for_recursive_timestamps() {
    use std::os::unix::fs::MetadataExt;

    let tmp = unique_temp_dir("fro-coreutils-cp-preserve");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source_root = tmp.join(format!("preserve-src-{suffix}"));
        let fro_dest_parent = tmp.join(format!("preserve-fro-{suffix}"));
        let sys_dest_parent = tmp.join(format!("preserve-sys-{suffix}"));
        let nested = source_root.join("nested/deeper");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(&fro_dest_parent).unwrap();
        fs::create_dir_all(&sys_dest_parent).unwrap();

        let file = source_root.join("small.txt");
        let nested_dir = source_root.join("nested");
        let link = nested_dir.join("link-small");
        fs::write(&file, b"alpha\nbeta\n").unwrap();
        fs::write(nested.join("large.bin"), b"payload").unwrap();
        symlink("../small.txt", &link).unwrap();

        set_file_mtime(&file, 1_700_210_000);
        set_file_mtime(&nested.join("large.bin"), 1_700_210_010);
        set_file_mtime(&nested_dir, 1_700_210_020);
        set_file_mtime(&source_root, 1_700_210_030);

        let mut fro_args = flags.clone();
        fro_args.extend([
            "-rp",
            source_root.to_str().unwrap(),
            fro_dest_parent.to_str().unwrap(),
        ]);
        let sys_args = [
            "-rp",
            source_root.to_str().unwrap(),
            sys_dest_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp preserve recursive {:?}", fro_args),
        );

        let copied_name = source_root.file_name().unwrap();
        let fro_root = fro_dest_parent.join(copied_name);
        let sys_root = sys_dest_parent.join(copied_name);
        let fro_tree = snapshot_tree(&fro_root);
        let sys_tree = snapshot_tree(&sys_root);
        assert_eq!(
            fro_tree, sys_tree,
            "recursive -p tree mismatch for {:?}",
            fro_args
        );

        for rel in [
            std::path::Path::new("small.txt"),
            std::path::Path::new("nested"),
            std::path::Path::new("nested/deeper/large.bin"),
        ] {
            let fro_meta = fs::metadata(fro_root.join(rel)).unwrap();
            let sys_meta = fs::metadata(sys_root.join(rel)).unwrap();
            assert_eq!(
                fro_meta.mtime(),
                sys_meta.mtime(),
                "mtime mismatch for {:?}",
                rel
            );
            assert_eq!(
                fro_meta.mtime_nsec(),
                sys_meta.mtime_nsec(),
                "mtime_nsec mismatch for {:?}",
                rel
            );
        }
    }
}

#[path = "cp_shred/cp_flags.rs"]
mod cp_flags;
