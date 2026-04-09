use super::*;
#[test]
fn cartesian_cp_and_shred_match_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-copy-shred-matrix");
    for flags in io_flag_sets() {
        let source = tmp.join(format!("cp-src-{}.bin", flags.join("_")));
        let fro_target = tmp.join(format!("cp-fro-{}.bin", flags.join("_")));
        let sys_target = tmp.join(format!("cp-sys-{}.bin", flags.join("_")));
        fs::write(
            &source,
            (0..131072)
                .map(|i| ((i * 5) % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut fro_args = flags.clone();
        fro_args.push(source.to_str().unwrap());
        fro_args.push(fro_target.to_str().unwrap());
        let sys_args = [source.to_str().unwrap(), sys_target.to_str().unwrap()];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp {:?}", fro_args),
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            fs::read(&sys_target).unwrap()
        );
        let fro_zero = tmp.join(format!("shred-fro-zero-{}.bin", flags.join("_")));
        let sys_zero = tmp.join(format!("shred-sys-zero-{}.bin", flags.join("_")));
        fs::write(&fro_zero, vec![0x55; 32768]).unwrap();
        fs::write(&sys_zero, vec![0x55; 32768]).unwrap();
        let mut fro_zero_args = flags.clone();
        fro_zero_args.extend(["-n", "0", "-z", fro_zero.to_str().unwrap()]);
        let sys_zero_args = ["-n", "0", "-z", sys_zero.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_zero_args),
            run_system("shred", &sys_zero_args),
            &format!("shred zero {:?}", fro_zero_args),
        );
        assert_eq!(fs::read(&fro_zero).unwrap(), fs::read(&sys_zero).unwrap());
        let fro_remove = tmp.join(format!("shred-fro-remove-{}.bin", flags.join("_")));
        let sys_remove = tmp.join(format!("shred-sys-remove-{}.bin", flags.join("_")));
        fs::write(&fro_remove, vec![0x99; 32768]).unwrap();
        fs::write(&sys_remove, vec![0x99; 32768]).unwrap();
        let mut fro_remove_args = flags;
        fro_remove_args.extend(["-n", "0", "-u", fro_remove.to_str().unwrap()]);
        let sys_remove_args = ["-n", "0", "-u", sys_remove.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_remove_args),
            run_system("shred", &sys_remove_args),
            &format!("shred remove {:?}", fro_remove_args),
        );
        assert_eq!(fro_remove.exists(), sys_remove.exists());
    }
}
#[test]
fn shred_size_verbose_and_force_match_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-shred-flags");
    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };
        let size_file = tmp.join(format!("shred-size-{suffix}.bin"));
        fs::write(&size_file, b"0123456789").unwrap();
        let mut fro_size_args = flags.clone();
        fro_size_args.extend(["-n", "0", "-z", "-s", "4", size_file.to_str().unwrap()]);
        let sys_size_args = ["-n", "0", "-z", "-s", "4", size_file.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_size_args),
            run_system("shred", &sys_size_args),
            &format!("shred size {:?}", fro_size_args),
        );
        assert_eq!(
            fs::read(&size_file).unwrap(),
            vec![0, 0, 0, 0, b'4', b'5', b'6', b'7', b'8', b'9']
        );
        let verbose_file = tmp.join(format!("shred-verbose-{suffix}.bin"));
        fs::write(&verbose_file, b"abcdef").unwrap();

        let mut fro_verbose_args = flags.clone();
        fro_verbose_args.extend([
            "-n",
            "1",
            "-z",
            "-v",
            "--size=2",
            verbose_file.to_str().unwrap(),
        ]);
        let sys_verbose_args = [
            "-n",
            "1",
            "-z",
            "-v",
            "--size=2",
            verbose_file.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("shred", &fro_verbose_args),
            run_system("shred", &sys_verbose_args),
            &format!("shred verbose {:?}", fro_verbose_args),
        );
        assert_eq!(
            fs::read(&verbose_file).unwrap(),
            vec![0, 0, b'c', b'd', b'e', b'f']
        );

        let force_file = tmp.join(format!("shred-force-{suffix}.bin"));
        fs::write(&force_file, b"XYZ123").unwrap();
        fs::set_permissions(&force_file, fs::Permissions::from_mode(0o400)).unwrap();

        let mut fro_force_args = flags.clone();
        fro_force_args.extend([
            "-n",
            "0",
            "-z",
            "-f",
            "--size",
            "2",
            force_file.to_str().unwrap(),
        ]);
        let sys_force_args = [
            "-n",
            "0",
            "-z",
            "-f",
            "--size",
            "2",
            force_file.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("shred", &fro_force_args),
            run_system("shred", &sys_force_args),
            &format!("shred force {:?}", fro_force_args),
        );
        assert_eq!(
            fs::metadata(&force_file).unwrap().permissions().mode() & 0o777,
            0o200
        );
        fs::set_permissions(&force_file, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            fs::read(&force_file).unwrap(),
            vec![0, 0, b'Z', b'1', b'2', b'3']
        );

        let no_force_file = tmp.join(format!("shred-no-force-{suffix}.bin"));
        fs::write(&no_force_file, b"locked").unwrap();
        fs::set_permissions(&no_force_file, fs::Permissions::from_mode(0o400)).unwrap();

        let mut fro_no_force_args = flags;
        fro_no_force_args.extend(["-n", "0", "-z", no_force_file.to_str().unwrap()]);
        let sys_no_force_args = ["-n", "0", "-z", no_force_file.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_no_force_args),
            run_system("shred", &sys_no_force_args),
            &format!("shred no-force {:?}", fro_no_force_args),
        );
    }
}

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

#[test]
fn cp_preserve_timestamps_attr_list_matches_system() {
    use std::os::unix::fs::MetadataExt;

    let tmp = unique_temp_dir("fro-coreutils-cp-preserve-timestamps");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source = tmp.join(format!("timestamps-source-{suffix}.txt"));
        let fro_target = tmp.join(format!("timestamps-fro-{suffix}.txt"));
        let sys_target = tmp.join(format!("timestamps-sys-{suffix}.txt"));
        fs::write(&source, b"preserve-timestamps").unwrap();
        set_file_mtime(&source, 1_700_215_000);

        let mut fro_args = flags.clone();
        fro_args.extend([
            "--preserve=timestamps",
            source.to_str().unwrap(),
            fro_target.to_str().unwrap(),
        ]);
        let sys_args = [
            "--preserve=timestamps",
            source.to_str().unwrap(),
            sys_target.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp preserve timestamps {:?}", fro_args),
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            fs::read(&sys_target).unwrap()
        );
        let fro_meta = fs::metadata(&fro_target).unwrap();
        let sys_meta = fs::metadata(&sys_target).unwrap();
        assert_eq!(fro_meta.mtime(), sys_meta.mtime());
        assert_eq!(fro_meta.mtime_nsec(), sys_meta.mtime_nsec());

        let source_root = tmp.join(format!("timestamps-tree-src-{suffix}"));
        let fro_dest_parent = tmp.join(format!("timestamps-tree-fro-{suffix}"));
        let sys_dest_parent = tmp.join(format!("timestamps-tree-sys-{suffix}"));
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

        set_file_mtime(&file, 1_700_215_010);
        set_file_mtime(&nested.join("large.bin"), 1_700_215_020);
        set_file_mtime(&nested_dir, 1_700_215_030);
        set_file_mtime(&source_root, 1_700_215_040);
        set_symlink_mtime(&link, 1_700_215_050);

        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-rP",
            "--preserve=mode,timestamps",
            source_root.to_str().unwrap(),
            fro_dest_parent.to_str().unwrap(),
        ]);
        let sys_recursive_args = [
            "-rP",
            "--preserve=mode,timestamps",
            source_root.to_str().unwrap(),
            sys_dest_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_args),
            run_system("cp", &sys_recursive_args),
            &format!("cp preserve attr-list recursive {:?}", fro_recursive_args),
        );

        let copied_name = source_root.file_name().unwrap();
        let fro_root = fro_dest_parent.join(copied_name);
        let sys_root = sys_dest_parent.join(copied_name);
        assert_eq!(snapshot_tree(&fro_root), snapshot_tree(&sys_root));

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

        let fro_link_meta = fs::symlink_metadata(fro_root.join("nested/link-small")).unwrap();
        let sys_link_meta = fs::symlink_metadata(sys_root.join("nested/link-small")).unwrap();
        assert_eq!(fro_link_meta.mtime(), sys_link_meta.mtime());
        assert_eq!(fro_link_meta.mtime_nsec(), sys_link_meta.mtime_nsec());
    }
}

#[test]
fn cp_archive_matches_system_for_recursive_timestamps() {
    use std::os::unix::fs::MetadataExt;

    let tmp = unique_temp_dir("fro-coreutils-cp-archive");

    for archive_flag in ["-a", "--archive"] {
        for flags in io_flag_sets() {
            let suffix = if flags.is_empty() {
                "auto".to_string()
            } else {
                flags.join("_").replace("--", "")
            };
            let archive_name = archive_flag.trim_start_matches('-').replace('-', "_");

            let source_root = tmp.join(format!("archive-src-{archive_name}-{suffix}"));
            let fro_dest_parent = tmp.join(format!("archive-fro-{archive_name}-{suffix}"));
            let sys_dest_parent = tmp.join(format!("archive-sys-{archive_name}-{suffix}"));
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

            set_file_mtime(&file, 1_700_220_000);
            set_file_mtime(&nested.join("large.bin"), 1_700_220_010);
            set_file_mtime(&nested_dir, 1_700_220_020);
            set_file_mtime(&source_root, 1_700_220_030);
            set_symlink_mtime(&link, 1_700_220_040);

            let mut fro_args = flags.clone();
            fro_args.extend([
                archive_flag,
                source_root.to_str().unwrap(),
                fro_dest_parent.to_str().unwrap(),
            ]);
            let sys_args = [
                archive_flag,
                source_root.to_str().unwrap(),
                sys_dest_parent.to_str().unwrap(),
            ];
            assert_same_result(
                run_fro("cp", &fro_args),
                run_system("cp", &sys_args),
                &format!("cp archive recursive {:?}", fro_args),
            );

            let copied_name = source_root.file_name().unwrap();
            let fro_root = fro_dest_parent.join(copied_name);
            let sys_root = sys_dest_parent.join(copied_name);
            assert_eq!(snapshot_tree(&fro_root), snapshot_tree(&sys_root));

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

            let fro_link_meta = fs::symlink_metadata(fro_root.join("nested/link-small")).unwrap();
            let sys_link_meta = fs::symlink_metadata(sys_root.join("nested/link-small")).unwrap();
            assert_eq!(fro_link_meta.mtime(), sys_link_meta.mtime());
            assert_eq!(fro_link_meta.mtime_nsec(), sys_link_meta.mtime_nsec());
        }
    }
}

#[test]
fn cp_no_dereference_matches_system_for_symlink_sources() {
    let tmp = unique_temp_dir("fro-coreutils-cp-no-dereference");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source_root = tmp.join(format!("symlink-src-{suffix}"));
        let target_root = tmp.join(format!("symlink-target-{suffix}"));
        fs::create_dir_all(source_root.join("dir")).unwrap();
        fs::write(source_root.join("dir/file.txt"), b"linked-data").unwrap();
        symlink("dir/file.txt", source_root.join("file-link")).unwrap();
        symlink("dir", source_root.join("dir-link")).unwrap();
        let file_link = source_root.join("file-link");
        let dir_link = source_root.join("dir-link");

        let fro_file_target = target_root.join("fro-file-link");
        let sys_file_target = target_root.join("sys-file-link");
        fs::create_dir_all(&target_root).unwrap();

        let mut fro_file_args = flags.clone();
        fro_file_args.extend([
            "-P",
            file_link.to_str().unwrap(),
            fro_file_target.to_str().unwrap(),
        ]);
        let sys_file_args = [
            "-P",
            file_link.to_str().unwrap(),
            sys_file_target.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_file_args),
            run_system("cp", &sys_file_args),
            &format!("cp -P file symlink {:?}", fro_file_args),
        );
        assert_eq!(
            fs::read_link(&fro_file_target).unwrap(),
            fs::read_link(&sys_file_target).unwrap()
        );

        let fro_dir_target = target_root.join("fro-dir-link");
        let sys_dir_target = target_root.join("sys-dir-link");
        let mut fro_dir_args = flags.clone();
        fro_dir_args.extend([
            "-P",
            dir_link.to_str().unwrap(),
            fro_dir_target.to_str().unwrap(),
        ]);
        let sys_dir_args = [
            "-P",
            dir_link.to_str().unwrap(),
            sys_dir_target.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_dir_args),
            run_system("cp", &sys_dir_args),
            &format!("cp -P dir symlink {:?}", fro_dir_args),
        );
        assert_eq!(
            fs::read_link(&fro_dir_target).unwrap(),
            fs::read_link(&sys_dir_target).unwrap()
        );

        let fro_recursive_parent = target_root.join("fro-recursive");
        let sys_recursive_parent = target_root.join("sys-recursive");
        fs::create_dir_all(&fro_recursive_parent).unwrap();
        fs::create_dir_all(&sys_recursive_parent).unwrap();
        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-rP",
            dir_link.to_str().unwrap(),
            fro_recursive_parent.to_str().unwrap(),
        ]);
        let sys_recursive_args = [
            "-rP",
            dir_link.to_str().unwrap(),
            sys_recursive_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_args),
            run_system("cp", &sys_recursive_args),
            &format!("cp -rP dir symlink {:?}", fro_recursive_args),
        );
        assert_eq!(
            snapshot_tree(&fro_recursive_parent),
            snapshot_tree(&sys_recursive_parent),
            "recursive -P tree mismatch for {:?}",
            fro_recursive_args
        );

        let fro_recursive_file_parent = target_root.join("fro-recursive-file");
        let sys_recursive_file_parent = target_root.join("sys-recursive-file");
        fs::create_dir_all(&fro_recursive_file_parent).unwrap();
        fs::create_dir_all(&sys_recursive_file_parent).unwrap();
        let mut fro_recursive_file_args = flags.clone();
        fro_recursive_file_args.extend([
            "-RP",
            file_link.to_str().unwrap(),
            fro_recursive_file_parent.to_str().unwrap(),
        ]);
        let sys_recursive_file_args = [
            "-RP",
            file_link.to_str().unwrap(),
            sys_recursive_file_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_file_args),
            run_system("cp", &sys_recursive_file_args),
            &format!("cp -RP file symlink {:?}", fro_recursive_file_args),
        );
        assert_eq!(
            snapshot_tree(&fro_recursive_file_parent),
            snapshot_tree(&sys_recursive_file_parent),
            "recursive file -P tree mismatch for {:?}",
            fro_recursive_file_args
        );
    }
}

#[test]
fn cp_verbose_matches_system_for_copy_and_skip_cases() {
    let tmp = unique_temp_dir("fro-coreutils-cp-verbose");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source = tmp.join(format!("verbose-source-{suffix}.txt"));
        let target = tmp.join(format!("verbose-target-{suffix}.txt"));
        fs::write(&source, b"hello-verbose").unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend(["-v", source.to_str().unwrap(), target.to_str().unwrap()]);
        let sys_args = ["-v", source.to_str().unwrap(), target.to_str().unwrap()];
        let system = run_system("cp", &sys_args);
        let expected = fs::read(&target).unwrap();
        fs::remove_file(&target).unwrap();
        let fro = run_fro("cp", &fro_args);
        assert_same_result(fro, system, &format!("cp verbose {:?}", fro_args));
        assert_eq!(fs::read(&target).unwrap(), expected);

        let skip_source = tmp.join(format!("verbose-skip-source-{suffix}.txt"));
        let skip_target = tmp.join(format!("verbose-skip-target-{suffix}.txt"));
        fs::write(&skip_source, b"source-skip").unwrap();
        fs::write(&skip_target, b"dest-skip").unwrap();

        let mut fro_skip_args = flags.clone();
        fro_skip_args.extend([
            "-vn",
            skip_source.to_str().unwrap(),
            skip_target.to_str().unwrap(),
        ]);
        let sys_skip_args = [
            "-vn",
            skip_source.to_str().unwrap(),
            skip_target.to_str().unwrap(),
        ];
        let system_skip = run_system("cp", &sys_skip_args);
        let expected_skip = fs::read(&skip_target).unwrap();
        fs::write(&skip_target, b"dest-skip").unwrap();
        let fro_skip = run_fro("cp", &fro_skip_args);
        assert_same_result(
            fro_skip,
            system_skip,
            &format!("cp verbose no-clobber {:?}", fro_skip_args),
        );
        assert_eq!(fs::read(&skip_target).unwrap(), expected_skip);
    }
}

#[test]
fn cp_no_target_directory_matches_system_for_file_and_recursive_copy() {
    let tmp = unique_temp_dir("fro-coreutils-cp-no-target-directory");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let source = tmp.join(format!("source-{suffix}.txt"));
        let target_dir = tmp.join(format!("target-dir-{suffix}"));
        fs::write(&source, b"no-target-directory").unwrap();
        fs::create_dir_all(&target_dir).unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend(["-T", source.to_str().unwrap(), target_dir.to_str().unwrap()]);
        let sys_args = ["-T", source.to_str().unwrap(), target_dir.to_str().unwrap()];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp -T file into existing dir {:?}", fro_args),
        );

        let source_root = tmp.join(format!("tree-src-{suffix}"));
        let nested = source_root.join("nested/deeper");
        fs::create_dir_all(&nested).unwrap();
        fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
        fs::write(
            nested.join("large.bin"),
            (0..(256 * 1024 + 333))
                .map(|i| ((i * 13) % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let fro_dest = tmp.join(format!("tree-fro-{suffix}"));
        let sys_dest = tmp.join(format!("tree-sys-{suffix}"));
        fs::create_dir_all(&fro_dest).unwrap();
        fs::create_dir_all(&sys_dest).unwrap();

        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-rT",
            source_root.to_str().unwrap(),
            fro_dest.to_str().unwrap(),
        ]);
        let sys_recursive_args = [
            "-rT",
            source_root.to_str().unwrap(),
            sys_dest.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_args),
            run_system("cp", &sys_recursive_args),
            &format!("cp recursive -T {:?}", fro_recursive_args),
        );

        let fro_tree = snapshot_tree(&fro_dest);
        let sys_tree = snapshot_tree(&sys_dest);
        assert_eq!(
            fro_tree, sys_tree,
            "recursive -T tree mismatch for {:?}",
            fro_recursive_args
        );
    }
}

#[test]
fn cp_target_directory_matches_system_for_multi_source_and_recursive_copy() {
    let tmp = unique_temp_dir("fro-coreutils-cp-target-directory");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        let src_a = tmp.join(format!("source-a-{suffix}.txt"));
        let src_b = tmp.join(format!("source-b-{suffix}.txt"));
        let fro_dest = tmp.join(format!("dest-fro-{suffix}"));
        let sys_dest = tmp.join(format!("dest-sys-{suffix}"));
        fs::write(&src_a, b"alpha-target-dir").unwrap();
        fs::write(&src_b, b"beta-target-dir").unwrap();
        fs::create_dir_all(&fro_dest).unwrap();
        fs::create_dir_all(&sys_dest).unwrap();

        let mut fro_args = flags.clone();
        fro_args.extend([
            "-t",
            fro_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
            src_b.to_str().unwrap(),
        ]);
        let sys_args = [
            "-t",
            sys_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
            src_b.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp -t multi-source {:?}", fro_args),
        );
        assert_eq!(snapshot_tree(&fro_dest), snapshot_tree(&sys_dest));

        let long_fro_dest = tmp.join(format!("dest-long-fro-{suffix}"));
        let long_sys_dest = tmp.join(format!("dest-long-sys-{suffix}"));
        fs::create_dir_all(&long_fro_dest).unwrap();
        fs::create_dir_all(&long_sys_dest).unwrap();
        let fro_long_target = format!("--target-directory={}", long_fro_dest.display());
        let sys_long_target = format!("--target-directory={}", long_sys_dest.display());
        let mut fro_long_args = flags.clone();
        fro_long_args.extend([fro_long_target.as_str(), src_a.to_str().unwrap()]);
        let sys_long_args = [sys_long_target.as_str(), src_a.to_str().unwrap()];
        assert_same_result(
            run_fro("cp", &fro_long_args),
            run_system("cp", &sys_long_args),
            &format!("cp --target-directory {:?}", fro_long_args),
        );
        assert_eq!(snapshot_tree(&long_fro_dest), snapshot_tree(&long_sys_dest));

        let sep_fro_dest = tmp.join(format!("dest-sep-fro-{suffix}"));
        let sep_sys_dest = tmp.join(format!("dest-sep-sys-{suffix}"));
        fs::create_dir_all(&sep_fro_dest).unwrap();
        fs::create_dir_all(&sep_sys_dest).unwrap();
        let mut fro_sep_args = flags.clone();
        fro_sep_args.extend([
            "--target-directory",
            sep_fro_dest.to_str().unwrap(),
            src_b.to_str().unwrap(),
        ]);
        let sys_sep_args = [
            "--target-directory",
            sep_sys_dest.to_str().unwrap(),
            src_b.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_sep_args),
            run_system("cp", &sys_sep_args),
            &format!("cp --target-directory separated {:?}", fro_sep_args),
        );
        assert_eq!(snapshot_tree(&sep_fro_dest), snapshot_tree(&sep_sys_dest));

        let source_root = tmp.join(format!("tree-src-target-{suffix}"));
        fs::create_dir_all(source_root.join("nested")).unwrap();
        fs::write(source_root.join("nested/file.txt"), b"recursive-target-dir").unwrap();
        let fro_recursive_dest = tmp.join(format!("tree-dest-fro-{suffix}"));
        let sys_recursive_dest = tmp.join(format!("tree-dest-sys-{suffix}"));
        fs::create_dir_all(&fro_recursive_dest).unwrap();
        fs::create_dir_all(&sys_recursive_dest).unwrap();

        let mut fro_recursive_args = flags.clone();
        fro_recursive_args.extend([
            "-rt",
            fro_recursive_dest.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ]);
        let sys_recursive_args = [
            "-rt",
            sys_recursive_dest.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_recursive_args),
            run_system("cp", &sys_recursive_args),
            &format!("cp -rt {:?}", fro_recursive_args),
        );
        let copied_name = source_root.file_name().unwrap();
        assert_eq!(
            snapshot_tree(&fro_recursive_dest.join(copied_name)),
            snapshot_tree(&sys_recursive_dest.join(copied_name))
        );

        let missing_dest = tmp.join(format!("missing-{suffix}"));
        let mut fro_missing_args = flags.clone();
        fro_missing_args.extend([
            "-t",
            missing_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ]);
        let sys_missing_args = [
            "-t",
            missing_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_missing_args),
            run_system("cp", &sys_missing_args),
            &format!("cp -t missing target {:?}", fro_missing_args),
        );

        let non_dir_target = tmp.join(format!("not-a-dir-{suffix}.txt"));
        fs::write(&non_dir_target, b"plain-file").unwrap();
        let mut fro_non_dir_args = flags.clone();
        fro_non_dir_args.extend([
            "-t",
            non_dir_target.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ]);
        let sys_non_dir_args = [
            "-t",
            non_dir_target.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_non_dir_args),
            run_system("cp", &sys_non_dir_args),
            &format!("cp -t non-directory {:?}", fro_non_dir_args),
        );

        let mut fro_conflict_args = flags.clone();
        fro_conflict_args.extend([
            "-T",
            "-t",
            fro_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ]);
        let sys_conflict_args = [
            "-T",
            "-t",
            fro_dest.to_str().unwrap(),
            src_a.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_conflict_args),
            run_system("cp", &sys_conflict_args),
            &format!("cp -T -t conflict {:?}", fro_conflict_args),
        );
    }
}
