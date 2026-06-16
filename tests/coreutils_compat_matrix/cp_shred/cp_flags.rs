use super::*;

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
fn cp_preserve_mode_and_all_match_system_for_single_file_metadata() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let tmp = unique_temp_dir("fro-coreutils-cp-preserve-mode-all");

    for preserve_flag in ["--preserve=mode", "--preserve=all"] {
        for flags in io_flag_sets() {
            let suffix = if flags.is_empty() {
                "auto".to_string()
            } else {
                flags.join("_").replace("--", "")
            };
            let flag_name = preserve_flag
                .trim_start_matches('-')
                .replace('=', "_")
                .replace(',', "_");

            let source = tmp.join(format!("{flag_name}-source-{suffix}.txt"));
            let fro_target = tmp.join(format!("{flag_name}-fro-{suffix}.txt"));
            let sys_target = tmp.join(format!("{flag_name}-sys-{suffix}.txt"));
            fs::write(&source, b"preserve-mode").unwrap();
            fs::set_permissions(&source, fs::Permissions::from_mode(0o751)).unwrap();
            set_file_mtime(&source, 1_700_216_000);

            let mut fro_args = flags.clone();
            fro_args.extend([
                preserve_flag,
                source.to_str().unwrap(),
                fro_target.to_str().unwrap(),
            ]);
            let sys_args = [
                preserve_flag,
                source.to_str().unwrap(),
                sys_target.to_str().unwrap(),
            ];
            assert_same_result(
                run_fro("cp", &fro_args),
                run_system("cp", &sys_args),
                &format!("cp preserve metadata {:?}", fro_args),
            );
            assert_eq!(
                fs::read(&fro_target).unwrap(),
                fs::read(&sys_target).unwrap()
            );

            let fro_meta = fs::metadata(&fro_target).unwrap();
            let sys_meta = fs::metadata(&sys_target).unwrap();
            assert_eq!(
                fro_meta.permissions().mode() & 0o7777,
                sys_meta.permissions().mode() & 0o7777
            );
            if preserve_flag == "--preserve=all" {
                assert_eq!(fro_meta.mtime(), sys_meta.mtime());
                assert_eq!(fro_meta.mtime_nsec(), sys_meta.mtime_nsec());
            }
        }
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
fn cp_dereference_matches_system_for_recursive_symlink_sources() {
    let tmp = unique_temp_dir("fro-coreutils-cp-dereference");

    for copy_flag in ["-rL", "-aL"] {
        for flags in io_flag_sets() {
            let suffix = if flags.is_empty() {
                "auto".to_string()
            } else {
                flags.join("_").replace("--", "")
            };
            let flag_name = copy_flag.trim_start_matches('-');
            let source_root = tmp.join(format!("dereference-src-{flag_name}-{suffix}"));
            let fro_dest_parent = tmp.join(format!("dereference-fro-{flag_name}-{suffix}"));
            let sys_dest_parent = tmp.join(format!("dereference-sys-{flag_name}-{suffix}"));
            fs::create_dir_all(source_root.join("dir")).unwrap();
            fs::create_dir_all(&fro_dest_parent).unwrap();
            fs::create_dir_all(&sys_dest_parent).unwrap();

            fs::write(source_root.join("dir/file.txt"), b"linked-data").unwrap();
            symlink("dir/file.txt", source_root.join("file-link")).unwrap();
            symlink("dir", source_root.join("dir-link")).unwrap();

            let mut fro_args = flags.clone();
            fro_args.extend([
                copy_flag,
                source_root.to_str().unwrap(),
                fro_dest_parent.to_str().unwrap(),
            ]);
            let sys_args = [
                copy_flag,
                source_root.to_str().unwrap(),
                sys_dest_parent.to_str().unwrap(),
            ];
            assert_same_result(
                run_fro("cp", &fro_args),
                run_system("cp", &sys_args),
                &format!("cp dereference recursive {:?}", fro_args),
            );

            let copied_name = source_root.file_name().unwrap();
            let fro_root = fro_dest_parent.join(copied_name);
            let sys_root = sys_dest_parent.join(copied_name);
            assert_eq!(snapshot_tree(&fro_root), snapshot_tree(&sys_root));
            assert!(fs::metadata(fro_root.join("file-link")).unwrap().is_file());
            assert!(fs::metadata(fro_root.join("dir-link")).unwrap().is_dir());
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
