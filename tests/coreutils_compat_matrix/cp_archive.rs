use super::*;

#[test]
fn cp_archive_matches_system_for_regular_file_metadata_and_recursive_symlinks() {
    use std::os::unix::fs::MetadataExt;

    let tmp = unique_temp_dir("fro-coreutils-cp-archive");

    for flags in io_flag_sets() {
        let suffix = if flags.is_empty() {
            "auto".to_string()
        } else {
            flags.join("_").replace("--", "")
        };

        for archive_flags in [["-a"], ["--archive"]] {
            let variant = archive_flags
                .iter()
                .map(|flag| flag.trim_start_matches('-').replace('-', "_"))
                .collect::<Vec<_>>()
                .join("_");

            let source = tmp.join(format!("archive-src-{suffix}-{variant}.txt"));
            let fro_target = tmp.join(format!("archive-fro-{suffix}-{variant}.txt"));
            let sys_target = tmp.join(format!("archive-sys-{suffix}-{variant}.txt"));
            fs::write(&source, b"archive-regular-file").unwrap();
            fs::set_permissions(&source, fs::Permissions::from_mode(0o754)).unwrap();
            set_file_mtime(&source, 1_700_220_000);

            let mut fro_args = flags.clone();
            fro_args.extend(archive_flags);
            fro_args.extend([source.to_str().unwrap(), fro_target.to_str().unwrap()]);
            let mut sys_args = archive_flags.to_vec();
            sys_args.extend([source.to_str().unwrap(), sys_target.to_str().unwrap()]);
            assert_same_result(
                run_fro("cp", &fro_args),
                run_system("cp", &sys_args),
                &format!("cp archive regular {:?}", fro_args),
            );
            assert_eq!(
                fs::read(&fro_target).unwrap(),
                fs::read(&sys_target).unwrap()
            );
            let fro_meta = fs::metadata(&fro_target).unwrap();
            let sys_meta = fs::metadata(&sys_target).unwrap();
            assert_eq!(
                fro_meta.permissions().mode() & 0o7777,
                sys_meta.permissions().mode() & 0o7777,
                "archive mode mismatch for {:?}",
                fro_args
            );
            assert_eq!(
                fro_meta.mtime(),
                sys_meta.mtime(),
                "archive mtime mismatch for {:?}",
                fro_args
            );
            assert_eq!(
                fro_meta.mtime_nsec(),
                sys_meta.mtime_nsec(),
                "archive mtime_nsec mismatch for {:?}",
                fro_args
            );

            let source_root = tmp.join(format!("archive-tree-src-{suffix}-{variant}"));
            let fro_dest_parent = tmp.join(format!("archive-tree-fro-{suffix}-{variant}"));
            let sys_dest_parent = tmp.join(format!("archive-tree-sys-{suffix}-{variant}"));
            let nested = source_root.join("nested/deeper");
            fs::create_dir_all(&nested).unwrap();
            fs::create_dir_all(&fro_dest_parent).unwrap();
            fs::create_dir_all(&sys_dest_parent).unwrap();
            fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
            fs::write(nested.join("large.bin"), b"payload").unwrap();
            symlink("../small.txt", source_root.join("nested/link-small")).unwrap();
            fs::set_permissions(
                source_root.join("small.txt"),
                fs::Permissions::from_mode(0o640),
            )
            .unwrap();
            fs::set_permissions(
                source_root.join("nested"),
                fs::Permissions::from_mode(0o751),
            )
            .unwrap();
            set_file_mtime(&source_root.join("small.txt"), 1_700_220_010);
            set_file_mtime(&source_root.join("nested"), 1_700_220_020);
            set_file_mtime(&source_root, 1_700_220_030);

            let mut fro_recursive_args = flags.clone();
            fro_recursive_args.extend(archive_flags);
            fro_recursive_args.extend([
                source_root.to_str().unwrap(),
                fro_dest_parent.to_str().unwrap(),
            ]);
            let mut sys_recursive_args = archive_flags.to_vec();
            sys_recursive_args.extend([
                source_root.to_str().unwrap(),
                sys_dest_parent.to_str().unwrap(),
            ]);
            assert_same_result(
                run_fro("cp", &fro_recursive_args),
                run_system("cp", &sys_recursive_args),
                &format!("cp archive recursive {:?}", fro_recursive_args),
            );

            let copied_name = source_root.file_name().unwrap();
            let fro_root = fro_dest_parent.join(copied_name);
            let sys_root = sys_dest_parent.join(copied_name);
            assert_eq!(
                snapshot_tree(&fro_root),
                snapshot_tree(&sys_root),
                "archive recursive tree mismatch for {:?}",
                fro_recursive_args
            );

            for rel in [
                std::path::Path::new("small.txt"),
                std::path::Path::new("nested"),
                std::path::Path::new("nested/link-small"),
            ] {
                let fro_meta = fs::symlink_metadata(fro_root.join(rel)).unwrap();
                let sys_meta = fs::symlink_metadata(sys_root.join(rel)).unwrap();
                assert_eq!(
                    fro_meta.permissions().mode() & 0o7777,
                    sys_meta.permissions().mode() & 0o7777,
                    "archive recursive mode mismatch for {:?} {:?}",
                    rel,
                    fro_recursive_args
                );
                assert_eq!(
                    fro_meta.mtime(),
                    sys_meta.mtime(),
                    "archive recursive mtime mismatch for {:?} {:?}",
                    rel,
                    fro_recursive_args
                );
                assert_eq!(
                    fro_meta.mtime_nsec(),
                    sys_meta.mtime_nsec(),
                    "archive recursive mtime_nsec mismatch for {:?} {:?}",
                    rel,
                    fro_recursive_args
                );
            }
        }
    }
}
