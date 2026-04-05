use super::*;

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
fn cartesian_mv_file_and_recursive_directory_match_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-rm-mv-mv-matrix");

    for verbose_flags in [Vec::<&str>::new(), vec!["-v"]] {
        let tag = if verbose_flags.is_empty() { "plain" } else { "verbose" };

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
        assert_eq!(fs::read(&fro_dst_file).unwrap(), fs::read(&sys_dst_file).unwrap());
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
