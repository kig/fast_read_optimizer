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
