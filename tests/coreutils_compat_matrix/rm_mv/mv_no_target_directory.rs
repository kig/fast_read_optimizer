use super::*;

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
