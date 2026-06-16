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
        let fro_zero_long = tmp.join(format!("shred-fro-zero-long-{}.bin", flags.join("_")));
        let sys_zero_long = tmp.join(format!("shred-sys-zero-long-{}.bin", flags.join("_")));
        fs::write(&fro_zero_long, vec![0x44; 32768]).unwrap();
        fs::write(&sys_zero_long, vec![0x44; 32768]).unwrap();
        let mut fro_zero_long_args = flags.clone();
        fro_zero_long_args.extend(["--iterations=0", "--zero", fro_zero_long.to_str().unwrap()]);
        let sys_zero_long_args = ["--iterations=0", "--zero", sys_zero_long.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_zero_long_args),
            run_system("shred", &sys_zero_long_args),
            &format!("shred zero long {:?}", fro_zero_long_args),
        );
        assert_eq!(
            fs::read(&fro_zero_long).unwrap(),
            fs::read(&sys_zero_long).unwrap()
        );
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
        let fro_remove_long = tmp.join("shred-fro-remove-long.bin");
        let sys_remove_long = tmp.join("shred-sys-remove-long.bin");
        fs::write(&fro_remove_long, vec![0x22; 32768]).unwrap();
        fs::write(&sys_remove_long, vec![0x22; 32768]).unwrap();
        let fro_remove_long_args = [
            "--iterations",
            "0",
            "--remove",
            fro_remove_long.to_str().unwrap(),
        ];
        let sys_remove_long_args = [
            "--iterations",
            "0",
            "--remove",
            sys_remove_long.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("shred", &fro_remove_long_args),
            run_system("shred", &sys_remove_long_args),
            &format!("shred remove long {:?}", fro_remove_long_args),
        );
        assert_eq!(fro_remove_long.exists(), sys_remove_long.exists());
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
        let exact_file = tmp.join(format!("shred-exact-{suffix}.bin"));
        fs::write(&exact_file, b"abcdefghij").unwrap();
        let mut fro_exact_args = flags.clone();
        fro_exact_args.extend([
            "--iterations",
            "0",
            "--zero",
            "--exact",
            "--size=4",
            exact_file.to_str().unwrap(),
        ]);
        let sys_exact_args = [
            "--iterations",
            "0",
            "--zero",
            "--exact",
            "--size=4",
            exact_file.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("shred", &fro_exact_args),
            run_system("shred", &sys_exact_args),
            &format!("shred exact {:?}", fro_exact_args),
        );
        assert_eq!(
            fs::read(&exact_file).unwrap(),
            vec![0, 0, 0, 0, b'e', b'f', b'g', b'h', b'i', b'j']
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
