#[cfg(unix)]
mod unix_cli_matrix {
    use std::fs;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process::{self, Command, Output};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "{}-{}-{}",
            prefix,
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn run_fro(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_fro"))
            .args(args)
            .output()
            .expect("failed to run fro")
    }

    fn assert_success(name: &str, out: Output) -> String {
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            out.status.success(),
            "{name} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        format!("{stdout}{stderr}")
    }

    fn for_each_combo<'a>(
        groups: &[&'a [&'a [&'a str]]],
        current: &mut Vec<&'a str>,
        f: &mut impl FnMut(&[&'a str]),
    ) {
        if let Some((first, rest)) = groups.split_first() {
            for option in *first {
                let start_len = current.len();
                current.extend(option.iter().copied());
                for_each_combo(rest, current, f);
                current.truncate(start_len);
            }
        } else {
            f(current);
        }
    }

    fn sample_bytes(len: usize) -> Vec<u8> {
        (0..len).map(|i| ((i * 17) % 251) as u8).collect()
    }

    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn running_as_root() -> bool {
        unsafe { libc::geteuid() == 0 }
    }

    #[derive(Clone, Copy)]
    enum ResultClass {
        Success,
        PermissionDenied,
        InvalidInputClass,
    }

    #[derive(Clone, Copy)]
    struct ExpectedClass {
        unprivileged: ResultClass,
        privileged: ResultClass,
    }

    impl ExpectedClass {
        const fn same(class: ResultClass) -> Self {
            Self {
                unprivileged: class,
                privileged: class,
            }
        }

        const fn privilege_sensitive(unprivileged: ResultClass, privileged: ResultClass) -> Self {
            Self {
                unprivileged,
                privileged,
            }
        }

        fn resolve(self) -> ResultClass {
            if running_as_root() {
                self.privileged
            } else {
                self.unprivileged
            }
        }
    }

    fn assert_cli_class(name: &str, out: Output, expected: ExpectedClass) {
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        match expected.resolve() {
            ResultClass::Success => assert!(
                out.status.success(),
                "{name}: expected success\noutput:\n{combined}"
            ),
            ResultClass::PermissionDenied => assert!(
                !out.status.success() && combined.contains("Permission denied"),
                "{name}: expected permission denied\noutput:\n{combined}"
            ),
            ResultClass::InvalidInputClass => assert!(
                !out.status.success() && !combined.contains("Permission denied"),
                "{name}: expected invalid-input-class failure\noutput:\n{combined}"
            ),
        }
    }

    fn hash_sidecars(path: &Path) {
        assert_success("hash sidecars", run_fro(&["hash", path.to_str().unwrap()]));
    }

    #[test]
    fn cli_flag_matrix_runs_on_valid_small_inputs() {
        let tmp = unique_temp_dir("fro-cli-flag-matrix");
        let read_path = tmp.join("read.bin");
        let grep_path = tmp.join("grep.bin");
        let write_existing = tmp.join("write-existing.bin");
        let diff_a = tmp.join("diff-a.bin");
        let diff_b = tmp.join("diff-b.bin");
        let copy_src = tmp.join("copy-src.bin");
        let recover_backup = tmp.join("recover-backup.bin");
        let base = sample_bytes(64 * 1024 + 333);
        let grep_bytes = {
            let mut bytes = sample_bytes(16 * 1024);
            bytes[4094..4098].copy_from_slice(b"NEED");
            bytes
        };
        fs::write(&read_path, &base).unwrap();
        fs::write(&grep_path, &grep_bytes).unwrap();
        fs::write(&write_existing, &base[..8192]).unwrap();
        fs::write(&diff_a, &base).unwrap();
        fs::write(&diff_b, &base).unwrap();
        fs::write(&copy_src, &base).unwrap();
        fs::write(&recover_backup, &base).unwrap();

        let io_opts: &[&[&str]] = &[&[], &["--no-direct"], &["--direct"]];
        let verbose_opts: &[&[&str]] = &[&[], &["-v"]];

        for_each_combo(&[io_opts, &[&[], &["--to-memory"]], verbose_opts], &mut Vec::new(), &mut |flags| {
            let mut args = vec!["read"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", read_path.to_str().unwrap()]);
            assert_success(&format!("read {:?}", flags), run_fro(&args));
        });

        for_each_combo(&[io_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let mut args = vec!["grep"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", "NEED", grep_path.to_str().unwrap()]);
            let output = assert_success(&format!("grep {:?}", flags), run_fro(&args));
            assert!(output.contains("4094:NEED"));
        });

        let write_mode_opts: &[&[&str]] = &[&[], &["--no-direct-write"], &["--direct-write"]];
        let create_opts: &[&[&str]] = &[&[], &["--create", "64KiB"]];
        for_each_combo(&[create_opts, io_opts, write_mode_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let target = tmp.join(format!("write-{:x}.bin", fxhash(flags)));
            if !flags.contains(&"--create") {
                fs::write(&target, &base[..4096]).unwrap();
            }
            let mut args = vec!["write"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", target.to_str().unwrap()]);
            assert_success(&format!("write {:?}", flags), run_fro(&args));
            assert!(target.exists());
        });

        for_each_combo(&[io_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let mut args = vec!["diff"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", diff_a.to_str().unwrap(), diff_b.to_str().unwrap()]);
            assert_success(&format!("diff {:?}", flags), run_fro(&args));
        });

        let hash_algo_opts: &[&[&str]] = &[&[], &["--sha256"]];
        let hash_only_opts: &[&[&str]] = &[&[], &["--hash-only"]];
        for_each_combo(&[io_opts, hash_algo_opts, hash_only_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let target = tmp.join(format!("hash-{:x}.bin", fxhash(flags)));
            fs::write(&target, &base).unwrap();
            let mut args = vec!["hash"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", target.to_str().unwrap()]);
            assert_success(&format!("hash {:?}", flags), run_fro(&args));
        });

        for_each_combo(&[io_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let target = tmp.join(format!("verify-{:x}.bin", fxhash(flags)));
            fs::write(&target, &base).unwrap();
            hash_sidecars(&target);
            let mut args = vec!["verify"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", target.to_str().unwrap()]);
            let output = assert_success(&format!("verify {:?}", flags), run_fro(&args));
            assert!(output.contains("bad_blocks=0"));
        });

        let recover_mode_opts: &[&[&str]] = &[&[], &["--fast"], &["--in-place-all"]];
        for_each_combo(&[io_opts, recover_mode_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let target = tmp.join(format!("recover-target-{:x}.bin", fxhash(flags)));
            let backup = tmp.join(format!("recover-backup-{:x}.bin", fxhash(flags)));
            fs::write(&target, &base).unwrap();
            fs::write(&backup, &base).unwrap();
            hash_sidecars(&target);
            hash_sidecars(&backup);
            let mut file = fs::OpenOptions::new().write(true).open(&target).unwrap();
            file.seek(SeekFrom::Start(1024)).unwrap();
            file.write_all(b"broken-block").unwrap();
            file.flush().unwrap();

            let mut args = vec!["recover"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", target.to_str().unwrap(), backup.to_str().unwrap()]);
            assert_success(&format!("recover {:?}", flags), run_fro(&args));
            assert_eq!(fs::read(&target).unwrap(), base);
        });

        let copy_strategy_opts: &[&[&str]] = &[
            &[],
            &["--threaded-copy"],
            &["--copy-file-range"],
            &["--copy-file-range-single"],
        ];
        let copy_rewrite_opts: &[&[&str]] = &[&[], &["--full"], &["--diff"]];
        let copy_io_opts: &[&[&str]] = &[&[], &["--no-direct"], &["--no-direct", "--direct-write"]];
        for_each_combo(
            &[copy_strategy_opts, copy_rewrite_opts, copy_io_opts, verbose_opts],
            &mut Vec::new(),
            &mut |flags| {
                let is_diff = flags.contains(&"--diff");
                let is_keep_size = false;
                let uses_copy_range = flags.contains(&"--copy-file-range");
                let uses_copy_range_single = flags.contains(&"--copy-file-range-single");
                let uses_direct_write = flags.contains(&"--direct-write");
                if (is_diff && (uses_copy_range || uses_copy_range_single))
                    || ((uses_copy_range || uses_copy_range_single) && uses_direct_write)
                {
                    return;
                }

                let src = tmp.join(format!("copy-src-{:x}.bin", fxhash(flags)));
                let dst = tmp.join(format!("copy-dst-{:x}.bin", fxhash(flags)));
                fs::write(&src, &base).unwrap();
                let mut target_bytes = base.clone();
                if is_diff {
                    target_bytes[2048..4096].fill(0x5a);
                }
                fs::write(&dst, &target_bytes).unwrap();

                let mut args = vec!["copy"];
                args.extend_from_slice(flags);
                args.extend_from_slice(&["-n", "1", src.to_str().unwrap(), dst.to_str().unwrap()]);
                assert_success(&format!("copy {:?}", flags), run_fro(&args));
                assert_eq!(fs::read(&dst).unwrap(), base, "{:?}", flags);
                assert!(!is_keep_size);
            },
        );

        let via_memory_io_opts: &[&[&str]] = &[&["--no-direct"], &["--direct"]];
        let via_memory_write_opts: &[&[&str]] = &[&[], &["--direct-write"]];
        for_each_combo(&[via_memory_io_opts, via_memory_write_opts, verbose_opts], &mut Vec::new(), &mut |flags| {
            let src = tmp.join(format!("copy-mem-src-{:x}.bin", fxhash(flags)));
            let dst = tmp.join(format!("copy-mem-dst-{:x}.bin", fxhash(flags)));
            fs::write(&src, &base).unwrap();
            let mut args = vec!["copy", "--via-memory"];
            args.extend_from_slice(flags);
            args.extend_from_slice(&["-n", "1", src.to_str().unwrap(), dst.to_str().unwrap()]);
            assert_success(&format!("copy via-memory {:?}", flags), run_fro(&args));
            assert_eq!(fs::read(&dst).unwrap(), base);
        });
    }

    #[test]
    fn cli_no_flags_file_and_permission_matrix() {
        let tmp = unique_temp_dir("fro-cli-no-flags-matrix");
        let readable = tmp.join("readable.bin");
        let unreadable = tmp.join("unreadable.bin");
        let directory = tmp.join("directory");
        let readable_symlink = tmp.join("readable-link");
        let directory_symlink = tmp.join("directory-link");
        let unreadable_symlink = tmp.join("unreadable-link");
        let copy_dest = tmp.join("copy-dest.bin");
        let readonly_dest = tmp.join("readonly-dest.bin");
        let locked_dir = tmp.join("locked-dir");
        let bytes = b"needle-and-bytes".repeat(512);

        fs::write(&readable, &bytes).unwrap();
        fs::write(&unreadable, &bytes).unwrap();
        set_mode(&unreadable, 0o000);
        fs::create_dir(&directory).unwrap();
        symlink(&readable, &readable_symlink).unwrap();
        symlink(&directory, &directory_symlink).unwrap();
        symlink(&unreadable, &unreadable_symlink).unwrap();
        fs::write(&readonly_dest, b"old").unwrap();
        set_mode(&readonly_dest, 0o444);
        fs::create_dir(&locked_dir).unwrap();
        set_mode(&locked_dir, 0o555);

        for (name, path, expected) in [
            ("read-regular", readable.as_path(), ExpectedClass::same(ResultClass::Success)),
            (
                "read-0000",
                unreadable.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
            ),
            (
                "read-directory",
                directory.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
            ),
            (
                "read-symlink",
                readable_symlink.as_path(),
                ExpectedClass::same(ResultClass::Success),
            ),
        ] {
            assert_cli_class(name, run_fro(&["read", path.to_str().unwrap()]), expected);
            assert_cli_class(
                &format!("grep-{name}"),
                run_fro(&["grep", "needle", path.to_str().unwrap()]),
                expected,
            );
            assert_cli_class(name, run_fro(&["hash", path.to_str().unwrap()]), expected);
        }

        hash_sidecars(&readable);
        hash_sidecars(&readable_symlink);
        for (name, path, expected) in [
            (
                "verify-regular",
                readable.as_path(),
                ExpectedClass::same(ResultClass::Success),
            ),
            (
                "verify-0000",
                unreadable.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
            ),
            (
                "verify-directory",
                directory.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
            ),
            (
                "verify-symlink",
                readable_symlink.as_path(),
                ExpectedClass::same(ResultClass::Success),
            ),
        ] {
            assert_cli_class(name, run_fro(&["verify", path.to_str().unwrap()]), expected);
        }

        fs::write(&copy_dest, b"old").unwrap();
        assert_cli_class(
            "write-regular",
            run_fro(&["write", copy_dest.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::Success),
        );
        assert_cli_class(
            "write-directory",
            run_fro(&["write", directory.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::InvalidInputClass),
        );
        assert_cli_class(
            "write-readonly",
            run_fro(&["write", readonly_dest.to_str().unwrap()]),
            ExpectedClass::privilege_sensitive(
                ResultClass::PermissionDenied,
                ResultClass::Success,
            ),
        );

        assert_cli_class(
            "copy-regular",
            run_fro(&["copy", readable.to_str().unwrap(), copy_dest.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::Success),
        );
        assert_cli_class(
            "copy-directory-source",
            run_fro(&["copy", directory.to_str().unwrap(), copy_dest.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::InvalidInputClass),
        );
        assert_cli_class(
            "copy-unreadable-source",
            run_fro(&["copy", unreadable.to_str().unwrap(), copy_dest.to_str().unwrap()]),
            ExpectedClass::privilege_sensitive(
                ResultClass::PermissionDenied,
                ResultClass::Success,
            ),
        );
        assert_cli_class(
            "copy-directory-dest",
            run_fro(&["copy", readable.to_str().unwrap(), directory.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::InvalidInputClass),
        );

        assert_cli_class(
            "diff-regular",
            run_fro(&["diff", readable.to_str().unwrap(), readable_symlink.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::Success),
        );
        assert_cli_class(
            "diff-directory",
            run_fro(&["diff", readable.to_str().unwrap(), directory.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::InvalidInputClass),
        );
        assert_cli_class(
            "diff-0000",
            run_fro(&["diff", readable.to_str().unwrap(), unreadable.to_str().unwrap()]),
            ExpectedClass::privilege_sensitive(
                ResultClass::PermissionDenied,
                ResultClass::Success,
            ),
        );

        let recover_target = tmp.join("recover-target.bin");
        let recover_backup = tmp.join("recover-backup.bin");
        fs::write(&recover_target, &bytes).unwrap();
        fs::write(&recover_backup, &bytes).unwrap();
        hash_sidecars(&recover_target);
        hash_sidecars(&recover_backup);
        let mut file = fs::OpenOptions::new().write(true).open(&recover_target).unwrap();
        file.seek(SeekFrom::Start(128)).unwrap();
        file.write_all(b"oops").unwrap();
        file.flush().unwrap();
        assert_cli_class(
            "recover-regular",
            run_fro(&[
                "recover",
                recover_target.to_str().unwrap(),
                recover_backup.to_str().unwrap(),
            ]),
            ExpectedClass::same(ResultClass::Success),
        );
        assert_cli_class(
            "recover-directory-target",
            run_fro(&["recover", directory.to_str().unwrap(), recover_backup.to_str().unwrap()]),
            ExpectedClass::same(ResultClass::InvalidInputClass),
        );
        assert_cli_class(
            "recover-unreadable-backup",
            run_fro(&["recover", recover_target.to_str().unwrap(), unreadable.to_str().unwrap()]),
            ExpectedClass::privilege_sensitive(
                ResultClass::PermissionDenied,
                ResultClass::Success,
            ),
        );

        let _ = directory_symlink;
        let _ = unreadable_symlink;
    }

    fn fxhash(flags: &[&str]) -> u64 {
        let mut hash = 1469598103934665603_u64;
        for flag in flags {
            for byte in flag.as_bytes() {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(1099511628211);
            }
        }
        hash
    }
}
