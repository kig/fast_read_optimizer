#[cfg(unix)]
mod unix_matrix {
    use std::fs;
    use std::io;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ResultClass {
        Success,
        PermissionDenied,
        InvalidInputClass,
    }

    #[derive(Debug, Clone, Copy)]
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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "{}-{}-{}",
            prefix,
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn running_as_root() -> bool {
        unsafe { libc::geteuid() == 0 }
    }

    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn is_invalid_input_class(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::InvalidInput
            || matches!(
                err.raw_os_error(),
                Some(libc::EISDIR | libc::ENOTDIR | libc::EINVAL | libc::ELOOP | libc::ENODEV)
            )
    }

    fn assert_error_class<T: std::fmt::Debug>(
        name: &str,
        result: io::Result<T>,
        expected: ExpectedClass,
    ) {
        match expected.resolve() {
            ResultClass::Success => {
                assert!(result.is_ok(), "{name}: expected success, got {result:?}");
            }
            ResultClass::PermissionDenied => {
                let err = result.expect_err(name);
                assert_eq!(
                    err.kind(),
                    io::ErrorKind::PermissionDenied,
                    "{name}: expected permission denied, got {err:?}"
                );
            }
            ResultClass::InvalidInputClass => {
                let err = result.expect_err(name);
                assert!(
                    is_invalid_input_class(&err),
                    "{name}: expected invalid-input-class error, got {err:?}"
                );
            }
        }
    }

    #[test]
    fn local_read_api_compatibility_matrix() {
        let tmp = unique_temp_dir("fro-compat-read");
        fs::create_dir_all(&tmp).unwrap();

        let readable = tmp.join("readable.bin");
        let unreadable = tmp.join("unreadable.bin");
        let directory = tmp.join("directory");
        let readable_symlink = tmp.join("readable-link");
        let directory_symlink = tmp.join("directory-link");
        let unreadable_symlink = tmp.join("unreadable-link");
        let bytes = b"compatibility-read-bytes".to_vec();

        fs::write(&readable, &bytes).unwrap();
        fs::write(&unreadable, &bytes).unwrap();
        set_mode(&unreadable, 0o000);
        fs::create_dir(&directory).unwrap();
        symlink(&readable, &readable_symlink).unwrap();
        symlink(&directory, &directory_symlink).unwrap();
        symlink(&unreadable, &unreadable_symlink).unwrap();

        let cases = [
            (
                "regular-0644",
                &readable,
                ExpectedClass::same(ResultClass::Success),
                Some(bytes.clone()),
            ),
            (
                "regular-0000",
                &unreadable,
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
                Some(bytes.clone()),
            ),
            (
                "directory-0755",
                &directory,
                ExpectedClass::same(ResultClass::InvalidInputClass),
                None,
            ),
            (
                "symlink-to-regular",
                &readable_symlink,
                ExpectedClass::same(ResultClass::Success),
                Some(bytes.clone()),
            ),
            (
                "symlink-to-directory",
                &directory_symlink,
                ExpectedClass::same(ResultClass::InvalidInputClass),
                None,
            ),
            (
                "symlink-to-0000",
                &unreadable_symlink,
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
                Some(bytes.clone()),
            ),
        ];

        for (name, path, expected, expected_bytes) in cases {
            match expected.resolve() {
                ResultClass::Success => {
                    let actual = fro::read_file_with_mode(path, fro::IOMode::PageCache)
                        .unwrap_or_else(|err| {
                            panic!("{name}: expected success, got {err:?}");
                        });
                    assert_eq!(Some(actual), expected_bytes, "{name}: byte mismatch");
                }
                _ => assert_error_class(
                    name,
                    fro::read_file_with_mode(path, fro::IOMode::PageCache),
                    expected,
                ),
            }
        }
    }

    #[test]
    fn local_write_api_compatibility_matrix() {
        let tmp = unique_temp_dir("fro-compat-write");
        fs::create_dir_all(&tmp).unwrap();

        let writable = tmp.join("writable.bin");
        let readonly = tmp.join("readonly.bin");
        let inaccessible = tmp.join("inaccessible.bin");
        let directory = tmp.join("directory");
        let writable_target = tmp.join("writable-target.bin");
        let writable_symlink = tmp.join("writable-link");
        let directory_symlink = tmp.join("directory-link");
        let locked_dir = tmp.join("locked-dir");
        let locked_child = locked_dir.join("child.bin");
        let bytes = b"compatibility-write-bytes".to_vec();

        fs::write(&writable, b"old").unwrap();
        fs::write(&readonly, b"old").unwrap();
        fs::write(&inaccessible, b"old").unwrap();
        fs::write(&writable_target, b"old").unwrap();
        set_mode(&readonly, 0o444);
        set_mode(&inaccessible, 0o000);
        fs::create_dir(&directory).unwrap();
        symlink(&writable_target, &writable_symlink).unwrap();
        symlink(&directory, &directory_symlink).unwrap();
        fs::create_dir(&locked_dir).unwrap();
        set_mode(&locked_dir, 0o555);

        let cases = [
            (
                "regular-0644",
                writable.as_path(),
                ExpectedClass::same(ResultClass::Success),
                Some(writable.as_path()),
            ),
            (
                "regular-0444",
                readonly.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
                Some(readonly.as_path()),
            ),
            (
                "regular-0000",
                inaccessible.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
                Some(inaccessible.as_path()),
            ),
            (
                "directory-0755",
                directory.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
                None,
            ),
            (
                "symlink-to-regular",
                writable_symlink.as_path(),
                ExpectedClass::same(ResultClass::Success),
                Some(writable_target.as_path()),
            ),
            (
                "symlink-to-directory",
                directory_symlink.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
                None,
            ),
            (
                "new-file-in-0555-directory",
                locked_child.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
                Some(locked_child.as_path()),
            ),
        ];

        for (name, path, expected, written_path) in cases {
            match expected.resolve() {
                ResultClass::Success => {
                    let written = fro::write_file_with_mode(path, &bytes, fro::IOMode::PageCache)
                        .unwrap_or_else(|err| {
                            panic!("{name}: expected success, got {err:?}");
                        });
                    assert_eq!(written, bytes.len() as u64, "{name}: byte count mismatch");
                    let final_path = written_path.unwrap();
                    assert_eq!(
                        fs::read(final_path).unwrap(),
                        bytes,
                        "{name}: byte mismatch"
                    );
                }
                _ => assert_error_class(
                    name,
                    fro::write_file_with_mode(path, &bytes, fro::IOMode::PageCache),
                    expected,
                ),
            }
        }
    }

    #[test]
    fn local_copy_api_compatibility_matrix() {
        let tmp = unique_temp_dir("fro-compat-copy");
        fs::create_dir_all(&tmp).unwrap();

        let readable = tmp.join("readable.bin");
        let unreadable = tmp.join("unreadable.bin");
        let directory = tmp.join("directory");
        let source_symlink = tmp.join("source-link");
        let copy_dest = tmp.join("copy.bin");
        let copy_from_symlink = tmp.join("copy-from-symlink.bin");
        let copy_from_directory = tmp.join("copy-from-directory.bin");
        let copy_from_inaccessible = tmp.join("copy-from-0000.bin");
        let locked_dir = tmp.join("locked-dir");
        let locked_dest = locked_dir.join("copy.bin");
        let bytes = b"compatibility-copy-bytes".to_vec();

        fs::write(&readable, &bytes).unwrap();
        fs::write(&unreadable, &bytes).unwrap();
        set_mode(&unreadable, 0o000);
        fs::create_dir(&directory).unwrap();
        symlink(&readable, &source_symlink).unwrap();
        fs::create_dir(&locked_dir).unwrap();
        set_mode(&locked_dir, 0o555);

        let cases = [
            (
                "regular-source",
                readable.as_path(),
                copy_dest.as_path(),
                ExpectedClass::same(ResultClass::Success),
            ),
            (
                "symlink-source",
                source_symlink.as_path(),
                copy_from_symlink.as_path(),
                ExpectedClass::same(ResultClass::Success),
            ),
            (
                "directory-source",
                directory.as_path(),
                copy_from_directory.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
            ),
            (
                "source-0000",
                unreadable.as_path(),
                copy_from_inaccessible.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
            ),
            (
                "destination-is-directory",
                readable.as_path(),
                directory.as_path(),
                ExpectedClass::same(ResultClass::InvalidInputClass),
            ),
            (
                "destination-in-0555-directory",
                readable.as_path(),
                locked_dest.as_path(),
                ExpectedClass::privilege_sensitive(
                    ResultClass::PermissionDenied,
                    ResultClass::Success,
                ),
            ),
        ];

        for (name, source, dest, expected) in cases {
            match expected.resolve() {
                ResultClass::Success => {
                    let copied = fro::copy_file_with_modes(
                        source,
                        dest,
                        fro::IOMode::PageCache,
                        fro::IOMode::PageCache,
                    )
                    .unwrap_or_else(|err| {
                        panic!("{name}: expected success, got {err:?}");
                    });
                    assert_eq!(copied, bytes.len() as u64, "{name}: byte count mismatch");
                    assert_eq!(fs::read(dest).unwrap(), bytes, "{name}: byte mismatch");
                }
                _ => assert_error_class(
                    name,
                    fro::copy_file_with_modes(
                        source,
                        dest,
                        fro::IOMode::PageCache,
                        fro::IOMode::PageCache,
                    ),
                    expected,
                ),
            }
        }
    }
}
