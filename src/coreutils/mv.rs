use super::*;
use std::os::unix::fs::symlink;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

fn non_directory_target_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Err(io::Error::new(
            io::ErrorKind::IsADirectory,
            "target path is a directory",
        )),
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

struct PendingReplacementTarget {
    final_path: PathBuf,
    temp_path: PathBuf,
    committed: bool,
}

impl PendingReplacementTarget {
    fn new(final_path: &Path) -> io::Result<Self> {
        Ok(Self {
            final_path: final_path.to_path_buf(),
            temp_path: replacement_temp_path(final_path)?,
            committed: false,
        })
    }

    fn working_path(&self) -> &Path {
        &self.temp_path
    }

    fn commit(mut self) -> io::Result<()> {
        fs::rename(&self.temp_path, &self.final_path)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingReplacementTarget {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

fn replacement_temp_path(target: &Path) -> io::Result<PathBuf> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let file_name = target.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("target path must include a file name: {}", target.display()),
        )
    })?;
    let file_name = file_name.to_string_lossy();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for attempt in 0..1024_u32 {
        let candidate = parent.join(format!(
            ".{file_name}.fro-mv-tmp-{}-{}-{}",
            process::id(),
            nanos,
            attempt
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "could not allocate a unique replacement target next to {}",
            target.display()
        ),
    ))
}

fn install_non_directory_replacement(
    target: &Path,
    create_target: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    if !non_directory_target_exists(target)? {
        return create_target(target);
    }
    let pending = PendingReplacementTarget::new(target)?;
    create_target(pending.working_path())?;
    pending.commit()
}

fn move_symlink_cross_fs_with(
    source: &Path,
    target: &Path,
    create_symlink: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let link_target = fs::read_link(source)?;
    install_non_directory_replacement(target, |working| create_symlink(&link_target, working))?;
    fs::remove_file(source)
}

fn move_symlink_cross_fs(source: &Path, target: &Path) -> io::Result<()> {
    move_symlink_cross_fs_with(source, target, |link_target, working| {
        symlink(link_target, working)
    })
}

fn move_file_cross_fs_with(
    source: &Path,
    target: &Path,
    copy_file: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    install_non_directory_replacement(target, |working| copy_file(source, working))?;
    fs::remove_file(source)
}

fn move_file_cross_fs(source: &Path, target: &Path) -> io::Result<()> {
    move_file_cross_fs_with(source, target, |source, working| {
        fro::copy_file_with_modes(source, working, fro::IOMode::Auto, fro::IOMode::Auto).map(|_| ())
    })
}

fn move_directory_cross_fs(source: &Path, target: &Path, verbose: bool) -> io::Result<()> {
    if verbose {
        fro::cio_eprintln!(
            "mv cross-fs: starting recursive move '{}' -> '{}'",
            source.display(),
            target.display()
        );
    }
    crate::main_app::move_directory_cross_filesystem(
        source,
        target,
        crate::common::IOMode::Auto,
        crate::common::IOMode::Auto,
        verbose,
    )?;
    if verbose {
        fro::cio_eprintln!(
            "mv cross-fs: recursive move complete '{}'",
            source.display()
        );
    }
    Ok(())
}

fn should_skip_target(
    source_meta: &fs::Metadata,
    target: &Path,
    no_clobber: bool,
    update: bool,
) -> io::Result<bool> {
    let target_meta = match fs::symlink_metadata(target) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    if no_clobber {
        return Ok(true);
    }
    if update {
        return Ok(source_meta.modified()? <= target_meta.modified()?);
    }
    Ok(false)
}

fn move_path(
    source: &Path,
    source_meta: &fs::Metadata,
    target: &Path,
    verbose: bool,
    no_clobber: bool,
    update: bool,
) -> io::Result<()> {
    if should_skip_target(source_meta, target, no_clobber, update)? {
        return Ok(());
    }
    match fs::rename(source, target) {
        Ok(()) => {
            if verbose {
                fro::cio_println!("renamed '{}' -> '{}'", source.display(), target.display());
            }
            Ok(())
        }
        Err(err) if err.raw_os_error() == Some(libc::EXDEV) => {
            let metadata = fs::symlink_metadata(source)?;
            if verbose {
                fro::cio_eprintln!(
                    "mv cross-fs fallback: '{}' -> '{}'",
                    source.display(),
                    target.display()
                );
            }
            if metadata.file_type().is_dir() {
                move_directory_cross_fs(source, target, verbose)?;
            } else if metadata.file_type().is_symlink() {
                move_symlink_cross_fs(source, target)?;
            } else {
                move_file_cross_fs(source, target)?;
            }
            if verbose {
                fro::cio_println!("renamed '{}' -> '{}'", source.display(), target.display());
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub(super) fn run_mv(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut verbose = false;
    let mut no_clobber = false;
    let mut update = false;
    let mut explicit_target_directory: Option<PathBuf> = None;
    let mut no_target_directory = false;
    let mut paths = Vec::new();
    let mut end_of_options = false;

    let mut index = 1usize;
    while index < args.len() {
        let arg = &args[index];
        if end_of_options {
            paths.push(arg.to_string());
            index += 1;
            continue;
        }
        match arg.as_str() {
            "--verbose" => verbose = true,
            "-n" | "--no-clobber" => no_clobber = true,
            "-u" | "--update" => update = true,
            "-T" | "--no-target-directory" => no_target_directory = true,
            "-t" | "--target-directory" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument for --target-directory",
                    )
                })?;
                explicit_target_directory = Some(PathBuf::from(value));
                index += 2;
                continue;
            }
            other if other.starts_with("--target-directory=") => {
                explicit_target_directory =
                    Some(PathBuf::from(&other["--target-directory=".len()..]));
            }
            "--" => end_of_options = true,
            other if other.starts_with("--") => paths.push(other.to_string()),
            other if other.starts_with('-') && other.len() > 1 => {
                let mut chars = other[1..].chars().peekable();
                while let Some(ch) = chars.next() {
                    match ch {
                        'v' | 'f' | 'T' | 'n' | 'u' => {
                            if ch == 'v' {
                                verbose = true;
                            } else if ch == 'T' {
                                no_target_directory = true;
                            } else if ch == 'n' {
                                no_clobber = true;
                            } else if ch == 'u' {
                                update = true;
                            }
                        }
                        't' => {
                            let remainder = chars.collect::<String>();
                            if !remainder.is_empty() {
                                explicit_target_directory = Some(PathBuf::from(remainder));
                            } else {
                                let value = args.get(index + 1).ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        "missing argument for -t",
                                    )
                                })?;
                                explicit_target_directory = Some(PathBuf::from(value));
                                index += 1;
                            }
                            break;
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("unsupported mv flag -{ch}"),
                            ))
                        }
                    }
                }
            }
            other => paths.push(other.to_string()),
        }
        index += 1;
    }

    if explicit_target_directory.is_some() && no_target_directory {
        fro::cio_eprintln!("mv: cannot combine --target-directory (-t) and --no-target-directory (-T)");
        return Ok(1);
    }

    let (destination, source_paths) = if let Some(target_directory) = explicit_target_directory {
        if paths.is_empty() {
            fro::cio_eprintln!(
                "Usage: {} [-f] [-n] [-u] [-v] [-T] [-t DIRECTORY] <source>... <target>",
                program
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing source operand",
            ));
        }
        (
            target_directory,
            paths.into_iter().map(PathBuf::from).collect::<Vec<_>>(),
        )
    } else {
        if no_target_directory && paths.len() > 2 {
            fro::cio_eprintln!("mv: extra operand '{}'", paths[2]);
            fro::cio_eprintln!("Try 'mv --help' for more information.");
            return Ok(1);
        }
        if paths.len() < 2 {
            fro::cio_eprintln!(
                "Usage: {} [-f] [-n] [-u] [-v] [-T] [-t DIRECTORY] <source>... <target>",
                program
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing source or target operand",
            ));
        }
        let destination = PathBuf::from(paths.pop().unwrap());
        (
            destination,
            paths.into_iter().map(PathBuf::from).collect::<Vec<_>>(),
        )
    };
    let destination_meta = fs::symlink_metadata(&destination).ok();
    let destination_is_dir = destination_meta
        .as_ref()
        .is_some_and(|metadata| metadata.file_type().is_dir());
    if source_paths.len() > 1 && !destination_is_dir {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "target must be an existing directory when moving multiple sources",
        ));
    }

    let mut exit_code = 0;
    for source in source_paths {
        let source_meta = match fs::symlink_metadata(&source) {
            Ok(metadata) => metadata,
            Err(err) => {
                write_warning_line("mv", &source, &err, "cannot stat");
                exit_code = 1;
                continue;
            }
        };
        if no_target_directory && destination_is_dir && !source_meta.file_type().is_dir() {
            fro::cio_eprintln!(
                "mv: cannot overwrite directory '{}' with non-directory",
                destination.display()
            );
            exit_code = 1;
            continue;
        }
        let target = if no_target_directory {
            destination.clone()
        } else if destination_is_dir {
            destination.join(source.file_name().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source has no final path component",
                )
            })?)
        } else if source_meta.file_type().is_dir() {
            crate::main_app::resolve_recursive_move_target(&source, &destination)?
        } else {
            destination.clone()
        };
        if let Err(err) = move_path(&source, &source_meta, &target, verbose, no_clobber, update) {
            write_warning_line("mv", &source, &err, "cannot move");
            exit_code = 1;
        }
    }
    Ok(exit_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::process;
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

    #[test]
    fn cross_fs_file_replacement_preserves_existing_target_on_copy_failure() {
        let tmp = unique_temp_dir("fro-mv-cross-fs-file-failure");
        let source = tmp.join("source.txt");
        let target = tmp.join("target.txt");
        fs::write(&source, b"new payload").unwrap();
        fs::write(&target, b"old payload").unwrap();

        let err = move_file_cross_fs_with(&source, &target, |_, working| {
            fs::write(working, b"partial payload").unwrap();
            Err(io::Error::other("copy failed"))
        })
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(fs::read(&target).unwrap(), b"old payload");
        assert_eq!(fs::read(&source).unwrap(), b"new payload");
        let leftovers = fs::read_dir(&tmp)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".fro-mv-tmp-"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[test]
    fn cross_fs_file_replacement_renames_temp_into_place() {
        let tmp = unique_temp_dir("fro-mv-cross-fs-file-success");
        let source = tmp.join("source.txt");
        let target = tmp.join("target.txt");
        fs::write(&source, b"new payload").unwrap();
        fs::write(&target, b"old payload").unwrap();

        move_file_cross_fs_with(&source, &target, |source, working| {
            fs::copy(source, working).map(|_| ())
        })
        .unwrap();

        assert_eq!(fs::read(&target).unwrap(), b"new payload");
        assert!(!source.exists());
    }

    #[test]
    fn cross_fs_symlink_replacement_preserves_existing_target_on_failure() {
        let tmp = unique_temp_dir("fro-mv-cross-fs-symlink-failure");
        let source = tmp.join("source-link");
        let target = tmp.join("target-link");
        symlink("new-destination", &source).unwrap();
        symlink("old-destination", &target).unwrap();

        let err = move_symlink_cross_fs_with(&source, &target, |link_target, working| {
            symlink(link_target, working)?;
            Err(io::Error::other("symlink failed"))
        })
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(
            fs::read_link(&target).unwrap(),
            PathBuf::from("old-destination")
        );
        assert_eq!(
            fs::read_link(&source).unwrap(),
            PathBuf::from("new-destination")
        );
        let leftovers = fs::read_dir(&tmp)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".fro-mv-tmp-"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[test]
    fn cross_fs_symlink_replacement_renames_temp_into_place() {
        let tmp = unique_temp_dir("fro-mv-cross-fs-symlink-success");
        let source = tmp.join("source-link");
        let target = tmp.join("target-link");
        symlink("new-destination", &source).unwrap();
        symlink("old-destination", &target).unwrap();

        move_symlink_cross_fs_with(&source, &target, |link_target, working| {
            symlink(link_target, working)
        })
        .unwrap();

        assert_eq!(
            fs::read_link(&target).unwrap(),
            PathBuf::from("new-destination")
        );
        assert!(!source.exists());
    }

    #[test]
    fn skip_target_obeys_no_clobber_and_update() {
        let tmp = unique_temp_dir("fro-mv-skip-target");
        let source = tmp.join("source.txt");
        let target = tmp.join("target.txt");
        fs::write(&source, b"source").unwrap();
        fs::write(&target, b"target").unwrap();

        let source_meta = fs::symlink_metadata(&source).unwrap();
        assert!(should_skip_target(&source_meta, &target, true, false).unwrap());
        assert!(should_skip_target(&source_meta, &target, true, true).unwrap());

        let newer_target = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let older_source = newer_target - 5;
        let times = [
            libc::timespec {
                tv_sec: older_source,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: older_source,
                tv_nsec: 0,
            },
        ];
        let source_c = std::ffi::CString::new(source.as_os_str().as_encoded_bytes()).unwrap();
        let target_times = [
            libc::timespec {
                tv_sec: newer_target,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: newer_target,
                tv_nsec: 0,
            },
        ];
        let target_c = std::ffi::CString::new(target.as_os_str().as_encoded_bytes()).unwrap();
        let source_rc =
            unsafe { libc::utimensat(libc::AT_FDCWD, source_c.as_ptr(), times.as_ptr(), 0) };
        let target_rc =
            unsafe { libc::utimensat(libc::AT_FDCWD, target_c.as_ptr(), target_times.as_ptr(), 0) };
        assert_eq!(source_rc, 0, "source utimensat failed");
        assert_eq!(target_rc, 0, "target utimensat failed");

        let older_source_meta = fs::symlink_metadata(&source).unwrap();
        assert!(should_skip_target(&older_source_meta, &target, false, true).unwrap());

        let newer_source_times = [
            libc::timespec {
                tv_sec: newer_target + 5,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: newer_target + 5,
                tv_nsec: 0,
            },
        ];
        let source_rc = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                source_c.as_ptr(),
                newer_source_times.as_ptr(),
                0,
            )
        };
        assert_eq!(source_rc, 0, "source utimensat failed");
        let newer_source_meta = fs::symlink_metadata(&source).unwrap();
        assert!(!should_skip_target(&newer_source_meta, &target, false, true).unwrap());
    }
}
