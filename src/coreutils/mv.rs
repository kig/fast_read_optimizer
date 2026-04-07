use super::*;
use std::os::unix::fs::symlink;

fn remove_existing_non_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Err(io::Error::new(
            io::ErrorKind::IsADirectory,
            "target path is a directory",
        )),
        Ok(_) => fs::remove_file(path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn move_symlink_cross_fs(source: &Path, target: &Path) -> io::Result<()> {
    remove_existing_non_directory(target)?;
    let link_target = fs::read_link(source)?;
    symlink(&link_target, target)?;
    fs::remove_file(source)
}

fn move_file_cross_fs(source: &Path, target: &Path) -> io::Result<()> {
    remove_existing_non_directory(target)?;
    fro::copy_file_with_modes(source, target, fro::IOMode::Auto, fro::IOMode::Auto)?;
    fs::remove_file(source)
}

fn move_directory_cross_fs(source: &Path, target: &Path, verbose: bool) -> io::Result<()> {
    if verbose {
        eprintln!(
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
        eprintln!(
            "mv cross-fs: recursive move complete '{}'",
            source.display()
        );
    }
    Ok(())
}

fn move_path(source: &Path, target: &Path, verbose: bool) -> io::Result<()> {
    match fs::rename(source, target) {
        Ok(()) => {
            if verbose {
                println!("renamed '{}' -> '{}'", source.display(), target.display());
            }
            Ok(())
        }
        Err(err) if err.raw_os_error() == Some(libc::EXDEV) => {
            let metadata = fs::symlink_metadata(source)?;
            if verbose {
                eprintln!(
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
                println!("renamed '{}' -> '{}'", source.display(), target.display());
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub(super) fn run_mv(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut verbose = false;
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
                        'v' | 'f' | 'T' => {
                            if ch == 'v' {
                                verbose = true;
                            } else if ch == 'T' {
                                no_target_directory = true;
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
        eprintln!("mv: cannot combine --target-directory (-t) and --no-target-directory (-T)");
        return Ok(1);
    }

    let (destination, source_paths) = if let Some(target_directory) = explicit_target_directory {
        if paths.is_empty() {
            eprintln!(
                "Usage: {} [-f] [-v] [-T] [-t DIRECTORY] <source>... <target>",
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
            eprintln!("mv: extra operand '{}'", paths[2]);
            eprintln!("Try 'mv --help' for more information.");
            return Ok(1);
        }
        if paths.len() < 2 {
            eprintln!(
                "Usage: {} [-f] [-v] [-T] [-t DIRECTORY] <source>... <target>",
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
            eprintln!(
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
        if let Err(err) = move_path(&source, &target, verbose) {
            write_warning_line("mv", &source, &err, "cannot move");
            exit_code = 1;
        }
    }
    Ok(exit_code)
}
