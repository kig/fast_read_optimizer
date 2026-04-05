use super::*;

fn mark_removed(path: &Path, is_dir: bool, verbose: bool) {
    if verbose {
        if is_dir {
            println!("removed directory '{}'", path.display());
        } else {
            println!("removed '{}'", path.display());
        }
    }
}

pub(super) fn run_rm(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut recursive = false;
    let mut force = false;
    let mut verbose = false;
    let mut targets = Vec::new();

    for arg in args.iter().skip(1) {
        match arg.as_str() {
            "--recursive" => recursive = true,
            "--force" => force = true,
            "--verbose" => verbose = true,
            "--" => {}
            other if other.starts_with("--") => targets.push(other.to_string()),
            other if other.starts_with('-') && other.len() > 1 => {
                for ch in other[1..].chars() {
                    match ch {
                        'r' | 'R' => recursive = true,
                        'f' => force = true,
                        'v' => verbose = true,
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("unsupported rm flag -{ch}"),
                            ))
                        }
                    }
                }
            }
            other => targets.push(other.to_string()),
        }
    }

    let targets = ensure_files(program, targets, "[-f] [-r|-R|--recursive] [-v] <file> [file ...]")?;
    let mut exit_code = 0;
    for target in targets {
        let path = Path::new(&target);
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(err) if force && err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => {
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
                continue;
            }
        };
        if metadata.file_type().is_dir() {
            if !recursive {
                let err = io::Error::new(io::ErrorKind::IsADirectory, "Is a directory");
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
                continue;
            }
            if let Err(err) = crate::main_app::remove_path_recursively(path, verbose) {
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
            } else {
                mark_removed(path, true, verbose);
            }
            continue;
        }
        match fs::remove_file(path) {
            Ok(()) => mark_removed(path, false, verbose),
            Err(err) if force && err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
            }
        }
    }
    Ok(exit_code)
}
