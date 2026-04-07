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

fn print_rm_help(program: &str) {
    println!("Usage: {program} [-f] [-d] [-r|-R|--recursive] [-v] <file> [file ...]");
    println!("Remove files or directories.");
    println!();
    println!("  -d, --dir          remove empty directories");
    println!("  -f, --force        ignore missing files and allow zero operands");
    println!("  -r, -R, --recursive remove directories and their contents recursively");
    println!("  -v, --verbose      print a line for each removed path");
    println!("  -h, --help         display this help and exit");
}

pub(super) fn run_rm(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut dir = false;
    let mut recursive = false;
    let mut force = false;
    let mut verbose = false;
    let mut targets = Vec::new();
    let mut end_of_options = false;

    for arg in args.iter().skip(1) {
        if end_of_options {
            targets.push(arg.to_string());
            continue;
        }
        match arg.as_str() {
            "-h" | "--help" => {
                print_rm_help(program);
                return Ok(0);
            }
            "--dir" => dir = true,
            "--recursive" => recursive = true,
            "--force" => force = true,
            "--verbose" => verbose = true,
            "--" => end_of_options = true,
            other if other.starts_with("--") => targets.push(other.to_string()),
            other if other.starts_with('-') && other.len() > 1 => {
                for ch in other[1..].chars() {
                    match ch {
                        'd' => dir = true,
                        'r' | 'R' => recursive = true,
                        'f' => force = true,
                        'v' => verbose = true,
                        'h' => {
                            print_rm_help(program);
                            return Ok(0);
                        }
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

    if targets.is_empty() {
        if force {
            return Ok(0);
        }
        eprintln!(
            "Usage: {} [-f] [-d] [-r|-R|--recursive] [-v] <file> [file ...]",
            program
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing file operand",
        ));
    }
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
            if recursive {
                if let Err(err) = crate::main_app::remove_path_recursively(path, verbose) {
                    write_warning_line("rm", path, &err, "cannot remove");
                    exit_code = 1;
                } else {
                    mark_removed(path, true, verbose);
                }
                continue;
            }
            if !dir {
                let err = io::Error::new(io::ErrorKind::IsADirectory, "Is a directory");
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
                continue;
            }
            if let Err(err) = fs::remove_dir(path) {
                let err = if err.raw_os_error() == Some(libc::ENOTEMPTY) {
                    io::Error::new(io::ErrorKind::DirectoryNotEmpty, "Directory not empty")
                } else {
                    err
                };
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
