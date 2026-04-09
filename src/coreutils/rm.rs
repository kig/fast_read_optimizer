use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromptMode {
    Never,
    Once,
    Always,
}

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
    println!(
        "Usage: {program} [-f] [-i|-I|--interactive[=WHEN]] [-d] [-r|-R|--recursive] [-v] <file> [file ...]"
    );
    println!("Remove files or directories.");
    println!();
    println!("  -d, --dir          remove empty directories");
    println!("  -f, --force        ignore missing files and allow zero operands");
    println!("  -i                 prompt before every removal");
    println!(
        "  -I                 prompt once before removing more than three files or recursively"
    );
    println!("      --interactive[=WHEN] prompt according to WHEN: never, once, or always");
    println!("  -r, -R, --recursive remove directories and their contents recursively");
    println!("  -v, --verbose      print a line for each removed path");
    println!("  -h, --help         display this help and exit");
}

fn parse_interactive_when(value: &str) -> io::Result<PromptMode> {
    match value {
        "never" => Ok(PromptMode::Never),
        "once" => Ok(PromptMode::Once),
        "always" => Ok(PromptMode::Always),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported rm interactive mode {value:?}"),
        )),
    }
}

fn prompt_user(question: &str) -> io::Result<bool> {
    let mut stderr = io::stderr().lock();
    stderr.write_all(question.as_bytes())?;
    stderr.flush()?;
    drop(stderr);

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    let trimmed = answer.trim_start();
    Ok(matches!(trimmed.as_bytes().first(), Some(b'y' | b'Y')))
}

fn prompt_once_question(target_count: usize, recursive: bool) -> Option<String> {
    if recursive {
        let noun = if target_count == 1 {
            "argument"
        } else {
            "arguments"
        };
        return Some(format!("rm: remove {target_count} {noun} recursively? "));
    }
    if target_count > 3 {
        let noun = if target_count == 1 {
            "argument"
        } else {
            "arguments"
        };
        return Some(format!("rm: remove {target_count} {noun}? "));
    }
    None
}

fn remove_prompt(path: &Path, metadata: &fs::Metadata, descending: bool) -> String {
    if descending {
        return format!("rm: descend into directory '{}'? ", path.display());
    }
    if metadata.file_type().is_dir() {
        format!("rm: remove directory '{}'? ", path.display())
    } else if metadata.file_type().is_symlink() {
        format!("rm: remove symbolic link '{}'? ", path.display())
    } else if metadata.file_type().is_file() {
        format!("rm: remove regular file '{}'? ", path.display())
    } else {
        format!("rm: remove '{}'? ", path.display())
    }
}

fn remove_file_entry(path: &Path, verbose: bool) -> io::Result<()> {
    fs::remove_file(path)?;
    mark_removed(path, false, verbose);
    Ok(())
}

fn remove_empty_dir(path: &Path, verbose: bool) -> io::Result<()> {
    fs::remove_dir(path)?;
    mark_removed(path, true, verbose);
    Ok(())
}

fn remove_path_recursively_with_prompts(path: &Path, verbose: bool) -> io::Result<bool> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interactive recursive delete requires a directory root",
        ));
    }
    if !prompt_user(&remove_prompt(path, &metadata, true))? {
        return Ok(false);
    }
    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let child_path = entry.path();
        let child_metadata = fs::symlink_metadata(&child_path)?;
        if child_metadata.file_type().is_dir() {
            let _ = remove_path_recursively_with_prompts(&child_path, verbose)?;
            continue;
        }
        if prompt_user(&remove_prompt(&child_path, &child_metadata, false))? {
            remove_file_entry(&child_path, verbose)?;
        }
    }
    if prompt_user(&remove_prompt(path, &metadata, false))? {
        remove_empty_dir(path, verbose)?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn run_rm(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut dir = false;
    let mut recursive = false;
    let mut force = false;
    let mut verbose = false;
    let mut prompt_mode = PromptMode::Never;
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
            "--force" => {
                force = true;
                prompt_mode = PromptMode::Never;
            }
            "--verbose" => verbose = true,
            "--interactive" => prompt_mode = PromptMode::Always,
            other if other.starts_with("--interactive=") => {
                prompt_mode = parse_interactive_when(&other["--interactive=".len()..])?;
            }
            "--" => end_of_options = true,
            other if other.starts_with("--") => targets.push(other.to_string()),
            other if other.starts_with('-') && other.len() > 1 => {
                for ch in other[1..].chars() {
                    match ch {
                        'd' => dir = true,
                        'r' | 'R' => recursive = true,
                        'f' => {
                            force = true;
                            prompt_mode = PromptMode::Never;
                        }
                        'i' => prompt_mode = PromptMode::Always,
                        'I' => prompt_mode = PromptMode::Once,
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
            "Usage: {} [-f] [-i|-I|--interactive[=WHEN]] [-d] [-r|-R|--recursive] [-v] <file> [file ...]",
            program
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing file operand",
        ));
    }

    if prompt_mode == PromptMode::Once {
        if let Some(question) = prompt_once_question(targets.len(), recursive) {
            if !prompt_user(&question)? {
                return Ok(0);
            }
        }
        prompt_mode = PromptMode::Never;
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
                if prompt_mode == PromptMode::Always {
                    if let Err(err) = remove_path_recursively_with_prompts(path, verbose) {
                        write_warning_line("rm", path, &err, "cannot remove");
                        exit_code = 1;
                    }
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
            if !dir {
                let err = io::Error::new(io::ErrorKind::IsADirectory, "Is a directory");
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
                continue;
            }
            if prompt_mode == PromptMode::Always
                && !prompt_user(&remove_prompt(path, &metadata, false))?
            {
                continue;
            }
            if let Err(err) = remove_empty_dir(path, verbose) {
                let err = if err.raw_os_error() == Some(libc::ENOTEMPTY) {
                    io::Error::new(io::ErrorKind::DirectoryNotEmpty, "Directory not empty")
                } else {
                    err
                };
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
            }
            continue;
        }
        if prompt_mode == PromptMode::Always
            && !prompt_user(&remove_prompt(path, &metadata, false))?
        {
            continue;
        }
        match remove_file_entry(path, verbose) {
            Ok(()) => {}
            Err(err) if force && err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                write_warning_line("rm", path, &err, "cannot remove");
                exit_code = 1;
            }
        }
    }
    Ok(exit_code)
}
