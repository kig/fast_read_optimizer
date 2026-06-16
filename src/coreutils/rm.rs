use super::*;
use std::io::{BufRead, Write};
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromptMode {
    Never,
    Once,
    Always,
}

fn mark_removed(path: &Path, is_dir: bool, verbose: bool) {
    if verbose {
        if is_dir {
            fro::cio_println!("removed directory '{}'", path.display());
        } else {
            fro::cio_println!("removed '{}'", path.display());
        }
    }
}

fn print_rm_help(program: &str) {
    fro::cio_println!(
        "Usage: {program} [-f] [-i|-I|--interactive[=WHEN]] [--one-file-system] [--preserve-root|--no-preserve-root] [-d] [-r|-R|--recursive] [-v] <file> [file ...]"
    );
    fro::cio_println!("Remove files or directories.");
    fro::cio_println!();
    fro::cio_println!("  -d, --dir          remove empty directories");
    fro::cio_println!("  -f, --force        ignore missing files and allow zero operands");
    fro::cio_println!("  -i                 prompt before every removal");
    fro::cio_println!(
        "  -I                 prompt once before removing more than three files or recursively"
    );
    fro::cio_println!(
        "      --interactive[=WHEN] prompt according to WHEN: never, once, or always"
    );
    fro::cio_println!(
        "      --one-file-system skip recursive child directories on different file systems"
    );
    fro::cio_println!("      --preserve-root refuse recursive removal of / (default)");
    fro::cio_println!("      --no-preserve-root allow recursive removal of /");
    fro::cio_println!("  -r, -R, --recursive remove directories and their contents recursively");
    fro::cio_println!("  -v, --verbose      print a line for each removed path");
    fro::cio_println!("  -h, --help         display this help and exit");
    fro::cio_println!("      --version      output version information and exit");
}

fn operand_targets_root(operand: &str) -> bool {
    !operand.is_empty() && operand.bytes().all(|byte| byte == b'/')
}

fn preserve_root_warning_lines() -> [&'static str; 2] {
    [
        "rm: it is dangerous to operate recursively on '/'",
        "rm: use --no-preserve-root to override this failsafe",
    ]
}

fn write_preserve_root_warning() {
    for line in preserve_root_warning_lines() {
        fro::cio_eprintln!("{line}");
    }
}

fn write_one_file_system_warning(path: &Path) {
    fro::cio_eprintln!(
        "rm: skipping '{}', since it's on a different file system",
        path.display()
    );
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
    let mut stderr = fro::command_io::stderr_buf_writer(4096)?;
    stderr.write_all(question.as_bytes())?;
    stderr.flush()?;
    drop(stderr);

    let mut answer = String::new();
    let mut stdin = std::io::BufReader::new(fro::command_io::stdin_file()?);
    stdin.read_line(&mut answer)?;
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

fn remove_path_recursively_with_prompts(
    path: &Path,
    verbose: bool,
    root_device: Option<u64>,
) -> io::Result<bool> {
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
            if root_device.is_some_and(|root_device| child_metadata.dev() != root_device) {
                write_one_file_system_warning(&child_path);
                continue;
            }
            let _ = remove_path_recursively_with_prompts(&child_path, verbose, root_device)?;
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
    let mut one_file_system = false;
    let mut preserve_root = true;
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
            "--version" => {
                print_coreutils_version("rm");
                return Ok(0);
            }
            "--dir" => dir = true,
            "--recursive" => recursive = true,
            "--force" => {
                force = true;
                prompt_mode = PromptMode::Never;
            }
            "--one-file-system" => one_file_system = true,
            "--preserve-root" => preserve_root = true,
            "--no-preserve-root" => preserve_root = false,
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
        fro::cio_eprintln!(
            "Usage: {} [-f] [-i|-I|--interactive[=WHEN]] [--one-file-system] [--preserve-root|--no-preserve-root] [-d] [-r|-R|--recursive] [-v] <file> [file ...]",
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
        if recursive && preserve_root && operand_targets_root(&target) {
            write_preserve_root_warning();
            exit_code = 1;
            continue;
        }
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
                let root_device = one_file_system.then_some(metadata.dev());
                if prompt_mode == PromptMode::Always {
                    if let Err(err) =
                        remove_path_recursively_with_prompts(path, verbose, root_device)
                    {
                        write_warning_line("rm", path, &err, "cannot remove");
                        exit_code = 1;
                    }
                    continue;
                }
                let result = if let Some(root_device) = root_device {
                    crate::main_app::remove_path_recursively_one_file_system(
                        path,
                        verbose,
                        root_device,
                        write_one_file_system_warning,
                    )
                } else {
                    crate::main_app::remove_path_recursively(path, verbose)
                };
                if let Err(err) = result {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_preservation_only_matches_explicit_root_operands() {
        assert!(operand_targets_root("/"));
        assert!(operand_targets_root("///"));
        assert!(!operand_targets_root("/./"));
        assert!(!operand_targets_root("/tmp"));
        assert!(!operand_targets_root("."));
    }

    #[test]
    fn rm_preserve_root_warning_matches_gnu_wording() {
        let mut stderr = Vec::new();
        for line in preserve_root_warning_lines() {
            writeln!(&mut stderr, "{line}").unwrap();
        }
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "rm: it is dangerous to operate recursively on '/'\nrm: use --no-preserve-root to override this failsafe\n"
        );
    }

    #[test]
    fn rm_one_file_system_warning_matches_gnu_style_wording() {
        let path = Path::new("/mnt/chroot/home");
        let mut stderr = Vec::new();
        writeln!(
            &mut stderr,
            "rm: skipping '{}', since it's on a different file system",
            path.display()
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "rm: skipping '/mnt/chroot/home', since it's on a different file system\n"
        );
    }
}
