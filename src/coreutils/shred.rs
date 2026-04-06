use super::*;
use rand::RngExt;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::PermissionsExt;

const DEFAULT_SHRED_PASSES: usize = 1;
const SHRED_CHUNK_SIZE: usize = 1024 * 1024;

pub(super) fn run_shred(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut passes = DEFAULT_SHRED_PASSES;
    let mut zero_last = false;
    let mut remove_after = false;
    let mut force = false;
    let mut verbose = false;
    let mut size = None;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-n" => {
                i += 1;
                let count = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for -n")
                })?;
                passes = count.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid pass count")
                })?;
            }
            "-s" | "--size" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for -s")
                })?;
                size = Some(parse_shred_size(value)?);
            }
            "-z" => zero_last = true,
            "-u" => remove_after = true,
            "-f" | "--force" => force = true,
            "-v" | "--verbose" => verbose = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            value if value.starts_with("--size=") => {
                size = Some(parse_shred_size(&value["--size=".len()..])?);
            }
            "--" => {}
            other if other.starts_with('-') && other.len() > 1 => {
                for ch in other[1..].chars() {
                    match ch {
                        'z' => zero_last = true,
                        'u' => remove_after = true,
                        'f' => force = true,
                        'v' => verbose = true,
                        'n' | 's' => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("use -{ch} with a separate value"),
                            ))
                        }
                        _ => files.push(other.to_string()),
                    }
                }
            }
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    let files = ensure_files(
        program,
        files,
        "[-n passes] [-s size] [-z] [-u] [-f] [-v] [--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    let mut exit_code = 0;
    for file in files {
        let mut file_failed = false;
        let path = Path::new(&file);
        let file_size = match fs::metadata(path) {
            Ok(metadata) => metadata.len(),
            Err(err) => {
                write_shred_error(path, &err, "failed to open for writing");
                exit_code = 1;
                continue;
            }
        };
        let target_size = size.unwrap_or(file_size);
        let total_passes = passes + usize::from(zero_last);

        let mut handle = match open_shred_target(path, force) {
            Ok(file) => file,
            Err(err) => {
                write_shred_error(path, &err, "failed to open for writing");
                exit_code = 1;
                continue;
            }
        };

        for pass_index in 0..passes {
            if verbose {
                print_shred_pass(path, pass_index + 1, total_passes, true);
            }
            if let Err(err) =
                overwrite_with_pattern(&mut handle, path, target_size, file_size, io_mode, true)
            {
                write_shred_error(path, &err, "failed to write");
                exit_code = 1;
                file_failed = true;
                break;
            }
        }
        if file_failed {
            continue;
        }
        if zero_last {
            if verbose {
                print_shred_pass(path, total_passes, total_passes, false);
            }
            if let Err(err) =
                overwrite_with_pattern(&mut handle, path, target_size, file_size, io_mode, false)
            {
                write_shred_error(path, &err, "failed to write");
                exit_code = 1;
                file_failed = true;
            }
        }
        if file_failed {
            continue;
        }
        drop(handle);

        if remove_after {
            if let Err(err) = fs::remove_file(path) {
                write_shred_error(path, &err, "failed to remove");
                exit_code = 1;
                continue;
            }
        }
    }
    Ok(exit_code)
}

fn parse_shred_size(value: &str) -> io::Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid size"));
    }

    let value_lc = value.to_ascii_lowercase();
    let split = value_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value_lc.len());
    let (num_str, suffix) = value_lc.split_at(split);
    let num: u64 = num_str
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid size"))?;
    let multiplier = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid size")),
    };
    num.checked_mul(multiplier)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid size"))
}

fn open_shred_target(path: &Path, force: bool) -> io::Result<fs::File> {
    match OpenOptions::new().write(true).open(path) {
        Ok(file) => Ok(file),
        Err(err) if force && is_shred_permission_error(&err) => {
            fs::set_permissions(path, fs::Permissions::from_mode(0o200))?;
            OpenOptions::new().write(true).open(path)
        }
        Err(err) => Err(err),
    }
}

fn overwrite_with_pattern(
    handle: &mut fs::File,
    path: &Path,
    target_size: u64,
    original_size: u64,
    io_mode: IOMode,
    random: bool,
) -> io::Result<()> {
    if target_size == 0 {
        return handle.sync_all();
    }
    if target_size == original_size {
        return overwrite_full_file(path, target_size, io_mode, random);
    }

    handle.seek(SeekFrom::Start(0))?;
    let mut remaining = target_size;
    let mut buffer = vec![0_u8; SHRED_CHUNK_SIZE.min(target_size as usize)];
    let mut rng = rand::rng();
    while remaining > 0 {
        let chunk = remaining.min(buffer.len() as u64) as usize;
        if random {
            rng.fill(&mut buffer[..chunk]);
        } else {
            buffer[..chunk].fill(0);
        }
        handle.write_all(&buffer[..chunk])?;
        remaining -= chunk as u64;
    }
    if target_size > original_size {
        handle.set_len(target_size)?;
    } else {
        handle.flush()?;
    }
    handle.sync_all()
}

fn overwrite_full_file(path: &Path, size: u64, io_mode: IOMode, random: bool) -> io::Result<()> {
    if size == 0 {
        return Ok(());
    }
    let config = load_config(None);
    let path_string = path.to_string_lossy();
    let page_cache = config.get_params_for_path("write", false, &path_string);
    let direct = config.get_params_for_path("write", true, &path_string);
    write_generated_file(
        &path_string,
        size,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        internal_io_mode(io_mode),
        if random {
            GeneratedWritePattern::Random
        } else {
            GeneratedWritePattern::Zero
        },
    )?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()
}

fn print_shred_pass(path: &Path, pass_index: usize, total_passes: usize, random: bool) {
    let label = if random { "random" } else { "000000" };
    eprintln!(
        "shred: {}: pass {}/{} ({label})...",
        path.display(),
        pass_index,
        total_passes
    );
}

fn write_shred_error(path: &Path, err: &io::Error, action: &str) {
    let detail = match err.kind() {
        io::ErrorKind::PermissionDenied => "Permission denied".to_string(),
        io::ErrorKind::NotFound => "No such file or directory".to_string(),
        _ => err.to_string(),
    };
    eprintln!("shred: {}: {action}: {detail}", path.display());
}

fn is_shred_permission_error(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::PermissionDenied)
}
