use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TarMode {
    Create,
    List,
    Extract,
}

fn infer_tar_compression(archive: &str, gzip_requested: bool) -> crate::main_app::TarCompression {
    if gzip_requested {
        return crate::main_app::TarCompression::Gzip;
    }
    let archive = archive.to_ascii_lowercase();
    if archive.ends_with(".tar.gz") || archive.ends_with(".tgz") {
        crate::main_app::TarCompression::Gzip
    } else {
        crate::main_app::TarCompression::None
    }
}

fn set_tar_mode(mode: &mut Option<TarMode>, next: TarMode) -> io::Result<()> {
    match mode {
        Some(current) if *current != next => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar accepts only one primary mode at a time",
        )),
        _ => {
            *mode = Some(next);
            Ok(())
        }
    }
}

fn parse_short_tar_flags(
    arg: &str,
    args: &[String],
    index: &mut usize,
    mode: &mut Option<TarMode>,
    verbose: &mut bool,
    gzip: &mut bool,
    archive: &mut Option<String>,
    extract_dir: &mut Option<String>,
) -> io::Result<()> {
    let chars = arg[1..].chars().collect::<Vec<_>>();
    let mut pos = 0usize;
    while pos < chars.len() {
        match chars[pos] {
            'c' => set_tar_mode(mode, TarMode::Create)?,
            't' => set_tar_mode(mode, TarMode::List)?,
            'x' => set_tar_mode(mode, TarMode::Extract)?,
            'v' => *verbose = true,
            'z' => *gzip = true,
            'f' => {
                if pos + 1 < chars.len() {
                    *archive = Some(chars[pos + 1..].iter().collect());
                    return Ok(());
                }
                *index += 1;
                *archive = Some(
                    args.get(*index)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "missing value for -f")
                        })?
                        .clone(),
                );
                return Ok(());
            }
            'C' => {
                if pos + 1 < chars.len() {
                    *extract_dir = Some(chars[pos + 1..].iter().collect());
                    return Ok(());
                }
                *index += 1;
                *extract_dir = Some(
                    args.get(*index)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "missing value for -C")
                        })?
                        .clone(),
                );
                return Ok(());
            }
            flag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported tar flag -{flag}"),
                ))
            }
        }
        pos += 1;
    }
    Ok(())
}

pub(super) fn run_tar(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut mode = None;
    let mut verbose = false;
    let mut gzip = false;
    let mut archive: Option<String> = None;
    let mut extract_dir: Option<String> = None;
    let mut paths = Vec::new();
    let mut end_flags = false;

    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
        } else if !end_flags && arg == "--create" {
            set_tar_mode(&mut mode, TarMode::Create)?;
        } else if !end_flags && arg == "--list" {
            set_tar_mode(&mut mode, TarMode::List)?;
        } else if !end_flags && arg == "--extract" {
            set_tar_mode(&mut mode, TarMode::Extract)?;
        } else if !end_flags && arg == "--verbose" {
            verbose = true;
        } else if !end_flags && matches!(arg.as_str(), "--gzip" | "--gunzip" | "--ungzip") {
            gzip = true;
        } else if !end_flags && arg == "--file" {
            i += 1;
            archive = Some(
                args.get(i)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing value for --file")
                    })?
                    .clone(),
            );
        } else if !end_flags && arg == "--directory" {
            i += 1;
            extract_dir = Some(
                args.get(i)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing value for --directory")
                    })?
                    .clone(),
            );
        } else if !end_flags && arg.starts_with("--directory=") {
            extract_dir = Some(arg["--directory=".len()..].to_string());
        } else if !end_flags && arg.starts_with('-') && arg.len() > 1 {
            parse_short_tar_flags(
                arg,
                args,
                &mut i,
                &mut mode,
                &mut verbose,
                &mut gzip,
                &mut archive,
                &mut extract_dir,
            )?;
        } else {
            paths.push(arg.clone());
        }
        i += 1;
    }

    let archive = archive.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Usage: {program} (-c[fz] <archive.tar[.gz]> <source> | -t[fvz] <archive.tar[.gz]> | -x[fvz] <archive.tar[.gz]> [-C <dir>])"
            ),
        )
    })?;
    let compression = infer_tar_compression(&archive, gzip);

    if extract_dir.is_some() && !matches!(mode, Some(TarMode::Extract)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar -C/--directory is currently supported only with extract mode",
        ));
    }

    match mode {
        Some(TarMode::Create) => {
            if paths.len() != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Usage: {program} -c[fz] <archive.tar[.gz]> <source>"),
                ));
            }
            crate::main_app::create_tar_archive(
                Path::new(&paths[0]),
                Path::new(&archive),
                verbose,
                compression,
            )?;
        }
        Some(TarMode::List) => {
            if !paths.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tar list mode currently supports only whole-archive listing",
                ));
            }
            crate::main_app::list_tar_archive(Path::new(&archive), verbose, compression)?;
        }
        Some(TarMode::Extract) => {
            if !paths.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tar extract mode currently supports only whole-archive extraction",
                ));
            }
            crate::main_app::extract_tar_archive(
                Path::new(&archive),
                extract_dir.as_deref().map(Path::new),
                verbose,
                compression,
            )?;
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tar currently supports create (-c/--create), whole-archive list (-t/--list), and whole-archive extract (-x/--extract) modes",
            ));
        }
    }
    Ok(0)
}
