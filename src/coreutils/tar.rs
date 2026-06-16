use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TarMode {
    Create,
    List,
    Extract,
}

fn infer_tar_compression(
    archive: &str,
    gzip_requested: bool,
    zstd_requested: bool,
    bzip2_requested: bool,
    xz_requested: bool,
    _auto_compress_requested: bool,
) -> crate::main_app::TarCompression {
    if gzip_requested {
        return crate::main_app::TarCompression::Gzip;
    }
    if zstd_requested {
        return crate::main_app::TarCompression::Zstd;
    }
    if bzip2_requested {
        return crate::main_app::TarCompression::Bzip2;
    }
    if xz_requested {
        return crate::main_app::TarCompression::Xz;
    }
    let archive = archive.to_ascii_lowercase();
    if archive.ends_with(".tar.gz") || archive.ends_with(".tgz") {
        crate::main_app::TarCompression::Gzip
    } else if archive.ends_with(".tar.bz2")
        || archive.ends_with(".tbz")
        || archive.ends_with(".tbz2")
    {
        crate::main_app::TarCompression::Bzip2
    } else if archive.ends_with(".tar.xz") || archive.ends_with(".txz") {
        crate::main_app::TarCompression::Xz
    } else if archive.ends_with(".tar.zst")
        || archive.ends_with(".tar.zstd")
        || archive.ends_with(".tzst")
    {
        crate::main_app::TarCompression::Zstd
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

fn resolve_tar_path(path: &str, cwd: Option<&Path>) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(cwd) = cwd {
        cwd.join(path)
    } else {
        path.to_path_buf()
    }
}

fn parse_tar_flag_bundle(
    chars: &[char],
    args: &[String],
    index: &mut usize,
    mode: &mut Option<TarMode>,
    verbose: &mut bool,
    gzip: &mut bool,
    bzip2: &mut bool,
    xz: &mut bool,
    _zstd: &mut bool,
    auto_compress: &mut bool,
    archive: &mut Option<String>,
    extract_dir: &mut Option<String>,
) -> io::Result<()> {
    let mut pos = 0usize;
    while pos < chars.len() {
        match chars[pos] {
            'c' => set_tar_mode(mode, TarMode::Create)?,
            't' => set_tar_mode(mode, TarMode::List)?,
            'x' => set_tar_mode(mode, TarMode::Extract)?,
            'v' => *verbose = true,
            'a' => *auto_compress = true,
            'j' => *bzip2 = true,
            'z' => *gzip = true,
            'J' => *xz = true,
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

fn parse_short_tar_flags(
    arg: &str,
    args: &[String],
    index: &mut usize,
    mode: &mut Option<TarMode>,
    verbose: &mut bool,
    gzip: &mut bool,
    bzip2: &mut bool,
    xz: &mut bool,
    zstd: &mut bool,
    auto_compress: &mut bool,
    archive: &mut Option<String>,
    extract_dir: &mut Option<String>,
) -> io::Result<()> {
    let chars = arg[1..].chars().collect::<Vec<_>>();
    parse_tar_flag_bundle(
        &chars,
        args,
        index,
        mode,
        verbose,
        gzip,
        bzip2,
        xz,
        zstd,
        auto_compress,
        archive,
        extract_dir,
    )
}

fn is_old_style_tar_flags(arg: &str) -> bool {
    !arg.is_empty()
        && arg.chars().any(|ch| matches!(ch, 'c' | 't' | 'x'))
        && arg.chars().all(|ch| {
            matches!(
                ch,
                'c' | 't' | 'x' | 'v' | 'a' | 'j' | 'z' | 'J' | 'f' | 'C'
            )
        })
}

pub(super) fn run_tar(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut mode = None;
    let mut verbose = false;
    let mut gzip = false;
    let mut bzip2 = false;
    let mut xz = false;
    let mut zstd = false;
    let mut auto_compress = false;
    let mut archive: Option<String> = None;
    let mut extract_dir: Option<String> = None;
    let mut cwd: Option<PathBuf> = None;
    let mut paths = Vec::new();
    let mut end_flags = false;

    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
        } else if !end_flags && arg == "--fro-cwd" {
            i += 1;
            cwd = Some(PathBuf::from(
                args.get(i)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing value for --fro-cwd")
                    })?
                    .clone(),
            ));
        } else if !end_flags && arg.starts_with("--fro-cwd=") {
            cwd = Some(PathBuf::from(arg["--fro-cwd=".len()..].to_string()));
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
        } else if !end_flags && arg == "--bzip2" {
            bzip2 = true;
        } else if !end_flags && arg == "--xz" {
            xz = true;
        } else if !end_flags && arg == "--zstd" {
            zstd = true;
        } else if !end_flags && arg == "--auto-compress" {
            auto_compress = true;
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
                &mut bzip2,
                &mut xz,
                &mut zstd,
                &mut auto_compress,
                &mut archive,
                &mut extract_dir,
            )?;
        } else if !end_flags
            && mode.is_none()
            && archive.is_none()
            && extract_dir.is_none()
            && paths.is_empty()
            && is_old_style_tar_flags(arg)
        {
            let chars = arg.chars().collect::<Vec<_>>();
            parse_tar_flag_bundle(
                &chars,
                args,
                &mut i,
                &mut mode,
                &mut verbose,
                &mut gzip,
                &mut bzip2,
                &mut xz,
                &mut zstd,
                &mut auto_compress,
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
                "Usage: {program} (-c[afzjJ] <archive.tar[.gz|.bz2|.xz|.zst]> <source> | -t[fvzjJ] <archive.tar[.gz|.bz2|.xz|.zst]> | -x[fvzjJ] <archive.tar[.gz|.bz2|.xz|.zst]> [-C <dir>])"
            ),
        )
    })?;
    let explicit_compression_count = [gzip, bzip2, xz, zstd]
        .into_iter()
        .filter(|flag| *flag)
        .count();
    if explicit_compression_count > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar accepts at most one compression flag of --gzip/-z, --bzip2/-j, --xz/-J, or --zstd",
        ));
    }
    let compression = infer_tar_compression(&archive, gzip, zstd, bzip2, xz, auto_compress);
    let archive_path = resolve_tar_path(&archive, cwd.as_deref());

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
                    format!("Usage: {program} -c[afzjJ] <archive.tar[.gz|.bz2|.xz|.zst]> <source>"),
                ));
            }
            let source_arg = PathBuf::from(&paths[0]);
            let source_fs = resolve_tar_path(&paths[0], cwd.as_deref());
            crate::main_app::create_tar_archive_from(
                &source_arg,
                &source_fs,
                &archive_path,
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
            crate::main_app::list_tar_archive(&archive_path, verbose, compression)?;
        }
        Some(TarMode::Extract) => {
            if !paths.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tar extract mode currently supports only whole-archive extraction",
                ));
            }
            let extract_dir = extract_dir
                .as_deref()
                .map(|path| resolve_tar_path(path, cwd.as_deref()));
            crate::main_app::extract_tar_archive(
                &archive_path,
                extract_dir.as_deref(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tar_test_temp_dir(name: &str) -> PathBuf {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn resolve_tar_path_uses_cwd_for_relative_paths() {
        let cwd = Path::new("/tmp/fro-tar-cwd");
        assert_eq!(
            resolve_tar_path("archive.tar", Some(cwd)),
            cwd.join("archive.tar")
        );
        assert_eq!(
            resolve_tar_path("/tmp/archive.tar", Some(cwd)),
            PathBuf::from("/tmp/archive.tar")
        );
    }

    #[test]
    fn tar_hidden_cwd_preserves_dot_root_name() {
        let base = tar_test_temp_dir("fro-tar-ipc");
        let source_dir = base.join("src");
        let archive = base.join("out.tar");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("file.txt"), b"alpha\n").unwrap();

        let args = vec![
            "tar".to_string(),
            format!("--fro-cwd={}", source_dir.display()),
            "-cf".to_string(),
            archive.to_string_lossy().into_owned(),
            ".".to_string(),
        ];
        run_tar(&args).unwrap();
        let listing = std::process::Command::new("tar")
            .arg("-tf")
            .arg(&archive)
            .output()
            .unwrap();
        assert!(listing.status.success());
        let stdout = String::from_utf8(listing.stdout).unwrap();
        assert!(stdout.contains("./"));
        assert!(stdout.contains("./file.txt"));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn tar_old_style_gzip_create_and_extract_work() {
        let base = tar_test_temp_dir("fro-tar-old-style-gzip");
        let source_root = base.join("source");
        let extract_root = base.join("extract");
        let archive = base.join("foo.tar.gz");
        let source_dir = source_root.join("foo");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&extract_root).unwrap();
        fs::write(source_dir.join("file.txt"), b"alpha\nbeta\n").unwrap();

        let create_args = vec![
            "tar".to_string(),
            format!("--fro-cwd={}", source_root.display()),
            "czf".to_string(),
            archive.to_string_lossy().into_owned(),
            "foo".to_string(),
        ];
        assert_eq!(run_tar(&create_args).unwrap(), 0);

        let extract_args = vec![
            "tar".to_string(),
            "xf".to_string(),
            archive.to_string_lossy().into_owned(),
            "-C".to_string(),
            extract_root.to_string_lossy().into_owned(),
        ];
        assert_eq!(run_tar(&extract_args).unwrap(), 0);
        assert_eq!(
            fs::read(extract_root.join("foo").join("file.txt")).unwrap(),
            b"alpha\nbeta\n"
        );

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn infer_tar_compression_recognizes_bzip2_xz_and_zstd_suffixes() {
        assert_eq!(
            infer_tar_compression("archive.tar.bz2", false, false, false, false, false),
            crate::main_app::TarCompression::Bzip2
        );
        assert_eq!(
            infer_tar_compression("archive.tbz2", false, false, false, false, false),
            crate::main_app::TarCompression::Bzip2
        );
        assert_eq!(
            infer_tar_compression("archive.txz", false, false, false, false, false),
            crate::main_app::TarCompression::Xz
        );
        assert_eq!(
            infer_tar_compression("archive.tar.zst", false, false, false, false, false),
            crate::main_app::TarCompression::Zstd
        );
    }

    #[test]
    fn tar_old_style_xz_and_long_zstd_flags_work() {
        if !std::process::Command::new("sh")
            .arg("-c")
            .arg("command -v xz >/dev/null 2>&1")
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
        {
            return;
        }
        let base = tar_test_temp_dir("fro-tar-extra-compression");
        let source_root = base.join("source");
        let extract_root = base.join("extract");
        let xz_archive = base.join("foo.tar.xz");
        let zstd_archive = base.join("foo.tar.zst");
        let source_dir = source_root.join("foo");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&extract_root).unwrap();
        fs::write(source_dir.join("file.txt"), b"alpha\nbeta\n").unwrap();

        let xz_create_args = vec![
            "tar".to_string(),
            format!("--fro-cwd={}", source_root.display()),
            "cJf".to_string(),
            xz_archive.to_string_lossy().into_owned(),
            "foo".to_string(),
        ];
        assert_eq!(run_tar(&xz_create_args).unwrap(), 0);

        let zstd_create_args = vec![
            "tar".to_string(),
            format!("--fro-cwd={}", source_root.display()),
            "--create".to_string(),
            "--zstd".to_string(),
            "--file".to_string(),
            zstd_archive.to_string_lossy().into_owned(),
            "foo".to_string(),
        ];
        assert_eq!(run_tar(&zstd_create_args).unwrap(), 0);

        let extract_args = vec![
            "tar".to_string(),
            "xf".to_string(),
            xz_archive.to_string_lossy().into_owned(),
            "-C".to_string(),
            extract_root.to_string_lossy().into_owned(),
        ];
        assert_eq!(run_tar(&extract_args).unwrap(), 0);
        assert_eq!(
            fs::read(extract_root.join("foo").join("file.txt")).unwrap(),
            b"alpha\nbeta\n"
        );

        let zstd_list_args = vec![
            "tar".to_string(),
            "--list".to_string(),
            "--file".to_string(),
            zstd_archive.to_string_lossy().into_owned(),
        ];
        assert_eq!(run_tar(&zstd_list_args).unwrap(), 0);

        let _ = fs::remove_dir_all(base);
    }
}
