use super::*;

pub(super) fn load_paths_from_manifest(path: &Path) -> io::Result<Vec<PathBuf>> {
    let file = fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    let mut paths = Vec::new();
    loop {
        line.clear();
        let read = std::io::BufRead::read_line(&mut reader, &mut line)?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        paths.push(PathBuf::from(trimmed));
    }
    Ok(paths)
}

pub(super) fn load_manifest_copy_entries(
    manifest: &Path,
    source_root: &Path,
) -> io::Result<Vec<ManifestCopyEntry>> {
    let paths = load_paths_from_manifest(manifest)?;
    let mut entries = Vec::with_capacity(paths.len());
    for source_path in paths {
        let metadata = fs::symlink_metadata(&source_path)?;
        if !metadata.file_type().is_file() {
            continue;
        }
        let relative_path = source_path.strip_prefix(source_root).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "manifest path {} is not under source root {}",
                    source_path.display(),
                    source_root.display()
                ),
            )
        })?;
        let relative_path = relative_path.to_path_buf();
        entries.push(ManifestCopyEntry {
            relative_path,
            size: metadata.len(),
            mode: metadata.permissions().mode(),
        });
    }
    Ok(entries)
}

pub(super) fn create_manifest_target_dirs(
    entries: &[ManifestCopyEntry],
    target_root: &Path,
) -> io::Result<(usize, std::time::Duration)> {
    let start = std::time::Instant::now();
    let mut dirs = std::collections::BTreeSet::<PathBuf>::new();
    for entry in entries {
        let mut current = PathBuf::new();
        if let Some(parent) = entry.relative_path.parent() {
            for component in parent.components() {
                current.push(component.as_os_str());
                dirs.insert(current.clone());
            }
        }
    }
    for dir in &dirs {
        fs::create_dir_all(target_root.join(dir))?;
    }
    Ok((dirs.len(), start.elapsed()))
}
