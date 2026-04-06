use super::*;

pub(super) fn current_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

pub(super) fn prospective_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.exists() {
        return path.canonicalize();
    }
    let absolute = current_absolute_path(path)?;
    let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.canonicalize()?.join(
        absolute
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing target name"))?,
    ))
}

pub(in crate::main_app) fn resolve_recursive_copy_root(
    source_root: &Path,
    target: &Path,
) -> io::Result<PathBuf> {
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(target.join(
            source_root
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source root has no final path component"))?,
        )),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy target must be a directory or a missing path when copying a directory recursively",
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(target.to_path_buf()),
        Err(err) => Err(err),
    }
}
