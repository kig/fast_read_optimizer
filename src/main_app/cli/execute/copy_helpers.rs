use super::*;

pub(super) fn resolve_cp_file_target_path(
    source_path: &Path,
    target_path: &Path,
    cp_compat: bool,
    cp_no_target_directory: bool,
) -> io::Result<PathBuf> {
    if !cp_compat || cp_no_target_directory {
        return Ok(target_path.to_path_buf());
    }
    if fs::symlink_metadata(target_path).is_ok_and(|metadata| metadata.file_type().is_dir()) {
        return Ok(target_path.join(source_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cp source has no final path component",
            )
        })?));
    }
    Ok(target_path.to_path_buf())
}

pub(super) fn preserve_copied_file_metadata(
    source_path: &Path,
    target_path: &Path,
    preserve_mode: bool,
    preserve_timestamps: bool,
) -> io::Result<()> {
    if preserve_mode {
        let source_mode = fs::metadata(source_path)?.permissions().mode();
        fs::set_permissions(target_path, fs::Permissions::from_mode(source_mode))?;
    }
    if preserve_timestamps {
        recursive::preserve_file_timestamps(source_path, target_path)?;
    }
    Ok(())
}
