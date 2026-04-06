use super::*;
use crate::io_util::checked_posix_fallocate;

fn append_tar_entry(
    entries: &mut Vec<TarEntry>,
    archive_path: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
    kind: TarEntryKind,
    next_offset: &mut u64,
) {
    let header_offset = *next_offset;
    *next_offset = next_offset.saturating_add(TAR_BLOCK_SIZE);
    let data_offset = *next_offset;
    if let TarEntryKind::RegularFile { size, .. } = &kind {
        *next_offset = next_offset.saturating_add(align_up(*size, TAR_BLOCK_SIZE));
    }
    entries.push(TarEntry {
        archive_path,
        mode,
        uid,
        gid,
        mtime,
        kind,
        header_offset,
        data_offset,
    });
}

pub(super) fn collect_tar_manifest(
    source: &Path,
    output: &Path,
) -> io::Result<(Vec<TarEntry>, u64)> {
    let source_meta = fs::symlink_metadata(source)?;
    let source_abs = if source_meta.file_type().is_dir() {
        source.canonicalize()?
    } else {
        paths::prospective_absolute_path(source)?
    };
    let output_abs = paths::prospective_absolute_path(output)?;
    if source_meta.file_type().is_dir() && output_abs.starts_with(&source_abs) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to archive directory {} into itself via {}",
                source.display(),
                output.display()
            ),
        ));
    }
    if !source_meta.file_type().is_dir() && output_abs == source_abs {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "refusing to overwrite the archived source path",
        ));
    }

    let mut entries = Vec::new();
    let mut next_offset = 0_u64;
    let root_name = file_name_bytes(source)?;
    let root_name_path = PathBuf::from(std::ffi::OsString::from_vec(root_name.clone()));
    let root_mtime = source_meta.mtime().max(0) as u64;

    if source_meta.file_type().is_dir() {
        append_tar_entry(
            &mut entries,
            {
                let mut path = root_name.clone();
                path.push(b'/');
                path
            },
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::Directory,
            &mut next_offset,
        );
        let mut stack = vec![(source.to_path_buf(), root_name_path)];
        while let Some((dir_path, archive_prefix)) = stack.pop() {
            let mut entries_in_dir =
                fs::read_dir(&dir_path)?.collect::<Result<Vec<_>, io::Error>>()?;
            entries_in_dir.sort_by_key(|entry| entry.file_name());
            let mut child_dirs = Vec::new();
            for entry in entries_in_dir {
                let file_type = entry.file_type()?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)?;
                let archive_path =
                    join_tar_path(&archive_prefix, &entry.file_name(), file_type.is_dir());
                let mtime = metadata.mtime().max(0) as u64;
                if file_type.is_dir() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::Directory,
                        &mut next_offset,
                    );
                    child_dirs.push((path, archive_prefix.join(entry.file_name())));
                } else if file_type.is_symlink() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::Symlink {
                            target: fs::read_link(&path)?.as_os_str().as_bytes().to_vec(),
                        },
                        &mut next_offset,
                    );
                } else if file_type.is_file() {
                    append_tar_entry(
                        &mut entries,
                        archive_path,
                        metadata.mode(),
                        metadata.uid(),
                        metadata.gid(),
                        mtime,
                        TarEntryKind::RegularFile {
                            size: metadata.len(),
                            source_path: path,
                        },
                        &mut next_offset,
                    );
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "tar only supports regular files, directories, and symlinks (saw {})",
                            path.display()
                        ),
                    ));
                }
            }
            child_dirs.reverse();
            stack.extend(child_dirs);
        }
    } else if source_meta.file_type().is_symlink() {
        append_tar_entry(
            &mut entries,
            root_name,
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::Symlink {
                target: fs::read_link(source)?.as_os_str().as_bytes().to_vec(),
            },
            &mut next_offset,
        );
    } else if source_meta.file_type().is_file() {
        append_tar_entry(
            &mut entries,
            root_name,
            source_meta.mode(),
            source_meta.uid(),
            source_meta.gid(),
            root_mtime,
            TarEntryKind::RegularFile {
                size: source_meta.len(),
                source_path: source.to_path_buf(),
            },
            &mut next_offset,
        );
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar create currently supports only regular files, directories, and symlinks",
        ));
    }

    Ok((entries, next_offset.saturating_add(TAR_EOF_BLOCKS)))
}

pub(super) fn prepare_tar_output(path: &Path, total_size: u64) -> io::Result<fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(total_size)?;
    checked_posix_fallocate(&file, 0, total_size, "failed to preallocate tar output")?;
    Ok(file)
}
