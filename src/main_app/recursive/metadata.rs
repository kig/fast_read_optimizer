use super::*;

#[derive(Clone, Copy)]
pub(crate) struct PreservedTimestamps {
    pub(super) atime_sec: i64,
    pub(super) atime_nsec: i64,
    pub(super) mtime_sec: i64,
    pub(super) mtime_nsec: i64,
}

#[derive(Clone)]
pub(crate) struct RecursiveDirectoryMetadataTask {
    pub(super) target_path: PathBuf,
    pub(super) timestamps: PreservedTimestamps,
}

pub(crate) fn preserved_timestamps_from_metadata(metadata: &fs::Metadata) -> PreservedTimestamps {
    PreservedTimestamps {
        atime_sec: metadata.atime(),
        atime_nsec: metadata.atime_nsec(),
        mtime_sec: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
    }
}

pub(super) fn set_path_timestamps(
    path: &Path,
    timestamps: PreservedTimestamps,
    nofollow_symlink: bool,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })?;
    let times = [
        libc::timespec {
            tv_sec: timestamps.atime_sec,
            tv_nsec: timestamps.atime_nsec,
        },
        libc::timespec {
            tv_sec: timestamps.mtime_sec,
            tv_nsec: timestamps.mtime_nsec,
        },
    ];
    let flags = if nofollow_symlink {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), flags) };
    if rc == 0 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

pub(crate) fn preserve_file_timestamps(source_path: &Path, target_path: &Path) -> io::Result<()> {
    let metadata = fs::metadata(source_path)?;
    set_path_timestamps(
        target_path,
        preserved_timestamps_from_metadata(&metadata),
        false,
    )
}

pub(crate) fn finalize_directory_timestamps(
    tasks: &[RecursiveDirectoryMetadataTask],
) -> io::Result<()> {
    let mut sorted = tasks.to_vec();
    sorted.sort_by_key(|task| std::cmp::Reverse(task.target_path.components().count()));
    for task in sorted {
        set_path_timestamps(&task.target_path, task.timestamps, false)?;
    }
    Ok(())
}
