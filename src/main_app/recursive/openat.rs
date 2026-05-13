use super::*;

pub(in crate::main_app) fn open_dir_fd(path: &Path) -> io::Result<fs::File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

pub(super) fn open_relative_fd(
    dir: &fs::File,
    relative: &Path,
    flags: i32,
    mode: libc::mode_t,
) -> io::Result<fs::File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_rel = CString::new(relative.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", relative.display()),
        )
    })?;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            c_rel.as_ptr(),
            flags | libc::O_CLOEXEC,
            mode as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

pub(super) fn open_relative_target_for_copy(
    dir: &fs::File,
    relative: &Path,
    mode: libc::mode_t,
) -> io::Result<(fs::File, bool)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_rel = CString::new(relative.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", relative.display()),
        )
    })?;
    let create_flags = libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            c_rel.as_ptr(),
            create_flags,
            mode as libc::c_uint,
        )
    };
    if fd >= 0 {
        return Ok((unsafe { fs::File::from_raw_fd(fd) }, true));
    }
    let err = io::Error::last_os_error();
    if err.kind() != io::ErrorKind::AlreadyExists {
        return Err(err);
    }
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            c_rel.as_ptr(),
            libc::O_WRONLY | libc::O_TRUNC | libc::O_CLOEXEC,
            mode as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((unsafe { fs::File::from_raw_fd(fd) }, false))
}

pub(super) const FAST_COPY_SENDFILE_CHUNK_SIZE: usize = 0x7fff_f000usize;

pub(super) fn copy_openat_via_sendfile(
    relative_path: &Path,
    source_len: u64,
    source: &fs::File,
    target: &fs::File,
) -> io::Result<u64> {
    let mut copied_total = 0_u64;
    let mut source_pos: fro::os::loff_t = 0;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(FAST_COPY_SENDFILE_CHUNK_SIZE as u64) as usize;
        let copied = match fro::os::sendfile(
            target.as_raw_fd(),
            source.as_raw_fd(),
            &mut source_pos,
            chunk,
        ) {
            Ok(v) => v,
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "sendfile/openat failed for {}: {}",
                        relative_path.display(),
                        err
                    ),
                ));
            }
        };
        if copied > 0 {
            copied_total = copied_total.saturating_add(copied as u64);
            continue;
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "sendfile/openat stopped early after {} of {} bytes for {}",
                    copied_total,
                    source_len,
                    relative_path.display()
                ),
            ));
        }
    }
    Ok(copied_total)
}

pub(super) fn copy_openat_via_copy_file_range(
    relative_path: &Path,
    source_len: u64,
    source: &fs::File,
    target: &fs::File,
) -> io::Result<u64> {
    let mut source_pos: fro::os::loff_t = 0;
    let mut target_pos: fro::os::loff_t = 0;
    let mut copied_total = 0_u64;
    while copied_total < source_len {
        let remaining = source_len - copied_total;
        let chunk = remaining.min(usize::MAX as u64) as usize;
        let copied = match fro::os::copy_file_range(
            source.as_raw_fd(),
            &mut source_pos,
            target.as_raw_fd(),
            &mut target_pos,
            chunk,
            0,
        ) {
            Ok(v) => v,
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "copy_file_range/openat failed for {}: {}",
                        relative_path.display(),
                        err
                    ),
                ));
            }
        };
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "copy_file_range/openat stopped early after {} of {} bytes for {}",
                    copied_total,
                    source_len,
                    relative_path.display()
                ),
            ));
        }
        copied_total = copied_total.saturating_add(copied as u64);
    }
    Ok(copied_total)
}

pub(in crate::main_app) fn copy_small_file_openat(
    entry: &ManifestCopyEntry,
    source_root_fd: &fs::File,
    target_root_fd: &fs::File,
    method: RelativeCopyMethod,
) -> io::Result<u64> {
    let source = open_relative_fd(source_root_fd, &entry.relative_path, libc::O_RDONLY, 0)?;
    let (target, created) = open_relative_target_for_copy(
        target_root_fd,
        &entry.relative_path,
        entry.mode as libc::mode_t,
    )?;
    let copied_total = match method {
        RelativeCopyMethod::CopyFileRange => {
            copy_openat_via_copy_file_range(&entry.relative_path, entry.size, &source, &target)?
        }
        RelativeCopyMethod::Sendfile => {
            copy_openat_via_sendfile(&entry.relative_path, entry.size, &source, &target)?
        }
    };
    if !created {
        let rc = unsafe { libc::fchmod(target.as_raw_fd(), entry.mode as libc::mode_t) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(copied_total)
}

pub(super) fn copy_relative_file_openat(
    relative_path: &Path,
    source_len: u64,
    source_mode: u32,
    source_root_fd: &fs::File,
    target_root_fd: &fs::File,
    method: RelativeCopyMethod,
) -> io::Result<u64> {
    let source = open_relative_fd(source_root_fd, relative_path, libc::O_RDONLY, 0)?;
    let (target, created) =
        open_relative_target_for_copy(target_root_fd, relative_path, source_mode as libc::mode_t)?;
    let copied_total = match method {
        RelativeCopyMethod::CopyFileRange => {
            copy_openat_via_copy_file_range(relative_path, source_len, &source, &target)?
        }
        RelativeCopyMethod::Sendfile => {
            copy_openat_via_sendfile(relative_path, source_len, &source, &target)?
        }
    };
    if !created {
        let rc = unsafe { libc::fchmod(target.as_raw_fd(), source_mode as libc::mode_t) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(copied_total)
}
