//! OS compatibility helpers: provide fallbacks or mappings for Linux-only syscalls and constants
//! so the crate can compile and run on macOS with reasonable fallbacks.

use std::io;
use std::os::unix::io::{RawFd, AsRawFd};
use std::fs::OpenOptions;
use std::ffi::CString;

#[cfg(target_os = "linux")]
pub const O_DIRECT: i32 = libc::O_DIRECT;
#[cfg(not(target_os = "linux"))]
pub const O_DIRECT: i32 = 0;

// POSIX fadvise constants: on non-Linux map to 0 (no-op) or to nearest available.
#[cfg(target_os = "linux")]
pub const POSIX_FADV_DONTNEED: i32 = libc::POSIX_FADV_DONTNEED;
#[cfg(not(target_os = "linux"))]
pub const POSIX_FADV_DONTNEED: i32 = 0;

#[cfg(target_os = "linux")]
pub const POSIX_FADV_NOREUSE: i32 = libc::POSIX_FADV_NOREUSE;
#[cfg(not(target_os = "linux"))]
pub const POSIX_FADV_NOREUSE: i32 = 0;

#[cfg(target_os = "linux")]
pub const POSIX_FADV_WILLNEED: i32 = libc::POSIX_FADV_WILLNEED;
#[cfg(not(target_os = "linux"))]
pub const POSIX_FADV_WILLNEED: i32 = 0;

// SOCK_CLOEXEC compatibility: on macOS / BSD the constant is named O_CLOEXEC
#[cfg(target_os = "linux")]
pub const SOCK_CLOEXEC: libc::c_int = libc::SOCK_CLOEXEC;
#[cfg(not(target_os = "linux"))]
pub const SOCK_CLOEXEC: libc::c_int = libc::O_CLOEXEC;

// posix_fadvise wrapper. On non-Linux this is a no-op and returns 0.
pub fn posix_fadvise(fd: RawFd, offset: i64, len: i64, advice: i32) -> i32 {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::posix_fadvise(fd, offset as libc::off_t, len as libc::off_t, advice)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, offset, len, advice);
        0
    }
}

// posix_fallocate wrapper. On non-Linux return success (best-effort).
pub fn posix_fallocate(fd: RawFd, offset: i64, len: i64) -> i32 {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::posix_fallocate(fd, offset as libc::off_t, len as libc::off_t)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, offset, len);
        0
    }
}

// copy_file_range wrapper: on Linux call syscall; on other platforms perform a chunked pread/pwrite fallback.
pub fn copy_file_range(
    src_fd: RawFd,
    src_off: &mut libc::off_t,
    dst_fd: RawFd,
    dst_off: &mut libc::off_t,
    len: usize,
    flags: u32,
) -> io::Result<isize> {
    #[cfg(target_os = "linux")]
    unsafe {
        // copy_file_range has this signature on Linux
        let ret = libc::copy_file_range(
            src_fd,
            src_off as *mut libc::off_t,
            dst_fd,
            dst_off as *mut libc::off_t,
            len,
            flags as libc::c_uint,
        );
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as isize)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::cmp;
        let mut remaining = len;
        let mut total_copied: isize = 0;
        let mut buf = vec![0u8; 128 * 1024];
        while remaining > 0 {
            let to_read = cmp::min(remaining, buf.len());
            let nread = unsafe {
                libc::pread(
                    src_fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    to_read,
                    *src_off,
                )
            };
            if nread < 0 {
                return Err(io::Error::last_os_error());
            }
            if nread == 0 {
                break;
            }
            let mut written = 0usize;
            while written < nread as usize {
                let nw = unsafe {
                    libc::pwrite(
                        dst_fd,
                        buf[written..nread as usize].as_ptr() as *const libc::c_void,
                        (nread as usize - written) as libc::size_t,
                        *dst_off,
                    )
                };
                if nw < 0 {
                    let err = io::Error::last_os_error();
                    match err.raw_os_error() {
                        Some(libc::EINTR) => continue,
                        _ => return Err(err),
                    }
                }
                written += nw as usize;
                *dst_off = (*dst_off).saturating_add(nw as libc::off_t);
            }
            *src_off = (*src_off).saturating_add(nread as libc::off_t);
            remaining = remaining.saturating_sub(nread as usize);
            total_copied += nread as isize;
        }
        Ok(total_copied)
    }
}

// Reflink constant (FICLONE) - on macOS use clonefile, but keep a Linux constant when available.
#[cfg(target_os = "linux")]
pub const FICLONE: libc::c_ulong = libc::FICLONE as libc::c_ulong;
#[cfg(not(target_os = "linux"))]
pub const FICLONE: libc::c_ulong = 0;

/// Attempt a platform-appropriate reflink/clone from `src` to `dst`.
/// On Linux this performs an ioctl(FICLONE) on the destination fd.
/// On macOS this calls clonefile(src, dst). On other platforms it falls back to a buffered copy.
pub fn reflink_paths(src: &str, dst: &str) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let src_f = OpenOptions::new().read(true).open(src)?;
        let dst_f = OpenOptions::new().read(true).write(true).open(dst)?;
        let rc = unsafe { libc::ioctl(dst_f.as_raw_fd(), FICLONE as libc::c_ulong, src_f.as_raw_fd()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        let csrc = CString::new(src).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"))?;
        let cdst = CString::new(dst).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination path contains NUL"))?;
        let rc = unsafe { libc::clonefile(csrc.as_ptr(), cdst.as_ptr(), 0) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        std::fs::copy(src, dst).map(|_| ()).map_err(|e| e)
    }
}

// Provide a loff_t alias for code that expects it.
pub type loff_t = libc::off_t;

#[cfg(target_os = "linux")]
pub const MADV_HUGEPAGE: libc::c_int = libc::MADV_HUGEPAGE;
#[cfg(not(target_os = "linux"))]
pub const MADV_HUGEPAGE: libc::c_int = 0;


// Grow pipe best-effort: attempt to set pipe size when supported (Linux), otherwise no-op.
pub fn grow_pipe_best_effort(fd: RawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let target_size = 1 << 20; // 1 MiB
        let rc = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, target_size) };
        if rc >= 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EPERM | libc::EINVAL | libc::EBUSY) => Ok(()),
            _ => Err(err),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = fd;
        Ok(())
    }
}

// Try to set pipe size to a specific value. On non-Linux platforms this returns Unsupported.
pub fn try_set_pipe_size(fd: RawFd, size: libc::c_int) -> io::Result<i32> {
    #[cfg(target_os = "linux")]
    unsafe {
        let rc = libc::fcntl(fd, libc::F_SETPIPE_SZ, size);
        if rc >= 0 {
            Ok(rc)
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, size);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "F_SETPIPE_SZ not supported on this platform",
        ))
    }
}

// sendfile wrapper: on Linux call sendfile; on other platforms provide a read/write fallback.
pub fn vmsplice_all(pipe_fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut written_total = 0usize;
        let mut ptr = buf.as_ptr();
        let mut remaining = buf.len();
        while remaining > 0 {
            let rc = if remaining % 4096 == 0 && ptr.align_offset(4096) == 0 {
                let iov = libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: remaining,
                };
                libc::vmsplice(pipe_fd, &iov as *const libc::iovec, 1, libc::SPLICE_F_GIFT)
            } else {
                libc::write(pipe_fd, ptr as *mut libc::c_void, remaining)
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EINTR | libc::EAGAIN) => continue,
                    _ => return Err(err),
                }
            }
            let n = rc as usize;
            written_total = written_total
                .checked_add(n)
                .ok_or_else(|| io::Error::other("vmsplice overflow"))?;
            ptr = ptr.add(n);
            remaining -= n;
        }
        Ok(written_total)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Fallback: just use write in a loop for portability.
        let mut written_total = 0usize;
        let mut ptr = buf.as_ptr();
        let mut remaining = buf.len();
        while remaining > 0 {
            let rc = unsafe { libc::write(pipe_fd, ptr as *const libc::c_void, remaining) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EINTR | libc::EAGAIN) => continue,
                    _ => return Err(err),
                }
            }
            let n = rc as usize;
            written_total = written_total
                .checked_add(n)
                .ok_or_else(|| io::Error::other("write overflow"))?;
            ptr = unsafe { ptr.add(n) };
            remaining -= n;
        }
        Ok(written_total)
    }
}
pub fn sendfile(out_fd: RawFd, in_fd: RawFd, offset: &mut libc::off_t, count: usize) -> io::Result<isize> {
// --- vmsplice_all portable implementation appended below ---

/// Portable vmsplice_all: on Linux uses vmsplice, on other platforms falls back to write.

// --- end vmsplice_all portable implementation ---

    #[cfg(target_os = "linux")]
    unsafe {
        let ret = libc::sendfile(out_fd, in_fd, offset, count as libc::size_t);
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as isize)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Fallback: perform a loop using pread + write to emulate sendfile semantics.
        // This reads from in_fd at *offset and writes to out_fd, advancing offset by the bytes copied.
        let mut remaining = count as usize;
        let mut total_copied: isize = 0;
        let mut buf = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            let nread = unsafe {
                libc::pread(
                    in_fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    to_read,
                    *offset,
                )
            };
            if nread < 0 {
                return Err(io::Error::last_os_error());
            }
            if nread == 0 {
                break;
            }
            let mut write_ptr = 0;
            while write_ptr < nread as usize {
                let nw = unsafe {
                    libc::write(
                        out_fd,
                        buf[write_ptr..nread as usize].as_ptr() as *const libc::c_void,
                        (nread as usize - write_ptr) as libc::size_t,
                    )
                };
                if nw < 0 {
                    return Err(io::Error::last_os_error());
                }
                write_ptr += nw as usize;
            }
            *offset = (*offset).saturating_add(nread as libc::off_t);
            remaining = remaining.saturating_sub(nread as usize);
            total_copied += nread as isize;
        }
            Ok(total_copied)
    }
}

// vmsplice_all: on Linux use vmsplice for zero-copy when possible; otherwise fall back to write

// pipe2 wrapper: on Linux call pipe2; on other platforms use pipe and set CLOEXEC when requested.
pub fn pipe2(flags: libc::c_int) -> io::Result<(RawFd, RawFd)> {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut fds = [0 as libc::c_int, 0 as libc::c_int];
        if libc::pipe2(fds.as_mut_ptr(), flags) != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((fds[0], fds[1]))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut fds = [0 as libc::c_int, 0 as libc::c_int];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if (flags & libc::O_CLOEXEC) != 0 {
            for &fd in &fds {
                // set FD_CLOEXEC
                unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            }
        }
        Ok((fds[0], fds[1]))
    }
}

// splice wrapper: on Linux call splice; on other platforms return Unsupported.
pub fn splice(
    fd_in: RawFd,
    off_in: *mut libc::off_t,
    fd_out: RawFd,
    off_out: *mut libc::off_t,
    len: usize,
    flags: u32,
) -> io::Result<isize> {
    #[cfg(target_os = "linux")]
    unsafe {
        let ret = libc::splice(fd_in, off_in as *mut libc::off_t, fd_out, off_out as *mut libc::off_t, len as libc::size_t, flags as libc::c_uint);
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as isize)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd_in, off_in, fd_out, off_out, len, flags);
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

