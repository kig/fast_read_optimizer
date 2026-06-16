use std::io;

pub(crate) const FORCE_NO_IO_URING_ENV: &str = "FRO_FORCE_NO_IO_URING";

pub(crate) fn io_uring_setup_should_fallback(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EPERM | libc::EACCES | libc::ENOSYS | libc::EOPNOTSUPP)
    )
}

#[cfg(target_os = "linux")]
pub(crate) fn io_uring_available(entries: u32) -> io::Result<bool> {
    use fro::uring::IoUring;
    if std::env::var_os(FORCE_NO_IO_URING_ENV).is_some() {
        return Ok(false);
    }
    match IoUring::new(entries) {
        Ok(_) => Ok(true),
        Err(err) if io_uring_setup_should_fallback(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn io_uring_available(entries: u32) -> io::Result<bool> {
    // Respect explicit opt-out.
    if std::env::var_os(FORCE_NO_IO_URING_ENV).is_some() {
        return Ok(false);
    }

    // The non-Linux IoUring implementation is a synchronous stub that executes
    // prepared SQEs in the calling thread. Returning `true` allows the reader
    // code to use the threaded execution model (one IoUring per worker thread),
    // which enables concurrent pread/pwrite across OS threads and restores
    // multi-threaded throughput on platforms like macOS.
    let _ = entries;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_uring_setup_fallback_classifies_expected_errno_values() {
        for errno in [libc::EPERM, libc::EACCES, libc::ENOSYS, libc::EOPNOTSUPP] {
            assert!(io_uring_setup_should_fallback(
                &io::Error::from_raw_os_error(errno)
            ));
        }
        assert!(!io_uring_setup_should_fallback(
            &io::Error::from_raw_os_error(libc::ENOMEM)
        ));
    }
}
