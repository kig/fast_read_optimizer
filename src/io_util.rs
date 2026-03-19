use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::prelude::OpenOptionsExt;

pub fn direct_open_should_fallback(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOTTY | libc::ESPIPE)
    ) || err.kind() == io::ErrorKind::InvalidInput
}

pub fn open_direct_reader_or_fallback(path: &str, fallback: &File) -> io::Result<File> {
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(err) if direct_open_should_fallback(&err) => fallback.try_clone(),
        Err(err) => Err(err),
    }
}

pub fn open_direct_writer_or_fallback(path: &str, fallback: &File) -> io::Result<File> {
    match OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(err) if direct_open_should_fallback(&err) => fallback.try_clone(),
        Err(err) => Err(err),
    }
}

pub fn open_reader_files(path: &str, use_direct: bool) -> io::Result<(File, File)> {
    let file = File::open(path)?;
    let file_direct = if use_direct {
        open_direct_reader_or_fallback(path, &file)?
    } else {
        file.try_clone()?
    };
    Ok((file, file_direct))
}
