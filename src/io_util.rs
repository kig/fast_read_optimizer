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

pub fn expected_read_len(file_size: u64, offset: u64, block_size: u64) -> io::Result<usize> {
    let remaining = file_size.checked_sub(offset).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "read offset exceeded the known file size",
        )
    })?;
    usize::try_from(remaining.min(block_size)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected read length does not fit in usize",
        )
    })
}

pub fn validate_read_result(
    operation: &str,
    offset: u64,
    expected_len: usize,
    result: u32,
) -> io::Result<usize> {
    let actual_len = result as usize;
    if actual_len == expected_len {
        return Ok(actual_len);
    }
    let kind = if actual_len == 0 {
        io::ErrorKind::UnexpectedEof
    } else {
        io::ErrorKind::UnexpectedEof
    };
    Err(io::Error::new(
        kind,
        format!(
            "{operation} short read at offset {offset}: expected {expected_len} bytes, got {actual_len}"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_read_len_uses_tail_for_final_block() {
        assert_eq!(expected_read_len(10_000, 9_500, 1_024).unwrap(), 500);
    }

    #[test]
    fn expected_read_len_rejects_offset_past_file_size() {
        let err = expected_read_len(4_096, 4_097, 4_096).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn validate_read_result_accepts_exact_completion() {
        assert_eq!(validate_read_result("read", 4_096, 512, 512).unwrap(), 512);
    }

    #[test]
    fn validate_read_result_rejects_short_completion() {
        let err = validate_read_result("read", 8_192, 4_096, 2_048).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("short read"));
    }

    #[test]
    fn validate_read_result_rejects_early_eof() {
        let err = validate_read_result("read", 12_288, 4_096, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("expected 4096 bytes, got 0"));
    }
}
