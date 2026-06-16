use super::*;

pub(super) fn align_up(value: u64, align: u64) -> u64 {
    if align == 0 {
        return value;
    }
    let rem = value % align;
    if rem == 0 {
        value
    } else {
        value + (align - rem)
    }
}

pub(super) fn file_name_bytes(path: &Path) -> io::Result<Vec<u8>> {
    if path == Path::new(".") {
        return Ok(vec![b'.']);
    }
    path.file_name()
        .map(|name| name.as_bytes().to_vec())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "source path has no final component",
            )
        })
}

pub(super) fn join_tar_path(prefix: &Path, name: &std::ffi::OsStr, is_dir: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    if !prefix.as_os_str().is_empty() {
        bytes.extend_from_slice(prefix.as_os_str().as_bytes());
        bytes.push(b'/');
    }
    bytes.extend_from_slice(name.as_bytes());
    if is_dir {
        bytes.push(b'/');
    }
    bytes
}

fn split_tar_path(path: &[u8]) -> io::Result<([u8; 100], [u8; 155])> {
    if path.len() <= 100 {
        let mut name = [0u8; 100];
        name[..path.len()].copy_from_slice(path);
        return Ok((name, [0u8; 155]));
    }
    for idx in (0..path.len()).rev() {
        if path[idx] != b'/' {
            continue;
        }
        let prefix = &path[..idx];
        let name_part = &path[idx + 1..];
        if name_part.len() <= 100 && prefix.len() <= 155 {
            let mut name = [0u8; 100];
            let mut prefix_field = [0u8; 155];
            name[..name_part.len()].copy_from_slice(name_part);
            prefix_field[..prefix.len()].copy_from_slice(prefix);
            return Ok((name, prefix_field));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "tar path is too long for ustar: {}",
            String::from_utf8_lossy(path)
        ),
    ))
}

fn encode_octal(value: u64, field_len: usize) -> io::Result<Vec<u8>> {
    if field_len < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar numeric field too small",
        ));
    }
    let digits = format!("{value:o}");
    if digits.len() + 1 > field_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar numeric field overflow for value {value}"),
        ));
    }
    let mut field = vec![b'0'; field_len];
    let start = field_len - digits.len() - 1;
    field[start..start + digits.len()].copy_from_slice(digits.as_bytes());
    field[field_len - 1] = 0;
    Ok(field)
}

fn write_field<const N: usize>(
    header: &mut [u8; 512],
    offset: usize,
    data: &[u8],
) -> io::Result<()> {
    if data.len() > N {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar field overflowed",
        ));
    }
    header[offset..offset + data.len()].copy_from_slice(data);
    Ok(())
}

pub(super) fn tar_header_bytes(entry: &TarEntry) -> io::Result<[u8; 512]> {
    let mut header = [0u8; 512];
    let (name, prefix) = split_tar_path(&entry.archive_path)?;
    header[0..100].copy_from_slice(&name);
    write_field::<8>(
        &mut header,
        100,
        &encode_octal(u64::from(entry.mode & 0o7777), 8)?,
    )?;
    write_field::<8>(&mut header, 108, &encode_octal(u64::from(entry.uid), 8)?)?;
    write_field::<8>(&mut header, 116, &encode_octal(u64::from(entry.gid), 8)?)?;
    let size = match &entry.kind {
        TarEntryKind::RegularFile { size, .. } => *size,
        TarEntryKind::Directory | TarEntryKind::Symlink { .. } => 0,
    };
    write_field::<12>(&mut header, 124, &encode_octal(size, 12)?)?;
    write_field::<12>(&mut header, 136, &encode_octal(entry.mtime, 12)?)?;
    header[148..156].fill(b' ');
    header[156] = match &entry.kind {
        TarEntryKind::RegularFile { .. } => b'0',
        TarEntryKind::Directory => b'5',
        TarEntryKind::Symlink { .. } => b'2',
    };
    if let TarEntryKind::Symlink { target } = &entry.kind {
        if target.len() > 100 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "symlink target too long for ustar: {}",
                    String::from_utf8_lossy(target)
                ),
            ));
        }
        header[157..157 + target.len()].copy_from_slice(target);
    }
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    header[345..500].copy_from_slice(&prefix);
    let checksum = header.iter().map(|byte| u32::from(*byte)).sum::<u32>() as u64;
    let digits = format!("{checksum:o}");
    if digits.len() > 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar header checksum overflow for {checksum}"),
        ));
    }
    let mut checksum_field = [b'0'; 8];
    let start = 6 - digits.len();
    checksum_field[start..start + digits.len()].copy_from_slice(digits.as_bytes());
    checksum_field[6] = 0;
    checksum_field[7] = b' ';
    header[148..156].copy_from_slice(&checksum_field);
    Ok(header)
}

pub(super) fn madvise_best_effort(
    ptr: *mut libc::c_void,
    len: usize,
    advice: libc::c_int,
) -> io::Result<()> {
    if unsafe { libc::madvise(ptr, len, advice) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINVAL | libc::ENOSYS) => Ok(()),
        _ => Err(err),
    }
}
