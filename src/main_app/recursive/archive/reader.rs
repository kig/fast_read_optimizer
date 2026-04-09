use super::*;
use std::io::{Read, Seek, SeekFrom, Write};

struct TarListEntry {
    path: Vec<u8>,
    size: u64,
}

fn trim_tar_string_field(field: &[u8]) -> &[u8] {
    &field[..field
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(field.len())]
}

fn trim_tar_octal_field(field: &[u8]) -> &[u8] {
    let field = trim_tar_string_field(field);
    let start = field
        .iter()
        .position(|byte| *byte != b' ')
        .unwrap_or(field.len());
    let field = &field[start..];
    let end = field
        .iter()
        .rposition(|byte| *byte != b' ')
        .map(|index| index + 1)
        .unwrap_or(0);
    &field[..end]
}

fn parse_tar_octal(field: &[u8], field_name: &str) -> io::Result<u64> {
    let field = trim_tar_octal_field(field);
    if field.is_empty() {
        return Ok(0);
    }
    let value = std::str::from_utf8(field).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tar {field_name} field is not valid UTF-8 octal"),
        )
    })?;
    u64::from_str_radix(value, 8).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tar {field_name} field is not valid octal: {value}"),
        )
    })
}

fn tar_header_checksum(header: &[u8; 512]) -> u64 {
    header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u64::from(b' ')
            } else {
                u64::from(*byte)
            }
        })
        .sum()
}

fn parse_tar_list_entry(header: &[u8; 512]) -> io::Result<TarListEntry> {
    let expected_checksum = parse_tar_octal(&header[148..156], "checksum")?;
    let actual_checksum = tar_header_checksum(header);
    if expected_checksum != actual_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "tar header checksum mismatch: expected {expected_checksum:o}, got {actual_checksum:o}"
            ),
        ));
    }

    let name = trim_tar_string_field(&header[0..100]);
    let prefix = trim_tar_string_field(&header[345..500]);
    let mut path = Vec::with_capacity(prefix.len().saturating_add(name.len()).saturating_add(1));
    if !prefix.is_empty() {
        path.extend_from_slice(prefix);
        path.push(b'/');
    }
    path.extend_from_slice(name);
    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tar header missing path",
        ));
    }

    Ok(TarListEntry {
        path,
        size: parse_tar_octal(&header[124..136], "size")?,
    })
}

pub(super) fn list_tar_archive(path: &Path) -> io::Result<()> {
    let mut file = fs::File::open(path)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    loop {
        let mut header = [0u8; 512];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("truncated tar header in {}", path.display()),
                ))
            }
            Err(err) => return Err(err),
        }

        if header.iter().all(|byte| *byte == 0) {
            break;
        }

        let entry = parse_tar_list_entry(&header)?;
        stdout.write_all(&entry.path)?;
        stdout.write_all(b"\n")?;

        let skip = align_up(entry.size, TAR_BLOCK_SIZE);
        let skip = i64::try_from(skip).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("tar entry too large to seek in {}", path.display()),
            )
        })?;
        file.seek(SeekFrom::Current(skip))?;
    }

    Ok(())
}
