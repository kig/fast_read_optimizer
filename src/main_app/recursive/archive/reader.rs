use super::*;
use std::io::{Read, Seek, SeekFrom, Write};

const TAR_OWNER_AND_SIZE_WIDTH: usize = 19;
const TAR_TIMESTAMP_FORMAT: &[u8] = b"%Y-%m-%d %H:%M\0";

struct TarListEntry {
    path: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: u64,
    typeflag: u8,
    link_target: Vec<u8>,
    uname: Vec<u8>,
    gname: Vec<u8>,
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
        mode: u32::try_from(parse_tar_octal(&header[100..108], "mode")?).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "tar mode does not fit in u32")
        })?,
        uid: u32::try_from(parse_tar_octal(&header[108..116], "uid")?).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "tar uid does not fit in u32")
        })?,
        gid: u32::try_from(parse_tar_octal(&header[116..124], "gid")?).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "tar gid does not fit in u32")
        })?,
        size: parse_tar_octal(&header[124..136], "size")?,
        mtime: parse_tar_octal(&header[136..148], "mtime")?,
        typeflag: header[156],
        link_target: trim_tar_string_field(&header[157..257]).to_vec(),
        uname: trim_tar_string_field(&header[265..297]).to_vec(),
        gname: trim_tar_string_field(&header[297..329]).to_vec(),
    })
}

fn tar_type_char(typeflag: u8) -> u8 {
    match typeflag {
        b'5' => b'd',
        b'2' => b'l',
        b'3' => b'c',
        b'4' => b'b',
        b'6' => b'p',
        _ => b'-',
    }
}

fn permission_bit(mode: u32, bit: u32, present: u8) -> u8 {
    if mode & bit != 0 {
        present
    } else {
        b'-'
    }
}

fn special_exec_char(mode: u32, exec_bit: u32, special_bit: u32, normal: u8, special: u8) -> u8 {
    match (mode & exec_bit != 0, mode & special_bit != 0) {
        (true, true) => special,
        (true, false) => normal,
        (false, true) => special.to_ascii_uppercase(),
        (false, false) => b'-',
    }
}

fn tar_permissions(mode: u32, typeflag: u8) -> [u8; 10] {
    [
        tar_type_char(typeflag),
        permission_bit(mode, 0o400, b'r'),
        permission_bit(mode, 0o200, b'w'),
        special_exec_char(mode, 0o100, 0o4000, b'x', b's'),
        permission_bit(mode, 0o040, b'r'),
        permission_bit(mode, 0o020, b'w'),
        special_exec_char(mode, 0o010, 0o2000, b'x', b's'),
        permission_bit(mode, 0o004, b'r'),
        permission_bit(mode, 0o002, b'w'),
        special_exec_char(mode, 0o001, 0o1000, b'x', b't'),
    ]
}

fn owner_group(entry: &TarListEntry) -> Vec<u8> {
    let user = if entry.uname.is_empty() {
        entry.uid.to_string().into_bytes()
    } else {
        entry.uname.clone()
    };
    let group = if entry.gname.is_empty() {
        entry.gid.to_string().into_bytes()
    } else {
        entry.gname.clone()
    };
    let mut combined = Vec::with_capacity(user.len().saturating_add(group.len()).saturating_add(1));
    combined.extend_from_slice(&user);
    combined.push(b'/');
    combined.extend_from_slice(&group);
    combined
}

fn format_tar_timestamp(mtime: u64) -> io::Result<[u8; 16]> {
    let seconds = libc::time_t::try_from(mtime).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tar mtime does not fit in time_t: {mtime}"),
        )
    })?;
    let mut tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    let tm_ptr = unsafe { libc::localtime_r(&seconds, tm.as_mut_ptr()) };
    if tm_ptr.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to format tar mtime: {mtime}"),
        ));
    }
    let tm = unsafe { tm.assume_init() };
    let mut buffer = [0u8; 32];
    let written = unsafe {
        libc::strftime(
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            TAR_TIMESTAMP_FORMAT.as_ptr().cast(),
            &tm,
        )
    };
    if written != 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to render tar timestamp for mtime {mtime}"),
        ));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&buffer[..16]);
    Ok(out)
}

fn write_plain_entry(stdout: &mut dyn Write, entry: &TarListEntry) -> io::Result<()> {
    stdout.write_all(&entry.path)?;
    stdout.write_all(b"\n")
}

fn write_verbose_entry(stdout: &mut dyn Write, entry: &TarListEntry) -> io::Result<()> {
    stdout.write_all(&tar_permissions(entry.mode, entry.typeflag))?;
    stdout.write_all(b" ")?;

    let owner_group = owner_group(entry);
    let size = entry.size.to_string();
    stdout.write_all(&owner_group)?;
    let padding =
        TAR_OWNER_AND_SIZE_WIDTH.saturating_sub(owner_group.len().saturating_add(size.len()));
    if padding > 0 {
        stdout.write_all(&vec![b' '; padding])?;
    }
    stdout.write_all(size.as_bytes())?;
    stdout.write_all(b" ")?;
    stdout.write_all(&format_tar_timestamp(entry.mtime)?)?;
    stdout.write_all(b" ")?;
    stdout.write_all(&entry.path)?;
    if entry.typeflag == b'2' {
        stdout.write_all(b" -> ")?;
        stdout.write_all(&entry.link_target)?;
    }
    stdout.write_all(b"\n")
}

pub(super) fn list_tar_archive(path: &Path, verbose: bool) -> io::Result<()> {
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
        if verbose {
            write_verbose_entry(&mut stdout, &entry)?;
        } else {
            write_plain_entry(&mut stdout, &entry)?;
        }

        let skip = align_up(entry.size, TAR_BLOCK_SIZE);
        let next_offset = file.stream_position()?.checked_add(skip).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tar entry offset overflow")
        })?;
        file.seek(SeekFrom::Start(next_offset))?;
    }

    Ok(())
}
