use super::*;
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Component;

const TAR_OWNER_AND_SIZE_WIDTH: usize = 19;
const TAR_TIMESTAMP_FORMAT: &[u8] = b"%Y-%m-%d %H:%M\0";

struct TarArchiveEntry {
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
    data_offset: u64,
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

fn parse_tar_header(header: &[u8; 512], data_offset: u64) -> io::Result<TarArchiveEntry> {
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

    Ok(TarArchiveEntry {
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
        data_offset,
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

fn owner_group(entry: &TarArchiveEntry) -> Vec<u8> {
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

fn write_plain_entry(stdout: &mut dyn Write, entry: &TarArchiveEntry) -> io::Result<()> {
    stdout.write_all(&entry.path)?;
    stdout.write_all(b"\n")
}

fn write_verbose_entry(stdout: &mut dyn Write, entry: &TarArchiveEntry) -> io::Result<()> {
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

fn discard_reader_bytes<R: Read + ?Sized>(reader: &mut R, remaining: u64) -> io::Result<()> {
    let mut remaining = remaining;
    let mut buffer = vec![0u8; TAR_COPY_BUFFER_SIZE];
    while remaining > 0 {
        let chunk = remaining.min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..chunk])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated tar payload while discarding bytes",
            ));
        }
        remaining = remaining.saturating_sub(read as u64);
    }
    Ok(())
}

fn visit_tar_archive<R, F>(reader: &mut R, archive_path: &Path, mut visit: F) -> io::Result<()>
where
    R: Read + ?Sized,
    F: FnMut(&TarArchiveEntry, &mut R) -> io::Result<u64>,
{
    loop {
        let mut header = [0u8; 512];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("truncated tar header in {}", archive_path.display()),
                ))
            }
            Err(err) => return Err(err),
        }

        if header.iter().all(|byte| *byte == 0) {
            break;
        }

        let entry = parse_tar_header(&header, 0)?;
        let consumed = visit(&entry, reader)?;
        if consumed > entry.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tar visitor consumed more payload bytes than the entry contains",
            ));
        }
        let to_discard = align_up(entry.size, TAR_BLOCK_SIZE).saturating_sub(consumed);
        discard_reader_bytes(reader, to_discard)?;
    }

    Ok(())
}

fn visit_tar_archive_seek<R, F>(reader: &mut R, archive_path: &Path, mut visit: F) -> io::Result<()>
where
    R: Read + Seek + ?Sized,
    F: FnMut(&TarArchiveEntry, &mut R) -> io::Result<u64>,
{
    loop {
        let mut header = [0u8; 512];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("truncated tar header in {}", archive_path.display()),
                ))
            }
            Err(err) => return Err(err),
        }

        if header.iter().all(|byte| *byte == 0) {
            break;
        }

        let data_offset = reader.stream_position()?;
        let entry = parse_tar_header(&header, data_offset)?;
        let consumed = visit(&entry, reader)?;
        if consumed > entry.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tar visitor consumed more payload bytes than the entry contains",
            ));
        }
        let to_skip = align_up(entry.size, TAR_BLOCK_SIZE).saturating_sub(consumed);
        if to_skip > 0 {
            reader.seek(SeekFrom::Current(i64::try_from(to_skip).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("tar skip length does not fit in i64: {to_skip}"),
                )
            })?))?;
        }
    }

    Ok(())
}

fn copy_reader_member_to_file<R: Read + ?Sized>(
    reader: &mut R,
    destination_path: &Path,
    entry: &TarArchiveEntry,
    buffer: &mut [u8],
) -> io::Result<()> {
    let mut destination = fs::File::create(destination_path)?;
    let mut remaining = entry.size;
    while remaining > 0 {
        let chunk = remaining.min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..chunk])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "truncated tar payload while extracting {}",
                    destination_path.display()
                ),
            ));
        }
        destination.write_all(&buffer[..read])?;
        remaining = remaining.saturating_sub(read as u64);
    }
    destination.set_permissions(fs::Permissions::from_mode(entry.mode & 0o7777))?;
    set_extracted_file_mtime(&destination, entry.mtime)?;
    Ok(())
}

pub(super) fn list_tar_archive_reader<R: Read + ?Sized>(
    reader: &mut R,
    archive_path: &Path,
    verbose: bool,
) -> io::Result<()> {
    let mut stdout = io::BufWriter::with_capacity(64 * 1024, fro::command_io::stdout_file()?);
    visit_tar_archive(reader, archive_path, |entry, _reader| {
        if verbose {
            write_verbose_entry(&mut stdout, &entry)?;
        } else {
            write_plain_entry(&mut stdout, &entry)?;
        }
        Ok(0)
    })
}

pub(super) fn list_tar_archive(path: &Path, verbose: bool) -> io::Result<()> {
    let mut file = fs::File::open(path)?;
    let mut stdout = io::BufWriter::with_capacity(64 * 1024, fro::command_io::stdout_file()?);
    visit_tar_archive_seek(&mut file, path, |entry, _reader| {
        if verbose {
            write_verbose_entry(&mut stdout, &entry)?;
        } else {
            write_plain_entry(&mut stdout, &entry)?;
        }
        Ok(0)
    })
}

fn sanitized_tar_path(path: &[u8]) -> io::Result<PathBuf> {
    let mut sanitized = PathBuf::new();
    let raw = std::ffi::OsString::from_vec(path.to_vec());
    let path = Path::new(&raw);
    if path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "tar extract currently rejects absolute member paths: {}",
                String::from_utf8_lossy(path.as_os_str().as_bytes())
            ),
        ));
    }
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => sanitized.push(part),
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "tar extract currently rejects parent-traversing member paths: {}",
                        String::from_utf8_lossy(path.as_os_str().as_bytes())
                    ),
                ))
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "tar extract currently rejects non-relative member paths: {}",
                        String::from_utf8_lossy(path.as_os_str().as_bytes())
                    ),
                ))
            }
        }
    }
    Ok(sanitized)
}

fn set_extracted_mtime(path: &Path, mtime: u64, nofollow_symlink: bool) -> io::Result<()> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains interior NUL: {}", path.display()),
        )
    })?;
    let seconds = libc::time_t::try_from(mtime).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar mtime does not fit in time_t: {mtime}"),
        )
    })?;
    let times = [
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
    ];
    let flags = if nofollow_symlink {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), flags) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn set_extracted_file_mtime(file: &fs::File, mtime: u64) -> io::Result<()> {
    let seconds = libc::time_t::try_from(mtime).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("tar mtime does not fit in time_t: {mtime}"),
        )
    })?;
    let times = [
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn apply_regular_file_metadata(path: &Path, entry: &TarArchiveEntry) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(entry.mode & 0o7777))?;
    set_extracted_mtime(path, entry.mtime, false)
}

fn apply_directory_metadata(path: &Path, mode: u32, mtime: u64) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o7777))?;
    set_extracted_mtime(path, mtime, false)
}

fn ensure_cached_parent_dir(path: &Path, created_dirs: &mut HashSet<PathBuf>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && created_dirs.insert(parent.to_path_buf()) {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

fn copy_archive_member_to_file(
    archive_path: &Path,
    destination_path: &Path,
    entry: &TarArchiveEntry,
    config: &config::LoadedConfig,
) -> io::Result<()> {
    if entry.size == 0 {
        let _ = fs::File::create(destination_path)?;
        return Ok(());
    }

    let target = destination_path.to_string_lossy().into_owned();
    let page_cache_params = config.get_params_for_path("copy", false, &target);
    let direct_params = config.get_params_for_path("copy", true, &target);
    let source = archive_path.to_string_lossy();
    copy_file_range_threaded(
        source.as_ref(),
        &target,
        entry.data_offset,
        0,
        entry.size,
        true,
        page_cache_params.num_threads,
        page_cache_params.block_size,
        page_cache_params.qd,
        direct_params.num_threads,
        direct_params.block_size,
        direct_params.qd,
        IOMode::Auto,
        IOMode::Auto,
        None,
    )?;
    Ok(())
}

const TAR_LOW_LATENCY_EXTRACT_ENTRY_THRESHOLD: u64 = 256 * 1024;

pub(super) fn extract_tar_archive(
    archive_path: &Path,
    destination: Option<&Path>,
    verbose: bool,
) -> io::Result<()> {
    let mut file = fs::File::open(archive_path)?;
    let destination_root = destination.unwrap_or_else(|| Path::new("."));
    let destination_meta = fs::metadata(destination_root).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "tar extract destination {} is not accessible: {err}",
                destination_root.display()
            ),
        )
    })?;
    if !destination_meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "tar extract destination must be a directory: {}",
                destination_root.display()
            ),
        ));
    }

    let mut stdout = io::BufWriter::with_capacity(64 * 1024, fro::command_io::stdout_file()?);
    let mut directory_entries = Vec::new();
    let mut created_dirs = HashSet::new();
    let mut copy_buffer = vec![0u8; TAR_COPY_BUFFER_SIZE];
    let config = config::load_config(None);

    loop {
        let mut header = [0u8; 512];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("truncated tar header in {}", archive_path.display()),
                ))
            }
            Err(err) => return Err(err),
        }

        if header.iter().all(|byte| *byte == 0) {
            break;
        }

        let data_offset = file.stream_position()?;
        let entry = parse_tar_header(&header, data_offset)?;
        let relative_path = sanitized_tar_path(&entry.path)?;
        let target_path = destination_root.join(&relative_path);

        match entry.typeflag {
            0 | b'0' => {
                if relative_path.as_os_str().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tar regular-file entry resolved to an empty extraction path",
                    ));
                }
                ensure_cached_parent_dir(&target_path, &mut created_dirs)?;
                if entry.size <= TAR_LOW_LATENCY_EXTRACT_ENTRY_THRESHOLD {
                    copy_reader_member_to_file(&mut file, &target_path, &entry, &mut copy_buffer)?;
                } else {
                    copy_archive_member_to_file(archive_path, &target_path, &entry, &config)?;
                    apply_regular_file_metadata(&target_path, &entry)?;
                }
            }
            b'5' => {
                if !relative_path.as_os_str().is_empty() {
                    fs::create_dir_all(&target_path)?;
                    created_dirs.insert(target_path.clone());
                    directory_entries.push((target_path.clone(), entry.mode, entry.mtime));
                }
            }
            b'2' => {
                if relative_path.as_os_str().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tar symlink entry resolved to an empty extraction path",
                    ));
                }
                ensure_cached_parent_dir(&target_path, &mut created_dirs)?;
                let link_target = std::ffi::OsString::from_vec(entry.link_target.clone());
                if fs::symlink_metadata(&target_path).is_ok() {
                    fs::remove_file(&target_path)?;
                }
                symlink(&link_target, &target_path)?;
                set_extracted_mtime(&target_path, entry.mtime, true)?;
            }
            typeflag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "tar extract currently supports only regular files, directories, and symlinks; found typeflag {:?} for {}",
                        typeflag as char,
                        String::from_utf8_lossy(&entry.path)
                    ),
                ))
            }
        }

        if verbose {
            stdout.write_all(&entry.path)?;
            stdout.write_all(b"\n")?;
        }

        let skip = align_up(entry.size, TAR_BLOCK_SIZE);
        let next_offset = data_offset.checked_add(skip).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tar entry offset overflow")
        })?;
        file.seek(SeekFrom::Start(next_offset))?;
    }

    directory_entries.sort_by_key(|(path, _, _)| std::cmp::Reverse(path.components().count()));
    for (path, mode, mtime) in directory_entries {
        apply_directory_metadata(&path, mode, mtime)?;
    }

    Ok(())
}

pub(super) fn extract_tar_archive_reader<R: Read + ?Sized>(
    reader: &mut R,
    archive_path: &Path,
    destination: Option<&Path>,
    verbose: bool,
) -> io::Result<()> {
    let destination_root = destination.unwrap_or_else(|| Path::new("."));
    let destination_meta = fs::metadata(destination_root).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "tar extract destination {} is not accessible: {err}",
                destination_root.display()
            ),
        )
    })?;
    if !destination_meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "tar extract destination must be a directory: {}",
                destination_root.display()
            ),
        ));
    }

    let mut stdout = io::BufWriter::with_capacity(64 * 1024, fro::command_io::stdout_file()?);
    let mut directory_entries = Vec::new();
    let mut created_dirs = HashSet::new();
    let mut copy_buffer = vec![0u8; TAR_COPY_BUFFER_SIZE];
    visit_tar_archive(reader, archive_path, |entry, reader| {
        let relative_path = sanitized_tar_path(&entry.path)?;
        let target_path = destination_root.join(&relative_path);
        let consumed = match entry.typeflag {
            0 | b'0' => {
                if relative_path.as_os_str().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tar regular-file entry resolved to an empty extraction path",
                    ));
                }
                ensure_cached_parent_dir(&target_path, &mut created_dirs)?;
                copy_reader_member_to_file(reader, &target_path, entry, &mut copy_buffer)?;
                entry.size
            }
            b'5' => {
                if !relative_path.as_os_str().is_empty() {
                    fs::create_dir_all(&target_path)?;
                    created_dirs.insert(target_path.clone());
                    directory_entries.push((target_path.clone(), entry.mode, entry.mtime));
                }
                0
            }
            b'2' => {
                if relative_path.as_os_str().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tar symlink entry resolved to an empty extraction path",
                    ));
                }
                ensure_cached_parent_dir(&target_path, &mut created_dirs)?;
                let link_target = std::ffi::OsString::from_vec(entry.link_target.clone());
                if fs::symlink_metadata(&target_path).is_ok() {
                    fs::remove_file(&target_path)?;
                }
                symlink(&link_target, &target_path)?;
                set_extracted_mtime(&target_path, entry.mtime, true)?;
                0
            }
            typeflag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "tar extract currently supports only regular files, directories, and symlinks; found typeflag {:?} for {}",
                        typeflag as char,
                        String::from_utf8_lossy(&entry.path)
                    ),
                ))
            }
        };

        if verbose {
            stdout.write_all(&entry.path)?;
            stdout.write_all(b"\n")?;
        }
        Ok(consumed)
    })?;

    directory_entries.sort_by_key(|(path, _, _)| std::cmp::Reverse(path.components().count()));
    for (path, mode, mtime) in directory_entries {
        apply_directory_metadata(&path, mode, mtime)?;
    }
    Ok(())
}
