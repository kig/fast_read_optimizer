use super::*;

pub(super) fn fs_stats_for_path(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) } != 0 {
        return None;
    }

    let frsize = if vfs.f_frsize == 0 {
        vfs.f_bsize
    } else {
        vfs.f_frsize
    } as u64;
    let total = frsize.saturating_mul(vfs.f_blocks as u64);
    let avail = frsize.saturating_mul(vfs.f_bavail as u64);
    Some((total, avail))
}

pub(super) fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or_else(|| s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);

    let num: u64 = num_str.parse().ok()?;
    let mult: u64 = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return None,
    };

    num.checked_mul(mult)
}

pub(super) fn align_down(bytes: u64, align: u64) -> u64 {
    if align == 0 {
        bytes
    } else {
        bytes / align * align
    }
}

pub(super) fn fmt_gib(bytes: u64) -> String {
    format!("{:.2} GiB", (bytes as f64) / (1024.0 * 1024.0 * 1024.0))
}
