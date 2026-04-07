use super::*;
use crate::block_hash;

pub(super) fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);
    let num: u64 = num_str.parse().ok()?;
    let mult = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return None,
    };
    num.checked_mul(mult)
}

pub(super) fn unique_temp_file(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
}

pub(super) fn format_phase_duration(duration: Option<std::time::Duration>) -> String {
    duration
        .map(|d| format!("{:.3} ms", d.as_secs_f64() * 1e3))
        .unwrap_or_else(|| "-".to_string())
}

pub(super) fn write_sweep_fixture(path: &Path, size: usize) -> io::Result<()> {
    let mut file = fs::File::create(path)?;
    let mut remaining = size;
    let mut seed = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining > 0 {
        for byte in &mut buffer {
            *byte = ((seed.wrapping_mul(17).wrapping_add(23)) % 251) as u8;
            seed = seed.wrapping_add(1);
        }
        let chunk = remaining.min(buffer.len());
        file.write_all(&buffer[..chunk])?;
        remaining -= chunk;
    }
    file.sync_all()?;
    Ok(())
}

pub(super) fn format_bytes_compact(size: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("GiB", 1024 * 1024 * 1024),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
        ("B", 1),
    ];
    for (suffix, unit) in UNITS {
        if size >= unit && size % unit == 0 {
            return format!("{}{}", size / unit, suffix);
        }
    }
    format!("{}B", size)
}

pub(super) fn print_verify_report(report: &block_hash::VerifyReport) {
    println!(
        "verify: loaded {}/3 hash replicas, ok_blocks={}, bad_blocks={}",
        report.loaded_manifests,
        report.ok_blocks,
        report.bad_blocks.len()
    );
    for issue in &report.bad_blocks {
        println!(
            "block {}: {}",
            issue.block_index,
            issue.decision.status_message()
        );
    }
}

pub(super) fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some(message)
    } else if let Some(message) = payload.downcast_ref::<String>() {
        Some(message.as_str())
    } else {
        None
    }
}

pub(super) fn is_broken_pipe_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

pub(super) fn is_broken_pipe_panic(payload: &(dyn std::any::Any + Send)) -> bool {
    panic_payload_message(payload).is_some_and(|message| message.contains("Broken pipe"))
}

pub(super) fn skip_copy_destination(
    source: &Path,
    target: &Path,
    update_only_if_newer: bool,
) -> io::Result<bool> {
    let target_meta = match fs::metadata(target) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    if update_only_if_newer {
        let source_meta = fs::metadata(source)?;
        if source_meta.modified()? > target_meta.modified()? {
            return Ok(false);
        }
    }
    Ok(true)
}
