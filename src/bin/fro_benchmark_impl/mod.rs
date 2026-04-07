use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::process;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fro::test_sizing::{
    align_down as shared_align_down, choose_auto_test_size, FsSizingStats, TestSizingPolicy,
};

#[derive(Clone)]
enum CacheState {
    None,
    Cold,
    Hot,
}

struct TestCase {
    name: &'static str,
    program: &'static str,
    args: Vec<String>,
    target: f64,
    cache_state: CacheState,
    files_to_prep: Vec<String>,
    bytes_hint: BytesHint,
    metric: MetricKind,
    kind: CommandKind,
}

#[derive(Clone, Copy)]
enum BytesHint {
    None,
    SourceFile,
    RecursiveTree,
}

#[derive(Clone, Copy)]
enum MetricKind {
    Gbps,
    FilesPerSecond,
}

impl MetricKind {
    fn label(self) -> &'static str {
        match self {
            Self::Gbps => "GB/s",
            Self::FilesPerSecond => "files/s",
        }
    }
}

#[derive(Clone, Copy)]
enum CommandKind {
    Fro,
    ExternalDiscardStdout,
}

const RECURSIVE_TREE_TARGET_FILES: usize = 100_000;
const RECURSIVE_TREE_MIN_FILE_SIZE: u64 = 4 * 1024;
const RECURSIVE_TREE_FILES_PER_DIR: usize = 100;
const RECURSIVE_TREE_POWER_ALPHA: f64 = 1.15;

#[derive(Debug, Clone, PartialEq)]
struct FroRunSummary {
    gbps: f64,
    params: Option<Vec<u64>>,
}

fn parse_reported_summary(text: &str) -> Option<FroRunSummary> {
    for line in text.lines().rev() {
        if !(line.contains(" bytes in ") || line.contains(" bytes across ")) {
            continue;
        }
        let idx = line.rfind(" GB/s")?;
        let start = line[..idx].rfind(' ').map(|i| i + 1).unwrap_or(0);
        let gbps = line[start..idx].trim().parse::<f64>().ok()?;
        let params = line
            .rfind('[')
            .and_then(|open| {
                line[open..].split_once(']').map(|(body, _)| {
                    body.trim_start_matches('[')
                        .split(',')
                        .filter_map(|part| part.trim().parse::<u64>().ok())
                        .collect::<Vec<_>>()
                })
            })
            .filter(|vals| !vals.is_empty());
        return Some(FroRunSummary { gbps, params });
    }
    None
}

fn run_test_command(
    fro_exe: &std::path::Path,
    test: &TestCase,
) -> std::io::Result<(std::process::Output, Duration)> {
    let mut command = match test.kind {
        CommandKind::Fro => Command::new(fro_exe),
        CommandKind::ExternalDiscardStdout => Command::new(test.program),
    };
    command.args(&test.args);
    if matches!(test.kind, CommandKind::ExternalDiscardStdout) {
        command.stdout(Stdio::null());
    }
    let start = Instant::now();
    let output = command.output()?;
    Ok((output, start.elapsed()))
}

fn evict_cache(path: &str) {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Some(child) = entry.path().to_str() {
                        evict_cache(child);
                    }
                }
            }
            return;
        }
    }
    if let Ok(file) = std::fs::File::open(path) {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
}

fn pre_cache(path: &str) {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Some(child) = entry.path().to_str() {
                        pre_cache(child);
                    }
                }
            }
            return;
        }
    }
    if let Ok(mut file) = std::fs::File::open(path) {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
        }
        use std::io::Read;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    }
    if let Ok(mut file) = std::fs::File::open(path) {
        use std::io::Read;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FsStats {
    total_bytes: u64,
    avail_bytes: u64,
}

fn fs_stats_for_path(path: &std::path::Path) -> Option<FsStats> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) };
    if rc != 0 {
        return None;
    }

    let frsize = if vfs.f_frsize == 0 {
        vfs.f_bsize
    } else {
        vfs.f_frsize
    } as u64;
    Some(FsStats {
        total_bytes: frsize.saturating_mul(vfs.f_blocks as u64),
        avail_bytes: frsize.saturating_mul(vfs.f_bavail as u64),
    })
}

fn parse_size(s: &str) -> Option<u64> {
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

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TIB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

    if bytes >= 1024_u64.pow(4) {
        format!("{:.2} TiB", bytes as f64 / TIB)
    } else if bytes >= 1024_u64.pow(3) {
        format!("{:.2} GiB", bytes as f64 / GIB)
    } else if bytes >= 1024_u64.pow(2) {
        format!("{:.2} MiB", bytes as f64 / MIB)
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / KIB)
    } else {
        format!("{} B", bytes)
    }
}

fn recursive_tree_file_count(total_bytes: u64) -> usize {
    if total_bytes == 0 {
        return 0;
    }
    if total_bytes < RECURSIVE_TREE_MIN_FILE_SIZE {
        return 1;
    }
    RECURSIVE_TREE_TARGET_FILES.min((total_bytes / RECURSIVE_TREE_MIN_FILE_SIZE) as usize)
}

fn recursive_tree_file_sizes(total_bytes: u64) -> Vec<u64> {
    let file_count = recursive_tree_file_count(total_bytes);
    if file_count == 0 {
        return Vec::new();
    }
    if file_count == 1 {
        return vec![total_bytes];
    }

    let base_total = RECURSIVE_TREE_MIN_FILE_SIZE * file_count as u64;
    let mut sizes = vec![RECURSIVE_TREE_MIN_FILE_SIZE; file_count];
    if total_bytes <= base_total {
        let even = total_bytes / file_count as u64;
        let mut remainder = total_bytes % file_count as u64;
        for size in &mut sizes {
            *size = even;
            if remainder > 0 {
                *size += 1;
                remainder -= 1;
            }
        }
        return sizes;
    }

    let remaining = total_bytes - base_total;
    let weights = (0..file_count)
        .map(|index| 1.0_f64 / ((index + 1) as f64).powf(RECURSIVE_TREE_POWER_ALPHA))
        .collect::<Vec<_>>();
    let weight_sum = weights.iter().sum::<f64>();
    let mut assigned = 0_u64;
    for (size, weight) in sizes.iter_mut().zip(weights.iter()) {
        let extra = ((remaining as f64) * (*weight / weight_sum)).floor() as u64;
        *size += extra;
        assigned += extra;
    }
    sizes[0] += remaining - assigned;
    sizes
}

fn write_fixture_file(path: &std::path::Path, size: u64, pattern: &[u8]) {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Could not create fixture file {} {}", path.display(), e));
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut remaining = size;
    while remaining > 0 {
        let chunk_len = remaining.min(pattern.len() as u64) as usize;
        writer
            .write_all(&pattern[..chunk_len])
            .unwrap_or_else(|e| panic!("Could not write fixture file {} {}", path.display(), e));
        remaining -= chunk_len as u64;
    }
    writer
        .flush()
        .unwrap_or_else(|e| panic!("Could not flush fixture file {} {}", path.display(), e));
}

fn create_recursive_tree_fixture(root: &std::path::Path, total_bytes: u64) {
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root).unwrap_or_else(|e| {
        panic!(
            "Could not create recursive benchmark tree {} {}",
            root.display(),
            e
        )
    });

    let sizes = recursive_tree_file_sizes(total_bytes);
    let pattern = (0..(1024 * 1024))
        .map(|i| (i & 0xff) as u8)
        .collect::<Vec<_>>();
    let mut current_dir = None::<std::path::PathBuf>;
    for (index, size) in sizes.into_iter().enumerate() {
        let shard = index / RECURSIVE_TREE_FILES_PER_DIR;
        let dir = root
            .join(format!("{:03}", shard / 100))
            .join(format!("{:03}", shard % 100));
        if current_dir.as_ref() != Some(&dir) {
            fs::create_dir_all(&dir).unwrap_or_else(|e| {
                panic!("Could not create tree directory {} {}", dir.display(), e)
            });
            current_dir = Some(dir.clone());
        }
        let path = dir.join(format!("file_{index:06}.bin"));
        write_fixture_file(&path, size, &pattern);
    }
}

fn write_recursive_tree_manifest(
    root: &std::path::Path,
    manifest: &std::path::Path,
) -> std::io::Result<usize> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut children = fs::read_dir(&dir)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.path());
        for entry in children.into_iter().rev() {
            let path = entry.path();
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(manifest)?;
    let mut writer = BufWriter::new(file);
    for path in &files {
        writeln!(writer, "{}", path.display())?;
    }
    writer.flush()?;
    Ok(files.len())
}

fn matches_any_pattern<S: AsRef<str>>(name: &str, patterns: &[S]) -> bool {
    patterns
        .iter()
        .any(|pattern| name.starts_with(pattern.as_ref()))
}

fn recursive_tree_fixture_stats(root: &std::path::Path) -> Option<(u64, usize)> {
    if !root.is_dir() {
        return None;
    }
    let mut total_bytes = 0_u64;
    let mut file_count = 0_usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            let metadata = entry.metadata().ok()?;
            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() {
                total_bytes = total_bytes.saturating_add(metadata.len());
                file_count += 1;
            }
        }
    }
    Some((total_bytes, file_count))
}

fn recursive_tree_effective_bytes(
    recursive_tree_fixture_bytes: u64,
    recursive_tree_equiv_files: u64,
    size: u64,
) -> u64 {
    recursive_tree_fixture_bytes.max(size.saturating_mul(recursive_tree_equiv_files))
}

fn choose_test_size(
    fs: FsStats,
    file_count: u64,
    num_full_writes: u64,
    fixed_write_bytes: u64,
    min_size: u64,
    max_size: u64,
    max_drive_writes: f64,
) -> u64 {
    choose_auto_test_size(
        FsSizingStats {
            total_bytes: fs.total_bytes,
            avail_bytes: fs.avail_bytes,
        },
        TestSizingPolicy {
            explicit_test_size: None,
            file_count,
            num_full_writes,
            fixed_write_bytes,
            min_test_size: min_size,
            max_test_size: max_size,
            max_drive_writes,
            fallback_test_size: 4 * 1024 * 1024 * 1024,
        },
    )
}

fn align_down(bytes: u64, align: u64) -> u64 {
    shared_align_down(bytes, align)
}

mod run;
mod test_matrix;
#[cfg(test)]
mod tests;

use test_matrix::build_tests;

pub(super) fn main_impl() {
    run::main_impl();
}
