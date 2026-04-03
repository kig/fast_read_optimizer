use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::process;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    kind: CommandKind,
}

#[derive(Clone, Copy)]
enum BytesHint {
    None,
    SourceFile,
    RecursiveTree,
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

fn parse_reported_gbps(text: &str) -> Option<f64> {
    for line in text.lines().rev() {
        if !(line.contains(" bytes in ") || line.contains(" bytes across ")) {
            continue;
        }
        if let Some(idx) = line.rfind(" GB/s") {
            let start = line[..idx].rfind(' ').map(|i| i + 1).unwrap_or(0);
            if let Ok(num) = line[start..idx].trim().parse::<f64>() {
                return Some(num);
            }
        }
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

fn align_down(bytes: u64, align: u64) -> u64 {
    if align == 0 {
        return bytes;
    }
    bytes / align * align
}

fn choose_test_size(
    fs: FsStats,
    file_count: u64,
    num_full_writes: u64,
    min_size: u64,
    max_size: u64,
    max_drive_writes: f64,
) -> u64 {
    if file_count == 0 {
        return 0;
    }

    let space_cap = ((fs.avail_bytes as f64) * 0.60 / (file_count as f64)) as u64;
    let wear_cap = if num_full_writes == 0 {
        u64::MAX
    } else {
        ((fs.total_bytes as f64) * max_drive_writes / (num_full_writes as f64)) as u64
    };

    let mut size = max_size.min(space_cap).min(wear_cap);
    size = align_down(size, 4096).max(4096);

    if size < min_size {
        eprintln!(
            "Warning: wear/space cap suggests a small test file: {} (min requested: {})",
            format_bytes(size),
            format_bytes(min_size),
        );
    }

    size
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--plan] [--skip-build] [--no-fail] [--iters <n>] [--repeat <n>] [--test-dir path] [--test-size <size>] [--max-drive-writes <fraction>] <test_prefix ...>",
            args[0]
        );
        println!(
            "\nAuto sizing (default): chooses a temp file size based on free space and a wear budget.\n\
              - --plan                  (print suggested test size + write load and exit)\n\
             - --test-size 1GiB         (force fixed size)\n\
              - --max-drive-writes 0.05  (cap total user-data writes per run to ~5% of FS capacity)\n\
              - --iters 5               (override internal -n for fro invocations; useful for quick runs/tests)\n\
              - --repeat 3              (run each benchmark multiple times; report min/max and judge by best steady-state run)\n\
              - --skip-build            (do not run `cargo build --release`; assume binaries already built)\n\
              - --no-fail               (do not exit nonzero on regressions; still prints PASS/REGRESSION)"
        );
        std::process::exit(0);
    }

    let mut test_size: Option<u64> = None;
    let mut min_test_size: u64 = 256 * 1024 * 1024;
    let mut max_test_size: u64 = 1024 * 1024 * 1024;
    let mut max_drive_writes: f64 = 0.05;
    let mut plan = false;
    let mut iters: u64 = 1;
    let mut repeat_count: usize = 3;
    let mut skip_build = false;
    let mut fail_on_regressions = true;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else if arg == "--test-size" {
            let v = &args[i];
            i += 1;
            test_size = parse_size(v);
            if test_size.is_none() {
                eprintln!("Invalid --test-size: {}", v);
                std::process::exit(2);
            }
        } else if arg == "--min-test-size" {
            let v = &args[i];
            i += 1;
            min_test_size = parse_size(v).unwrap_or_else(|| {
                eprintln!("Invalid --min-test-size: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--max-test-size" {
            let v = &args[i];
            i += 1;
            max_test_size = parse_size(v).unwrap_or_else(|| {
                eprintln!("Invalid --max-test-size: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--max-drive-writes" {
            let v = &args[i];
            i += 1;
            max_drive_writes = v.parse().unwrap_or_else(|_| {
                eprintln!("Invalid --max-drive-writes: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--plan" {
            plan = true;
        } else if arg == "--iters" {
            let v = &args[i];
            i += 1;
            iters = v.parse().unwrap_or_else(|_| {
                eprintln!("Invalid --iters: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--repeat" {
            let v = &args[i];
            i += 1;
            repeat_count = v.parse().unwrap_or_else(|_| {
                eprintln!("Invalid --repeat: {}", v);
                std::process::exit(2);
            });
            if repeat_count == 0 {
                eprintln!("Invalid --repeat: must be greater than zero");
                std::process::exit(2);
            }
        } else if arg == "--skip-build" {
            skip_build = true;
        } else if arg == "--no-fail" {
            fail_on_regressions = false;
        } else {
            patterns.push(arg);
        }
    }

    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fro");

    let test_path = std::path::Path::new(test_dir);
    let run_dir = test_path.join(format!(
        "fro-bench-run-{}-{}",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&run_dir).unwrap_or_else(|e| {
        panic!(
            "Could not create benchmark temp dir {} {}",
            run_dir.display(),
            e
        )
    });

    let source_file = run_dir.join("fro_bench_tmp_source").display().to_string();
    let target_file_dir = run_dir.join("fro_bench_tmp_direct").display().to_string();
    let target_file_cache = run_dir.join("fro_bench_tmp_cache").display().to_string();
    let recursive_tree = test_path.join("fro_bench_recursive_tree");
    let recursive_tree_str = recursive_tree.display().to_string();
    let recursive_copy_target = run_dir
        .join("fro_bench_recursive_copy_out")
        .display()
        .to_string();

    let tests = vec![
        TestCase {
            name: "bench-diff (memory)",
            program: "fro",
            args: vec!["bench-diff".into()],
            target: 60.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "write (direct)",
            program: "fro",
            args: vec![
                "write".into(),
                "--direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                target_file_dir.clone(),
            ],
            target: 10.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_dir.clone()],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "write (page cache, cold)",
            program: "fro",
            args: vec![
                "write".into(),
                "--no-direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                target_file_cache.clone(),
            ],
            target: 1.6,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_cache.clone()],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "write (page cache, hot)",
            program: "fro",
            args: vec![
                "write".into(),
                "--no-direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                target_file_cache.clone(),
            ],
            target: 3.6,
            cache_state: CacheState::Hot,
            files_to_prep: vec![target_file_cache.clone()],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "write (auto, cold)",
            program: "fro",
            args: vec![
                "write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                target_file_cache.clone(),
            ],
            target: 8.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_cache.clone()],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "write (auto, hot)",
            program: "fro",
            args: vec![
                "write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                target_file_cache.clone(),
            ],
            target: 10.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![target_file_cache.clone()],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (direct)",
            program: "fro",
            args: vec![
                "read".into(),
                "--direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (forced page cache, hot)",
            program: "fro",
            args: vec![
                "read".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (auto, cold)",
            program: "fro",
            args: vec![
                "read".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (auto, hot)",
            program: "fro",
            args: vec![
                "read".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (to memory, hot)",
            program: "fro",
            args: vec![
                "read".into(),
                "--to-memory".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 5.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (to memory, cold)",
            program: "fro",
            args: vec![
                "read".into(),
                "--to-memory".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (to memory, direct)",
            program: "fro",
            args: vec![
                "read".into(),
                "--to-memory".into(),
                "--direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "read (to memory, paged shared buffer, hot)",
            program: "fro",
            args: vec![
                "read".into(),
                "--to-memory".into(),
                "--paged-shared-buffer".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
            ],
            target: 8.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "recursive-read-bench (hot)",
            program: "fro",
            args: vec![
                "recursive-read-bench".into(),
                "--no-direct".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.clone(),
            ],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "fd read tree (hot)",
            program: "fd",
            args: vec!["-u".into(), ".".into(), recursive_tree_str.clone()],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::ExternalDiscardStdout,
        },
        TestCase {
            name: "rg tree scan (hot)",
            program: "rg",
            args: vec![
                "-Fboauuu".into(),
                "__fro_bench_pattern_that_should_not_match__".into(),
                recursive_tree_str.clone(),
            ],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::ExternalDiscardStdout,
        },
        TestCase {
            name: "copy (recursive, hot)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--recursive".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.clone(),
                recursive_copy_target.clone(),
            ],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "cp -r (recursive, hot)",
            program: "cp",
            args: vec!["-r".into(), recursive_tree_str.clone(), recursive_copy_target.clone()],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::ExternalDiscardStdout,
        },
        TestCase {
            name: "rsync (recursive, hot)",
            program: "rsync",
            args: vec!["-a".into(), recursive_tree_str.clone(), recursive_copy_target.clone()],
            target: 0.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![recursive_tree_str.clone()],
            bytes_hint: BytesHint::RecursiveTree,
            kind: CommandKind::ExternalDiscardStdout,
        },
        TestCase {
            name: "copy (direct)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 5.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (threaded, page cache, cold)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--threaded-copy".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 1.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (range, cold)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--copy-file-range".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 1.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (range, hot)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--copy-file-range".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 3.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (range 1T, cold)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--copy-file-range-single".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 0.8,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (range 1T, hot)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--copy-file-range-single".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 3.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (hot R, direct W)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--no-direct".into(),
                "--direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 2.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (hot R, direct W, pre-sized target)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--keep-target-size".into(),
                "--no-direct".into(),
                "--direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 3.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (via-mem, hot R, direct W)",
            program: "fro",
            args: vec![
                "copy".into(),
                "--via-memory".into(),
                "--no-direct".into(),
                "--direct-write".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 0.8,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (auto, cold)",
            program: "fro",
            args: vec![
                "copy".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 5.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "copy (auto, hot)",
            program: "fro",
            args: vec![
                "copy".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 4.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "diff (direct)",
            program: "fro",
            args: vec![
                "diff".into(),
                "--direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "diff (page cache, cold)",
            program: "fro",
            args: vec![
                "diff".into(),
                "--no-direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 3.5,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "diff (page cache, hot)",
            program: "fro",
            args: vec![
                "diff".into(),
                "--no-direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "diff (auto, cold)",
            program: "fro",
            args: vec![
                "diff".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "diff (auto, hot)",
            program: "fro",
            args: vec![
                "diff".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "dual-read-bench (direct)",
            program: "fro",
            args: vec![
                "dual-read-bench".into(),
                "--direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "dual-read-bench (page cache, cold)",
            program: "fro",
            args: vec![
                "dual-read-bench".into(),
                "--no-direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 3.5,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "dual-read-bench (page cache, hot)",
            program: "fro",
            args: vec![
                "dual-read-bench".into(),
                "--no-direct".into(),
                "-v".into(),
                source_file.clone(),
                target_file_cache.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "dual-read-bench (auto, cold)",
            program: "fro",
            args: vec![
                "dual-read-bench".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "dual-read-bench (auto, hot)",
            program: "fro",
            args: vec![
                "dual-read-bench".into(),
                "-v".into(),
                source_file.clone(),
                target_file_dir.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "grep (direct)",
            program: "fro",
            args: vec![
                "grep".into(),
                "--direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                "needle".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "grep (forced page cache, hot)",
            program: "fro",
            args: vec![
                "grep".into(),
                "--no-direct".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                "needle".into(),
                source_file.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "grep (auto, cold)",
            program: "fro",
            args: vec![
                "grep".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                "needle".into(),
                source_file.clone(),
            ],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "grep (auto, hot)",
            program: "fro",
            args: vec![
                "grep".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                "needle".into(),
                source_file.clone(),
            ],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
            bytes_hint: BytesHint::SourceFile,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "bench-mmap-write",
            program: "fro",
            args: vec!["bench-mmap-write".into(), target_file_cache.clone()],
            target: 0.7,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
        TestCase {
            name: "bench-write",
            program: "fro",
            args: vec!["bench-write".into(), target_file_cache.clone()],
            target: 0.5,
            cache_state: CacheState::None,
            files_to_prep: vec![],
            bytes_hint: BytesHint::None,
            kind: CommandKind::Fro,
        },
    ];

    if !plan && !skip_build {
        let build_output = Command::new("cargo")
            .args(["build", "--release"])
            .output()
            .unwrap_or_else(|e| panic!("Failed to compile: {}", e));

        let out_str = String::from_utf8_lossy(&build_output.stdout);
        let err_str = String::from_utf8_lossy(&build_output.stderr);
        let combined = format!("{}\n{}", out_str, err_str);
        println!("{}", combined);
    }

    let recursive_selected_explicitly =
        matches_any_pattern("recursive-read-bench (hot)", &patterns)
            || matches_any_pattern("copy (recursive, hot)", &patterns);
    let recursive_fixture_stats = recursive_tree_fixture_stats(&recursive_tree);
    let recursive_fixture_exists = recursive_fixture_stats.is_some();
    let mut selected_tests = Vec::new();
    for t in tests {
        if patterns.is_empty() {
            let needs_recursive_fixture =
                t.name.starts_with("recursive-read-bench") || t.name.starts_with("copy (recursive");
            if needs_recursive_fixture && !recursive_fixture_exists {
                continue;
            }
            selected_tests.push(t);
            continue;
        }

        if matches_any_pattern(t.name, &patterns) {
            selected_tests.push(t);
        }
    }

    if selected_tests.is_empty() {
        eprintln!("No benchmarks selected.");
        return;
    }

    if iters != 1 {
        for t in &mut selected_tests {
            let mut j = 0;
            while j + 1 < t.args.len() {
                if t.args[j] == "-n" {
                    t.args[j + 1] = iters.to_string();
                    break;
                }
                j += 1;
            }
        }
    }

    let mut need_source = false;
    let mut need_target_dir = false;
    let mut need_target_cache = false;
    let mut need_recursive_tree = false;
    let mut need_target_dir_matching = false;
    let mut need_target_cache_matching = false;

    let mut num_full_writes: u64 = 0;
    let mut fixed_write_bytes: u64 = 0;
    for t in &selected_tests {
        let op = t.args.get(0).map(|s| s.as_str()).unwrap_or("");

        if t.args.iter().any(|s| s == &source_file) {
            need_source = true;
        }
        if t.args.iter().any(|s| s == &target_file_dir) {
            need_target_dir = true;
        }
        if t.args.iter().any(|s| s == &target_file_cache) {
            need_target_cache = true;
        }
        if t.args.iter().any(|s| s == &recursive_tree_str) {
            need_recursive_tree = true;
        }

        if (op == "diff" || op == "dual-read-bench") && t.args.iter().any(|s| s == &target_file_dir)
        {
            need_target_dir_matching = true;
        }
        if (op == "diff" || op == "dual-read-bench")
            && t.args.iter().any(|s| s == &target_file_cache)
        {
            need_target_cache_matching = true;
        }

        if op == "write" || op == "copy" {
            let mut n = 1_u64;
            let mut i = 0;
            while i + 1 < t.args.len() {
                if t.args[i] == "-n" {
                    n = t.args[i + 1].parse::<u64>().unwrap_or(1);
                    break;
                }
                i += 1;
            }
            num_full_writes = num_full_writes.saturating_add(n);
        } else if op == "bench-write" || op == "bench-mmap-write" {
            fixed_write_bytes = fixed_write_bytes.saturating_add(1024 * 1024 * 1024);
        }
    }

    let recursive_tree_equiv_files = if need_recursive_tree && !recursive_fixture_exists {
        1
    } else {
        0
    };
    let file_count = (need_source as u64)
        + (need_target_dir as u64)
        + (need_target_cache as u64)
        + recursive_tree_equiv_files;
    let setup_writes = (need_source as u64)
        + (need_target_dir_matching as u64)
        + (need_target_cache_matching as u64)
        + ((need_recursive_tree && recursive_selected_explicitly && !recursive_fixture_exists)
            as u64);
    num_full_writes = num_full_writes.saturating_add(setup_writes);

    // Create temp files (only if needed by the selected tests).
    let size = if file_count == 0 && need_recursive_tree && recursive_fixture_exists {
        recursive_fixture_stats.map(|(bytes, _)| bytes).unwrap_or(0)
    } else if file_count == 0 {
        0
    } else if let Some(s) = test_size {
        align_down(s, 4096).max(4096)
    } else if let Some(fs) = fs_stats_for_path(test_path) {
        choose_test_size(
            fs,
            file_count,
            num_full_writes,
            min_test_size,
            max_test_size,
            max_drive_writes,
        )
    } else {
        eprintln!("Warning: statvfs failed for --test-dir; falling back to 4GiB");
        4 * 1024 * 1024 * 1024
    };

    let est_user_writes = size
        .saturating_mul(num_full_writes)
        .saturating_add(fixed_write_bytes);
    let alloc = size.saturating_mul(file_count);
    let recursive_tree_fixture_files = if let Some((_, file_count)) = recursive_fixture_stats {
        file_count
    } else if need_recursive_tree && recursive_selected_explicitly {
        recursive_tree_file_count(size)
    } else {
        0
    };
    let recursive_tree_fixture_bytes = recursive_fixture_stats.map(|(bytes, _)| bytes).unwrap_or(0);

    if file_count > 0 && test_size.is_none() {
        eprintln!(
            "Auto-sized test file: {} (alloc={} across {} files; est_writes={} + fixed={} => est_user_writes={})",
            format_bytes(size),
            format_bytes(alloc),
            file_count,
            num_full_writes,
            format_bytes(fixed_write_bytes),
            format_bytes(est_user_writes),
        );
    }

    if plan {
        println!("fro-benchmark plan");
        println!("  test_dir: {}", test_dir);
        println!("  test_size: {}", format_bytes(size));
        println!(
            "  file_count: {} (source={} direct_target={} cache_target={} recursive_tree={} recursive_tree_files={})",
            file_count,
            need_source,
            need_target_dir,
            need_target_cache,
            need_recursive_tree,
            recursive_tree_fixture_files
        );
        if need_recursive_tree {
            println!(
                "  recursive_tree_bytes: {}",
                format_bytes(recursive_tree_fixture_bytes.max(size * recursive_tree_equiv_files))
            );
        }
        println!("  alloc_total: {}", format_bytes(alloc));
        println!("  est_full_writes: {}", num_full_writes);
        println!(
            "  est_fixed_write_bytes: {}",
            format_bytes(fixed_write_bytes)
        );
        println!("  est_user_writes: {}", format_bytes(est_user_writes));
        println!("  max_drive_writes: {}", max_drive_writes);
        println!("  repeat_count: {}", repeat_count);
        println!("  selected_benchmarks:");
        for t in &selected_tests {
            println!("    {}", t.name);
        }
        return;
    }

    if need_source {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(source_file.clone())
            .unwrap_or_else(|e| panic!("Could not create test file {} {}", source_file, e));
        file.set_len(size)
            .unwrap_or_else(|e| panic!("Could not set file length for {} {}", source_file, e));
        Command::new(&fro_exe)
            .args(["write", &source_file.clone()])
            .output()
            .unwrap_or_else(|e| panic!("Failed to write test file {} {}", source_file, e));
    }

    if need_target_dir {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(target_file_dir.clone())
            .unwrap_or_else(|e| panic!("Could not create test file {} {}", target_file_dir, e));
        let _ = file.set_len(size);
        if need_target_dir_matching {
            Command::new(&fro_exe)
                .args(["copy", &source_file.clone(), &target_file_dir.clone()])
                .output()
                .unwrap_or_else(|e| panic!("Failed to create test file {} {}", target_file_dir, e));
        }
    }

    if need_target_cache {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(target_file_cache.clone())
            .unwrap_or_else(|e| panic!("Could not create test file {} {}", target_file_cache, e));
        let _ = file.set_len(size);
        if need_target_cache_matching {
            Command::new(&fro_exe)
                .args(["copy", &source_file.clone(), &target_file_cache.clone()])
                .output()
                .unwrap_or_else(|e| {
                    panic!("Failed to create test file {} {}", target_file_cache, e)
                });
        }
    }

    if need_recursive_tree && recursive_selected_explicitly && !recursive_fixture_exists {
        create_recursive_tree_fixture(&recursive_tree, size);
    }

    let mut regressions = false;

    println!(
        "{:<35} | {:<12} | {:<12} | {:<12} | {:<12} | {:<10}",
        "Benchmark", "Best (GB/s)", "Min", "Max", "Target", "Status"
    );
    println!(
        "{:-<35}-|-{:-<12}-|-{:-<12}-|-{:-<12}-|-{:-<12}-|-{:-<10}",
        "", "", "", "", "", ""
    );

    for t in selected_tests {
        let mut min_speed = f64::INFINITY;
        let mut max_speed = 0.0_f64;
        let mut outputs = Vec::with_capacity(repeat_count);

        for _ in 0..repeat_count {
            if matches!(t.bytes_hint, BytesHint::RecursiveTree)
                && t.args.last().map(String::as_str) == Some(recursive_copy_target.as_str())
            {
                if let Some(target) = t.args.last() {
                    let target_path = std::path::Path::new(target);
                    let _ = std::fs::remove_dir_all(target_path);
                    let _ = std::fs::remove_file(target_path);
                }
            }
            match t.cache_state {
                CacheState::Cold => {
                    for f in &t.files_to_prep {
                        evict_cache(f);
                    }
                }
                CacheState::Hot => {
                    for f in &t.files_to_prep {
                        pre_cache(f);
                    }
                }
                CacheState::None => {}
            }

            let (output, elapsed) = run_test_command(&fro_exe, &t)
                .unwrap_or_else(|e| panic!("Failed to execute process for {}: {}", t.name, e));

            let out_str = String::from_utf8_lossy(&output.stdout);
            let err_str = String::from_utf8_lossy(&output.stderr);
            let combined = format!("{}\n{}", out_str, err_str);
            let bytes_for_effective_speed = match t.bytes_hint {
                BytesHint::None => None,
                BytesHint::SourceFile => Some(size),
                BytesHint::RecursiveTree => Some(recursive_tree_effective_bytes(
                    recursive_tree_fixture_bytes,
                    recursive_tree_equiv_files,
                    size,
                )),
            };
            let speed = match t.kind {
                CommandKind::Fro => parse_reported_gbps(&combined).unwrap_or_else(|| {
                    bytes_for_effective_speed
                        .map(|bytes| bytes as f64 / elapsed.as_secs_f64().max(1e-9) / 1e9)
                        .unwrap_or(0.0)
                }),
                CommandKind::ExternalDiscardStdout => bytes_for_effective_speed
                    .map(|bytes| bytes as f64 / elapsed.as_secs_f64().max(1e-9) / 1e9)
                    .unwrap_or(0.0),
            };
            min_speed = min_speed.min(speed);
            max_speed = max_speed.max(speed);
            outputs.push(combined);
        }

        let best_speed = max_speed;
        let min_speed = if min_speed.is_finite() {
            min_speed
        } else {
            0.0
        };

        let status = if best_speed == 0.0 && t.target > 0.0 {
            regressions = true;
            "FAILED"
        } else if best_speed < t.target * 0.90 {
            // Allow 10% variance before calling it a regression
            regressions = true;
            "REGRESSION"
        } else {
            "PASS"
        };

        println!(
            "{:<35} | {:<12.2} | {:<12.2} | {:<12.2} | {:<12.2} | {}",
            t.name, best_speed, min_speed, max_speed, t.target, status
        );

        if best_speed == 0.0 {
            for (idx, combined) in outputs.iter().enumerate() {
                println!("--- Output run {} ---\n{}", idx + 1, combined);
            }
        }

        thread::sleep(Duration::from_millis(150));
    }

    std::fs::remove_dir_all(&run_dir)
        .unwrap_or_else(|e| println!("Failed to delete temp dir {} {}", run_dir.display(), e));

    if regressions {
        println!("\nWARNING: Some benchmarks showed regressions or failed.");
        if fail_on_regressions {
            std::process::exit(1);
        }
    } else {
        println!("\nAll benchmarks passed successfully.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_accepts_common_suffixes() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1KiB"), Some(1024));
        assert_eq!(parse_size("2MiB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_size("4GiB"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1g"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("nope"), None);
    }

    #[test]
    fn parse_reported_gbps_extracts_speed() {
        let sample = "copy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
        assert_eq!(parse_reported_gbps(sample), Some(10.0));
        let sampled = "recursive-copy sample t=0.010s bytes=123 items=4 window=1.230 GB/s avg=1.230 GB/s items/s=400.0\ncopy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
        assert_eq!(parse_reported_gbps(sampled), Some(10.0));
        assert_eq!(parse_reported_gbps("no throughput here"), None);
    }

    #[test]
    fn choose_test_size_respects_wear_cap() {
        let fs = FsStats {
            total_bytes: 1024_u64.pow(4), // 1 TiB
            avail_bytes: 1024_u64.pow(4),
        };
        let size = choose_test_size(fs, 3, 13, 256 * 1024 * 1024, 4 * 1024 * 1024 * 1024, 0.01);
        // 1% of 1TiB / 13 ~= 0.78 GiB (aligned down)
        assert!(size <= 900 * 1024 * 1024);
    }
}
