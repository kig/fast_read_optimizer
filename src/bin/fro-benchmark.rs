use std::process::Command;
use std::fs::OpenOptions;
use std::env;

#[derive(Clone)]
enum CacheState {
    None,
    Cold,
    Hot,
}

struct TestCase {
    name: &'static str,
    args: Vec<String>,
    target: f64,
    cache_state: CacheState,
    files_to_prep: Vec<String>,
}

fn evict_cache(path: &str) {
    if let Ok(file) = std::fs::File::open(path) {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
}

fn pre_cache(path: &str) {
    if let Ok(mut file) = std::fs::File::open(path) {
        use std::io::Read;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 { break; }
        }
    }
    if let Ok(mut file) = std::fs::File::open(path) {
        use std::io::Read;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 { break; }
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

    let frsize = if vfs.f_frsize == 0 { vfs.f_bsize } else { vfs.f_frsize } as u64;
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
        size
    } else {
        size
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--plan] [--test-dir path] [--test-size <size>] [--max-drive-writes <fraction>] <test_prefix ...>",
            args[0]
        );
        println!(
            "\nAuto sizing (default): chooses a temp file size based on free space and a wear budget.\n\
             - --plan                  (print suggested test size + write load and exit)\n\
             - --test-size 1GiB         (force fixed size)\n\
             - --max-drive-writes 0.05  (cap total user-data writes per run to ~5% of FS capacity)"
        );
        std::process::exit(0);
    }

    let mut test_size: Option<u64> = None;
    let mut min_test_size: u64 = 256 * 1024 * 1024;
    let mut max_test_size: u64 = 1024 * 1024 * 1024;
    let mut max_drive_writes: f64 = 0.05;
    let mut plan = false;

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
        } else {
            patterns.push(arg);
        }
    }

    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fro");

    let test_path = std::path::Path::new(test_dir);

    let source_file = test_path.join("fro_bench_tmp_source").display().to_string();
    let target_file_dir = test_path.join("fro_bench_tmp_direct").display().to_string();
    let target_file_cache = test_path.join("fro_bench_tmp_cache").display().to_string();

    let tests = vec![
        TestCase {
            name: "bench-diff (memory)",
            args: vec!["bench-diff".into()],
            target: 60.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        },
        TestCase {
            name: "write (direct)",
            args: vec!["write".into(), "--direct-write".into(), "-v".into(), "-n".into(), "1".into(), target_file_dir.clone()],
            target: 10.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_dir.clone()],
        },
        TestCase {
            name: "write (page cache, cold)",
            args: vec!["write".into(), "--no-direct-write".into(), "-v".into(), "-n".into(), "1".into(), target_file_cache.clone()],
            target: 8.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_dir.clone()],
        },
        TestCase {
            name: "write (page cache, hot)",
            args: vec!["write".into(), "--no-direct-write".into(), "-v".into(), "-n".into(), "1".into(), target_file_cache.clone()],
            target: 8.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![target_file_dir.clone()],
        },
        TestCase {
            name: "write (auto, cold)",
            args: vec!["write".into(), "-v".into(), "-n".into(), "1".into(), target_file_cache.clone()],
            target: 8.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![target_file_dir.clone()],
        },
        TestCase {
            name: "write (auto, hot)",
            args: vec!["write".into(), "-v".into(), "-n".into(), "1".into(), target_file_cache.clone()],
            target: 10.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![target_file_dir.clone()],
        },
        TestCase {
            name: "read (direct)",
            args: vec!["read".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "read (forced page cache, hot)",
            args: vec!["read".into(), "--no-direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "read (auto, cold)",
            args: vec!["read".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "read (auto, hot)",
            args: vec!["read".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "copy (direct)",
            args: vec!["copy".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_dir.clone()],
            target: 5.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
        },
        TestCase {
            name: "copy (page cache, cold)",
            args: vec!["copy".into(), "--no-direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_cache.clone()],
            target: 1.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "copy (hot cache R, direct W)",
            args: vec!["copy".into(), "--no-direct".into(), "--direct-write".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_cache.clone()],
            target: 2.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "copy (auto, cold)",
            args: vec!["copy".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_cache.clone()],
            target: 5.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "copy (auto, hot)",
            args: vec!["copy".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_cache.clone()],
            target: 4.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "diff (direct)",
            args: vec!["diff".into(), "--direct".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        },
        TestCase {
            name: "diff (page cache, cold)",
            args: vec!["diff".into(), "--no-direct".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 3.5,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "diff (page cache, hot)",
            args: vec!["diff".into(), "--no-direct".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "diff (auto, cold)",
            args: vec!["diff".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
        },
        TestCase {
            name: "diff (auto, hot)",
            args: vec!["diff".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
        },
        TestCase {
            name: "dual-read-bench (direct)",
            args: vec!["dual-read-bench".into(), "--direct".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        },
        TestCase {
            name: "dual-read-bench (page cache, cold)",
            args: vec!["dual-read-bench".into(), "--no-direct".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 3.5,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "dual-read-bench (page cache, hot)",
            args: vec!["dual-read-bench".into(), "--no-direct".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_cache.clone()],
        },
        TestCase {
            name: "dual-read-bench (auto, cold)",
            args: vec!["dual-read-bench".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
        },
        TestCase {
            name: "dual-read-bench (auto, hot)",
            args: vec!["dual-read-bench".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone(), target_file_dir.clone()],
        },
        TestCase {
            name: "grep (direct)",
            args: vec!["grep".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 20.0,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        },
        TestCase {
            name: "grep (forced page cache, hot)",
            args: vec!["grep".into(), "--no-direct".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "grep (auto, cold)",
            args: vec!["grep".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 20.0,
            cache_state: CacheState::Cold,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "grep (auto, hot)",
            args: vec!["grep".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 50.0,
            cache_state: CacheState::Hot,
            files_to_prep: vec![source_file.clone()],
        },
        TestCase {
            name: "bench-mmap-write",
            args: vec!["bench-mmap-write".into(), target_file_cache.clone()],
            target: 0.7,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        },
        TestCase {
            name: "bench-write",
            args: vec!["bench-write".into(), target_file_cache.clone()],
            target: 0.5,
            cache_state: CacheState::None,
            files_to_prep: vec![],
        }
    ];

    if !plan {
        let build_output = Command::new("cargo")
            .args(["build", "--release"])
            .output()
            .unwrap_or_else(|e| panic!("Failed to compile: {}", e));

        let out_str = String::from_utf8_lossy(&build_output.stdout);
        let err_str = String::from_utf8_lossy(&build_output.stderr);
        let combined = format!("{}\n{}", out_str, err_str);
        println!("{}", combined);
    }

    let mut selected_tests = Vec::new();
    for t in tests {
        if patterns.is_empty() {
            selected_tests.push(t);
            continue;
        }

        let mut ok = false;
        for p in patterns.iter() {
            if t.name.starts_with(p.as_str()) {
                ok = true;
                break;
            }
        }
        if ok {
            selected_tests.push(t);
        }
    }

    if selected_tests.is_empty() {
        eprintln!("No benchmarks selected.");
        return;
    }

    let mut need_source = false;
    let mut need_target_dir = false;
    let mut need_target_cache = false;
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

        if (op == "diff" || op == "dual-read-bench") && t.args.iter().any(|s| s == &target_file_dir) {
            need_target_dir_matching = true;
        }
        if (op == "diff" || op == "dual-read-bench") && t.args.iter().any(|s| s == &target_file_cache) {
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

    let file_count = (need_source as u64) + (need_target_dir as u64) + (need_target_cache as u64);
    let setup_writes = (need_source as u64)
        + (need_target_dir_matching as u64)
        + (need_target_cache_matching as u64);
    num_full_writes = num_full_writes.saturating_add(setup_writes);

    // Create temp files (only if needed by the selected tests).
    let size = if file_count == 0 {
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
            "  file_count: {} (source={} direct_target={} cache_target={})",
            file_count, need_source, need_target_dir, need_target_cache
        );
        println!("  alloc_total: {}", format_bytes(alloc));
        println!("  est_full_writes: {}", num_full_writes);
        println!("  est_fixed_write_bytes: {}", format_bytes(fixed_write_bytes));
        println!("  est_user_writes: {}", format_bytes(est_user_writes));
        println!("  max_drive_writes: {}", max_drive_writes);
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
                .unwrap_or_else(|e| panic!("Failed to create test file {} {}", target_file_cache, e));
        }
    }

    let mut regressions = false;

    println!("{:<35} | {:<12} | {:<12} | {:<10}", "Benchmark", "Speed (GB/s)", "Target", "Status");
    println!("{:-<35}-|-{:-<12}-|-{:-<12}-|-{:-<10}", "", "", "", "");

    for t in selected_tests {
        match t.cache_state {
            CacheState::Cold => {
                for f in &t.files_to_prep { evict_cache(f); }
            }
            CacheState::Hot => {
                for f in &t.files_to_prep { pre_cache(f); }
            }
            CacheState::None => {}
        }

        let output = Command::new(&fro_exe)
            .args(&t.args)
            .output()
            .unwrap_or_else(|e| panic!("Failed to execute process for {}: {}", t.name, e));

        let out_str = String::from_utf8_lossy(&output.stdout);
        let err_str = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}\n{}", out_str, err_str);

        let mut speed = 0.0;
        for line in combined.lines() {
            if let Some(idx) = line.find(" GB/s") {
                let start = line[..idx].rfind(' ').map(|i| i + 1).unwrap_or(0);
                if let Ok(num) = line[start..idx].trim().parse::<f64>() {
                    speed = num;
                    break;
                }
            }
        }

        let status = if speed == 0.0 {
            regressions = true;
            "FAILED"
        } else if speed < t.target * 0.90 { // Allow 10% variance before calling it a regression
            regressions = true;
            "REGRESSION"
        } else {
            "PASS"
        };

        println!("{:<35} | {:<12.2} | {:<12.2} | {}", t.name, speed, t.target, status);
        
        if speed == 0.0 {
            println!("--- Output ---\n{}", combined);
        }
    }

    for filename in [source_file, target_file_cache, target_file_dir].iter() {
        std::fs::remove_file(filename).unwrap_or_else(|e| println!("Failed to delete temp file {} {}", filename, e));
    }
    
    if regressions {
        println!("\nWARNING: Some benchmarks showed regressions or failed.");
        std::process::exit(1);
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
    fn choose_test_size_respects_wear_cap() {
        let fs = FsStats {
            total_bytes: 1024_u64.pow(4), // 1 TiB
            avail_bytes: 1024_u64.pow(4),
        };
        let size = choose_test_size(
            fs,
            3,
            13,
            256 * 1024 * 1024,
            4 * 1024 * 1024 * 1024,
            0.01,
        );
        // 1% of 1TiB / 13 ~= 0.78 GiB (aligned down)
        assert!(size <= 900 * 1024 * 1024);
    }
}
