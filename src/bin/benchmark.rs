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

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args[1] == "--help" {
        println!("USAGE: {} [--test-dir path] <test_prefix ...>", args[0]);
        std::process::exit(0);
    }
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else {
            patterns.push(arg);
        }
    }

    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fast_read_optimizer");

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

    let build_output = Command::new("cargo")
            .args(["build", "--release"])
            .output()
            .unwrap_or_else(|e| panic!("Failed to compile: {}", e));

    let out_str = String::from_utf8_lossy(&build_output.stdout);
    let err_str = String::from_utf8_lossy(&build_output.stderr);
    let combined = format!("{}\n{}", out_str, err_str);
    println!("{}", combined);

    // Create temp files
    let size = 1 * 1024 * 1024 * 1024; // 1 GB

    let file = OpenOptions::new().write(true).create(true).open(source_file.clone()).unwrap_or_else(|e| panic!("Could not create test file {} {}", source_file, e));
    file.set_len(size).unwrap_or_else(|e| panic!("Could not set file length for {} {}", source_file, e));
    Command::new(&fro_exe).args(["write", &source_file.clone()]).output().unwrap_or_else(|e| panic!("Failed to write test file {} {}", source_file, e));
    Command::new(&fro_exe).args(["copy", &source_file.clone(), &target_file_dir.clone()]).output().unwrap_or_else(|e| panic!("Failed to create test file {} {}", target_file_dir, e));
    Command::new(&fro_exe).args(["copy", &source_file.clone(), &target_file_cache.clone()]).output().unwrap_or_else(|e| panic!("Failed to create test file {} {}", target_file_cache, e));

    let mut regressions = false;

    println!("{:<35} | {:<12} | {:<12} | {:<10}", "Benchmark", "Speed (GB/s)", "Target", "Status");
    println!("{:-<35}-|-{:-<12}-|-{:-<12}-|-{:-<10}", "", "", "", "");

    for t in tests {
        if patterns.len() > 0 {
            let mut pattern_found = false;
            for p in patterns.iter() {
                if t.name.starts_with(*p) {
                    pattern_found = true;
                    break;
                }
            }
            if !pattern_found { continue; }
        }
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
