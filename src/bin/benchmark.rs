use std::process::Command;
use std::env;

struct TestCase {
    name: &'static str,
    args: Vec<String>,
    target: f64,
}

fn main() {
    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fast_read_optimizer");

    let source_file = "/data/repos/sadtalker/checkpoints.tar.gz".to_string();
    let target_file_dir = "/data/repos/sadtalker/benchmark-target-dir.tmp".to_string();
    let target_file_cache = "/data/repos/sadtalker/benchmark-target-cache.tmp".to_string();

    let tests = vec![
        TestCase {
            name: "bench-diff (memory)",
            args: vec!["bench-diff".into()],
            target: 30.0,
        },
        TestCase {
            name: "write (direct)",
            args: vec!["write".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), target_file_dir.clone()],
            target: 4.5,
        },
        TestCase {
            name: "write (page cache)",
            args: vec!["write".into(), "-v".into(), "-n".into(), "1".into(), target_file_cache.clone()],
            target: 2.0, // Can be fast, but keeping conservative
        },
        TestCase {
            name: "read (direct)",
            args: vec!["read".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 23.0,
        },
        TestCase {
            name: "read (page cache)",
            args: vec!["read".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone()],
            target: 10.0,
        },
        TestCase {
            name: "copy (direct)",
            args: vec!["copy".into(), "--force-direct".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_dir.clone()],
            target: 4.5,
        },
        TestCase {
            name: "copy (page cache)",
            args: vec!["copy".into(), "--no-direct-io".into(), "-v".into(), "-n".into(), "1".into(), source_file.clone(), target_file_cache.clone()],
            target: 0.5,
        },
        TestCase {
            name: "diff (direct)",
            args: vec!["diff".into(), "--force-direct".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 14.5,
        },
        TestCase {
            name: "diff (page cache)",
            args: vec!["diff".into(), "--no-direct-io".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 10.0,
        },
        TestCase {
            name: "dual-read-bench (direct)",
            args: vec!["dual-read-bench".into(), "--force-direct".into(), "-v".into(), source_file.clone(), target_file_dir.clone()],
            target: 23.0,
        },
        TestCase {
            name: "dual-read-bench (page cache)",
            args: vec!["dual-read-bench".into(), "--no-direct-io".into(), "-v".into(), source_file.clone(), target_file_cache.clone()],
            target: 10.0,
        },
        TestCase {
            name: "grep (direct)",
            args: vec!["grep".into(), "--direct".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 14.5,
        },
        TestCase {
            name: "grep (page cache)",
            args: vec!["grep".into(), "-v".into(), "-n".into(), "1".into(), "needle".into(), source_file.clone()],
            target: 20.0,
        },
        TestCase {
            name: "bench-mmap-write",
            args: vec!["bench-mmap-write".into(), target_file_cache.clone()],
            target: 0.7,
        },
        TestCase {
            name: "bench-write",
            args: vec!["bench-write".into(), target_file_cache.clone()],
            target: 0.5,
        }
    ];

    let mut regressions = false;

    // Create target files with 4GB size so write modes don't panic on missing file
    if let Ok(f) = std::fs::File::create(&target_file_dir) {
        let _ = f.set_len(4 * 1024 * 1024 * 1024);
    }
    if let Ok(f) = std::fs::File::create(&target_file_cache) {
        let _ = f.set_len(4 * 1024 * 1024 * 1024);
    }

    println!("{:<25} | {:<12} | {:<12} | {:<10}", "Benchmark", "Speed (GB/s)", "Target", "Status");
    println!("{:-<25}-|-{:-<12}-|-{:-<12}-|-{:-<10}", "", "", "", "");

    for t in tests {
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

        println!("{:<25} | {:<12.2} | {:<12.2} | {}", t.name, speed, t.target, status);
        
        if speed == 0.0 {
            println!("--- Output ---\n{}", combined);
        }
    }
    
    // Cleanup temporary files
    let _ = std::fs::remove_file(target_file_dir);
    let _ = std::fs::remove_file(target_file_cache);

    if regressions {
        println!("\nWARNING: Some benchmarks showed regressions or failed.");
        std::process::exit(1);
    } else {
        println!("\nAll benchmarks passed successfully.");
    }
}
