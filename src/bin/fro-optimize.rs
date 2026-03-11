use std::process::Command;
use std::env;
use std::fs;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && args[1] == "--help" {
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
    fro_exe.set_file_name("fro");

    let test_path = std::path::Path::new(test_dir);

    let source_file = test_path.join("fro_bench_tmp_source").display().to_string();
    let target_file_dir = test_path.join("fro_bench_tmp_direct").display().to_string();
    let target_file_cache = test_path.join("fro_bench_tmp_cache").display().to_string();

    // Setup temp files
    if let Ok(f) = fs::File::create(&source_file) { let _ = f.set_len(4 * 1024 * 1024 * 1024); }
    let _ = Command::new(&fro_exe).args(["write", &source_file]).status().expect("Failed to write fro_bench_tmp_source");
    let _ = Command::new(&fro_exe).args(["copy", &source_file, &target_file_dir]).status().expect("Failed to write fro_bench_tmp_direct");
    let _ = Command::new(&fro_exe).args(["copy", &source_file, &target_file_cache]).status().expect("Failed to write fro_bench_tmp_cache");

    let configs = vec![
        vec!["read", "-s", "--direct", "-n", "100", &source_file],
        vec!["read", "-s", "--no-direct", "-n", "100", &source_file],
        vec!["grep", "-s", "--direct", "-n", "100", "needle", &source_file],
        vec!["grep", "-s", "--no-direct", "-n", "100", "needle", &source_file],
        vec!["write", "-s", "--direct", "-n", "20", &target_file_dir],
        vec!["write", "-s", "--no-direct", "-n", "20", &target_file_cache],
        vec!["copy", "-s", "--direct", "-n", "20", &source_file, &target_file_dir],
        vec!["copy", "-s", "--no-direct", "-n", "20", &source_file, &target_file_cache],
        vec!["diff", "-s", "--direct", "-n", "100", &source_file, &target_file_dir],
        vec!["diff", "-s", "--no-direct", "-n", "100", &source_file, &target_file_cache],
        vec!["dual-read-bench", "-s", "--direct", "-n", "100", &source_file, &target_file_dir],
        vec!["dual-read-bench", "-s", "--no-direct", "-n", "100", &source_file, &target_file_cache],
    ];

    println!("Running optimizer for all modes... (This may take a while)");

    for args in configs {
        if patterns.len() > 0 {
            let mut pattern_found = false;
            for p in patterns.iter() {
                if args[0].starts_with(*p) {
                    pattern_found = true;
                    break;
                }
            }
            if !pattern_found { continue; }
        }
        println!("Optimizing: fro {:?}", args);
        let status = Command::new(&fro_exe)
            .args(&args)
            .status()
            .expect("Failed to execute process");

        if !status.success() {
            eprintln!("Warning: Optimization failed for {:?}", args);
        }
    }

    let _ = fs::remove_file(source_file);
    let _ = fs::remove_file(target_file_dir);
    let _ = fs::remove_file(target_file_cache);

    println!("Optimization complete! Results saved to fro.json");
}
