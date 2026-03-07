use std::process::Command;
use std::env;
use std::fs;

fn main() {
    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fast_read_optimizer");

    let source_file = "/data/repos/sadtalker/checkpoints.tar.gz";
    let target_file_dir = "/data/repos/sadtalker/optimizer-target-dir.tmp";
    let target_file_cache = "/data/repos/sadtalker/optimizer-target-cache.tmp";

    // Setup temp files
    if let Ok(f) = fs::File::create(target_file_dir) { let _ = f.set_len(1024 * 1024 * 1024); }
    if let Ok(f) = fs::File::create(target_file_cache) { let _ = f.set_len(1024 * 1024 * 1024); }

    let configs = vec![
        vec!["read", "--direct", "-n", "100", source_file],
        vec!["read", "-n", "100", source_file],
        vec!["grep", "--direct", "-n", "100", "needle", source_file],
        vec!["grep", "-n", "100", "needle", source_file],
        vec!["write", "--direct", "-n", "20", target_file_dir],
        vec!["write", "-n", "20", target_file_cache],
        vec!["copy", "--force-direct", "-n", "20", source_file, target_file_dir],
        vec!["copy", "--no-direct-io", "-n", "20", source_file, target_file_cache],
        vec!["diff", "--force-direct", "-n", "20", source_file, target_file_dir],
        vec!["diff", "--no-direct-io", "-n", "20", source_file, target_file_cache],
        vec!["dual-read-bench", "--force-direct", "-n", "20", source_file, target_file_dir],
        vec!["dual-read-bench", "--no-direct-io", "-n", "20", source_file, target_file_cache],
    ];

    println!("Running optimizer for all modes... (This may take a while)");

    for args in configs {
        println!("Optimizing: fast_read_optimizer {:?}", args);
        let status = Command::new(&fro_exe)
            .args(&args)
            .status()
            .expect("Failed to execute process");

        if !status.success() {
            eprintln!("Warning: Optimization failed for {:?}", args);
        }
    }

    let _ = fs::remove_file(target_file_dir);
    let _ = fs::remove_file(target_file_cache);

    println!("Optimization complete! Results saved to fast_read_optimizer.json");
}
