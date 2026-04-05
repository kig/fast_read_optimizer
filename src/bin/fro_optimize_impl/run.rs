use super::*;
use super::cli_utils::{align_down, fmt_gib, fs_stats_for_path, parse_size};
use super::inspect::{
    collect_home_targets, device_db_match, fill_device_info, find_writable_dir_for_mount,
    is_disk_backed_mount, load_device_db, read_mountinfo,
};

pub(super) fn main_impl() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--list-devices | --list-devices-all] [--all] [-c config.json] [--plan] [--test-dir path] [--test-size <size>] [--max-drive-writes <fraction>] <test_prefix ...>",
            args[0]
        );
        println!(
            "\nAuto sizing (default): chooses a temp file size based on free space and a wear budget.\n\
             - --plan                  (print suggested test size + write load and exit)\n\
             - --test-size 1GiB         (force fixed size)\n\
             - --max-drive-writes 0.05  (cap total user-data writes per run to ~5% of FS capacity)"
        );
        println!("\n--list-devices prints likely disk-backed mountpoints as JSON and exits.");
        println!("--list-devices-all prints all mountpoints from /proc/self/mountinfo as JSON and exits.");
        println!("--all runs the optimizer for each discovered disk-backed mount (best-effort writable_dir discovery).");
        println!("--all-dir <path> can be repeated to provide an explicit list of directories to optimize.");
        println!("--iters <n> overrides the internal -n used for each optimized mode (useful for quick runs/tests).");
        std::process::exit(0);
    }

    let mut test_size: Option<u64> = None;
    let mut min_test_size: u64 = 256 * 1024 * 1024;
    let mut max_test_size: u64 = 1024 * 1024 * 1024;
    let mut max_drive_writes: f64 = 0.05;
    let mut plan = false;
    let mut all = false;
    let mut list_devices = false;
    let mut list_devices_all = false;
    let mut config_path: Option<&str> = None;
    let mut all_dirs: Vec<String> = Vec::new();
    let mut iters_override: Option<u64> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--list-devices" {
            list_devices = true;
        } else if arg == "--list-devices-all" {
            list_devices_all = true;
        } else if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else if arg == "-c" || arg == "--config" {
            config_path = Some(args[i].as_str());
            i += 1;
        } else if arg == "-a" || arg == "--all" {
            all = true;
        } else if arg == "--all-dir" {
            all = true;
            all_dirs.push(args[i].clone());
            i += 1;
        } else if arg == "--iters" {
            let v = &args[i];
            i += 1;
            iters_override = Some(v.parse().unwrap_or_else(|_| {
                eprintln!("Invalid --iters: {}", v);
                std::process::exit(2);
            }));
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

    if list_devices || list_devices_all {
        let home_targets = collect_home_targets();
        let db = load_device_db();

        let entries = read_mountinfo();
        let mut entries: Vec<_> = if list_devices_all {
            entries
        } else {
            entries
                .into_iter()
                .filter(|e| is_disk_backed_mount(e))
                .collect()
        };

        for e in entries.iter_mut() {
            let mp = Path::new(&e.mount_point);
            let wd = find_writable_dir_for_mount(mp, &home_targets);
            e.writable_dir = wd.map(|p| p.display().to_string());

            fill_device_info(e);
            if let Some(ref db) = db {
                if let Some(p) = device_db_match(db, e) {
                    e.device_db_profile = Some(p.id.clone());
                    e.device_db_read_direct = Some(p.params.read.direct.clone());
                }
            }
        }

        let out = serde_json::to_string_pretty(&entries).unwrap_or_else(|_| "[]".to_string());
        println!("{}", out);
        return;
    }

    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fro");

    macro_rules! argsv {
        ($($s:expr),* $(,)?) => {
            vec![$($s.to_string()),*]
        };
    }

    let read_iters = iters_override.unwrap_or(100).to_string();
    let write_iters = iters_override.unwrap_or(20).to_string();

    let run_for_dir = |test_dir: &str| {
        let test_path = std::path::Path::new(test_dir);

        let source_file = test_path.join("fro_bench_tmp_source").display().to_string();
        let target_file_dir = test_path.join("fro_bench_tmp_direct").display().to_string();
        let target_file_cache = test_path.join("fro_bench_tmp_cache").display().to_string();
        let recursive_tree = test_path.join("fro_optimize_recursive_small_tree");
        let recursive_tree_str = recursive_tree.display().to_string();

        let mut configs: Vec<Vec<String>> = vec![
            argsv!("read", "-s", "--direct", "-n", &read_iters, &source_file),
            argsv!("read", "-s", "--no-direct", "-n", &read_iters, &source_file),
            argsv!(
                "read",
                "--to-memory",
                "-s",
                "--direct",
                "-n",
                &read_iters,
                &source_file
            ),
            argsv!(
                "read",
                "--to-memory",
                "--paged-shared-buffer",
                "-s",
                "--no-direct",
                "-n",
                &read_iters,
                &source_file
            ),
            argsv!(
                "grep",
                "-s",
                "--direct",
                "-n",
                &read_iters,
                "needle",
                &source_file
            ),
            argsv!(
                "grep",
                "-s",
                "--no-direct",
                "-n",
                &read_iters,
                "needle",
                &source_file
            ),
            argsv!(
                "write",
                "-s",
                "--direct",
                "-n",
                &write_iters,
                &target_file_dir
            ),
            argsv!(
                "write",
                "-s",
                "--no-direct",
                "-n",
                &write_iters,
                &target_file_cache
            ),
            argsv!(
                "copy",
                "-s",
                "--direct",
                "-n",
                &write_iters,
                &source_file,
                &target_file_dir
            ),
            argsv!(
                "copy",
                "-s",
                "--no-direct",
                "-n",
                &write_iters,
                &source_file,
                &target_file_cache
            ),
            argsv!(
                "copy",
                "-s",
                "--copy-file-range",
                "--no-direct",
                "-n",
                &write_iters,
                &source_file,
                &target_file_cache
            ),
            argsv!(
                "diff",
                "-s",
                "--direct",
                "-n",
                &read_iters,
                &source_file,
                &target_file_dir
            ),
            argsv!(
                "diff",
                "-s",
                "--no-direct",
                "-n",
                &read_iters,
                &source_file,
                &target_file_cache
            ),
            argsv!(
                "dual-read-bench",
                "-s",
                "--direct",
                "-n",
                &read_iters,
                &source_file,
                &target_file_dir
            ),
            argsv!(
                "dual-read-bench",
                "-s",
                "--no-direct",
                "-n",
                &read_iters,
                &source_file,
                &target_file_cache
            ),
            argsv!(
                "bench-recursive-small-file-threads",
                "--no-direct",
                "--cold",
                "-s",
                &recursive_tree_str
            ),
            argsv!(
                "bench-recursive-small-file-threads",
                "--no-direct",
                "--hot",
                "-s",
                &recursive_tree_str
            ),
        ];

        if let Some(cfg) = config_path {
            for c in configs.iter_mut() {
                c.insert(1, cfg.to_string());
                c.insert(1, "-c".to_string());
            }
        }

        let mut selected = Vec::new();
        for cfg in configs {
            if patterns.is_empty() {
                selected.push(cfg);
                continue;
            }
            let mut ok = false;
            let is_to_memory = cfg.iter().any(|arg| arg == "--to-memory");
            for p in patterns.iter() {
                let pattern = p.as_str();
                if cfg[0].starts_with(pattern)
                    || ((pattern == "read-to-memory" || pattern == "read_to_memory")
                        && is_to_memory)
                {
                    ok = true;
                    break;
                }
            }
            if ok {
                selected.push(cfg);
            }
        }

        if selected.is_empty() {
            eprintln!("No optimizer modes selected.");
            return;
        }

        let mut need_source = false;
        let mut need_target_dir = false;
        let mut need_target_cache = false;
        let mut need_target_dir_matching = false;
        let mut need_target_cache_matching = false;
        let mut need_recursive_tree = false;

        let mut num_full_writes: u64 = 0;
        for cfg in &selected {
            let op = cfg[0].as_str();

            if cfg.iter().any(|s| s == &source_file) {
                need_source = true;
            }
            if cfg.iter().any(|s| s == &target_file_dir) {
                need_target_dir = true;
            }
            if cfg.iter().any(|s| s == &target_file_cache) {
                need_target_cache = true;
            }
            if cfg.iter().any(|s| s == &recursive_tree_str) {
                need_recursive_tree = true;
            }

            if (op == "diff" || op == "dual-read-bench")
                && cfg.iter().any(|s| s == &target_file_dir)
            {
                need_target_dir_matching = true;
            }
            if (op == "diff" || op == "dual-read-bench")
                && cfg.iter().any(|s| s == &target_file_cache)
            {
                need_target_cache_matching = true;
            }

            if op == "write" || op == "copy" {
                let mut n = 1_u64;
                let mut j = 0;
                while j + 1 < cfg.len() {
                    if cfg[j] == "-n" {
                        n = cfg[j + 1].parse::<u64>().unwrap_or(1);
                        break;
                    }
                    j += 1;
                }
                num_full_writes = num_full_writes.saturating_add(n);
            }
        }

        if need_target_dir || need_target_cache {
            need_source = true;
        }

        let file_count = (need_source as u64)
            + (need_target_dir as u64)
            + (need_target_cache as u64)
            + (need_recursive_tree as u64);
        let setup_writes = (need_source as u64)
            + (need_target_dir_matching as u64)
            + (need_target_cache_matching as u64);
        num_full_writes = num_full_writes.saturating_add(setup_writes);

        let size = if let Some(s) = test_size {
            align_down(s, 4096).max(4096)
        } else if let Some((total, avail)) = fs_stats_for_path(test_path) {
            let space_cap = ((avail as f64) * 0.60 / (file_count as f64)) as u64;
            let wear_cap = if num_full_writes == 0 {
                u64::MAX
            } else {
                ((total as f64) * max_drive_writes / (num_full_writes as f64)) as u64
            };

            let mut size = max_test_size.min(space_cap).min(wear_cap);
            size = align_down(size, 4096).max(4096);

            if size < min_test_size {
                eprintln!(
                    "Warning: wear/space cap suggests a small test file: {} (min requested: {})",
                    fmt_gib(size),
                    fmt_gib(min_test_size)
                );
            }

            size
        } else {
            eprintln!("Warning: statvfs failed for --test-dir; falling back to 4GiB");
            4 * 1024 * 1024 * 1024
        };

        let est_user_writes = size.saturating_mul(num_full_writes);
        let alloc = size.saturating_mul(file_count);

        if test_size.is_none() {
            eprintln!(
            "Auto-sized test file: {} (alloc={} across {} files; est_writes={} => est_user_writes={})",
            fmt_gib(size),
            fmt_gib(alloc),
            file_count,
            num_full_writes,
            fmt_gib(est_user_writes),
        );
        }

        if plan {
            println!("fro-optimize plan");
            println!("  test_dir: {}", test_dir);
            println!("  test_size: {}", fmt_gib(size));
            println!(
                "  file_count: {} (source={} direct_target={} cache_target={})",
                file_count, need_source, need_target_dir, need_target_cache
            );
            println!("  alloc_total: {}", fmt_gib(alloc));
            println!("  est_full_writes: {}", num_full_writes);
            println!("  est_user_writes: {}", fmt_gib(est_user_writes));
            println!("  max_drive_writes: {}", max_drive_writes);
            println!("  selected_modes:");
            for cfg in &selected {
                println!("    {}", cfg.join(" "));
            }
            return;
        }

        // Setup temp files (only the ones we actually need).
        if need_source {
            if let Ok(f) = fs::File::create(&source_file) {
                let _ = f.set_len(size);
            }
            let mut cmd = Command::new(&fro_exe);
            cmd.arg("write");
            if let Some(cfg) = config_path {
                cmd.args(["-c", cfg]);
            }
            let _ = cmd
                .arg(&source_file)
                .status()
                .expect("Failed to write fro_bench_tmp_source");
        }
        if need_target_dir {
            if let Ok(f) = fs::File::create(&target_file_dir) {
                let _ = f.set_len(size);
            }
            if need_target_dir_matching {
                let mut cmd = Command::new(&fro_exe);
                cmd.arg("copy");
                if let Some(cfg) = config_path {
                    cmd.args(["-c", cfg]);
                }
                let _ = cmd
                    .args([&source_file, &target_file_dir])
                    .status()
                    .expect("Failed to prepare fro_bench_tmp_direct");
            }
        }
        if need_target_cache {
            if let Ok(f) = fs::File::create(&target_file_cache) {
                let _ = f.set_len(size);
            }
            if need_target_cache_matching {
                let mut cmd = Command::new(&fro_exe);
                cmd.arg("copy");
                if let Some(cfg) = config_path {
                    cmd.args(["-c", cfg]);
                }
                let _ = cmd
                    .args([&source_file, &target_file_cache])
                    .status()
                    .expect("Failed to prepare fro_bench_tmp_cache");
            }
        }
        if need_recursive_tree {
            let _ = fs::remove_dir_all(&recursive_tree);
            fs::create_dir_all(&recursive_tree).expect("Failed to create recursive tuning tree");
            for i in 0..16384_u64 {
                let shard = format!("{:03}", i / 64);
                let dir = recursive_tree.join(shard);
                let _ = fs::create_dir_all(&dir);
                let path = dir.join(format!("file_{:06}.bin", i));
                fs::write(&path, vec![0x5a_u8; 4096]).expect("Failed to create recursive tuning file");
            }
        }

        println!("Running optimizer for all modes... (This may take a while)");

        for args in &selected {
            println!("Optimizing: fro {:?}", args);
            let status = Command::new(&fro_exe)
                .args(args)
                .status()
                .expect("Failed to execute process");

            if !status.success() {
                eprintln!("Warning: Optimization failed for {:?}", args);
            }
        }

        if selected
            .iter()
            .any(|cfg| cfg.first().is_some_and(|op| op == "copy"))
        {
            let mut cfg = fro::config::load_config(config_path);
            cfg.update_copy_auto_mode_for_path(&target_file_cache, fro::CopyAutoMode::Heuristic);
            cfg.save();
            println!("Saved copy auto mode: {:?}", fro::CopyAutoMode::Heuristic);
        }

        let _ = fs::remove_file(source_file);
        let _ = fs::remove_file(target_file_dir);
        let _ = fs::remove_file(target_file_cache);
        let _ = fs::remove_dir_all(recursive_tree);

        let saved_to = config_path.map(|p| p.to_string()).unwrap_or_else(|| {
            fro::config::resolve_default_config_path()
                .display()
                .to_string()
        });
        println!("Optimization complete! Results saved to {}", saved_to);
    };

    if all {
        let mut dirs: Vec<String> = Vec::new();

        if !all_dirs.is_empty() {
            dirs.extend(all_dirs);
        } else {
            let home_targets = collect_home_targets();
            let entries = read_mountinfo();
            for e in entries.into_iter().filter(|e| is_disk_backed_mount(e)) {
                let mp = Path::new(&e.mount_point);
                if let Some(wd) = find_writable_dir_for_mount(mp, &home_targets) {
                    dirs.push(wd.display().to_string());
                }
            }
        }

        dirs.sort();
        dirs.dedup();

        if dirs.is_empty() {
            eprintln!("No writable mountpoints found for --all");
            std::process::exit(2);
        }

        for d in dirs {
            println!("=== Optimizing for {} ===", d);
            run_for_dir(&d);
        }
    } else {
        run_for_dir(test_dir);
    }

}
