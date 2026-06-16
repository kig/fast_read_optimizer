use super::*;

fn resets_recursive_copy_target(test: &TestCase, recursive_copy_target: &str) -> bool {
    matches!(test.bytes_hint, BytesHint::RecursiveTree)
        && test.args.last().map(String::as_str) == Some(recursive_copy_target)
}

fn resets_recursive_tree_fixture(test: &TestCase, recursive_tree: &str) -> bool {
    if !matches!(test.bytes_hint, BytesHint::RecursiveTree) {
        return false;
    }
    if test.args.last().map(String::as_str) != Some(recursive_tree) {
        return false;
    }
    match test.program {
        "fro" => test.args.first().map(String::as_str) == Some("rm"),
        "rm" => true,
        _ => false,
    }
}

pub(super) fn main_impl() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--plan] [--skip-build] [--no-fail] [-c config.json] [--iters <n>] [--repeat <n>] [--test-dir path] [--test-size <size>] [--max-drive-writes <fraction>] <test_prefix ...>",
            args[0]
        );
        println!(
            "\nAuto sizing (default): chooses a temp file size based on free space and a wear budget.\n\
              - --plan                  (print suggested test size + write load and exit)\n\
              - --test-size 1GiB         (force fixed size)\n\
              - --max-drive-writes 0.05  (cap total user-data writes per run to ~5% of FS capacity)\n\
              - --iters 5               (override internal -n for fro invocations; useful for quick runs/tests)\n\
              - --repeat 3              (run each benchmark multiple times; report min/max and judge by best steady-state run)\n\
              - metric column           (GB/s for throughput rows; files/s for tree-walk comparisons)\n\
              - -c, --config cfg.json   (pass the same config to every fro subprocess in the benchmark run)\n\
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
    let mut config_path: Option<&str> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else if arg == "-c" || arg == "--config" {
            config_path = Some(args[i].as_str());
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
    let recursive_tree_manifest = run_dir.join("fro_bench_recursive_tree_manifest.txt");
    let recursive_tree_manifest_str = recursive_tree_manifest.display().to_string();
    let recursive_copy_target = run_dir
        .join("fro_bench_recursive_copy_out")
        .display()
        .to_string();

    let tests = build_tests(
        source_file.clone(),
        target_file_dir.clone(),
        target_file_cache.clone(),
        recursive_tree_str.clone(),
        recursive_tree_manifest_str.clone(),
        recursive_copy_target.clone(),
    );

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

    let recursive_selected_explicitly = [
        "recursive-read-bench (hot)",
        "copy (recursive, hot)",
        "tree compare:",
        "rm (recursive, hot)",
        "tree compare: rm -r (hot)",
    ]
    .iter()
    .any(|pattern| matches_any_pattern(pattern, &patterns));
    let recursive_fixture_stats = recursive_tree_fixture_stats(&recursive_tree);
    let recursive_fixture_exists = recursive_fixture_stats.is_some();
    let mut selected_tests = Vec::new();
    for t in tests {
        if patterns.is_empty() {
            let needs_recursive_fixture = matches!(t.bytes_hint, BytesHint::RecursiveTree);
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

    let wants_large_hot_source = selected_tests.iter().any(|t| {
        matches!(t.cache_state, CacheState::Hot)
            && matches!(t.bytes_hint, BytesHint::SourceFile)
            && (t.args.first().map(String::as_str) == Some("read")
                || t.args.first().map(String::as_str) == Some("grep"))
    });
    if test_size.is_none() && wants_large_hot_source {
        let recommended_hot_source_size = 4 * 1024 * 1024 * 1024;
        min_test_size = min_test_size.max(recommended_hot_source_size);
        max_test_size = max_test_size.max(recommended_hot_source_size);
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
    let mut need_recursive_tree_manifest = false;

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
        if t.args.iter().any(|s| s == &recursive_tree_manifest_str) {
            need_recursive_tree_manifest = true;
            need_recursive_tree = true;
        }

        if (op == "diff" || op == "dual-read-bench" || op == "cmp")
            && t.args.iter().any(|s| s == &target_file_dir)
        {
            need_target_dir_matching = true;
        }
        if (op == "diff" || op == "dual-read-bench" || op == "cmp")
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
            fixed_write_bytes,
            min_test_size,
            max_test_size,
            max_drive_writes,
        )
    } else {
        eprintln!("Warning: statvfs failed for --test-dir; falling back to 4GiB");
        4 * 1024 * 1024 * 1024
    };

    if file_count > 0 && test_size.is_none() && size < min_test_size {
        eprintln!(
            "Warning: wear/space cap suggests a small test file: {} (min requested: {})",
            format_bytes(size),
            format_bytes(min_test_size),
        );
    }

    let est_user_writes =
        fro::test_sizing::estimate_user_writes(size, num_full_writes, fixed_write_bytes);
    let alloc = fro::test_sizing::estimate_alloc_bytes(size, file_count);
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
        println!("  large_hot_source: {}", wants_large_hot_source);
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
        let mut command = fro_subcommand_command(&fro_exe, "write", config_path);
        command
            .arg(&source_file.clone())
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
            let mut command = fro_subcommand_command(&fro_exe, "copy", config_path);
            command
                .arg(&source_file.clone())
                .arg(&target_file_dir.clone())
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
            let mut command = fro_subcommand_command(&fro_exe, "copy", config_path);
            command
                .arg(&source_file.clone())
                .arg(&target_file_cache.clone())
                .output()
                .unwrap_or_else(|e| {
                    panic!("Failed to create test file {} {}", target_file_cache, e)
                });
        }
    }

    if need_recursive_tree && recursive_selected_explicitly && !recursive_fixture_exists {
        create_recursive_tree_fixture(&recursive_tree, size);
    }
    if need_recursive_tree_manifest && recursive_tree.is_dir() {
        write_recursive_tree_manifest(&recursive_tree, &recursive_tree_manifest).unwrap_or_else(
            |e| {
                panic!(
                    "Could not create recursive benchmark manifest {} {}",
                    recursive_tree_manifest.display(),
                    e
                )
            },
        );
    }

    let mut regressions = false;

    println!(
        "{:<48} | {:<10} | {:<12} | {:<12} | {:<12} | {:<12} | {:<10}",
        "Benchmark", "Metric", "Best", "Min", "Max", "Target", "Status"
    );
    println!(
        "{:-<48}-|-{:-<10}-|-{:-<12}-|-{:-<12}-|-{:-<12}-|-{:-<12}-|-{:-<10}",
        "", "", "", "", "", "", ""
    );

    for t in selected_tests {
        let mut min_speed = f64::INFINITY;
        let mut max_speed = 0.0_f64;
        let mut outputs = Vec::with_capacity(repeat_count);
        let mut run_summaries: Vec<Option<FroRunSummary>> = Vec::with_capacity(repeat_count);

        for _ in 0..repeat_count {
            if resets_recursive_copy_target(&t, &recursive_copy_target) {
                if let Some(target) = t.args.last() {
                    let target_path = std::path::Path::new(target);
                    let _ = std::fs::remove_dir_all(target_path);
                    let _ = std::fs::remove_file(target_path);
                }
            }
            if resets_recursive_tree_fixture(&t, &recursive_tree_str) {
                create_recursive_tree_fixture(&recursive_tree, size);
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

            let (output, elapsed) = run_test_command(&fro_exe, &t, config_path)
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
            let file_count_for_effective_speed = match t.bytes_hint {
                BytesHint::RecursiveTree => Some(recursive_tree_fixture_files as f64),
                _ => None,
            };
            let summary = match t.kind {
                CommandKind::Fro | CommandKind::FroDiscardStdout => {
                    parse_reported_summary(&combined)
                }
                CommandKind::ExternalDiscardStdout => None,
            };
            let speed = match t.metric {
                MetricKind::Gbps => match t.kind {
                    CommandKind::Fro | CommandKind::FroDiscardStdout => {
                        summary.as_ref().map(|s| s.gbps).unwrap_or_else(|| {
                            bytes_for_effective_speed
                                .map(|bytes| bytes as f64 / elapsed.as_secs_f64().max(1e-9) / 1e9)
                                .unwrap_or(0.0)
                        })
                    }
                    CommandKind::ExternalDiscardStdout => bytes_for_effective_speed
                        .map(|bytes| bytes as f64 / elapsed.as_secs_f64().max(1e-9) / 1e9)
                        .unwrap_or(0.0),
                },
                MetricKind::FilesPerSecond => file_count_for_effective_speed
                    .map(|files| files / elapsed.as_secs_f64().max(1e-9))
                    .unwrap_or(0.0),
            };
            min_speed = min_speed.min(speed);
            max_speed = max_speed.max(speed);
            outputs.push(combined);
            run_summaries.push(summary);
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

        let best_params = run_summaries
            .iter()
            .filter_map(|summary| summary.as_ref())
            .find(|summary| (summary.gbps - best_speed).abs() < 1e-9)
            .and_then(|summary| summary.params.as_ref())
            .map(|params| format!(" {:?}", params))
            .unwrap_or_default();

        println!(
            "{:<48} | {:<10} | {:<12.2} | {:<12.2} | {:<12.2} | {:<12.2} | {}{}",
            t.name,
            t.metric.label(),
            best_speed,
            min_speed,
            max_speed,
            t.target,
            status,
            best_params
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
