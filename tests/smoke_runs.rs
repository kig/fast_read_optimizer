use std::process::Command;

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    std::fs::create_dir_all(&base).unwrap();

    let p = base.join(format!("{}-{}-{}", prefix, pid, nanos));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn smoke_optimize_runs_with_limited_iters_and_size() {
    let tmp = unique_temp_dir("fro-smoke-optimize");

    let fro_optimize = env!("CARGO_BIN_EXE_fro-optimize");

    let status = Command::new(fro_optimize)
        .args([
            "--test-dir",
            tmp.to_str().unwrap(),
            "--test-size",
            "64MiB",
            "--iters",
            "5",
            "read",
        ])
        .status()
        .expect("failed to run fro-optimize");

    assert!(status.success());
}

#[test]
fn smoke_benchmark_runs_with_limited_iters_and_size() {
    let tmp = unique_temp_dir("fro-smoke-benchmark");

    let fro_benchmark = env!("CARGO_BIN_EXE_fro-benchmark");

    let out = Command::new(fro_benchmark)
        .args([
            "--skip-build",
            "--no-fail",
            "--iters",
            "5",
            "--test-dir",
            tmp.to_str().unwrap(),
            "--test-size",
            "64MiB",
            "read (forced page cache, hot)",
        ])
        .output()
        .expect("failed to run fro-benchmark");

    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}\n",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("read (forced page cache, hot)"));
}

#[test]
fn smoke_memcpy_bench_runs_with_small_size() {
    let fro = env!("CARGO_BIN_EXE_fro");

    let out = Command::new(fro)
        .args(["bench-memcpy", "--size", "64MiB", "--threads", "2"])
        .output()
        .expect("failed to run bench-memcpy");

    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}\n",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Memory memcpy"));
}
