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
fn optimize_all_writes_target_config_and_selection_uses_mount_overrides() {
    let tmp = unique_temp_dir("fro-optimize-all-test");
    let d1 = tmp.join("d1");
    let d2 = tmp.join("d2");
    std::fs::create_dir_all(&d1).unwrap();
    std::fs::create_dir_all(&d2).unwrap();

    let cfg = tmp.join("fro.json");

    // Force deterministic, low-cost behavior.
    // (Provide dirs explicitly so this test doesn't depend on the host's mount list.)

    let fro_optimize = env!("CARGO_BIN_EXE_fro-optimize");

    let status = Command::new(fro_optimize)
        .args([
            "--all",
            "--all-dir",
            d1.to_str().unwrap(),
            "--all-dir",
            d2.to_str().unwrap(),
            "--iters",
            "1",
            "-c",
            cfg.to_str().unwrap(),
            "--test-size",
            "4KiB",
            "read",
        ])
        .status()
        .expect("failed to run fro-optimize");
    assert!(status.success());

    // Verify it wrote to the requested config path.
    assert!(cfg.exists());
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&cfg).unwrap()).unwrap();

    let by_mount = v
        .get_mut("mount_overrides")
        .and_then(|m| m.get_mut("by_mountpoint"))
        .and_then(|m| m.as_object_mut())
        .expect("expected mount_overrides.by_mountpoint");

    assert!(
        !by_mount.is_empty(),
        "expected at least one mount override entry"
    );

    // Pick one override and mutate it to a unique value; then assert config selection returns it.
    let mp = by_mount.keys().next().unwrap().to_string();

    let entry = by_mount.get_mut(&mp).unwrap();
    if entry.get("read").is_none() {
        entry["read"] = serde_json::json!({});
    }
    if entry["read"].get("direct").is_none() {
        entry["read"]["direct"] = serde_json::json!({});
    }

    entry["read"]["direct"]["num_threads"] = serde_json::json!(77);
    entry["read"]["direct"]["block_size"] = serde_json::json!(256 * 1024);
    entry["read"]["direct"]["qd"] = serde_json::json!(9);

    std::fs::write(&cfg, serde_json::to_string_pretty(&v).unwrap()).unwrap();

    let file1 = d1.join("x.bin");
    std::fs::write(&file1, b"hello").unwrap();

    let loaded = fro::config::load_config(Some(cfg.to_str().unwrap()));
    let p = loaded.get_params_for_path("read", true, file1.to_str().unwrap());
    assert_eq!(p.num_threads, 77);
    assert_eq!(p.block_size, 256 * 1024);
    assert_eq!(p.qd, 9);

    // End-to-end check: `fro` should actually use the mount override for this file.
    let fro_exe = env!("CARGO_BIN_EXE_fro");
    let out = Command::new(fro_exe)
        .args([
            "read",
            "--direct",
            "-n",
            "1",
            "-c",
            cfg.to_str().unwrap(),
            file1.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run fro");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("[31, 131072, 1, 77, 262144, 9]"),
        "expected to see overridden params in output, got:\n{}",
        combined
    );
}
