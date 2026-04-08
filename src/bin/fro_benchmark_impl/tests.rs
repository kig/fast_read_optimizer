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
fn parse_reported_summary_extracts_speed_without_params() {
    let sample = "copy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
    let summary = parse_reported_summary(sample).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(summary.params.unwrap(), vec![1, 2, 3]);
    let sampled = "recursive-copy sample t=0.010s bytes=123 items=4 window=1.230 GB/s avg=1.230 GB/s items/s=400.0\ncopy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
    let summary = parse_reported_summary(sampled).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(summary.params.unwrap(), vec![1, 2, 3]);
    assert_eq!(parse_reported_summary("no throughput here"), None);
}

#[test]
fn choose_test_size_respects_wear_cap() {
    let fs = FsStats {
        total_bytes: 1024_u64.pow(4),
        avail_bytes: 1024_u64.pow(4),
    };
    let size = choose_test_size(
        fs,
        3,
        13,
        0,
        256 * 1024 * 1024,
        4 * 1024 * 1024 * 1024,
        0.01,
    );
    assert!(size <= 900 * 1024 * 1024);
}

#[test]
fn choose_test_size_counts_fixed_write_budget_against_wear_cap() {
    let gib = 1024_u64.pow(3);
    let fs = FsStats {
        total_bytes: 1024_u64.pow(4),
        avail_bytes: 1024_u64.pow(4),
    };
    let size = choose_test_size(fs, 1, 10, 8 * gib, 256 * 1024 * 1024, 4 * gib, 0.01);
    let total_write_budget = ((fs.total_bytes as f64) * 0.01) as u64;
    let expected = align_down(total_write_budget.saturating_sub(8 * gib) / 10, 4096);
    assert_eq!(size, expected);
}

#[test]
fn parse_reported_summary_extracts_speed_and_params() {
    let sample = "read 1073741824 bytes in 0.1076 s, 10.0 GB/s, [31, 131072, 1, 16, 3145728, 2, 4, 524288, 4]";
    let summary = parse_reported_summary(sample).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(
        summary.params.unwrap(),
        vec![31, 131072, 1, 16, 3145728, 2, 4, 524288, 4]
    );
}

#[test]
fn write_recursive_tree_manifest_lists_all_files_in_sorted_order() {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!(
            "recursive-tree-manifest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let root = base.join("tree");
    let manifest = base.join("tree.txt");
    std::fs::create_dir_all(root.join("b")).unwrap();
    std::fs::create_dir_all(root.join("a")).unwrap();
    std::fs::write(root.join("b").join("second.bin"), b"2").unwrap();
    std::fs::write(root.join("a").join("first.bin"), b"1").unwrap();
    std::fs::write(root.join("root.bin"), b"0").unwrap();

    let count = write_recursive_tree_manifest(&root, &manifest).unwrap();
    assert_eq!(count, 3);

    let content = std::fs::read_to_string(&manifest).unwrap();
    let lines = content.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            root.join("a").join("first.bin").display().to_string(),
            root.join("b").join("second.bin").display().to_string(),
            root.join("root.bin").display().to_string(),
        ]
    );

    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn build_tests_includes_coreutils_big_file_cases() {
    let tests = build_tests(
        "source.bin".to_string(),
        "target-direct.bin".to_string(),
        "target-cache.bin".to_string(),
        "tree".to_string(),
        "tree.txt".to_string(),
        "copy-out".to_string(),
    );
    let names = tests.iter().map(|test| test.name).collect::<Vec<_>>();
    for expected in [
        "coreutils cat (hot)",
        "coreutils fgrep --count (hot)",
        "coreutils wc -l (hot)",
        "coreutils cksum (hot)",
        "coreutils sha256sum (hot)",
        "coreutils base64 encode (hot)",
    ] {
        assert!(names.contains(&expected), "missing benchmark {expected}");
    }
}
