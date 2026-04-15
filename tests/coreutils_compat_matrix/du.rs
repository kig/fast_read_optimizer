use super::*;

fn assert_same_sorted_nul_records(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    let mut fro_records = fro
        .stdout
        .split(|byte| *byte == b'\0')
        .filter(|record| !record.is_empty())
        .map(|record| record.to_vec())
        .collect::<Vec<_>>();
    let mut sys_records = system
        .stdout
        .split(|byte| *byte == b'\0')
        .filter(|record| !record.is_empty())
        .map(|record| record.to_vec())
        .collect::<Vec<_>>();
    fro_records.sort();
    sys_records.sort();
    assert_eq!(
        fro_records, sys_records,
        "{label}: sorted NUL stdout mismatch"
    );
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
}

#[test]
fn cartesian_du_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-matrix");
    let tree = tmp.join("tree");
    let nested = tree.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), b"root\n").unwrap();
    fs::write(tree.join("nested/child.txt"), vec![0x55; 8192]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 16384]).unwrap();

    for du_args in [
        vec![tree.to_str().unwrap()],
        vec!["-s", tree.to_str().unwrap()],
        vec!["-a", tree.to_str().unwrap()],
        vec![tree.join("root.txt").to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du {:?}", du_args),
        );
    }
}

#[test]
fn du_hcs_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-hcs");
    let tree = tmp.join("tree");
    let nested = tree.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(tree.join("nested/child.txt"), vec![0x22; 8192]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 16384]).unwrap();

    assert_same_result(
        run_fro("du", &["-hcs", tree.to_str().unwrap()]),
        run_system("du", &["-hcs", tree.to_str().unwrap()]),
        "du -hcs",
    );
}

#[test]
fn du_separate_dirs_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-separate-dirs");
    let tree = tmp.join("tree");
    let nested = tree.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(tree.join("nested/child.txt"), vec![0x22; 8192]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 16384]).unwrap();

    for du_args in [
        vec!["-S", tree.to_str().unwrap()],
        vec!["--separate-dirs", tree.to_str().unwrap()],
        vec!["-aS", tree.to_str().unwrap()],
        vec!["-sS", tree.to_str().unwrap()],
        vec!["-cS", tree.to_str().unwrap()],
        vec!["-S", "--max-depth=1", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du separate-dirs {:?}", du_args),
        );
    }
}

#[test]
fn du_max_depth_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-max-depth");
    let tree = tmp.join("tree");
    let nested = tree.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(tree.join("nested/child.txt"), vec![0x22; 8192]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 16384]).unwrap();

    for du_args in [
        vec!["--max-depth=0", tree.to_str().unwrap()],
        vec!["--max-depth=1", tree.to_str().unwrap()],
        vec!["--max-depth", "1", tree.to_str().unwrap()],
        vec!["-d1", tree.to_str().unwrap()],
        vec!["-a", "--max-depth=1", tree.to_str().unwrap()],
        vec!["-hd1", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du max-depth {:?}", du_args),
        );
    }
}

#[test]
fn du_max_depth_conflicts_and_errors_match_system() {
    let tmp = unique_temp_dir("fro-coreutils-du-max-depth-errors");
    let tree = tmp.join("tree");
    fs::create_dir_all(tree.join("nested")).unwrap();
    fs::write(tree.join("nested/file.bin"), vec![0x33; 4096]).unwrap();

    for du_args in [
        vec!["-s", "--max-depth=0", tree.to_str().unwrap()],
        vec!["-s", "--max-depth=1", tree.to_str().unwrap()],
        vec!["--max-depth=bad", tree.to_str().unwrap()],
        vec!["-d", "bad", tree.to_str().unwrap()],
    ] {
        assert_same_result(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du max-depth error {:?}", du_args),
        );
    }
}

#[test]
fn du_matches_system_for_symlinks_broken_symlinks_and_fifos() {
    let tmp = unique_temp_dir("fro-coreutils-du-special");
    let tree = tmp.join("tree");
    let nested = tree.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let regular = tree.join("regular.txt");
    let symlink_path = tree.join("regular-link");
    let broken_symlink = tree.join("broken-link");
    let fifo = tree.join("events.fifo");
    fs::write(&regular, vec![0x55; 4096]).unwrap();
    symlink(&regular, &symlink_path).unwrap();
    symlink(tree.join("missing-target"), &broken_symlink).unwrap();
    make_fifo(&fifo);
    fs::write(nested.join("leaf.bin"), vec![0x33; 8192]).unwrap();

    for du_args in [
        vec![tree.to_str().unwrap()],
        vec!["-a", tree.to_str().unwrap()],
        vec![symlink_path.to_str().unwrap()],
        vec![broken_symlink.to_str().unwrap()],
        vec![fifo.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du special {:?}", du_args),
        );
    }
}

#[test]
fn du_dereference_args_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-dereference-args");
    let real = tmp.join("real");
    let nested = real.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(real.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x22; 8192]).unwrap();

    let dir_link = tmp.join("real-link");
    let file_link = tmp.join("file-link");
    symlink(&real, &dir_link).unwrap();
    symlink(real.join("root.txt"), &file_link).unwrap();

    for du_args in [
        vec!["-aH", dir_link.to_str().unwrap()],
        vec!["--dereference-args", "-a", dir_link.to_str().unwrap()],
        vec!["-H", file_link.to_str().unwrap()],
        vec!["-aH", tmp.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du dereference-args {:?}", du_args),
        );
    }
}

#[test]
fn du_dereference_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-dereference");
    let tree = tmp.join("tree");
    let linked_dir = tmp.join("linked-dir");
    let nested = linked_dir.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&tree).unwrap();
    fs::write(linked_dir.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x22; 8192]).unwrap();
    let linked_file = tmp.join("linked-file.bin");
    fs::write(&linked_file, vec![0x33; 6144]).unwrap();

    let dir_link = tree.join("dir-link");
    let file_link = tree.join("file-link");
    symlink(&linked_dir, &dir_link).unwrap();
    symlink(&linked_file, &file_link).unwrap();

    for du_args in [
        vec!["-aL", tree.to_str().unwrap()],
        vec!["--dereference", "-a", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du dereference {:?}", du_args),
        );
    }

    for du_args in [
        vec!["-L", dir_link.to_str().unwrap()],
        vec!["--dereference", file_link.to_str().unwrap()],
    ] {
        assert_same_result(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du dereference {:?}", du_args),
        );
    }
}

#[test]
fn du_explicit_no_dereference_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-no-dereference");
    let real = tmp.join("real");
    fs::create_dir_all(&real).unwrap();
    fs::write(real.join("root.txt"), vec![0x11; 4096]).unwrap();

    let dir_link = tmp.join("real-link");
    symlink(&real, &dir_link).unwrap();

    for du_args in [
        vec!["-P", dir_link.to_str().unwrap()],
        vec!["--no-dereference", dir_link.to_str().unwrap()],
        vec!["-H", "-P", dir_link.to_str().unwrap()],
        vec!["-P", "-H", dir_link.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du no-dereference {:?}", du_args),
        );
    }
}

#[test]
fn du_dereference_last_flag_wins_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-dereference-precedence");
    let tree = tmp.join("tree");
    let linked_dir = tmp.join("linked-dir");
    let nested = linked_dir.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&tree).unwrap();
    fs::write(linked_dir.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x22; 8192]).unwrap();
    symlink(&linked_dir, tree.join("dir-link")).unwrap();

    for du_args in [
        vec!["-L", "-P", "-a", tree.to_str().unwrap()],
        vec!["-P", "-L", "-a", tree.to_str().unwrap()],
        vec!["-H", "-L", "-a", tree.to_str().unwrap()],
        vec!["-L", "-H", "-a", tree.to_str().unwrap()],
        vec![
            "--dereference",
            "--no-dereference",
            "-a",
            tree.to_str().unwrap(),
        ],
        vec![
            "--no-dereference",
            "--dereference",
            "-a",
            tree.to_str().unwrap(),
        ],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du dereference precedence {:?}", du_args),
        );
    }
}

#[test]
fn du_block_size_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-block-size");
    let tree = tmp.join("tree");
    let nested = tree.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(tree.join("nested/child.txt"), vec![0x22; 8192]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 16384]).unwrap();

    for du_args in [
        vec!["-B", "1", tree.to_str().unwrap()],
        vec!["-B1K", tree.to_str().unwrap()],
        vec!["-aB2K", tree.to_str().unwrap()],
        vec!["--block-size=1", tree.to_str().unwrap()],
        vec!["--block-size", "512", tree.to_str().unwrap()],
        vec!["-a", "--block-size=2K", tree.to_str().unwrap()],
        vec![
            "--human-readable",
            "--block-size=1K",
            tree.to_str().unwrap(),
        ],
        vec![
            "--block-size=1K",
            "--human-readable",
            tree.to_str().unwrap(),
        ],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du block-size {:?}", du_args),
        );
    }
}

#[test]
fn du_display_size_shortcuts_and_si_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-display-size");
    let tree = tmp.join("tree");
    fs::create_dir_all(&tree).unwrap();
    fs::write(tree.join("regular.bin"), vec![0x11; 1500]).unwrap();
    let sparse = tree.join("sparse.bin");
    let file = fs::File::create(&sparse).unwrap();
    file.set_len(1024 * 1024).unwrap();

    for du_args in [
        vec!["-k", tree.to_str().unwrap()],
        vec!["-m", tree.to_str().unwrap()],
        vec!["--si", tree.to_str().unwrap()],
        vec!["-b", "-k", sparse.to_str().unwrap()],
        vec!["-k", "-b", sparse.to_str().unwrap()],
        vec!["--si", "-B1", tree.to_str().unwrap()],
        vec!["-B1", "--si", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du display-size {:?}", du_args),
        );
    }
}

#[test]
fn du_apparent_size_and_bytes_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-apparent-size");
    let tree = tmp.join("tree");
    fs::create_dir_all(&tree).unwrap();
    fs::write(tree.join("regular.bin"), vec![0x11; 3]).unwrap();
    let sparse = tree.join("sparse.bin");
    let file = fs::File::create(&sparse).unwrap();
    file.set_len(1024 * 1024).unwrap();
    symlink("regular.bin", tree.join("regular-link")).unwrap();

    for du_args in [
        vec!["--apparent-size", sparse.to_str().unwrap()],
        vec!["-b", sparse.to_str().unwrap()],
        vec!["--bytes", tree.to_str().unwrap()],
        vec!["-ab", tree.to_str().unwrap()],
        vec!["--apparent-size", "--block-size=1K", tree.to_str().unwrap()],
        vec!["--bytes", "--human-readable", tree.to_str().unwrap()],
        vec!["--human-readable", "--bytes", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du apparent-size {:?}", du_args),
        );
    }
}

#[test]
fn du_null_terminated_output_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-null");
    let tree = tmp.join("tree");
    let nested = tree.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x22; 8192]).unwrap();

    for du_args in [
        vec!["-a0", tree.to_str().unwrap()],
        vec!["--all", "--null", tree.to_str().unwrap()],
        vec!["-c0", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_nul_records(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du null {:?}", du_args),
        );
    }
}

#[test]
fn du_threshold_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-threshold");
    let tree = tmp.join("tree");
    let nested = tree.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("small.bin"), vec![0x11; 3]).unwrap();
    fs::write(tree.join("medium.bin"), vec![0x22; 4096]).unwrap();
    let sparse = tree.join("sparse.bin");
    let file = fs::File::create(&sparse).unwrap();
    file.set_len(4096).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 3]).unwrap();

    for du_args in [
        vec!["-a", "-t", "2K", tree.to_str().unwrap()],
        vec!["-a", "--threshold=2K", tree.to_str().unwrap()],
        vec!["-a", "-t-2K", tree.to_str().unwrap()],
        vec!["-ac", "--threshold=3K", tree.to_str().unwrap()],
        vec![
            "-a",
            "--apparent-size",
            "-B1",
            "--threshold=2K",
            tree.to_str().unwrap(),
        ],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du threshold {:?}", du_args),
        );
    }
}

#[test]
fn du_threshold_errors_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-du-threshold-errors");
    let tree = tmp.join("tree");
    fs::create_dir_all(&tree).unwrap();
    fs::write(tree.join("file.bin"), vec![0x11; 3]).unwrap();

    for du_args in [
        vec!["--threshold=bad", tree.to_str().unwrap()],
        vec!["-t", "-0", tree.to_str().unwrap()],
    ] {
        assert_same_result(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du threshold error {:?}", du_args),
        );
    }
}

#[test]
fn du_help_and_version_surface_stay_wired() {
    let help = run_fro("du", &["--help"]);
    assert_eq!(
        help.status.code(),
        Some(0),
        "du --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&help.stdout),
        String::from_utf8_lossy(&help.stderr),
    );
    assert!(
        help.stderr.is_empty(),
        "du --help unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&help.stderr),
    );
    let help_stdout = String::from_utf8(help.stdout).expect("du help should be UTF-8");
    for token in [
        "bounded GNU du-compatible accounting/traversal slice",
        "-c/--total",
        "--null",
        "--count-links",
        "--exclude",
        "--exclude-from",
        "--files0-from",
        "--inodes",
        "--one-file-system",
        "--threshold",
        "--bytes",
        "--apparent-size",
        "--block-size",
        "--no-dereference",
        "--dereference",
        "--si",
        "--time",
        "--time-style",
        "--help",
        "--version",
        "-0",
        "-D",
        "-L",
        "-X",
        "-c",
        "-l",
        "-t",
        "-k",
        "-m",
        "-x",
        "--max-depth",
    ] {
        assert!(
            help_stdout.contains(token),
            "du --help output should mention {token}:\n{help_stdout}"
        );
    }

    let version = run_fro("du", &["--version"]);
    assert_eq!(
        version.status.code(),
        Some(0),
        "du --version failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr),
    );
    assert!(
        version.stderr.is_empty(),
        "du --version unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&version.stderr),
    );
    let version_stdout = String::from_utf8(version.stdout).expect("du version should be UTF-8");
    assert!(
        version_stdout.starts_with("du "),
        "du --version should start with the du command name:\n{version_stdout}"
    );
}

#[test]
fn du_double_dash_treats_dash_prefixed_paths_as_operands() {
    let tmp = unique_temp_dir("fro-coreutils-du-double-dash");
    let tree = tmp.join("-tree");
    let nested = tree.join("-nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(tree.join("root.txt"), vec![0x11; 4096]).unwrap();
    fs::write(nested.join("leaf.bin"), vec![0x33; 8192]).unwrap();

    for du_args in [
        vec!["--", tree.to_str().unwrap()],
        vec!["-a", "--", tree.to_str().unwrap()],
        vec!["--max-depth=1", "--", tree.to_str().unwrap()],
    ] {
        assert_same_sorted_lines(
            run_fro("du", &du_args),
            run_system("du", &du_args),
            &format!("du double-dash {:?}", du_args),
        );
    }
}

#[test]
fn du_warns_and_continues_on_permission_denied_directory() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }

    let tmp = unique_temp_dir("fro-coreutils-du-perms");
    let root = tmp.join("tree");
    let blocked = root.join("blocked");
    fs::create_dir_all(&blocked).unwrap();
    fs::write(root.join("visible.txt"), vec![0x55; 4096]).unwrap();
    fs::write(blocked.join("hidden.bin"), vec![0x33; 8192]).unwrap();

    let mut perms = fs::metadata(&blocked).unwrap().permissions();
    perms.set_mode(0);
    fs::set_permissions(&blocked, perms).unwrap();

    let fro = run_fro("du", &[root.to_str().unwrap()]);
    let system = run_system("du", &[root.to_str().unwrap()]);

    let mut restore = fs::metadata(&blocked).unwrap().permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&blocked, restore).unwrap();

    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "du status mismatch"
    );
    let mut fro_lines = String::from_utf8_lossy(&fro.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut sys_lines = String::from_utf8_lossy(&system.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    fro_lines.sort();
    sys_lines.sort();
    assert_eq!(fro_lines, sys_lines, "du stdout mismatch");
    assert!(String::from_utf8_lossy(&fro.stderr).contains("Permission denied"));
    assert!(String::from_utf8_lossy(&system.stderr).contains("Permission denied"));
}
