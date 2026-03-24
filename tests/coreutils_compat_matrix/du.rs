use super::*;

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
