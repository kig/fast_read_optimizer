use super::*;

fn assert_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn cartesian_tar_create_matches_system_extraction() {
    let tmp = unique_temp_dir("fro-coreutils-tar-matrix");
    let source_root = tmp.join("tar-src");
    let source_name = source_root
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let nested = source_root.join("nested/deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
    fs::write(
        nested.join("large.bin"),
        (0..(2 * 1024 * 1024 + 333))
            .map(|i| ((i * 11) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    symlink("../small.txt", source_root.join("nested/link-small")).unwrap();

    let fro_tar = tmp.join("fro.tar");
    let sys_tar = tmp.join("sys.tar");
    let fro_out = assert_success(run_fro(
        "tar",
        &[
            "-cf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));
    assert!(
        fro_out.stderr.is_empty(),
        "stderr:\n{}",
        String::from_utf8_lossy(&fro_out.stderr)
    );
    let sys_out = run_system(
        "bash",
        &[
            "-lc",
            &format!(
                "cd {} && tar -cf {} {}",
                tmp.display(),
                sys_tar.display(),
                source_name
            ),
        ],
    );
    assert!(
        sys_out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&sys_out.stdout),
        String::from_utf8_lossy(&sys_out.stderr)
    );

    let fro_list = assert_success(run_system("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system("tar", &["-tf", sys_tar.to_str().unwrap()]));
    let mut fro_lines = String::from_utf8_lossy(&fro_list.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut sys_lines = String::from_utf8_lossy(&sys_list.stdout)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    fro_lines.sort();
    sys_lines.sort();
    assert_eq!(fro_lines, sys_lines);

    let fro_extract = tmp.join("fro-extract");
    let sys_extract = tmp.join("sys-extract");
    fs::create_dir_all(&fro_extract).unwrap();
    fs::create_dir_all(&sys_extract).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "-xf",
            fro_tar.to_str().unwrap(),
            "-C",
            fro_extract.to_str().unwrap(),
        ],
    ));
    assert_success(run_system(
        "tar",
        &[
            "-xf",
            sys_tar.to_str().unwrap(),
            "-C",
            sys_extract.to_str().unwrap(),
        ],
    ));
    let root_name = source_root.file_name().unwrap();
    let fro_tree = snapshot_tree(&fro_extract.join(root_name));
    let sys_tree = snapshot_tree(&sys_extract.join(root_name));
    assert_eq!(fro_tree, sys_tree);
}
