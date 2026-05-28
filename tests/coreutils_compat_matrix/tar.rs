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

fn tar_fixture(prefix: &str) -> (PathBuf, PathBuf, String) {
    let tmp = unique_temp_dir(prefix);
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
    (tmp, source_root, source_name)
}

fn system_tar_supports_zstd() -> bool {
    let output = run_system("tar", &["--help"]);
    output.status.success() && String::from_utf8_lossy(&output.stdout).contains("--zstd")
}

fn command_available(program: &str) -> bool {
    run_system(
        "sh",
        &["-c", &format!("command -v {program} >/dev/null 2>&1")],
    )
    .status
    .success()
}

fn system_tar_supports_bzip2() -> bool {
    let output = run_system("tar", &["--help"]);
    output.status.success()
        && String::from_utf8_lossy(&output.stdout).contains("--bzip2")
        && (command_available("lbzip2") || command_available("bzip2"))
}

fn system_tar_supports_xz() -> bool {
    let output = run_system("tar", &["--help"]);
    output.status.success()
        && String::from_utf8_lossy(&output.stdout).contains("--xz")
        && command_available("xz")
}

fn encode_tar_octal(value: u64, field_len: usize) -> Vec<u8> {
    let digits = format!("{value:o}");
    let mut field = vec![b'0'; field_len];
    let start = field_len - digits.len() - 1;
    field[start..start + digits.len()].copy_from_slice(digits.as_bytes());
    field[field_len - 1] = 0;
    field
}

fn manual_tar_header(path: &str, size: u64, typeflag: u8) -> [u8; 512] {
    let path_bytes = path.as_bytes();
    assert!(path_bytes.len() <= 100);
    let mut header = [0u8; 512];
    header[..path_bytes.len()].copy_from_slice(path_bytes);
    header[100..108].copy_from_slice(&encode_tar_octal(0o644, 8));
    header[108..116].copy_from_slice(&encode_tar_octal(0, 8));
    header[116..124].copy_from_slice(&encode_tar_octal(0, 8));
    header[124..136].copy_from_slice(&encode_tar_octal(size, 12));
    header[136..148].copy_from_slice(&encode_tar_octal(1_700_000_000, 12));
    header[148..156].fill(b' ');
    header[156] = typeflag;
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum = header.iter().map(|byte| u32::from(*byte)).sum::<u32>() as u64;
    let checksum_digits = format!("{checksum:o}");
    let mut checksum_field = [b'0'; 8];
    let start = 6 - checksum_digits.len();
    checksum_field[start..start + checksum_digits.len()]
        .copy_from_slice(checksum_digits.as_bytes());
    checksum_field[6] = 0;
    checksum_field[7] = b' ';
    header[148..156].copy_from_slice(&checksum_field);
    header
}

#[test]
fn cartesian_tar_create_matches_system_extraction() {
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-matrix");
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

#[test]
fn cartesian_tar_extract_matches_system_tar() {
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-extract");
    let fro_tar = tmp.join("fro.tar");
    let sys_tar = tmp.join("sys.tar");

    assert_success(run_fro(
        "tar",
        &[
            "-cf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));
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

    for archive in [&fro_tar, &sys_tar] {
        let fro_extract = tmp.join(format!(
            "fro-extract-{}",
            archive.file_stem().unwrap().to_string_lossy()
        ));
        let sys_extract = tmp.join(format!(
            "sys-extract-{}",
            archive.file_stem().unwrap().to_string_lossy()
        ));
        fs::create_dir_all(&fro_extract).unwrap();
        fs::create_dir_all(&sys_extract).unwrap();

        let archive_str = archive.to_str().unwrap();
        assert_same_result(
            run_fro(
                "tar",
                &["-xf", archive_str, "-C", fro_extract.to_str().unwrap()],
            ),
            run_system(
                "tar",
                &["-xf", archive_str, "-C", sys_extract.to_str().unwrap()],
            ),
            &format!("extract {archive_str}"),
        );

        let fro_tree = snapshot_tree(&fro_extract.join(&source_name));
        let sys_tree = snapshot_tree(&sys_extract.join(&source_name));
        assert_eq!(fro_tree, sys_tree, "tree mismatch for {archive_str}");
    }
}

#[test]
fn cartesian_tar_extract_verbose_matches_system_tar() {
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-extract-verbose");
    let archive = tmp.join("archive.tar");

    assert_success(run_fro(
        "tar",
        &[
            "-cf",
            archive.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_extract = tmp.join("fro-extract");
    let sys_extract = tmp.join("sys-extract");
    fs::create_dir_all(&fro_extract).unwrap();
    fs::create_dir_all(&sys_extract).unwrap();

    assert_same_result(
        run_fro(
            "tar",
            &[
                "--extract",
                "--verbose",
                "--file",
                archive.to_str().unwrap(),
                "--directory",
                fro_extract.to_str().unwrap(),
            ],
        ),
        run_system(
            "tar",
            &[
                "--extract",
                "--verbose",
                "--file",
                archive.to_str().unwrap(),
                "--directory",
                sys_extract.to_str().unwrap(),
            ],
        ),
        "long extract verbose flags",
    );

    let fro_tree = snapshot_tree(&fro_extract.join(&source_name));
    let sys_tree = snapshot_tree(&sys_extract.join(&source_name));
    assert_eq!(fro_tree, sys_tree);
}

#[test]
fn tar_extract_rejects_absolute_member_paths() {
    let tmp = unique_temp_dir("fro-coreutils-tar-extract-absolute");
    let archive = tmp.join("absolute.tar");
    let extract_dir = tmp.join("extract");
    fs::create_dir_all(&extract_dir).unwrap();
    let payload = b"unsafe\n";
    let mut archive_bytes = Vec::new();
    archive_bytes.extend_from_slice(&manual_tar_header("/abs.txt", payload.len() as u64, b'0'));
    archive_bytes.extend_from_slice(payload);
    archive_bytes.resize(1024, 0);
    archive_bytes.extend_from_slice(&[0u8; 1024]);
    fs::write(&archive, archive_bytes).unwrap();

    let fro_out = run_fro(
        "tar",
        &[
            "-xf",
            archive.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    );
    assert!(
        !fro_out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fro_out.stdout),
        String::from_utf8_lossy(&fro_out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&fro_out.stderr).contains("rejects absolute member paths"),
        "stderr:\n{}",
        String::from_utf8_lossy(&fro_out.stderr)
    );
}

#[test]
fn cartesian_tar_list_matches_system_tar() {
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-list");
    let fro_tar = tmp.join("fro.tar");
    let sys_tar = tmp.join("sys.tar");

    assert_success(run_fro(
        "tar",
        &[
            "-cf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));
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

    for archive in [&fro_tar, &sys_tar] {
        let archive_str = archive.to_str().unwrap();
        assert_same_result(
            run_fro("tar", &["-tf", archive_str]),
            run_system("tar", &["-tf", archive_str]),
            archive_str,
        );
        assert_same_result(
            run_fro("tar", &["-tvf", archive_str]),
            run_system("tar", &["-tvf", archive_str]),
            &format!("verbose {archive_str}"),
        );
    }

    assert_same_result(
        run_fro("tar", &["--list", "--file", sys_tar.to_str().unwrap()]),
        run_system("tar", &["--list", "--file", sys_tar.to_str().unwrap()]),
        "long flags",
    );
    assert_same_result(
        run_fro(
            "tar",
            &["--list", "--verbose", "--file", sys_tar.to_str().unwrap()],
        ),
        run_system(
            "tar",
            &["--list", "--verbose", "--file", sys_tar.to_str().unwrap()],
        ),
        "long verbose flags",
    );
}

#[test]
fn cartesian_tar_gzip_create_and_list_match_system_tar() {
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-gzip-create");
    let fro_tar = tmp.join("fro.tar.gz");

    assert_success(run_fro(
        "tar",
        &[
            "-czf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_list = assert_success(run_fro("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system("tar", &["-tzf", fro_tar.to_str().unwrap()]));
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

    let extract_dir = tmp.join("fro-extract");
    fs::create_dir_all(&extract_dir).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "-xzf",
            fro_tar.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    ));
    let extracted = snapshot_tree(&extract_dir.join(&source_name));
    let original = snapshot_tree(&source_root);
    assert_eq!(extracted, original);
}

#[test]
fn cartesian_tar_gzip_extract_matches_system_tar() {
    let (tmp, _source_root, source_name) = tar_fixture("fro-coreutils-tar-gzip-extract");
    let archive = tmp.join("sys.tar.gz");
    let sys_out = run_system(
        "bash",
        &[
            "-lc",
            &format!(
                "cd {} && tar -czf {} {}",
                tmp.display(),
                archive.display(),
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

    let fro_extract = tmp.join("fro-extract");
    let sys_extract = tmp.join("sys-extract");
    fs::create_dir_all(&fro_extract).unwrap();
    fs::create_dir_all(&sys_extract).unwrap();

    assert_same_result(
        run_fro(
            "tar",
            &[
                "-xf",
                archive.to_str().unwrap(),
                "-C",
                fro_extract.to_str().unwrap(),
            ],
        ),
        run_system(
            "tar",
            &[
                "-xzf",
                archive.to_str().unwrap(),
                "-C",
                sys_extract.to_str().unwrap(),
            ],
        ),
        "gzip extract",
    );

    let fro_tree = snapshot_tree(&fro_extract.join(&source_name));
    let sys_tree = snapshot_tree(&sys_extract.join(&source_name));
    assert_eq!(fro_tree, sys_tree);
}

#[test]
fn cartesian_tar_zstd_create_and_list_match_system_tar() {
    if !system_tar_supports_zstd() {
        return;
    }
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-zstd-create");
    let fro_tar = tmp.join("fro.tar.zst");

    assert_success(run_fro(
        "tar",
        &[
            "--zstd",
            "-cf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_list = assert_success(run_fro("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system(
        "tar",
        &["--zstd", "-tf", fro_tar.to_str().unwrap()],
    ));
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

    let extract_dir = tmp.join("fro-zstd-extract");
    fs::create_dir_all(&extract_dir).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "--zstd",
            "-xf",
            fro_tar.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    ));
    let extracted = snapshot_tree(&extract_dir.join(&source_name));
    let original = snapshot_tree(&source_root);
    assert_eq!(extracted, original);
}

#[test]
fn cartesian_tar_zstd_extract_matches_system_tar() {
    if !system_tar_supports_zstd() {
        return;
    }
    let (tmp, _source_root, source_name) = tar_fixture("fro-coreutils-tar-zstd-extract");
    let archive = tmp.join("sys.tar.zst");
    let sys_out = run_system(
        "bash",
        &[
            "-lc",
            &format!(
                "cd {} && tar --zstd -cf {} {}",
                tmp.display(),
                archive.display(),
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

    let fro_extract = tmp.join("fro-zstd-extract");
    let sys_extract = tmp.join("sys-zstd-extract");
    fs::create_dir_all(&fro_extract).unwrap();
    fs::create_dir_all(&sys_extract).unwrap();

    assert_same_result(
        run_fro(
            "tar",
            &[
                "-xf",
                archive.to_str().unwrap(),
                "-C",
                fro_extract.to_str().unwrap(),
            ],
        ),
        run_system(
            "tar",
            &[
                "--zstd",
                "-xf",
                archive.to_str().unwrap(),
                "-C",
                sys_extract.to_str().unwrap(),
            ],
        ),
        "zstd extract",
    );

    let fro_tree = snapshot_tree(&fro_extract.join(&source_name));
    let sys_tree = snapshot_tree(&sys_extract.join(&source_name));
    assert_eq!(fro_tree, sys_tree);
}

#[test]
fn cartesian_tar_bzip2_create_and_list_match_system_tar() {
    if !system_tar_supports_bzip2() {
        return;
    }
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-bzip2-create");
    let fro_tar = tmp.join("fro.tar.bz2");

    assert_success(run_fro(
        "tar",
        &[
            "-cjf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_list = assert_success(run_fro("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system("tar", &["-tjf", fro_tar.to_str().unwrap()]));
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

    let extract_dir = tmp.join("fro-bzip2-extract");
    fs::create_dir_all(&extract_dir).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "-xjf",
            fro_tar.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    ));
    let extracted = snapshot_tree(&extract_dir.join(&source_name));
    let original = snapshot_tree(&source_root);
    assert_eq!(extracted, original);
}

#[test]
fn cartesian_tar_xz_create_and_extract_match_system_tar() {
    if !system_tar_supports_xz() {
        return;
    }
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-xz-create");
    let fro_tar = tmp.join("fro.tar.xz");

    assert_success(run_fro(
        "tar",
        &[
            "-cJf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_list = assert_success(run_fro("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system("tar", &["-tJf", fro_tar.to_str().unwrap()]));
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

    let extract_dir = tmp.join("fro-xz-extract");
    fs::create_dir_all(&extract_dir).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "-xJf",
            fro_tar.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    ));
    let extracted = snapshot_tree(&extract_dir.join(&source_name));
    let original = snapshot_tree(&source_root);
    assert_eq!(extracted, original);
}

#[test]
fn cartesian_tar_auto_compress_xz_matches_system_tar() {
    if !system_tar_supports_xz() {
        return;
    }
    let (tmp, source_root, source_name) = tar_fixture("fro-coreutils-tar-auto-compress");
    let fro_tar = tmp.join("fro-auto.tar.xz");

    assert_success(run_fro(
        "tar",
        &[
            "-acf",
            fro_tar.to_str().unwrap(),
            source_root.to_str().unwrap(),
        ],
    ));

    let fro_list = assert_success(run_fro("tar", &["-tf", fro_tar.to_str().unwrap()]));
    let sys_list = assert_success(run_system("tar", &["-tJf", fro_tar.to_str().unwrap()]));
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

    let extract_dir = tmp.join("fro-auto-extract");
    fs::create_dir_all(&extract_dir).unwrap();
    assert_success(run_system(
        "tar",
        &[
            "-xJf",
            fro_tar.to_str().unwrap(),
            "-C",
            extract_dir.to_str().unwrap(),
        ],
    ));
    let extracted = snapshot_tree(&extract_dir.join(&source_name));
    let original = snapshot_tree(&source_root);
    assert_eq!(extracted, original);
}
