use super::*;

#[test]
fn shred_help_mentions_long_aliases_and_version() {
    let output = run_fro("shred", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--iterations"));
    assert!(stdout.contains("--size"));
    assert!(stdout.contains("--zero"));
    assert!(stdout.contains("--remove"));
    assert!(stdout.contains("--exact"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro shred version string and exits."));
    assert!(stdout.contains("--random-source remains unsupported"));
}

#[test]
fn shred_version_prints_version_string() {
    let output = run_fro("shred", &["--version"]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("shred (fro coreutils) {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn shred_long_aliases_and_exact_forms_match_system() {
    let tmp = unique_temp_dir("fro-shred-long-aliases");

    let fro_zero_file = tmp.join("fro-zero.bin");
    let sys_zero_file = tmp.join("sys-zero.bin");
    std::fs::write(&fro_zero_file, vec![0x61; 64 * 1024]).unwrap();
    std::fs::write(&sys_zero_file, vec![0x61; 64 * 1024]).unwrap();
    let zero_fro_args = vec![
        "--iterations",
        "0",
        "--zero",
        fro_zero_file.to_str().unwrap(),
    ];
    let zero_sys_args = vec![
        "--iterations",
        "0",
        "--zero",
        sys_zero_file.to_str().unwrap(),
    ];
    assert_same_result(
        run_fro("shred", &zero_fro_args),
        run_system("shred", &zero_sys_args),
        &format!("shred alias {:?}", zero_fro_args),
    );

    let fro_remove_file = tmp.join("fro-remove.bin");
    let sys_remove_file = tmp.join("sys-remove.bin");
    std::fs::write(&fro_remove_file, b"ghijkl").unwrap();
    std::fs::write(&sys_remove_file, b"ghijkl").unwrap();
    let remove_fro_args = vec![
        "--iterations=0",
        "--remove",
        fro_remove_file.to_str().unwrap(),
    ];
    let remove_sys_args = vec![
        "--iterations=0",
        "--remove",
        sys_remove_file.to_str().unwrap(),
    ];
    assert_same_result(
        run_fro("shred", &remove_fro_args),
        run_system("shred", &remove_sys_args),
        &format!("shred alias {:?}", remove_fro_args),
    );

    let fro_exact_long_file = tmp.join("fro-exact-long.bin");
    let sys_exact_long_file = tmp.join("sys-exact-long.bin");
    std::fs::write(&fro_exact_long_file, b"0123456789").unwrap();
    std::fs::write(&sys_exact_long_file, b"0123456789").unwrap();
    let exact_long_fro_args = vec![
        "--iterations",
        "0",
        "--zero",
        "--exact",
        "--size=4",
        fro_exact_long_file.to_str().unwrap(),
    ];
    let exact_long_sys_args = vec![
        "--iterations",
        "0",
        "--zero",
        "--exact",
        "--size=4",
        sys_exact_long_file.to_str().unwrap(),
    ];
    assert_same_result(
        run_fro("shred", &exact_long_fro_args),
        run_system("shred", &exact_long_sys_args),
        &format!("shred alias {:?}", exact_long_fro_args),
    );

    let fro_exact_short_file = tmp.join("fro-exact-short.bin");
    let sys_exact_short_file = tmp.join("sys-exact-short.bin");
    std::fs::write(&fro_exact_short_file, b"ABCDEFGHIJ").unwrap();
    std::fs::write(&sys_exact_short_file, b"ABCDEFGHIJ").unwrap();
    let exact_short_fro_args = vec![
        "--iterations",
        "0",
        "--zero",
        "-x",
        "--size",
        "4",
        fro_exact_short_file.to_str().unwrap(),
    ];
    let exact_short_sys_args = vec![
        "--iterations",
        "0",
        "--zero",
        "-x",
        "--size",
        "4",
        sys_exact_short_file.to_str().unwrap(),
    ];
    assert_same_result(
        run_fro("shred", &exact_short_fro_args),
        run_system("shred", &exact_short_sys_args),
        &format!("shred alias {:?}", exact_short_fro_args),
    );

    assert_eq!(
        std::fs::read(&fro_zero_file).unwrap(),
        std::fs::read(&sys_zero_file).unwrap()
    );
    assert!(std::fs::read(&fro_zero_file)
        .unwrap()
        .iter()
        .all(|&byte| byte == 0));
    assert_eq!(fro_remove_file.exists(), sys_remove_file.exists());
    assert!(!fro_remove_file.exists());
    assert_eq!(
        std::fs::read(&fro_exact_long_file).unwrap(),
        std::fs::read(&sys_exact_long_file).unwrap()
    );
    assert_eq!(
        std::fs::read(&fro_exact_short_file).unwrap(),
        std::fs::read(&sys_exact_short_file).unwrap()
    );
    assert_eq!(
        std::fs::read(&fro_exact_long_file).unwrap(),
        vec![0, 0, 0, 0, b'4', b'5', b'6', b'7', b'8', b'9']
    );
    assert_eq!(
        std::fs::read(&fro_exact_short_file).unwrap(),
        vec![0, 0, 0, 0, b'E', b'F', b'G', b'H', b'I', b'J']
    );
}
