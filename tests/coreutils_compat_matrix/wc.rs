use super::*;

#[test]
fn cartesian_wc_matches_system_output() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-wc-matrix");
    let path = fixture.text_file;

    for flags in io_flag_sets() {
        for wc_flags in [
            vec![],
            vec!["-l"],
            vec!["-w"],
            vec!["-c"],
            vec!["-L"],
            vec!["-l", "-w"],
            vec!["-l", "-c"],
            vec!["-w", "-c"],
            vec!["-m", "-L"],
            vec!["-l", "-w", "-c"],
        ] {
            let mut args = flags.clone();
            args.extend(wc_flags.iter().copied());
            args.push(path.to_str().unwrap());
            let mut sys_args = wc_flags;
            sys_args.push(path.to_str().unwrap());
            assert_same_wc(
                run_fro("wc", &args),
                run_system("wc", &sys_args),
                &format!("wc {:?}", args),
            );
        }
    }
}

#[test]
fn wc_short_flag_bundles_match_system_output() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-wc-bundles");
    let path = fixture.text_file;

    for flags in io_flag_sets() {
        for wc_flags in [
            vec!["-lw"],
            vec!["-cl"],
            vec!["-lwc"],
            vec!["-mL"],
            vec!["-lmL"],
        ] {
            let mut args = flags.clone();
            args.extend(wc_flags.iter().copied());
            args.push(path.to_str().unwrap());
            let mut sys_args = wc_flags;
            sys_args.push(path.to_str().unwrap());
            assert_same_wc(
                run_fro("wc", &args),
                run_system("wc", &sys_args),
                &format!("wc bundled {:?}", args),
            );
        }
    }
}

#[test]
fn wc_files0_from_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0");
    let one = tmp.join("one.txt");
    let two = tmp.join("two words.txt");
    fs::write(&one, b"one two\n").unwrap();
    fs::write(&two, "aé🙂\nline\twide\n".as_bytes()).unwrap();

    let list = tmp.join("inputs.list0");
    let mut list_bytes = Vec::new();
    list_bytes.extend_from_slice(one.as_os_str().as_bytes());
    list_bytes.push(0);
    list_bytes.extend_from_slice(two.as_os_str().as_bytes());
    list_bytes.push(0);
    fs::write(&list, list_bytes).unwrap();

    for flags in io_flag_sets() {
        for wc_flags in [
            vec![],
            vec!["-l"],
            vec!["-m"],
            vec!["-L"],
            vec!["-l", "-m", "-L"],
        ] {
            let mut args = flags.clone();
            args.extend(wc_flags.iter().copied());
            args.push("--files0-from");
            args.push(list.to_str().unwrap());

            let mut sys_args = wc_flags;
            sys_args.push("--files0-from");
            sys_args.push(list.to_str().unwrap());

            assert_same_wc_exact(
                run_fro("wc", &args),
                run_system("wc", &sys_args),
                &format!("wc files0 {:?}", args),
            );
        }
    }
}

#[test]
fn wc_files0_from_stdin_matches_system_without_dash_entries() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0-stdin");
    let file = tmp.join("stdin-list.txt");
    fs::write(&file, b"alpha beta\n").unwrap();

    let mut list_bytes = Vec::new();
    list_bytes.extend_from_slice(file.as_os_str().as_bytes());
    list_bytes.push(0);

    assert_same_wc_exact(
        run_fro_with_stdin("wc", &["--files0-from=-"], &list_bytes),
        run_system_with_stdin("wc", &["--files0-from=-"], &list_bytes),
        "wc files0 stdin",
    );
}

#[test]
fn wc_files0_from_rejects_dash_from_stdin_list_like_system() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0-dash-reject");
    let file = tmp.join("ok.txt");
    fs::write(&file, b"abc\n").unwrap();

    let mut list_bytes = Vec::new();
    list_bytes.extend_from_slice(file.as_os_str().as_bytes());
    list_bytes.push(0);
    list_bytes.extend_from_slice(b"-\0");

    assert_same_wc_exact(
        run_fro_with_stdin("wc", &["--files0-from=-"], &list_bytes),
        run_system_with_stdin("wc", &["--files0-from=-"], &list_bytes),
        "wc files0 stdin dash reject",
    );
}

#[test]
fn wc_files0_from_mixed_missing_matches_system() {
    let tmp = unique_temp_dir("fro-coreutils-wc-files0-missing");
    let ok = tmp.join("ok.txt");
    let missing = tmp.join("missing.txt");
    let ok2 = tmp.join("ok2.txt");
    fs::write(&ok, b"a\n").unwrap();
    fs::write(&ok2, b"bb\n").unwrap();

    let list = tmp.join("inputs.list0");
    let mut list_bytes = Vec::new();
    for path in [&ok, &missing, &ok2] {
        list_bytes.extend_from_slice(path.as_os_str().as_bytes());
        list_bytes.push(0);
    }
    fs::write(&list, list_bytes).unwrap();

    assert_same_wc_exact(
        run_fro("wc", &["--files0-from", list.to_str().unwrap()]),
        run_system("wc", &["--files0-from", list.to_str().unwrap()]),
        "wc files0 mixed missing",
    );
}

#[test]
fn wc_files0_from_rejects_extra_operands_like_system() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-wc-files0-extra");
    let list = fixture.root.join("inputs.list0");
    let mut list_bytes = Vec::new();
    list_bytes.extend_from_slice(fixture.text_file.as_os_str().as_bytes());
    list_bytes.push(0);
    fs::write(&list, list_bytes).unwrap();

    assert_same_wc_exact(
        run_fro(
            "wc",
            &[
                "--files0-from",
                list.to_str().unwrap(),
                fixture.text_file.to_str().unwrap(),
            ],
        ),
        run_system(
            "wc",
            &[
                "--files0-from",
                list.to_str().unwrap(),
                fixture.text_file.to_str().unwrap(),
            ],
        ),
        "wc files0 extra operand",
    );
}

#[test]
fn wc_files0_from_missing_argument_matches_system() {
    assert_same_wc_exact(
        run_fro("wc", &["--files0-from"]),
        run_system("wc", &["--files0-from"]),
        "wc files0 missing argument",
    );
}

#[test]
fn wc_matches_system_for_bash_process_substitution() {
    let payload = "alpha beta\\ngamma delta\\n";
    let fro_output = Command::new("bash")
        .arg("-lc")
        .arg(format!(
            "{} wc <(printf '%b' '{payload}')",
            env!("CARGO_BIN_EXE_fro"),
        ))
        .output()
        .expect("failed to run bash process substitution for fro");
    let sys_output = Command::new("bash")
        .arg("-lc")
        .arg(format!("wc <(printf '%b' '{payload}')"))
        .output()
        .expect("failed to run bash process substitution for wc");
    assert_same_wc(fro_output, sys_output, "wc process substitution");
}

#[test]
fn wc_matches_system_on_binary_whitespace_boundaries() {
    let tmp = unique_temp_dir("fro-coreutils-wc-binary");
    let path = tmp.join("binary.bin");
    let parts: &[&[u8]] = &[
        b"alpha",
        &[0x0b],
        b"beta gamma",
        &[0x0c, b'\r'],
        b"delta",
        &[0x00, 0x80, b' '],
        b"epsilon",
        &[b'\n', 0x0b],
        b"zeta",
    ];
    let bytes = parts
        .iter()
        .flat_map(|part| part.iter().copied())
        .collect::<Vec<_>>();
    fs::write(&path, bytes).unwrap();

    for flags in io_flag_sets() {
        let mut args = flags.clone();
        args.push(path.to_str().unwrap());
        assert_same_wc(
            run_fro("wc", &args),
            run_system("wc", &[path.to_str().unwrap()]),
            &format!("wc binary {:?}", args),
        );
    }
}

#[test]
fn wc_byte_count_matches_system_on_sparse_regular_file() {
    let tmp = unique_temp_dir("fro-coreutils-wc-sparse");
    let path = tmp.join("sparse.bin");
    let file = fs::File::create(&path).unwrap();
    file.set_len((32_u64 << 20) + 17).unwrap();

    for flags in io_flag_sets() {
        let mut args = flags.clone();
        args.push("-c");
        args.push(path.to_str().unwrap());
        assert_same_wc(
            run_fro("wc", &args),
            run_system("wc", &["-c", path.to_str().unwrap()]),
            &format!("wc sparse {:?}", args),
        );
    }
}

#[test]
fn wc_long_bytes_flag_matches_system_on_sparse_regular_file() {
    let tmp = unique_temp_dir("fro-coreutils-wc-sparse-long");
    let path = tmp.join("sparse.bin");
    let file = fs::File::create(&path).unwrap();
    file.set_len((16_u64 << 20) + 33).unwrap();

    for flags in io_flag_sets() {
        let mut args = flags.clone();
        args.push("--bytes");
        args.push(path.to_str().unwrap());
        assert_same_wc(
            run_fro("wc", &args),
            run_system("wc", &["--bytes", path.to_str().unwrap()]),
            &format!("wc long bytes {:?}", args),
        );
    }
}

#[test]
fn wc_character_count_matches_system_on_utf8_and_invalid_bytes() {
    let tmp = unique_temp_dir("fro-coreutils-wc-chars");
    let utf8 = tmp.join("utf8.txt");
    let invalid = tmp.join("invalid.bin");
    fs::write(&utf8, "aé🙂\nβeta\n".as_bytes()).unwrap();
    fs::write(&invalid, b"\xff\x80a\n\xe2\x82").unwrap();

    for path in [&utf8, &invalid] {
        for flags in io_flag_sets() {
            for wc_flags in [vec!["-m"], vec!["-m", "-c"], vec!["-l", "-m"]] {
                let mut args = flags.clone();
                args.extend(wc_flags.iter().copied());
                args.push(path.to_str().unwrap());
                let mut sys_args = wc_flags;
                sys_args.push(path.to_str().unwrap());
                assert_same_wc(
                    run_fro("wc", &args),
                    run_system("wc", &sys_args),
                    &format!("wc chars {:?}", args),
                );
            }
        }
    }
}

#[test]
fn wc_max_line_length_matches_system_on_display_width_cases() {
    let tmp = unique_temp_dir("fro-coreutils-wc-max-line");
    let combining = tmp.join("combining.txt");
    let wide = tmp.join("wide.txt");
    let tabbed = tmp.join("tabbed.txt");
    let invalid = tmp.join("invalid.bin");
    fs::write(&combining, "e\u{0301}\n".as_bytes()).unwrap();
    fs::write(&wide, "中\n".as_bytes()).unwrap();
    fs::write(&tabbed, b"1234567\tX\n").unwrap();
    fs::write(&invalid, b"\xffa\n").unwrap();

    for path in [&combining, &wide, &tabbed, &invalid] {
        for flags in io_flag_sets() {
            for wc_flags in [vec!["-L"], vec!["--max-line-length"], vec!["-m", "-L"]] {
                let mut args = flags.clone();
                args.extend(wc_flags.iter().copied());
                args.push(path.to_str().unwrap());
                let mut sys_args = wc_flags;
                sys_args.push(path.to_str().unwrap());
                assert_same_wc(
                    run_fro("wc", &args),
                    run_system("wc", &sys_args),
                    &format!("wc max-line {:?}", args),
                );
            }
        }
    }
}
