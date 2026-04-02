use super::*;

#[test]
fn base64_encode_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-encode");
    let path = tmp.join("input.bin");
    let bytes = (0..211)
        .map(|i| ((i * 37 + 11) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec![],
            vec!["-w", "0"],
            vec!["--wrap=0"],
            vec!["-w", "12"],
            vec!["--wrap=12"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 encode {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [vec![], vec!["-w", "0"], vec!["--wrap=12"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("base64", &fro_args, &bytes),
                run_system_with_stdin("base64", &compat_flags, &bytes),
                &format!("base64 stdin encode {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn base64_decode_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-decode");
    let path = tmp.join("encoded.txt");
    let dirty_path = tmp.join("encoded-dirty.txt");
    let bytes = (0..197)
        .map(|i| ((i * 17 + 5) % 251) as u8)
        .collect::<Vec<_>>();
    let encoded = run_system_with_stdin("base64", &["-w", "16"], &bytes);
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    fs::write(&path, &encoded.stdout).unwrap();

    let dirty = b"!!"
        .iter()
        .copied()
        .chain(encoded.stdout.iter().copied())
        .chain(b"??\n".iter().copied())
        .collect::<Vec<_>>();
    fs::write(&dirty_path, &dirty).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec!["-d"], vec!["--decode"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 decode {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [vec!["-d", "-i"], vec!["--decode", "--ignore-garbage"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(dirty_path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(dirty_path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 ignore {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn base64_decode_large_wrapped_input_matches_original_bytes() {
    let tmp = unique_temp_dir("fro-coreutils-base64-large-decode");
    let raw_path = tmp.join("raw.bin");
    let encoded_path = tmp.join("wrapped.txt");
    let bytes = (0..((6 * 1024 * 1024) + 2048))
        .map(|i| ((i * 19 + 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&raw_path, &bytes).unwrap();

    let encoded = run_system("base64", &["-w", "17", raw_path.to_str().unwrap()]);
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    fs::write(&encoded_path, &encoded.stdout).unwrap();

    for io_flags in io_flag_sets() {
        let mut fro_args = io_flags.clone();
        fro_args.extend(["-d", encoded_path.to_str().unwrap()]);
        let fro = run_fro("base64", &fro_args);
        assert!(
            fro.status.success(),
            "base64 decode {:?} failed\nstdout:\n{}\nstderr:\n{}",
            io_flags,
            String::from_utf8_lossy(&fro.stdout),
            String::from_utf8_lossy(&fro.stderr),
        );
        assert_eq!(fro.stdout, bytes, "base64 decode {:?}", io_flags);
        assert!(
            fro.stderr.is_empty(),
            "unexpected stderr for {:?}: {}",
            io_flags,
            String::from_utf8_lossy(&fro.stderr)
        );
    }
}

#[test]
fn base64_rejects_extra_operands_like_system() {
    let tmp = unique_temp_dir("fro-coreutils-base64-extra");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    fs::write(&a, b"a").unwrap();
    fs::write(&b, b"b").unwrap();

    assert_same_result(
        run_fro("base64", &[a.to_str().unwrap(), b.to_str().unwrap()]),
        run_system("base64", &[a.to_str().unwrap(), b.to_str().unwrap()]),
        "base64 extra operand",
    );
}
