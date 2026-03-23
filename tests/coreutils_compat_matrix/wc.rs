use super::*;

#[test]
fn cartesian_wc_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-wc-matrix");
    let path = tmp.join("text.txt");
    fs::write(&path, b"one two\nthree four\n").unwrap();

    for flags in io_flag_sets() {
        for wc_flags in [
            vec![],
            vec!["-l"],
            vec!["-w"],
            vec!["-c"],
            vec!["-l", "-w"],
            vec!["-l", "-c"],
            vec!["-w", "-c"],
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
