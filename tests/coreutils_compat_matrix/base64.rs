use super::*;

#[test]
fn cartesian_base64_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-matrix");
    let raw = tmp.join("raw.bin");
    let encoded = tmp.join("encoded.txt");
    let bytes = (0..321)
        .map(|i| ((i * 23 + 9) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&raw, &bytes).unwrap();

    let encoded_out = run_system("base64", &["-w", "20", raw.to_str().unwrap()]);
    assert!(
        encoded_out.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded_out.stderr)
    );
    fs::write(&encoded, &encoded_out.stdout).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec![], vec!["-w", "0"], vec!["--wrap=20"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(raw.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(raw.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 encode {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [vec!["-d"], vec!["--decode"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(encoded.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(encoded.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 decode {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}
