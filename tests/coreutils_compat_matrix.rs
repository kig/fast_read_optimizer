#![cfg(unix)]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn run_fro(command: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .output()
        .expect("failed to run fro subcommand")
}

fn run_system(program: &str, args: &[&str]) -> Output {
    Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"))
}

fn assert_same_result(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
    assert_eq!(
        fro.stdout, system.stdout,
        "{label}: stdout mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
    assert_eq!(
        fro.stderr, system.stderr,
        "{label}: stderr mismatch\nfro stdout:\n{}\nfro stderr:\n{}\nsys stdout:\n{}\nsys stderr:\n{}",
        String::from_utf8_lossy(&fro.stdout),
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stdout),
        String::from_utf8_lossy(&system.stderr),
    );
}

fn assert_same_wc(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: wc status mismatch"
    );
    let fro_tokens = String::from_utf8_lossy(&fro.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let sys_tokens = String::from_utf8_lossy(&system.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(fro_tokens, sys_tokens, "{label}: wc token mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: wc stderr mismatch");
}

fn io_flag_sets() -> Vec<Vec<&'static str>> {
    vec![vec![], vec!["--no-direct"], vec!["--direct"]]
}

#[test]
fn cartesian_cat_and_tac_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-matrix");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"alpha\nbeta\n").unwrap();
    fs::write(&b, b"uno\ndos\n").unwrap();

    for flags in io_flag_sets() {
        for files in [
            vec![a.to_str().unwrap()],
            vec![a.to_str().unwrap(), b.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(files.iter().copied());
            assert_same_result(
                run_fro("cat", &args),
                run_system("cat", &files),
                &format!("cat {:?}", args),
            );
            assert_same_result(
                run_fro("tac", &args),
                run_system("tac", &files),
                &format!("tac {:?}", args),
            );
        }
    }
}

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
fn cartesian_hash_tools_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-hash-matrix");
    let path_a = tmp.join("a.bin");
    let path_b = tmp.join("b.bin");
    fs::write(
        &path_a,
        (0..65599).map(|i| (i % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_b,
        (0..32791)
            .map(|i| ((i * 7) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sum", "sum"),
        ("cksum", "cksum"),
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
    ] {
        for flags in io_flag_sets() {
            for files in [
                vec![path_a.to_str().unwrap()],
                vec![path_a.to_str().unwrap(), path_b.to_str().unwrap()],
            ] {
                let mut args = flags.clone();
                args.extend(files.iter().copied());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &files),
                    &format!("{name} {:?}", args),
                );
            }
        }
    }
}

#[test]
fn cartesian_cmp_and_fgrep_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cmp-grep-matrix");
    let equal_a = tmp.join("equal-a.txt");
    let equal_b = tmp.join("equal-b.txt");
    let diff_a = tmp.join("diff-a.txt");
    let diff_b = tmp.join("diff-b.txt");
    let eof_a = tmp.join("eof-a.txt");
    let eof_b = tmp.join("eof-b.txt");
    let grep_a = tmp.join("grep-a.txt");
    let grep_b = tmp.join("grep-b.txt");

    fs::write(&equal_a, b"same\nbytes\n").unwrap();
    fs::write(&equal_b, b"same\nbytes\n").unwrap();
    fs::write(&diff_a, b"same\nbytes\n").unwrap();
    fs::write(&diff_b, b"same\nbytex\n").unwrap();
    fs::write(&eof_a, b"short\n").unwrap();
    fs::write(&eof_b, b"short\nextra\n").unwrap();
    fs::write(&grep_a, b"alpha\nneedle beta\nomega\n").unwrap();
    fs::write(&grep_b, b"needle gamma\nzeta\n").unwrap();

    for flags in io_flag_sets() {
        for files in [
            vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
            vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
            vec![eof_a.to_str().unwrap(), eof_b.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(files.iter().copied());
            assert_same_result(
                run_fro("cmp", &args),
                run_system("cmp", &files),
                &format!("cmp {:?}", args),
            );
        }

        for grep_args in [
            vec!["needle", grep_a.to_str().unwrap()],
            vec!["-n", "needle", grep_a.to_str().unwrap()],
            vec!["needle", grep_a.to_str().unwrap(), grep_b.to_str().unwrap()],
            vec!["missing", grep_a.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(grep_args.iter().copied());
            let system_program = "grep";
            let mut system_args = vec!["-F"];
            system_args.extend(grep_args.iter().copied());
            assert_same_result(
                run_fro("fgrep", &args),
                run_system(system_program, &system_args),
                &format!("fgrep {:?}", args),
            );
        }
    }
}

#[test]
fn cartesian_cp_and_shred_match_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-copy-shred-matrix");

    for flags in io_flag_sets() {
        let source = tmp.join(format!("cp-src-{}.bin", flags.join("_")));
        let fro_target = tmp.join(format!("cp-fro-{}.bin", flags.join("_")));
        let sys_target = tmp.join(format!("cp-sys-{}.bin", flags.join("_")));
        fs::write(
            &source,
            (0..131072)
                .map(|i| ((i * 5) % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let mut fro_args = flags.clone();
        fro_args.push(source.to_str().unwrap());
        fro_args.push(fro_target.to_str().unwrap());
        let sys_args = [source.to_str().unwrap(), sys_target.to_str().unwrap()];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp {:?}", fro_args),
        );
        assert_eq!(
            fs::read(&fro_target).unwrap(),
            fs::read(&sys_target).unwrap()
        );

        let fro_zero = tmp.join(format!("shred-fro-zero-{}.bin", flags.join("_")));
        let sys_zero = tmp.join(format!("shred-sys-zero-{}.bin", flags.join("_")));
        fs::write(&fro_zero, vec![0x55; 32768]).unwrap();
        fs::write(&sys_zero, vec![0x55; 32768]).unwrap();
        let mut fro_zero_args = flags.clone();
        fro_zero_args.extend(["-n", "0", "-z", fro_zero.to_str().unwrap()]);
        let sys_zero_args = ["-n", "0", "-z", sys_zero.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_zero_args),
            run_system("shred", &sys_zero_args),
            &format!("shred zero {:?}", fro_zero_args),
        );
        assert_eq!(fs::read(&fro_zero).unwrap(), fs::read(&sys_zero).unwrap());

        let fro_remove = tmp.join(format!("shred-fro-remove-{}.bin", flags.join("_")));
        let sys_remove = tmp.join(format!("shred-sys-remove-{}.bin", flags.join("_")));
        fs::write(&fro_remove, vec![0x99; 32768]).unwrap();
        fs::write(&sys_remove, vec![0x99; 32768]).unwrap();
        let mut fro_remove_args = flags;
        fro_remove_args.extend(["-n", "0", "-u", fro_remove.to_str().unwrap()]);
        let sys_remove_args = ["-n", "0", "-u", sys_remove.to_str().unwrap()];
        assert_same_result(
            run_fro("shred", &fro_remove_args),
            run_system("shred", &sys_remove_args),
            &format!("shred remove {:?}", fro_remove_args),
        );
        assert_eq!(fro_remove.exists(), sys_remove.exists());
    }
}
