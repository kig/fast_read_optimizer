#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

static FIFO_TEST_LOCK: Mutex<()> = Mutex::new(());

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

fn run_fro_with_stdin(command: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn fro command");
    child
        .stdin
        .take()
        .expect("missing fro stdin")
        .write_all(stdin_bytes)
        .expect("failed to write fro stdin");
    child
        .wait_with_output()
        .expect("failed to collect fro output")
}

fn run_system_with_stdin(program: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn {program}: {err}"));
    child
        .stdin
        .take()
        .expect("missing system stdin")
        .write_all(stdin_bytes)
        .expect("failed to write system stdin");
    child
        .wait_with_output()
        .unwrap_or_else(|err| panic!("failed to collect {program} output: {err}"))
}

fn make_fifo(path: &Path) {
    let fifo = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let rc = unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
}

fn with_fifo_input<F, T>(path: &Path, data: &[u8], run: F) -> T
where
    F: FnOnce(&str) -> T,
{
    make_fifo(path);
    let fifo_path = path.to_string_lossy().into_owned();
    let data = data.to_vec();
    let writer = thread::spawn({
        let fifo_path = fifo_path.clone();
        move || {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&fifo_path)
                .expect("failed to open fifo for writing");
            file.write_all(&data).expect("failed to write fifo data");
        }
    });
    let output = run(&fifo_path);
    writer.join().expect("fifo writer panicked");
    fs::remove_file(path).unwrap();
    output
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

fn assert_same_sorted_lines(fro: Output, system: Output, label: &str) {
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
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
    assert_eq!(fro_lines, sys_lines, "{label}: sorted stdout mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
}

fn snapshot_tree(root: &Path) -> Vec<(String, u8, Vec<u8>)> {
    fn walk(root: &Path, rel: &Path, out: &mut Vec<(String, u8, Vec<u8>)>) {
        let dir = if rel.as_os_str().is_empty() {
            root.to_path_buf()
        } else {
            root.join(rel)
        };
        let mut entries = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name();
            let rel_path = if rel.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                rel.join(&name)
            };
            let metadata = fs::symlink_metadata(entry.path()).unwrap();
            let rel_string = rel_path.to_string_lossy().into_owned();
            if metadata.file_type().is_dir() {
                out.push((rel_string.clone(), 0, Vec::new()));
                walk(root, &rel_path, out);
            } else if metadata.file_type().is_symlink() {
                out.push((
                    rel_string,
                    2,
                    fs::read_link(entry.path())
                        .unwrap()
                        .as_os_str()
                        .as_bytes()
                        .to_vec(),
                ));
            } else {
                out.push((rel_string, 1, fs::read(entry.path()).unwrap()));
            }
        }
    }

    let mut out = Vec::new();
    walk(root, Path::new(""), &mut out);
    out
}

fn io_flag_sets() -> Vec<Vec<&'static str>> {
    vec![vec![], vec!["--no-direct"], vec!["--direct"]]
}

fn wc_flag_sets() -> Vec<Vec<&'static str>> {
    vec![
        vec![],
        vec!["-l"],
        vec!["-w"],
        vec!["-c"],
        vec!["-l", "-w", "-c"],
    ]
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
fn cartesian_stream_coreutils_match_system_for_input_kinds() {
    let tmp = unique_temp_dir("fro-coreutils-stream-matrix");
    let text = b"alpha\nneedle beta\nomega\n".to_vec();
    let binary = (0..65557)
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();

    let text_path = tmp.join("text.txt");
    let text_symlink = tmp.join("text-link.txt");
    let binary_path = tmp.join("binary.bin");
    let binary_symlink = tmp.join("binary-link.bin");
    fs::write(&text_path, &text).unwrap();
    fs::write(&binary_path, &binary).unwrap();
    symlink(&text_path, &text_symlink).unwrap();
    symlink(&binary_path, &binary_symlink).unwrap();

    for flags in io_flag_sets() {
        for path in [&text_path, &text_symlink] {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cat", &args),
                run_system("cat", &[file]),
                &format!("cat path {:?}", args),
            );
            assert_same_result(
                run_fro("tac", &args),
                run_system("tac", &[file]),
                &format!("tac path {:?}", args),
            );
            for wc_flags in wc_flag_sets() {
                let mut fro_args = flags.clone();
                fro_args.extend(wc_flags.iter().copied());
                fro_args.push(file);
                let mut sys_args = wc_flags;
                sys_args.push(file);
                assert_same_wc(
                    run_fro("wc", &fro_args),
                    run_system("wc", &sys_args),
                    &format!("wc path {:?}", fro_args),
                );
            }
            for grep_args in [vec!["needle", file], vec!["-n", "needle", file]] {
                let mut fro_args = flags.clone();
                fro_args.extend(grep_args.iter().copied());
                let mut sys_args = vec!["-F"];
                sys_args.extend(grep_args.iter().copied());
                assert_same_result(
                    run_fro("fgrep", &fro_args),
                    run_system("grep", &sys_args),
                    &format!("fgrep path {:?}", fro_args),
                );
            }
        }

        for path in [&binary_path, &binary_symlink] {
            let file = path.to_str().unwrap();
            let mut args = flags.clone();
            args.push(file);
            assert_same_result(
                run_fro("cksum", &args),
                run_system("cksum", &[file]),
                &format!("cksum path {:?}", args),
            );
            assert_same_result(
                run_fro("sha256sum", &args),
                run_system("sha256sum", &[file]),
                &format!("sha256sum path {:?}", args),
            );
        }
    }

    assert_same_result(
        run_fro_with_stdin("cat", &[], &text),
        run_system_with_stdin("cat", &[], &text),
        "cat stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("cat", &["-"], &text),
        run_system_with_stdin("cat", &["-"], &text),
        "cat dash",
    );
    assert_same_result(
        run_fro_with_stdin("tac", &[], &text),
        run_system_with_stdin("tac", &[], &text),
        "tac stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("tac", &["-"], &text),
        run_system_with_stdin("tac", &["-"], &text),
        "tac dash",
    );

    for wc_flags in wc_flag_sets() {
        assert_same_wc(
            run_fro_with_stdin("wc", &wc_flags, &text),
            run_system_with_stdin("wc", &wc_flags, &text),
            &format!("wc stdin {:?}", wc_flags),
        );
        let mut dash_args = wc_flags.clone();
        dash_args.push("-");
        assert_same_wc(
            run_fro_with_stdin("wc", &dash_args, &text),
            run_system_with_stdin("wc", &dash_args, &text),
            &format!("wc dash {:?}", dash_args),
        );
    }

    for grep_args in [vec!["needle"], vec!["-n", "needle"]] {
        let mut sys_args = vec!["-F"];
        sys_args.extend(grep_args.iter().copied());
        assert_same_result(
            run_fro_with_stdin("fgrep", &grep_args, &text),
            run_system_with_stdin("grep", &sys_args, &text),
            &format!("fgrep stdin {:?}", grep_args),
        );

        let mut dash_args = grep_args.clone();
        dash_args.push("-");
        let mut sys_dash_args = vec!["-F"];
        sys_dash_args.extend(grep_args.iter().copied());
        sys_dash_args.push("-");
        assert_same_result(
            run_fro_with_stdin("fgrep", &dash_args, &text),
            run_system_with_stdin("grep", &sys_dash_args, &text),
            &format!("fgrep dash {:?}", dash_args),
        );
    }

    assert_same_result(
        run_fro_with_stdin("cksum", &[], &binary),
        run_system_with_stdin("cksum", &[], &binary),
        "cksum stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("cksum", &["-"], &binary),
        run_system_with_stdin("cksum", &["-"], &binary),
        "cksum dash",
    );
    assert_same_result(
        run_fro_with_stdin("sha256sum", &[], &binary),
        run_system_with_stdin("sha256sum", &[], &binary),
        "sha256sum stdin []",
    );
    assert_same_result(
        run_fro_with_stdin("sha256sum", &["-"], &binary),
        run_system_with_stdin("sha256sum", &["-"], &binary),
        "sha256sum dash",
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
fn fifo_text_inputs_match_system_output() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let tmp = unique_temp_dir("fro-coreutils-fifo-matrix");
    let text = b"alpha\nneedle beta\nomega\n".to_vec();
    let text_fifo = tmp.join("text-default.fifo");

    let fro = with_fifo_input(&text_fifo, &text, |fifo_path| run_fro("cat", &[fifo_path]));
    let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
        run_system("cat", &[fifo_path])
    });
    assert_same_result(fro, sys, "cat fifo");

    let fro = with_fifo_input(&text_fifo, &text, |fifo_path| run_fro("tac", &[fifo_path]));
    let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
        run_system("tac", &[fifo_path])
    });
    assert_same_result(fro, sys, "tac fifo");

    for wc_flags in wc_flag_sets() {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = wc_flags.clone();
            args.push(fifo_path);
            run_fro("wc", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = wc_flags.clone();
            args.push(fifo_path);
            run_system("wc", &args)
        });
        let mut label_args = wc_flags.clone();
        label_args.push("FIFO");
        assert_same_wc(fro, sys, &format!("wc fifo {:?}", label_args));
    }

    for grep_prefix in [vec!["needle"], vec!["-n", "needle"]] {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = grep_prefix.clone();
            args.push(fifo_path);
            run_fro("fgrep", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut grep_args = grep_prefix.clone();
            grep_args.push(fifo_path);
            let mut args = vec!["-F"];
            args.extend(grep_args.iter().copied());
            run_system("grep", &args)
        });
        let mut label_args = grep_prefix.clone();
        label_args.push("FIFO");
        assert_same_result(fro, sys, &format!("fgrep fifo {:?}", label_args));
    }
}

#[test]
fn fifo_hash_inputs_match_system_output() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let tmp = unique_temp_dir("fro-coreutils-fifo-hash");
    let binary = (0..65557)
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    let binary_fifo = tmp.join("binary-default.fifo");

    let fro = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_fro("cksum", &[fifo_path])
    });
    let sys = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_system("cksum", &[fifo_path])
    });
    assert_same_result(fro, sys, "cksum fifo");

    let fro = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_fro("sha256sum", &[fifo_path])
    });
    let sys = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_system("sha256sum", &[fifo_path])
    });
    assert_same_result(fro, sys, "sha256sum fifo");
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
        ("cksum", "cksum"),
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
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
fn digest_family_format_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-flags");
    let path = tmp.join("hash file.txt");
    fs::write(
        &path,
        (0..65599)
            .map(|i| ((i * 11 + 3) % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        for compat_flags in [
            vec!["-b"],
            vec!["-t"],
            vec!["--tag"],
            vec!["-z"],
            vec!["-b", "-z"],
        ] {
            for io_flags in io_flag_sets() {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.push(path.to_str().unwrap());
                let mut sys_args = compat_flags.clone();
                sys_args.push(path.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &fro_args),
                    run_system(system_name, &sys_args),
                    &format!("{name} {:?} {:?}", io_flags, compat_flags),
                );
            }
        }
    }
}

#[test]
fn digest_family_check_flag_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    fs::write(
        &path_ok,
        (0..8193).map(|i| ((i * 19 + 7) % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..8193).map(|i| ((i * 23 + 5) % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-ok.txt"));
        let manifest_bad = tmp.join(format!("{name}-bad.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(
            ok_manifest.status.success(),
            "{}",
            String::from_utf8_lossy(&ok_manifest.stderr)
        );
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let mut bad_manifest = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        bad_manifest = bad_manifest.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for manifest in [manifest_ok.as_path(), manifest_bad.as_path()] {
            let args = ["-c", manifest.to_str().unwrap()];
            assert_same_result(
                run_fro(name, &args),
                run_system(system_name, &args),
                &format!("{name} check {:?}", manifest),
            );
        }
    }
}

#[test]
fn digest_family_check_quiet_and_status_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-digest-check-flags");
    let path_ok = tmp.join("ok.bin");
    let path_bad = tmp.join("bad.bin");
    fs::write(
        &path_ok,
        (0..4097).map(|i| ((i * 29 + 7) % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();
    fs::write(
        &path_bad,
        (0..4097).map(|i| ((i * 31 + 9) % 251) as u8).collect::<Vec<_>>(),
    )
    .unwrap();

    for (name, system_name) in [
        ("sha224sum", "sha224sum"),
        ("sha256sum", "sha256sum"),
        ("sha384sum", "sha384sum"),
        ("sha512sum", "sha512sum"),
        ("md5sum", "md5sum"),
        ("b2sum", "b2sum"),
    ] {
        let manifest_ok = tmp.join(format!("{name}-ok-flags.txt"));
        let manifest_bad = tmp.join(format!("{name}-bad-flags.txt"));
        let ok_manifest = run_system(system_name, &[path_ok.to_str().unwrap()]);
        assert!(ok_manifest.status.success());
        fs::write(&manifest_ok, &ok_manifest.stdout).unwrap();

        let mut bad_manifest = String::from_utf8(ok_manifest.stdout.clone()).unwrap();
        bad_manifest = bad_manifest.replace(path_ok.to_str().unwrap(), path_bad.to_str().unwrap());
        fs::write(&manifest_bad, bad_manifest).unwrap();

        for extra_flags in [vec!["--quiet", "-c"], vec!["--status", "-c"]] {
            for manifest in [manifest_ok.as_path(), manifest_bad.as_path()] {
                let mut args = extra_flags.clone();
                args.push(manifest.to_str().unwrap());
                assert_same_result(
                    run_fro(name, &args),
                    run_system(system_name, &args),
                    &format!("{name} {:?} {:?}", extra_flags, manifest),
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

        for quiet_flags in [vec!["-s"], vec!["--quiet"], vec!["--silent"]] {
            for files in [
                vec![equal_a.to_str().unwrap(), equal_b.to_str().unwrap()],
                vec![diff_a.to_str().unwrap(), diff_b.to_str().unwrap()],
                vec![eof_a.to_str().unwrap(), eof_b.to_str().unwrap()],
            ] {
                let mut fro_args = flags.clone();
                fro_args.extend(quiet_flags.iter().copied());
                fro_args.extend(files.iter().copied());
                let mut sys_args = quiet_flags.clone();
                sys_args.extend(files.iter().copied());
                assert_same_result(
                    run_fro("cmp", &fro_args),
                    run_system("cmp", &sys_args),
                    &format!("cmp {:?} {:?}", flags, quiet_flags),
                );
            }
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

#[test]
fn cartesian_cp_recursive_matches_system_side_effects() {
    let tmp = unique_temp_dir("fro-coreutils-cp-recursive-matrix");

    for flags in io_flag_sets() {
        let source_root = tmp.join(format!("cp-tree-src-{}", flags.join("_")));
        let fro_dest_parent = tmp.join(format!("cp-tree-fro-{}", flags.join("_")));
        let sys_dest_parent = tmp.join(format!("cp-tree-sys-{}", flags.join("_")));
        let nested = source_root.join("nested/deeper");
        fs::create_dir_all(&nested).unwrap();
        fs::write(source_root.join("small.txt"), b"alpha\nbeta\n").unwrap();
        fs::write(
            nested.join("large.bin"),
            (0..(2 * 1024 * 1024 + 333))
                .map(|i| ((i * 13) % 251) as u8)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        symlink("../small.txt", source_root.join("nested/link-small")).unwrap();
        fs::create_dir_all(&fro_dest_parent).unwrap();
        fs::create_dir_all(&sys_dest_parent).unwrap();

        let mut fro_args = flags.clone();
        fro_args.push("-r");
        fro_args.push(source_root.to_str().unwrap());
        fro_args.push(fro_dest_parent.to_str().unwrap());
        let sys_args = [
            "-r",
            source_root.to_str().unwrap(),
            sys_dest_parent.to_str().unwrap(),
        ];
        assert_same_result(
            run_fro("cp", &fro_args),
            run_system("cp", &sys_args),
            &format!("cp recursive {:?}", fro_args),
        );

        let copied_name = source_root.file_name().unwrap();
        let fro_tree = snapshot_tree(&fro_dest_parent.join(copied_name));
        let sys_tree = snapshot_tree(&sys_dest_parent.join(copied_name));
        assert_eq!(
            fro_tree, sys_tree,
            "recursive tree mismatch for {:?}",
            fro_args
        );
    }
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

    assert_eq!(fro.status.code(), system.status.code(), "du status mismatch");
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
