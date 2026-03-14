use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::process::Command;

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();

    let p = base.join(format!("{}-{}-{}", prefix, pid, nanos));
    fs::create_dir_all(&p).unwrap();
    p
}

fn run_fro(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro")
}

fn sidecar_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    path.with_extension(format!("bin.fro-hash.{}.json", suffix))
}

#[test]
fn hash_verify_recover_round_trip() {
    let tmp = unique_temp_dir("fro-hash-cli");
    let target = tmp.join("target.bin");
    let backup = tmp.join("backup.bin");

    let block = 1024 * 1024;
    let data = (0..(block * 3 + 1234))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&target, &data).unwrap();
    fs::write(&backup, &data).unwrap();

    let out = run_fro(&["hash", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run_fro(&["hash", "--no-direct", "-n", "1", backup.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&target, suffix).exists());
        assert!(sidecar_path(&backup, suffix).exists());
    }

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));

    let mut file = fs::OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(block as u64 + 7)).unwrap();
    file.write_all(&[0xAA, 0xBB, 0xCC, 0xDD]).unwrap();
    file.flush().unwrap();

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(
        !out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=1"));

    let out = run_fro(&[
        "recover",
        "--no-direct",
        "-n",
        "1",
        target.to_str().unwrap(),
        backup.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("repaired_blocks=1"));

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));
}

#[test]
fn recover_fast_uses_target_only_clean_path() {
    let tmp = unique_temp_dir("fro-hash-fast");
    let target = tmp.join("target.bin");
    let backup = tmp.join("backup.bin");

    let block = 1024 * 1024;
    let data = (0..(block * 2 + 321))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&target, &data).unwrap();
    fs::write(&backup, &data).unwrap();

    assert!(run_fro(&["hash", "--no-direct", "-n", "1", target.to_str().unwrap()])
        .status
        .success());
    assert!(run_fro(&["hash", "--no-direct", "-n", "1", backup.to_str().unwrap()])
        .status
        .success());

    let mut backup_file = fs::OpenOptions::new().write(true).open(&backup).unwrap();
    backup_file.seek(SeekFrom::Start(block as u64 + 3)).unwrap();
    backup_file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
    backup_file.flush().unwrap();

    let out = run_fro(&[
        "recover",
        "--fast",
        "--no-direct",
        "-n",
        "1",
        target.to_str().unwrap(),
        backup.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("used_fast_path=true"));
    assert!(stdout.contains("fell_back_to_full_scan=false"));
    assert!(stdout.contains("repaired_blocks=0"));
}

#[test]
fn recover_in_place_all_repairs_all_inputs_and_restores_sidecars() {
    let tmp = unique_temp_dir("fro-hash-in-place-all");
    let target = tmp.join("target.bin");
    let backup = tmp.join("backup.bin");

    let block = 1024 * 1024;
    let data = (0..(block * 3 + 123))
        .map(|i| ((i * 37) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&target, &data).unwrap();
    fs::write(&backup, &data).unwrap();

    assert!(run_fro(&["hash", "--no-direct", "-n", "1", target.to_str().unwrap()])
        .status
        .success());
    assert!(run_fro(&["hash", "--no-direct", "-n", "1", backup.to_str().unwrap()])
        .status
        .success());

    let mut target_file = fs::OpenOptions::new().write(true).open(&target).unwrap();
    target_file.seek(SeekFrom::Start(7)).unwrap();
    target_file.write_all(&[0x11, 0x22, 0x33, 0x44]).unwrap();
    target_file.flush().unwrap();

    let mut backup_file = fs::OpenOptions::new().write(true).open(&backup).unwrap();
    backup_file.seek(SeekFrom::Start(block as u64 + 9)).unwrap();
    backup_file.write_all(&[0xAA, 0xBB, 0xCC, 0xDD]).unwrap();
    backup_file.flush().unwrap();

    fs::remove_file(sidecar_path(&target, "1")).unwrap();
    fs::remove_file(sidecar_path(&backup, "2")).unwrap();

    let out = run_fro(&[
        "recover",
        "--in-place-all",
        "--no-direct",
        "-n",
        "1",
        target.to_str().unwrap(),
        backup.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("repaired_blocks=2"));
    assert!(stdout.contains("repaired_files=2"));
    assert!(stdout.contains("sidecars_refreshed=2"));

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(out.status.success(), "target verify failed: {}", String::from_utf8_lossy(&out.stdout));
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));

    let out = run_fro(&["verify", "--no-direct", "-n", "1", backup.to_str().unwrap()]);
    assert!(out.status.success(), "backup verify failed: {}", String::from_utf8_lossy(&out.stdout));
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));

    for suffix in ["0", "1", "2"] {
        assert!(sidecar_path(&target, suffix).exists());
        assert!(sidecar_path(&backup, suffix).exists());
    }
}
