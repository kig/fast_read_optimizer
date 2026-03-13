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
    assert!(out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let out = run_fro(&["hash", "--no-direct", "-n", "1", backup.to_str().unwrap()]);
    assert!(out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));

    for suffix in ["0", "1", "2"] {
        assert!(target.with_extension(format!("bin.fro-hash.{}.json", suffix)).exists());
        assert!(backup.with_extension(format!("bin.fro-hash.{}.json", suffix)).exists());
    }

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));

    let mut file = fs::OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(block as u64 + 7)).unwrap();
    file.write_all(&[0xAA, 0xBB, 0xCC, 0xDD]).unwrap();
    file.flush().unwrap();

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(!out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=1"));

    let out = run_fro(&["recover", "--no-direct", "-n", "1", target.to_str().unwrap(), backup.to_str().unwrap()]);
    assert!(out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("repaired_blocks=1"));

    let out = run_fro(&["verify", "--no-direct", "-n", "1", target.to_str().unwrap()]);
    assert!(out.status.success(), "stdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("bad_blocks=0"));
}
