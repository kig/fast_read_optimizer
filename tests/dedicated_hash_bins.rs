use openssl::hash::{hash, MessageDigest};
use std::fs;
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

    let path = base.join(format!("{prefix}-{pid}-{nanos}"));
    fs::create_dir_all(&path).unwrap();
    path
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[test]
fn dedicated_md5sum_matches_expected_output() {
    let tmp = unique_temp_dir("dedicated-md5sum");
    let file = tmp.join("input.bin");
    let bytes = (0..(256 * 1024 + 19))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&file, &bytes).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_md5sum"))
        .arg("--no-direct")
        .arg(file.to_str().unwrap())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let expected = format!(
        "{}  {}\n",
        hex_digest(&hash(MessageDigest::md5(), &bytes).unwrap()),
        file.display()
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), expected);
}

#[test]
fn dedicated_sha256sum_matches_expected_output() {
    let tmp = unique_temp_dir("dedicated-sha256sum");
    let file = tmp.join("input.bin");
    let bytes = (0..(256 * 1024 + 37))
        .map(|i| ((i * 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&file, &bytes).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_sha256sum"))
        .arg("--no-direct")
        .arg(file.to_str().unwrap())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let expected = format!(
        "{}  {}\n",
        hex_digest(&hash(MessageDigest::sha256(), &bytes).unwrap()),
        file.display()
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), expected);
}

#[test]
fn dedicated_hash_bins_expose_help_and_version() {
    for (exe, name) in [
        (env!("CARGO_BIN_EXE_md5sum"), "md5sum"),
        (env!("CARGO_BIN_EXE_sha256sum"), "sha256sum"),
    ] {
        let help = Command::new(exe).arg("--help").output().unwrap();
        assert!(help.status.success());
        let help_text = String::from_utf8_lossy(&help.stdout);
        assert!(help_text.contains("Usage:"));
        assert!(help_text.contains("--direct"));
        assert!(help_text.contains("-h, --help"));
        assert!(help_text.contains("--version"));

        let version = Command::new(exe).arg("--version").output().unwrap();
        assert!(version.status.success());
        let version_text = String::from_utf8_lossy(&version.stdout);
        assert!(version_text.starts_with(&format!("{name} (fro dedicated) ")));
        assert!(version_text.contains(env!("CARGO_PKG_VERSION")));
    }
}
