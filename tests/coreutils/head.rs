use super::*;

#[test]
fn head_help_mentions_negative_counts_and_headers() {
    let output = run_fro("head", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("all but the last"));
    assert!(stdout.contains("--lines"));
    assert!(stdout.contains("--bytes"));
    assert!(stdout.contains("--quiet"));
    assert!(stdout.contains("--verbose"));
}
