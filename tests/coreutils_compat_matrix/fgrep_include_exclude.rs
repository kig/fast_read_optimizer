// ── fgrep --include / --exclude / --exclude-dir / --exclude-from parity ──
//
// Compares `fro fgrep` glob-filter flags against system `grep -F` on
// representative directory fixtures.  Output order may differ so most tests
// use `assert_same_sorted_lines`.
//
// Limit note: patterns are matched against the file/directory **basename**
// using POSIX `fnmatch(3)`, matching GNU grep behaviour.  Path-separator
// matching (`/` in patterns) is not supported via fnmatch without FNM_PATHNAME
// and GNU grep does not use that flag, so e.g. `--include=sub/*.rs` will NOT
// match; only basename patterns like `*.rs` are reliable.

use super::*;

// ── helpers ──────────────────────────────────────────────────────────────

fn fro_fgrep(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg("fgrep")
        .args(args)
        .output()
        .expect("failed to spawn fro fgrep")
}

fn sys_grep(args: &[&str]) -> Output {
    let mut all: Vec<&str> = vec!["-F"];
    all.extend_from_slice(args);
    system_command("grep")
        .args(&all)
        .output()
        .expect("failed to spawn grep")
}

/// Build the standard directory fixture:
///
/// ```
/// <root>/
///   src/
///     main.rs        "fn main() { // hello\n"
///     lib.rs         "// hello library\n"
///     helper.rs      "fn helper() {}\n"
///   docs/
///     readme.md      "# hello docs\n"
///     notes.txt      "just notes\n"
///   logs/
///     app.log        "hello from log\n"
///     debug.log      "debug entry\n"
///   root.rs          "// hello root\n"
///   root.md          "# root doc\n"
/// ```
fn make_fixture(root: &std::path::Path) -> String {
    let src = root.join("src");
    let docs = root.join("docs");
    let logs = root.join("logs");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&docs).unwrap();
    std::fs::create_dir_all(&logs).unwrap();

    std::fs::write(src.join("main.rs"), b"fn main() { // hello\n").unwrap();
    std::fs::write(src.join("lib.rs"), b"// hello library\n").unwrap();
    std::fs::write(src.join("helper.rs"), b"fn helper() {}\n").unwrap();
    std::fs::write(docs.join("readme.md"), b"# hello docs\n").unwrap();
    std::fs::write(docs.join("notes.txt"), b"just notes\n").unwrap();
    std::fs::write(logs.join("app.log"), b"hello from log\n").unwrap();
    std::fs::write(logs.join("debug.log"), b"debug entry\n").unwrap();
    std::fs::write(root.join("root.rs"), b"// hello root\n").unwrap();
    std::fs::write(root.join("root.md"), b"# root doc\n").unwrap();

    root.to_string_lossy().into_owned()
}

// ── --include=GLOB ────────────────────────────────────────────────────────

#[test]
fn include_rs_limits_to_rust_files() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-rs");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--include=*.rs", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.rs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--include=*.rs");
}

#[test]
fn include_md_limits_to_markdown_files() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-md");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--include=*.md", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.md", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--include=*.md");
}

#[test]
fn include_no_match_returns_exit_1() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-nomatch");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--include=*.xyz", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.xyz", "hello", &dir]);

    assert_same_result(fro, sys, "--include=*.xyz no match");
}

#[test]
fn include_multiple_patterns_union() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-multi");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--include=*.rs", "--include=*.md", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.rs", "--include=*.md", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--include=*.rs --include=*.md union");
}

// ── --exclude=GLOB ────────────────────────────────────────────────────────

#[test]
fn exclude_log_skips_log_files() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-log");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--exclude=*.log", "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude=*.log", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude=*.log");
}

#[test]
fn exclude_rs_skips_rust_files() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-rs");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--exclude=*.rs", "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude=*.rs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude=*.rs");
}

#[test]
fn exclude_multiple_patterns() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-multi");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--exclude=*.log", "--exclude=*.md", "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude=*.log", "--exclude=*.md", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude=*.log --exclude=*.md");
}

// ── --exclude-dir=GLOB ────────────────────────────────────────────────────

#[test]
fn exclude_dir_logs_skips_logs_subtree() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-dir-logs");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&["-r", "--exclude-dir=logs", "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude-dir=logs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude-dir=logs");
}

#[test]
fn exclude_dir_multiple_patterns() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-dir-multi");
    let dir = make_fixture(&fixture.root);

    let fro = fro_fgrep(&[
        "-r",
        "--exclude-dir=logs",
        "--exclude-dir=docs",
        "hello",
        &dir,
    ]);
    let sys = sys_grep(&[
        "-r",
        "--exclude-dir=logs",
        "--exclude-dir=docs",
        "hello",
        &dir,
    ]);

    assert_same_sorted_lines(fro, sys, "--exclude-dir=logs --exclude-dir=docs");
}

#[test]
fn exclude_dir_glob_wildcard() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-dir-glob");
    let dir = make_fixture(&fixture.root);

    // Exclude all directories starting with 's'
    let fro = fro_fgrep(&["-r", "--exclude-dir=s*", "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude-dir=s*", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude-dir=s*");
}

// ── --exclude-from=FILE ───────────────────────────────────────────────────

#[test]
fn exclude_from_file_skips_matching_files() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-from");
    let dir = make_fixture(&fixture.root);

    // Write a patterns file: exclude *.log and *.md
    let patterns_file = fixture.root.join("exclude.patterns");
    std::fs::write(&patterns_file, b"*.log\n*.md\n").unwrap();

    let pf = patterns_file.to_string_lossy().into_owned();
    let fro = fro_fgrep(&["-r", "--exclude-from", &pf, "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude-from", &pf, "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude-from with *.log and *.md");
}

#[test]
fn exclude_from_equals_syntax() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-from-eq");
    let dir = make_fixture(&fixture.root);

    let patterns_file = fixture.root.join("excl.patterns");
    std::fs::write(&patterns_file, b"*.rs\n").unwrap();

    let pf = patterns_file.to_string_lossy().into_owned();
    let arg = format!("--exclude-from={pf}");
    let fro = fro_fgrep(&["-r", &arg, "hello", &dir]);
    let sys = sys_grep(&["-r", &arg, "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude-from=FILE equals syntax");
}

#[test]
fn exclude_from_ignores_empty_lines() {
    let fixture = CoreutilsParityFixture::new("fgrep-exclude-from-empty");
    let dir = make_fixture(&fixture.root);

    // File with blank lines and comments (blank lines should be ignored)
    let patterns_file = fixture.root.join("blank.patterns");
    std::fs::write(&patterns_file, b"\n*.log\n\n").unwrap();

    let pf = patterns_file.to_string_lossy().into_owned();
    let fro = fro_fgrep(&["-r", "--exclude-from", &pf, "hello", &dir]);
    let sys = sys_grep(&["-r", "--exclude-from", &pf, "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--exclude-from blank lines ignored");
}

// ── combined flags ────────────────────────────────────────────────────────

#[test]
fn include_and_exclude_combined() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-exclude-combo");
    let dir = make_fixture(&fixture.root);

    // Include only *.rs but exclude helper.rs
    let fro = fro_fgrep(&["-r", "--include=*.rs", "--exclude=helper.rs", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.rs", "--exclude=helper.rs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--include=*.rs --exclude=helper.rs");
}

#[test]
fn include_exclude_dir_combined() {
    let fixture = CoreutilsParityFixture::new("fgrep-include-excl-dir");
    let dir = make_fixture(&fixture.root);

    // Search *.rs but skip src/ directory
    let fro = fro_fgrep(&["-r", "--include=*.rs", "--exclude-dir=src", "hello", &dir]);
    let sys = sys_grep(&["-r", "--include=*.rs", "--exclude-dir=src", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--include=*.rs --exclude-dir=src");
}

// ── symlink fixture interactions ──────────────────────────────────────────
//
// These tests use `unique_temp_dir` directly (not `CoreutilsParityFixture`)
// because `CoreutilsParityFixture` creates a broken symlink in the root dir.
// Under `-R` (dereference mode) fro calls `fs::metadata()` on each entry and
// the broken symlink causes an error (exit 2), while `grep` prints a warning
// but still exits 0 when matches were found.  A clean tree avoids the mismatch.

#[test]
fn include_with_deref_recursive() {
    let root = unique_temp_dir("fgrep-include-deref");
    let dir = make_fixture(&root);

    let fro = fro_fgrep(&["-R", "--include=*.rs", "hello", &dir]);
    let sys = sys_grep(&["-R", "--include=*.rs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-R --include=*.rs");
}

#[test]
fn exclude_dir_with_deref_recursive() {
    let root = unique_temp_dir("fgrep-excl-dir-deref");
    let dir = make_fixture(&root);

    let fro = fro_fgrep(&["-R", "--exclude-dir=logs", "hello", &dir]);
    let sys = sys_grep(&["-R", "--exclude-dir=logs", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-R --exclude-dir=logs");
}
