// ── fgrep --directories=recurse / -r parity tests ───────────────────────
//
// Compares `fro fgrep -r` (and `--directories=recurse`) against system
// `grep -F -r` on representative directory fixtures.  Output order may
// differ between implementations so most tests use `assert_same_sorted_lines`.

use super::*;

// ── helpers ──────────────────────────────────────────────────────────────

/// Run `fro fgrep <args>` and collect stdout/stderr/status.
fn fro_fgrep(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg("fgrep")
        .args(args)
        .output()
        .expect("failed to spawn fro fgrep")
}

/// Run system `grep -F <args>` and collect stdout/stderr/status.
fn sys_grep(args: &[&str]) -> Output {
    let mut all: Vec<&str> = vec!["-F"];
    all.extend_from_slice(args);
    system_command("grep")
        .args(&all)
        .output()
        .expect("failed to spawn grep")
}

/// Build a small deterministic directory tree under the fixture root:
///
/// ```
/// <prefix>/
///   a.txt          "hello world\n"
///   b.txt          "no match here\n"
///   sub/
///     c.txt        "hello from sub\n"
///     d.txt        "unrelated\n"
///     deep/
///       e.txt      "hello deep\n"
/// ```
///
/// Returns the path to the root directory as a String.
fn make_tree(root: &std::path::Path) -> String {
    std::fs::write(root.join("a.txt"), b"hello world\n").unwrap();
    std::fs::write(root.join("b.txt"), b"no match here\n").unwrap();
    let sub = root.join("sub");
    std::fs::create_dir_all(sub.join("deep")).unwrap();
    std::fs::write(sub.join("c.txt"), b"hello from sub\n").unwrap();
    std::fs::write(sub.join("d.txt"), b"unrelated\n").unwrap();
    std::fs::write(sub.join("deep").join("e.txt"), b"hello deep\n").unwrap();
    root.to_string_lossy().into_owned()
}

// ── basic recursion ───────────────────────────────────────────────────────

#[test]
fn recurse_long_flag_matches_files_in_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-long");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["--directories=recurse", "hello", &dir]);
    let sys = sys_grep(&["-r", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "--directories=recurse basic");
}

#[test]
fn recurse_short_flag_r_matches_files_in_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-r");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "hello", &dir]);
    let sys = sys_grep(&["-r", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r basic");
}

#[test]
fn recurse_short_flag_d_recurse_matches_files_in_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-d");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-d", "recurse", "hello", &dir]);
    let sys = sys_grep(&["-d", "recurse", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-d recurse basic");
}

#[test]
fn recurse_no_match_returns_exit_1() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-nomatch");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "XYZZY_no_match", &dir]);
    let sys = sys_grep(&["-r", "XYZZY_no_match", &dir]);

    assert_same_result(fro, sys, "-r no match exit code");
}

#[test]
fn recurse_empty_directory_returns_exit_1() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-emptydir");
    let empty = fixture.empty_dir.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "needle", &empty]);
    let sys = sys_grep(&["-r", "needle", &empty]);

    assert_same_result(fro, sys, "-r empty dir");
}

// ── flag interactions ─────────────────────────────────────────────────────

#[test]
fn recurse_with_line_numbers() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-linenum");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-n", "hello", &dir]);
    let sys = sys_grep(&["-r", "-n", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -n");
}

#[test]
fn recurse_count_only() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-count");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-c", "hello", &dir]);
    let sys = sys_grep(&["-r", "-c", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -c");
}

#[test]
fn recurse_files_with_matches() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-files-l");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-l", "hello", &dir]);
    let sys = sys_grep(&["-r", "-l", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -l");
}

#[test]
fn recurse_files_without_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-files-L");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-L", "hello", &dir]);
    let sys = sys_grep(&["-r", "-L", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -L");
}

#[test]
fn recurse_invert_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-invert");
    // Use a subdirectory to avoid binary.bin in fixture.root triggering
    // grep's "binary file matches" stderr warning (fro does not emit it).
    let sub = fixture.root.join("text-only");
    std::fs::create_dir_all(&sub).unwrap();
    let dir = make_tree(&sub);

    let fro = fro_fgrep(&["-r", "-v", "hello", &dir]);
    let sys = sys_grep(&["-r", "-v", "hello", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -v");
}

#[test]
fn recurse_quiet_exits_zero_on_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-quiet");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-q", "hello", &dir]);
    let sys = sys_grep(&["-r", "-q", "hello", &dir]);

    assert_same_result(fro, sys, "-r -q match");
}

#[test]
fn recurse_with_ignore_case() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-icase");
    let dir = make_tree(&fixture.root);

    let fro = fro_fgrep(&["-r", "-i", "HELLO", &dir]);
    let sys = sys_grep(&["-r", "-i", "HELLO", &dir]);

    assert_same_sorted_lines(fro, sys, "-r -i");
}

// ── mixed file and directory arguments ───────────────────────────────────

#[test]
fn recurse_mixed_file_and_dir_args() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-mixed");
    let dir = make_tree(&fixture.root);
    // Pass both the sub-tree and one explicit file
    let file_a = fixture.root.join("a.txt").to_string_lossy().into_owned();
    let sub = fixture.root.join("sub").to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "hello", &file_a, &sub]);
    let sys = sys_grep(&["-r", "hello", &file_a, &sub]);

    assert_same_sorted_lines(fro, sys, "-r mixed file+dir");
}

// ── symlink handling (symlinks to dirs are not followed under -r) ─────────

#[test]
fn recurse_does_not_follow_directory_symlinks_during_traversal() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-dirsym");
    // fixture.tree_root contains nested files; fixture.directory_symlink → tree_root
    // When recursing into tree_root the directory_symlink inside root should
    // NOT be followed (it points back outside the tree_root subtree anyway,
    // but more importantly it is a dir symlink in the tree).
    let tree = fixture.tree_root.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "needle", &tree]);
    let sys = sys_grep(&["-r", "needle", &tree]);

    assert_same_sorted_lines(fro, sys, "-r dir symlink not followed");
}

#[test]
fn recurse_explicit_directory_symlink_arg_is_entered() {
    // When the argument itself is a symlink to a directory we DO enter it,
    // matching GNU grep -r behaviour for explicit arguments.
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-explsym");
    let link = fixture.directory_symlink.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "needle", &link]);
    let sys = sys_grep(&["-r", "needle", &link]);

    assert_same_sorted_lines(fro, sys, "-r explicit dir symlink arg");
}

// ── fixture tree (CoreutilsParityFixture) ─────────────────────────────────

#[test]
fn recurse_parity_fixture_tree() {
    // Use the parity fixture tree directly to confirm path/content alignment.
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-fixture");
    let tree = fixture.tree_root.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "needle", &tree]);
    let sys = sys_grep(&["-r", "needle", &tree]);

    assert_same_sorted_lines(fro, sys, "-r parity fixture tree");
}

#[test]
fn recurse_parity_fixture_tree_with_word_regexp() {
    let fixture = CoreutilsParityFixture::new("fgrep-recurse-word");
    let tree = fixture.tree_root.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-r", "-w", "needle", &tree]);
    let sys = sys_grep(&["-r", "-w", "needle", &tree]);

    assert_same_sorted_lines(fro, sys, "-r -w parity");
}
