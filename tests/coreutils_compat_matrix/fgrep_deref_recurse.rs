// ── fgrep -R / --dereference-recursive parity tests ─────────────────────
//
// Compares `fro fgrep -R` against system `grep -F -R` on directory fixtures
// that contain symlinks to files and directories.  Output order may differ
// between implementations so most tests use `assert_same_sorted_lines`.

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

/// Build a small tree with a symlink to a regular file inside it:
///
/// ```
/// <root>/
///   real.txt          "needle found\n"
///   other.txt         "no match\n"
///   link.txt  →  real.txt
///   sub/
///     sub.txt         "needle in sub\n"
/// ```
fn make_tree_with_file_symlink(root: &std::path::Path) -> String {
    std::fs::write(root.join("real.txt"), b"needle found\n").unwrap();
    std::fs::write(root.join("other.txt"), b"no match\n").unwrap();
    std::os::unix::fs::symlink(root.join("real.txt"), root.join("link.txt")).unwrap();
    let sub = root.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(sub.join("sub.txt"), b"needle in sub\n").unwrap();
    root.to_string_lossy().into_owned()
}

/// Build a tree with a symlink to a subdirectory inside it:
///
/// ```
/// <root>/
///   real_sub/
///     a.txt      "needle here\n"
///   link_sub  →  real_sub/
/// ```
fn make_tree_with_dir_symlink(root: &std::path::Path) -> String {
    let real_sub = root.join("real_sub");
    std::fs::create_dir_all(&real_sub).unwrap();
    std::fs::write(real_sub.join("a.txt"), b"needle here\n").unwrap();
    std::os::unix::fs::symlink(&real_sub, root.join("link_sub")).unwrap();
    root.to_string_lossy().into_owned()
}

/// Build a tree with a circular symlink to detect infinite-loop safety:
///
/// ```
/// <root>/
///   sub/
///     a.txt         "needle cycle\n"
///     parent_link → <root>/   (cycle!)
/// ```
fn make_tree_with_cycle(root: &std::path::Path) -> String {
    let sub = root.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(sub.join("a.txt"), b"needle cycle\n").unwrap();
    std::os::unix::fs::symlink(root, sub.join("parent_link")).unwrap();
    root.to_string_lossy().into_owned()
}

// ── basic -R / --dereference-recursive ───────────────────────────────────

#[test]
fn deref_recurse_short_flag_matches_files_in_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-r-basic");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "needle", &dir]);
    let sys = sys_grep(&["-R", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R basic");
}

#[test]
fn deref_recurse_long_flag_matches_files_in_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-long-basic");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["--dereference-recursive", "needle", &dir]);
    let sys = sys_grep(&["--dereference-recursive", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "--dereference-recursive basic");
}

#[test]
fn deref_recurse_follows_file_symlinks_during_traversal() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-filesym");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "needle", &dir]);
    let sys = sys_grep(&["-R", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R follows file symlinks");
}

#[test]
fn deref_recurse_follows_dir_symlinks_during_traversal() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-dirsym");
    let dir = make_tree_with_dir_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "needle", &dir]);
    let sys = sys_grep(&["-R", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R follows dir symlinks");
}

#[test]
fn deref_recurse_does_not_infinite_loop_on_cycles() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-cycle");
    let dir = make_tree_with_cycle(&fixture.root);

    // Both fro and system grep should terminate and agree.
    let fro = fro_fgrep(&["-R", "needle", &dir]);
    let sys = sys_grep(&["-R", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R cycle termination");
}

#[test]
fn deref_recurse_no_match_returns_exit_1() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-nomatch");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "XYZZY_no_match", &dir]);
    let sys = sys_grep(&["-R", "XYZZY_no_match", &dir]);

    assert_same_result(fro, sys, "-R no match exit code");
}

#[test]
fn deref_recurse_empty_directory_returns_exit_1() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-emptydir");
    let empty = fixture.empty_dir.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-R", "needle", &empty]);
    let sys = sys_grep(&["-R", "needle", &empty]);

    assert_same_result(fro, sys, "-R empty dir");
}

// ── flag interactions ─────────────────────────────────────────────────────

#[test]
fn deref_recurse_with_line_numbers() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-linenum");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "-n", "needle", &dir]);
    let sys = sys_grep(&["-R", "-n", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -n");
}

#[test]
fn deref_recurse_count_only() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-count");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "-c", "needle", &dir]);
    let sys = sys_grep(&["-R", "-c", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -c");
}

#[test]
fn deref_recurse_files_with_matches() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-files-l");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "-l", "needle", &dir]);
    let sys = sys_grep(&["-R", "-l", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -l");
}

#[test]
fn deref_recurse_files_without_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-files-L");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "-L", "needle", &dir]);
    let sys = sys_grep(&["-R", "-L", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -L");
}

#[test]
fn deref_recurse_quiet_exits_zero_on_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-quiet");
    // Use a subdirectory without broken symlinks so -q parity is not
    // affected by early-exit vs full-expand differences.
    let sub = fixture.root.join("clean-tree");
    std::fs::create_dir_all(&sub).unwrap();
    let dir = make_tree_with_file_symlink(&sub);

    let fro = fro_fgrep(&["-R", "-q", "needle", &dir]);
    let sys = sys_grep(&["-R", "-q", "needle", &dir]);

    assert_same_result(fro, sys, "-R -q match");
}

#[test]
fn deref_recurse_with_ignore_case() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-icase");
    let dir = make_tree_with_file_symlink(&fixture.root);

    let fro = fro_fgrep(&["-R", "-i", "NEEDLE", &dir]);
    let sys = sys_grep(&["-R", "-i", "NEEDLE", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -i");
}

#[test]
fn deref_recurse_invert_match() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-invert");
    // Isolate to a text-only subtree to avoid binary-file stderr divergence.
    let sub = fixture.root.join("text-only");
    std::fs::create_dir_all(&sub).unwrap();
    let dir = make_tree_with_file_symlink(&sub);

    let fro = fro_fgrep(&["-R", "-v", "needle", &dir]);
    let sys = sys_grep(&["-R", "-v", "needle", &dir]);

    assert_same_sorted_lines(fro, sys, "-R -v");
}

// ── explicit directory-symlink argument ───────────────────────────────────

#[test]
fn deref_recurse_explicit_directory_symlink_arg_is_entered() {
    // A symlink-to-dir supplied as an explicit CLI argument must be entered
    // by -R (same as -r; both enter explicit dir-symlink args).
    let fixture = CoreutilsParityFixture::new("fgrep-deref-explsym");
    let link = fixture.directory_symlink.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-R", "needle", &link]);
    let sys = sys_grep(&["-R", "needle", &link]);

    assert_same_sorted_lines(fro, sys, "-R explicit dir symlink arg");
}

// ── parity fixture tree ───────────────────────────────────────────────────

#[test]
fn deref_recurse_parity_fixture_tree() {
    let fixture = CoreutilsParityFixture::new("fgrep-deref-fixture");
    let tree = fixture.tree_root.to_string_lossy().into_owned();

    let fro = fro_fgrep(&["-R", "needle", &tree]);
    let sys = sys_grep(&["-R", "needle", &tree]);

    assert_same_sorted_lines(fro, sys, "-R parity fixture tree");
}
