#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
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

fn run_fro(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .args(args)
        .output()
        .expect("failed to run fro find")
}

fn run_system(args: &[&str]) -> Output {
    Command::new("find")
        .args(args)
        .output()
        .expect("failed to run system find")
}

fn make_fifo(path: &std::path::Path) {
    let fifo = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let rc = unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
}

fn assert_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn sorted_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines = String::from_utf8_lossy(bytes)
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn sorted_nul_fields(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut fields = bytes
        .split(|&byte| byte == b'\0')
        .filter(|field| !field.is_empty())
        .map(|field| field.to_vec())
        .collect::<Vec<_>>();
    fields.sort();
    fields
}

#[test]
fn find_matches_system_for_nested_tree_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-tree");
    let root = tmp.join("root");
    let nested = root.join("a").join("b");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(root.join("a").join("mid.txt"), b"mid").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_matches_system_for_multiple_roots_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-multi");
    let dir = tmp.join("dir");
    let nested = dir.join("nested");
    let standalone = tmp.join("standalone.txt");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("child.txt"), b"child").unwrap();
    fs::write(&standalone, b"standalone").unwrap();

    let fro = assert_success(run_fro(&[
        "find",
        dir.to_str().unwrap(),
        standalone.to_str().unwrap(),
    ]));
    let system = assert_success(run_system(&[
        dir.to_str().unwrap(),
        standalone.to_str().unwrap(),
    ]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_matches_system_for_wide_tree_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-wide");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    for i in 0..24 {
        let branch = root.join(format!("branch-{i}"));
        let nested = branch.join("nested").join("leaf");
        fs::create_dir_all(&nested).unwrap();
        fs::write(branch.join("root.txt"), format!("root-{i}\n")).unwrap();
        fs::write(nested.join("leaf.txt"), format!("leaf-{i}\n")).unwrap();
    }

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_warns_and_continues_on_permission_denied_directory() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }

    let tmp = unique_temp_dir("fro-find-perms");
    let root = tmp.join("root");
    let blocked = root.join("blocked");
    fs::create_dir_all(&blocked).unwrap();
    fs::write(root.join("visible.txt"), b"visible").unwrap();
    fs::write(blocked.join("hidden.txt"), b"hidden").unwrap();

    let mut perms = fs::metadata(&blocked).unwrap().permissions();
    perms.set_mode(0);
    fs::set_permissions(&blocked, perms).unwrap();

    let fro = run_fro(&["find", root.to_str().unwrap()]);
    let system = run_system(&[root.to_str().unwrap()]);

    let mut restore = fs::metadata(&blocked).unwrap().permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&blocked, restore).unwrap();

    assert_eq!(fro.status.code(), system.status.code());
    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert!(String::from_utf8_lossy(&fro.stderr).contains("Permission denied"));
    assert!(String::from_utf8_lossy(&system.stderr).contains("Permission denied"));
}

#[test]
fn find_matches_system_for_symlinks_broken_symlinks_and_fifos() {
    let tmp = unique_temp_dir("fro-find-special");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let regular = root.join("regular.txt");
    let symlink_path = root.join("regular-link");
    let broken_symlink = root.join("broken-link");
    let fifo = root.join("events.fifo");
    fs::write(&regular, b"regular").unwrap();
    std::os::unix::fs::symlink(&regular, &symlink_path).unwrap();
    std::os::unix::fs::symlink(root.join("missing-target"), &broken_symlink).unwrap();
    make_fifo(&fifo);
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap()]));
    let system = assert_success(run_system(&[root.to_str().unwrap()]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_type_f_matches_system_for_regular_files_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-type-f");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let regular = root.join("regular.txt");
    let symlink_path = root.join("regular-link");
    let fifo = root.join("events.fifo");
    fs::write(&regular, b"regular").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();
    std::os::unix::fs::symlink(&regular, &symlink_path).unwrap();
    make_fifo(&fifo);

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap(), "-type", "f"]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-type", "f"]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_type_d_matches_system_for_directories_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-type-d");
    let root = tmp.join("root");
    let nested = root.join("nested").join("deeper");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap(), "-type", "d"]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-type", "d"]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_name_matches_system_for_basename_globs_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-name");
    let root = tmp.join("root");
    let nested = root.join("nested");
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(root.join("top.log"), b"log").unwrap();
    fs::write(nested.join("mid.txt"), b"mid").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();
    fs::write(deeper.join("leaf.md"), b"leaf-md").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap(), "-name", "*.txt"]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-name", "*.txt"]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_name_combines_with_type_and_print0_like_system_find() {
    let tmp = unique_temp_dir("fro-find-name-print0");
    let root = tmp.join("root");
    let nested = root.join("nested.rs");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("main.rs"), b"fn main() {}\n").unwrap();
    fs::write(root.join("main.txt"), b"txt").unwrap();
    fs::write(nested.join("lib.rs"), b"pub fn lib() {}\n").unwrap();
    fs::write(nested.join("note.rs.bak"), b"bak").unwrap();

    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-type",
        "f",
        "-name",
        "*.rs",
        "-print0",
    ]));
    let system = assert_success(run_system(&[
        root.to_str().unwrap(),
        "-type",
        "f",
        "-name",
        "*.rs",
        "-print0",
    ]));

    assert_eq!(
        sorted_nul_fields(&fro.stdout),
        sorted_nul_fields(&system.stdout)
    );
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_iname_matches_system_for_case_insensitive_basename_globs_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-iname");
    let root = tmp.join("root");
    let nested = root.join("NeStEd");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("main.rs"), b"main").unwrap();
    fs::write(root.join("MAIN.TXT"), b"txt").unwrap();
    fs::write(nested.join("lib.RS"), b"lib").unwrap();
    fs::write(nested.join("note.md"), b"md").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap(), "-iname", "*.rs"]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-iname", "*.rs"]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_path_matches_system_for_emitted_path_globs_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-path");
    let root = tmp.join("root");
    let nested = root.join("nested");
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(nested.join("mid.txt"), b"mid").unwrap();
    fs::write(deeper.join("leaf.log"), b"leaf").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();

    let pattern = format!("{}/*/deeper/*.txt", root.display());
    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-path",
        &pattern,
    ]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-path", &pattern]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_ipath_combines_with_type_and_print0_like_system_find() {
    let tmp = unique_temp_dir("fro-find-ipath-print0");
    let root = tmp.join("root");
    let nested = root.join("NeStEd");
    let deeper = nested.join("DeEpEr");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.rs"), b"top").unwrap();
    fs::write(nested.join("mid.RS"), b"mid").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();
    fs::write(deeper.join("Leaf.Rs"), b"leaf-rs").unwrap();

    let pattern = format!("{}/*/*/*.rs", root.display());
    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-type",
        "f",
        "-ipath",
        &pattern,
        "-print0",
    ]));
    let system = assert_success(run_system(&[
        root.to_str().unwrap(),
        "-type",
        "f",
        "-ipath",
        &pattern,
        "-print0",
    ]));

    assert_eq!(
        sorted_nul_fields(&fro.stdout),
        sorted_nul_fields(&system.stdout)
    );
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_path_combines_with_type_and_print0_like_system_find() {
    let tmp = unique_temp_dir("fro-find-path-print0");
    let root = tmp.join("root");
    let nested = root.join("nested");
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.rs"), b"top").unwrap();
    fs::write(nested.join("mid.rs"), b"mid").unwrap();
    fs::write(deeper.join("leaf.rs"), b"leaf").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();

    let pattern = format!("{}/*/*.rs", root.display());
    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-type",
        "f",
        "-path",
        &pattern,
        "-print0",
    ]));
    let system = assert_success(run_system(&[
        root.to_str().unwrap(),
        "-type",
        "f",
        "-path",
        &pattern,
        "-print0",
    ]));

    assert_eq!(
        sorted_nul_fields(&fro.stdout),
        sorted_nul_fields(&system.stdout)
    );
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_maxdepth_matches_system_for_common_tree_limits_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-maxdepth");
    let root = tmp.join("root");
    let nested = root.join("nested");
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(nested.join("mid.txt"), b"mid").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&["find", root.to_str().unwrap(), "-maxdepth", "1"]));
    let system = assert_success(run_system(&[root.to_str().unwrap(), "-maxdepth", "1"]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_maxdepth_combines_with_type_and_name_like_system_find() {
    let tmp = unique_temp_dir("fro-find-maxdepth-name");
    let root = tmp.join("root");
    let nested = root.join("nested");
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(nested.join("mid.txt"), b"mid").unwrap();
    fs::write(deeper.join("leaf.txt"), b"leaf").unwrap();
    fs::write(nested.join("mid.log"), b"log").unwrap();

    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-maxdepth",
        "2",
        "-type",
        "f",
        "-name",
        "*.txt",
    ]));
    let system = assert_success(run_system(&[
        root.to_str().unwrap(),
        "-maxdepth",
        "2",
        "-type",
        "f",
        "-name",
        "*.txt",
    ]));

    assert_eq!(sorted_lines(&fro.stdout), sorted_lines(&system.stdout));
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_print0_matches_system_for_regular_files_ignoring_order() {
    let tmp = unique_temp_dir("fro-find-print0");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let spaced = root.join("space name.txt");
    fs::write(&spaced, b"regular").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = assert_success(run_fro(&[
        "find",
        root.to_str().unwrap(),
        "-type",
        "f",
        "-print0",
    ]));
    let system = assert_success(run_system(&[
        root.to_str().unwrap(),
        "-type",
        "f",
        "-print0",
    ]));

    assert_eq!(
        sorted_nul_fields(&fro.stdout),
        sorted_nul_fields(&system.stdout)
    );
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn find_help_and_version_surface_stay_wired() {
    let output = run_fro(&["find", "--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(0));
    assert!(stdout.contains(
        "find - Walk one or more directory trees with a bounded GNU find-compatible predicate slice."
    ));
    assert!(stdout.contains("[-P|-H|-L] [-Olevel] [path ...] [-mindepth N] [-maxdepth N]"));
    assert!(stdout.contains(
        "This is a bounded GNU find-compatible path-walking/predicate slice, not the full GNU find expression language."
    ));
    assert!(stdout.contains(
        "Supported in-process families now cover bounded path walking, GNU symlink policy flags (-P/-H/-L/-follow)"
    ));
    assert!(stdout.contains(
        "Parentheses, batched -exec ... +, and GNU debug output (-D) remain intentionally omitted"
    ));
    assert!(stdout.contains(
        "Tracked GNU find tokens implemented in this bounded in-process path-walking slice"
    ));
    assert!(!stdout
        .contains("GNU find tokens intentionally omitted from this bounded in-process slice"));
    assert!(stdout.contains("-D"));
    assert!(stdout.contains("-delete"));
    assert!(stdout.contains("-regex"));
    assert!(stdout.contains("regex path matching (-regex/-iregex with bounded -regextype values)"));
    assert!(stdout.contains(
        "bounded formatter/listing/file-output actions (-printf/-fprintf/-fprint/-fprint0/-ls/-fls)"
    ));
    assert!(stdout.contains(
        "-P keeps GNU find's default no-follow mode, -H follows command-line symlink roots only, and -L/-follow follow symlinked roots and discovered child symlinks during traversal."
    ));
    assert!(stdout.contains(
        "-mindepth/-maxdepth bound which levels are emitted and descended, and -mount/-xdev keep traversal on the same device as each starting path."
    ));
    assert!(stdout.contains(
        "-type supports the common GNU/POSIX letters b, c, d, p, f, l, and s; -xtype uses the symlink target type under the default no-follow mode."
    ));
    assert!(stdout.contains(
        "-wholename/-iwholename are GNU aliases for -path/-ipath, and -lname/-ilname match symlink targets."
    ));
    assert!(stdout.contains(
        "-regex/-iregex match the whole emitted path via the Rust regex crate; bounded -regextype support currently accepts emacs/findutils-default and posix-extended/egrep syntax families."
    ));
    assert!(stdout.contains(
        "-newer/-anewer/-cnewer and the -mtime/-mmin/-atime/-amin/-ctime/-cmin family are supported in-process; -used compares access time versus status-change time in whole days; -fstype compares the containing filesystem type; -context matches SELinux contexts when SELinux is enabled; and -daystart shifts subsequent day-granularity predicates to the start of the current local day."
    ));
    assert!(stdout.contains(
        "-true, -false, -noleaf, !/-not, and -a/-and/-o/-or are accepted in the current bounded evaluator."
    ));
    assert!(stdout.contains(
        "-depth emits directories after their descendants; -delete also forces depth-first deletion of the current directory entry itself; -prune suppresses descent into the current matched directory; and -quit stops the walk immediately after the current match."
    ));
    assert!(stdout.contains(
        "-ignore_readdir_race suppresses ENOENT races discovered during traversal, while -noignore_readdir_race restores the default warning behavior."
    ));
    assert!(stdout.contains(
        "-print is the default action; -print0 emits NUL-delimited paths; -fprint/-fprint0 write newline- or NUL-delimited matches to a file; bounded -printf/-fprintf currently support %%p, %%f, %%h, %%s, %%y, %%%% plus \\n/\\t/\\\\ escapes; -ls/-fls emit a GNU-like inode/block/owner/group listing; -exec/-execdir run one command per match; and -ok/-okdir add GNU-style confirmation prompts before each per-match command."
    ));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro find version string and exits."));
    assert!(stdout.contains(
        "-mindepth/-maxdepth bound which levels are emitted and descended, and -mount/-xdev keep traversal on the same device as each starting path."
    ));
    assert!(stdout.contains("-type supports the common GNU/POSIX letters"));
    assert!(stdout.contains("-name matches only the final path component"));
    assert!(stdout.contains("-iname matches basenames case-insensitively"));
    assert!(stdout.contains("-path matches the whole emitted path"));
    assert!(stdout.contains("-ipath matches whole emitted paths case-insensitively"));
    assert!(stdout.contains("-print0 emits NUL-delimited paths"));
    assert!(output.stderr.is_empty());

    let version = run_fro(&["find", "--version"]);
    assert_eq!(version.status.code(), Some(0));
    assert!(
        version.stderr.is_empty(),
        "find --version unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&version.stderr)
    );
    let version_stdout = String::from_utf8_lossy(&version.stdout);
    assert!(version_stdout.starts_with("find "));
    assert!(version_stdout.contains(env!("CARGO_PKG_VERSION")));
}
