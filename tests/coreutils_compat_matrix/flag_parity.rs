//! Find-only TODO coverage stubs plus helper predicates for bounded find parity work.
//!
//! Layout
//! ──────
//! 1. local helper routines reused by find TODO coverage probes
//! 2. `#[ignore = "TODO"]` stubs for remaining find system-only flags

use super::*;
use std::ffi::CStr;
use std::os::unix::fs::symlink;
use std::os::unix::fs::MetadataExt;

fn assert_find_same_sorted(root: &Path, extra_args: &[&str], label: &str) {
    let root_arg = root.to_str().unwrap();
    let mut fro_args = vec![root_arg];
    fro_args.extend_from_slice(extra_args);
    let mut sys_args = vec![root_arg];
    sys_args.extend_from_slice(extra_args);
    assert_same_sorted_lines(
        run_fro("find", &fro_args),
        run_system("find", &sys_args),
        label,
    );
}

fn assert_find_same_exact(root: &Path, extra_args: &[&str], label: &str) {
    let root_arg = root.to_str().unwrap();
    let mut fro_args = vec![root_arg];
    fro_args.extend_from_slice(extra_args);
    let mut sys_args = vec![root_arg];
    sys_args.extend_from_slice(extra_args);
    assert_same_result(
        run_fro("find", &fro_args),
        run_system("find", &sys_args),
        label,
    );
}

fn current_fstype(path: &Path) -> String {
    let path_arg = path.to_str().unwrap();
    let output = run_system("stat", &["-f", "-c", "%T", path_arg]);
    assert_eq!(output.status.code(), Some(0), "stat -f -c %T failed");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn set_file_times(path: &Path, atime_sec: i64, mtime_sec: i64) {
    let path_c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let times = [
        libc::timespec {
            tv_sec: atime_sec,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: mtime_sec,
            tv_nsec: 0,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(
        rc,
        0,
        "utimensat failed for {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

fn current_user_name() -> String {
    let uid = unsafe { libc::getuid() };
    let mut buf_len = 1024usize;
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getpwuid_r(
                uid,
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        assert_eq!(
            rc,
            0,
            "getpwuid_r failed: {}",
            std::io::Error::from_raw_os_error(rc)
        );
        let result = result.cast_const();
        assert!(!result.is_null(), "current uid {uid} has no passwd entry");
        return unsafe { CStr::from_ptr((*result).pw_name) }
            .to_str()
            .unwrap()
            .to_string();
    }
}

fn current_group_name() -> String {
    let gid = unsafe { libc::getgid() };
    let mut buf_len = 1024usize;
    loop {
        let mut grp = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getgrgid_r(
                gid,
                grp.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        assert_eq!(
            rc,
            0,
            "getgrgid_r failed: {}",
            std::io::Error::from_raw_os_error(rc)
        );
        let result = result.cast_const();
        assert!(!result.is_null(), "current gid {gid} has no group entry");
        return unsafe { CStr::from_ptr((*result).gr_name) }
            .to_str()
            .unwrap()
            .to_string();
    }
}

// ════════════════════════════════════════════════════════════════════
// §1  find: TODO stubs for remaining system-only flags
//
// Each stub corresponds to one entry in the `remaining` slice of the
// find CoverageRow in src/help_compat.rs.  When a predicate is natively
// supported, move its test to §2 or find_cli.rs and remove the stub.
// ════════════════════════════════════════════════════════════════════

// ── Debug / optimisation level ──────────────────────────────────────

#[test]
#[ignore = "TODO: find -D (debug options) not yet implemented in fro bounded find slice"]
fn find_debug_todo() {}

#[test]
fn find_optimise_level_todo() {
    let tmp = unique_temp_dir("fro-fp-find-olevel");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"file").unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_sorted_lines(
        run_fro("find", &["-O9", root_arg, "-type", "f"]),
        run_system("find", &["-O9", root_arg, "-type", "f"]),
        "find -O9 -type f",
    );
}

// ── Symlink-traversal top-level flags ───────────────────────────────

#[test]
fn find_h_symlink_todo() {
    let tmp = unique_temp_dir("fro-fp-find-h");
    let target = tmp.join("target");
    let nested = target.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("inside.txt"), b"inside").unwrap();
    let child_link = target.join("child-link");
    symlink(&nested, &child_link).unwrap();
    let root_link = tmp.join("root-link");
    symlink(&target, &root_link).unwrap();
    let root_arg = root_link.to_str().unwrap();
    assert_same_sorted_lines(
        run_fro("find", &["-H", root_arg, "-type", "f"]),
        run_system("find", &["-H", root_arg, "-type", "f"]),
        "find -H -type f",
    );
}

#[test]
fn find_l_symlink_todo() {
    let tmp = unique_temp_dir("fro-fp-find-l");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("inside.txt"), b"inside").unwrap();
    let child_link = root.join("child-link");
    symlink(&nested, &child_link).unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_sorted_lines(
        run_fro("find", &["-L", root_arg, "-type", "f"]),
        run_system("find", &["-L", root_arg, "-type", "f"]),
        "find -L -type f",
    );
}

#[test]
fn find_p_symlink_todo() {
    let tmp = unique_temp_dir("fro-fp-find-p");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_sorted_lines(
        run_fro("find", &["-P", root_arg, "-type", "f"]),
        run_system("find", &["-P", root_arg, "-type", "f"]),
        "find -P -type f",
    );
}

// ── Boolean combinators ─────────────────────────────────────────────

#[test]
fn find_and_todo() {
    let tmp = unique_temp_dir("fro-fp-find-and");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("beta.log"), b"beta").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-a", "-name", "*.txt"], "find -a");
}

#[test]
fn find_and_long_todo() {
    let tmp = unique_temp_dir("fro-fp-find-and-long");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("beta.log"), b"beta").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-and", "-name", "*.txt"],
        "find -and",
    );
}

#[test]
fn find_or_short_todo() {
    let tmp = unique_temp_dir("fro-fp-find-or-short");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("beta.log"), b"beta").unwrap();
    fs::write(root.join("sub/gamma.md"), b"gamma").unwrap();
    assert_find_same_sorted(
        &root,
        &["-name", "*.txt", "-o", "-name", "*.log"],
        "find -o",
    );
}

#[test]
fn find_or_long_todo() {
    let tmp = unique_temp_dir("fro-fp-find-or-long");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("beta.log"), b"beta").unwrap();
    fs::write(root.join("sub/gamma.md"), b"gamma").unwrap();
    assert_find_same_sorted(
        &root,
        &["-name", "*.txt", "-or", "-name", "*.md"],
        "find -or",
    );
}

#[test]
fn find_not_todo() {
    let tmp = unique_temp_dir("fro-fp-find-not");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("beta.log"), b"beta").unwrap();
    fs::write(root.join("sub/gamma.md"), b"gamma").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-not", "-name", "*.txt"],
        "find -not",
    );
}

// ── Time-based predicates ───────────────────────────────────────────

#[test]
fn find_atime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-atime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-atime", "0"], "find -atime 0");
}

#[test]
fn find_amin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-amin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-amin", "-1"], "find -amin -1");
}

#[test]
fn find_anewer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-anewer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-anewer", reference.to_str().unwrap()],
        "find -anewer ref",
    );
}

#[test]
fn find_ctime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ctime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-ctime", "0"], "find -ctime 0");
}

#[test]
fn find_cmin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-cmin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-cmin", "-1"], "find -cmin -1");
}

#[test]
fn find_cnewer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-cnewer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-cnewer", reference.to_str().unwrap()],
        "find -cnewer ref",
    );
}

#[test]
fn find_mtime_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mtime");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-mtime", "0"], "find -mtime 0");
}

#[test]
fn find_mmin_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mmin");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("recent.txt"), b"recent").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-mmin", "-1"], "find -mmin -1");
}

#[test]
fn find_newer_todo() {
    let tmp = unique_temp_dir("fro-fp-find-newer");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let reference = root.join("reference.txt");
    fs::write(&reference, b"reference").unwrap();
    set_file_times(&reference, 1, 1);
    fs::write(root.join("candidate.txt"), b"candidate").unwrap();
    assert_find_same_sorted(
        &root,
        &["-type", "f", "-newer", reference.to_str().unwrap()],
        "find -newer ref",
    );
}

#[test]
fn find_daystart_todo() {
    let tmp = unique_temp_dir("fro-fp-find-daystart");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let midnight = tmp.join("midnight");
    fs::write(&midnight, b"midnight").unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as libc::time_t;
    let midnight_secs = {
        let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
        assert!(
            !unsafe { libc::localtime_r(&now, &mut local) }.is_null(),
            "localtime_r failed"
        );
        local.tm_hour = 0;
        local.tm_min = 0;
        local.tm_sec = 0;
        unsafe { libc::mktime(&mut local) }
    };
    assert!(midnight_secs > 120, "midnight timestamp unexpectedly small");
    let older = root.join("older.txt");
    let recent = root.join("recent.txt");
    fs::write(&older, b"older").unwrap();
    fs::write(&recent, b"recent").unwrap();
    set_file_times(&older, midnight_secs - 60, midnight_secs - 60);
    set_file_times(&recent, midnight_secs + 60, midnight_secs + 60);
    assert_find_same_sorted(&root, &["-mtime", "0"], "find -mtime 0");
    assert_find_same_sorted(
        &root,
        &["-daystart", "-mtime", "0"],
        "find -daystart -mtime 0",
    );
}

#[test]
fn find_used_todo() {
    let tmp = unique_temp_dir("fro-fp-find-used");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("candidate.txt");
    fs::write(&path, b"candidate").unwrap();
    set_file_times(&path, 1, 1);
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o600);
    fs::set_permissions(&path, perms).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"candidate");
    assert_find_same_sorted(&root, &["-type", "f", "-used", "-1"], "find -used -1");
}

// ── Size / count predicates ─────────────────────────────────────────

#[test]
fn find_size_todo() {
    let tmp = unique_temp_dir("fro-fp-find-size");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("empty.txt"), b"").unwrap();
    fs::write(root.join("five.txt"), b"12345").unwrap();
    fs::write(root.join("six.txt"), b"123456").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-size", "+0c"], "find -size +0c");
    assert_find_same_sorted(&root, &["-type", "f", "-size", "5c"], "find -size 5c");
}

#[test]
fn find_links_todo() {
    let tmp = unique_temp_dir("fro-fp-find-links");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("single.txt"), b"single").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-links", "1"], "find -links 1");
}

#[test]
fn find_inum_todo() {
    let tmp = unique_temp_dir("fro-fp-find-inum");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let target = root.join("target.txt");
    fs::write(&target, b"target").unwrap();
    let inum = fs::metadata(&target).unwrap().ino().to_string();
    assert_find_same_sorted(&root, &["-inum", &inum], "find -inum");
}

// ── Ownership / permission predicates ───────────────────────────────

#[test]
fn find_perm_todo() {
    let tmp = unique_temp_dir("fro-fp-find-perm");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let exact = root.join("exact.txt");
    let other = root.join("other.txt");
    fs::write(&exact, b"exact").unwrap();
    fs::write(&other, b"other").unwrap();
    fs::set_permissions(&exact, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(&other, fs::Permissions::from_mode(0o755)).unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-perm", "0644"], "find -perm 0644");
}

#[test]
fn find_uid_todo() {
    let tmp = unique_temp_dir("fro-fp-find-uid");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let uid = unsafe { libc::getuid() }.to_string();
    assert_find_same_sorted(&root, &["-type", "f", "-uid", &uid], "find -uid");
}

#[test]
fn find_gid_todo() {
    let tmp = unique_temp_dir("fro-fp-find-gid");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let gid = unsafe { libc::getgid() }.to_string();
    assert_find_same_sorted(&root, &["-type", "f", "-gid", &gid], "find -gid");
}

#[test]
fn find_user_todo() {
    let tmp = unique_temp_dir("fro-fp-find-user");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let user = current_user_name();
    assert_find_same_sorted(&root, &["-type", "f", "-user", &user], "find -user");
}

#[test]
fn find_group_todo() {
    let tmp = unique_temp_dir("fro-fp-find-group");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    let group = current_group_name();
    assert_find_same_sorted(&root, &["-type", "f", "-group", &group], "find -group");
}

#[test]
fn find_nouser_todo() {
    let tmp = unique_temp_dir("fro-fp-find-nouser");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-nouser"], "find -nouser");
}

#[test]
fn find_nogroup_todo() {
    let tmp = unique_temp_dir("fro-fp-find-nogroup");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-nogroup"], "find -nogroup");
}

#[test]
fn find_readable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-readable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("readable.txt"), b"readable").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-readable"], "find -readable");
}

#[test]
fn find_writable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-writable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("writable.txt"), b"writable").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-writable"], "find -writable");
}

#[test]
fn find_executable_todo() {
    let tmp = unique_temp_dir("fro-fp-find-executable");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    let exec = root.join("exec.sh");
    let plain = root.join("plain.txt");
    fs::write(&exec, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::write(&plain, b"plain").unwrap();
    fs::set_permissions(&exec, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&plain, fs::Permissions::from_mode(0o644)).unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-executable"], "find -executable");
}

#[test]
fn find_empty_todo() {
    let tmp = unique_temp_dir("fro-fp-find-empty");
    let root = tmp.join("root");
    let empty_dir = root.join("empty-dir");
    let full_dir = root.join("full-dir");
    fs::create_dir_all(&empty_dir).unwrap();
    fs::create_dir_all(&full_dir).unwrap();
    fs::write(root.join("empty.txt"), b"").unwrap();
    fs::write(root.join("full.txt"), b"full").unwrap();
    fs::write(full_dir.join("nested.txt"), b"nested").unwrap();
    assert_find_same_sorted(&root, &["-type", "f", "-empty"], "find -empty -type f");
    assert_find_same_sorted(&root, &["-type", "d", "-empty"], "find -empty -type d");
}

// ── Regex predicates ────────────────────────────────────────────────

#[test]
fn find_regex_todo() {
    let tmp = unique_temp_dir("fro-fp-find-regex");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(sub.join("BETA.TXT"), b"beta").unwrap();
    fs::write(sub.join("gamma.log"), b"gamma").unwrap();
    assert_find_same_sorted(&root, &["-regex", ".*txt"], "find -regex");
}

#[test]
fn find_iregex_todo() {
    let tmp = unique_temp_dir("fro-fp-find-iregex");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(sub.join("BETA.TXT"), b"beta").unwrap();
    fs::write(sub.join("gamma.log"), b"gamma").unwrap();
    assert_find_same_sorted(&root, &["-iregex", ".*txt"], "find -iregex");
}

#[test]
fn find_regextype_todo() {
    let tmp = unique_temp_dir("fro-fp-find-regextype");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(sub.join("BETA.TXT"), b"beta").unwrap();
    fs::write(sub.join("gamma.log"), b"gamma").unwrap();
    assert_find_same_sorted(
        &root,
        &[
            "-regextype",
            "posix-extended",
            "-regex",
            ".*/(alpha|BETA)\\.(txt|TXT)",
        ],
        "find -regextype posix-extended",
    );
}

#[test]
fn find_lname_todo() {
    let tmp = unique_temp_dir("fro-fp-find-lname");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("target.txt"), b"target").unwrap();
    std::os::unix::fs::symlink("target.txt", root.join("match-link")).unwrap();
    std::os::unix::fs::symlink("other.txt", root.join("other-link")).unwrap();
    assert_find_same_sorted(&root, &["-lname", "target*"], "find -lname target*");
}

#[test]
fn find_ilname_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ilname");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("target.txt"), b"target").unwrap();
    std::os::unix::fs::symlink("TARGET.TXT", root.join("upper-link")).unwrap();
    std::os::unix::fs::symlink("misc.bin", root.join("other-link")).unwrap();
    assert_find_same_sorted(&root, &["-ilname", "target*"], "find -ilname target*");
}

#[test]
fn find_wholename_todo() {
    let tmp = unique_temp_dir("fro-fp-find-wholename");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("match.txt"), b"match").unwrap();
    assert_find_same_sorted(&root, &["-wholename", "*/sub/*"], "find -wholename */sub/*");
}

#[test]
fn find_iwholename_todo() {
    let tmp = unique_temp_dir("fro-fp-find-iwholename");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("match.txt"), b"match").unwrap();
    assert_find_same_sorted(
        &root,
        &["-iwholename", "*/SUB/*"],
        "find -iwholename */SUB/*",
    );
}

// ── Filesystem / traversal control ──────────────────────────────────

#[test]
fn find_mindepth_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mindepth");
    let root = tmp.join("root");
    let level1 = root.join("a");
    let level2 = level1.join("b");
    fs::create_dir_all(&level2).unwrap();
    fs::write(level2.join("file.txt"), b"leaf").unwrap();
    assert_find_same_sorted(&root, &["-mindepth", "2"], "find -mindepth 2");
}

#[test]
fn find_depth_todo() {
    let tmp = unique_temp_dir("fro-fp-find-depth");
    let root = tmp.join("root");
    let level1 = root.join("a");
    let level2 = level1.join("b");
    fs::create_dir_all(&level2).unwrap();
    fs::write(level2.join("leaf.txt"), b"leaf").unwrap();
    assert_find_same_exact(&root, &["-depth"], "find -depth");
}

#[test]
fn find_mount_xdev_todo() {
    let tmp = unique_temp_dir("fro-fp-find-mount");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"file").unwrap();
    assert_find_same_sorted(&root, &["-mount", "-type", "f"], "find -mount -type f");
}

#[test]
fn find_xdev_todo() {
    let tmp = unique_temp_dir("fro-fp-find-xdev");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"file").unwrap();
    assert_find_same_sorted(&root, &["-xdev", "-type", "f"], "find -xdev -type f");
}

#[test]
fn find_noleaf_todo() {
    let tmp = unique_temp_dir("fro-fp-find-noleaf");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-noleaf", "-type", "f"], "find -noleaf");
}

#[test]
fn find_follow_todo() {
    let tmp = unique_temp_dir("fro-fp-find-follow");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"alpha").unwrap();
    symlink(&sub, root.join("linkdir")).unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_sorted_lines(
        run_fro("find", &[root_arg, "-follow", "-type", "f"]),
        run_system("find", &[root_arg, "-follow", "-type", "f"]),
        "find -follow -type f",
    );
}

#[test]
fn find_ignore_readdir_race_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ignore-race");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"alpha").unwrap();
    assert_find_same_sorted(
        &root,
        &["-ignore_readdir_race", "-type", "f"],
        "find -ignore_readdir_race",
    );
}

#[test]
fn find_noignore_readdir_race_todo() {
    let tmp = unique_temp_dir("fro-fp-find-noignore-race");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"alpha").unwrap();
    assert_find_same_sorted(
        &root,
        &[
            "-ignore_readdir_race",
            "-noignore_readdir_race",
            "-type",
            "f",
        ],
        "find -noignore_readdir_race",
    );
}

#[test]
fn find_fstype_todo() {
    let tmp = unique_temp_dir("fro-fp-find-fstype");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/file.txt"), b"alpha").unwrap();
    let fstype = current_fstype(&root);
    assert_find_same_sorted(&root, &["-fstype", &fstype], "find -fstype match");
    assert_find_same_exact(
        &root,
        &["-fstype", "definitely-not-a-real-fstype"],
        "find -fstype mismatch",
    );
}

#[test]
fn find_context_todo() {
    let tmp = unique_temp_dir("fro-fp-find-context");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("file.txt"), b"alpha").unwrap();
    if Path::new("/sys/fs/selinux/enforce").exists() {
        assert_find_same_sorted(&root, &["-context", "*"], "find -context *");
    } else {
        assert_find_same_exact(&root, &["-context", "*"], "find -context disabled");
    }
}

#[test]
fn find_xtype_todo() {
    let tmp = unique_temp_dir("fro-fp-find-xtype");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(root.join("target.txt"), b"target").unwrap();
    std::os::unix::fs::symlink("target.txt", root.join("link-to-file")).unwrap();
    std::os::unix::fs::symlink("sub", root.join("link-to-dir")).unwrap();
    std::os::unix::fs::symlink("missing", root.join("broken-link")).unwrap();
    assert_find_same_sorted(&root, &["-xtype", "f"], "find -xtype f");
    assert_find_same_sorted(&root, &["-xtype", "d"], "find -xtype d");
    assert_find_same_sorted(&root, &["-xtype", "l"], "find -xtype l");
}

// ── Constant predicates ──────────────────────────────────────────────

#[test]
fn find_true_todo() {
    let tmp = unique_temp_dir("fro-fp-find-true");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-true"], "find -true");
}

#[test]
fn find_false_todo() {
    let tmp = unique_temp_dir("fro-fp-find-false");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("owned.txt"), b"owned").unwrap();
    assert_find_same_sorted(&root, &["-false"], "find -false");
}
