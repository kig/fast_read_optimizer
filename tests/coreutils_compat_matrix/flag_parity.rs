//! Find-only TODO coverage stubs plus helper predicates for bounded find parity work.
//!
//! Layout
//! ──────
//! 1. local helper routines reused by find TODO coverage probes
//! 2. `#[ignore = "TODO"]` stubs for remaining find system-only flags (69 items)

use super::*;
use std::ffi::CStr;
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
// §1  find: TODO stubs for remaining system-only flags (69 items)
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
#[ignore = "TODO: find -Olevel (optimisation level) not yet implemented in fro bounded find slice"]
fn find_optimise_level_todo() {}

// ── Symlink-traversal top-level flags ───────────────────────────────

#[test]
#[ignore = "TODO: find -H (follow symlinks only for command-line args) not yet implemented in fro bounded find slice"]
fn find_h_symlink_todo() {}

#[test]
#[ignore = "TODO: find -L (follow all symlinks) not yet implemented in fro bounded find slice"]
fn find_l_symlink_todo() {}

#[test]
#[ignore = "TODO: find -N (ignore read errors on symlinks) not yet implemented in fro bounded find slice"]
fn find_n_symlink_todo() {}

#[test]
#[ignore = "TODO: find -P (never follow symlinks, default) not yet implemented as explicit flag in fro bounded find slice"]
fn find_p_symlink_todo() {}

// ── Boolean combinators ─────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -a / -and (explicit AND operator) not yet implemented in fro bounded find slice"]
fn find_and_todo() {}

#[test]
#[ignore = "TODO: find -o (OR operator) not yet implemented in fro bounded find slice"]
fn find_or_short_todo() {}

#[test]
#[ignore = "TODO: find -or (long OR operator) not yet implemented in fro bounded find slice"]
fn find_or_long_todo() {}

#[test]
#[ignore = "TODO: find -not (NOT operator) not yet implemented in fro bounded find slice"]
fn find_not_todo() {}

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
#[ignore = "TODO: find -daystart not yet implemented in fro bounded find slice"]
fn find_daystart_todo() {}

#[test]
#[ignore = "TODO: find -used not yet implemented in fro bounded find slice"]
fn find_used_todo() {}

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
#[ignore = "TODO: find -regex not yet implemented in fro bounded find slice"]
fn find_regex_todo() {}

#[test]
#[ignore = "TODO: find -iregex not yet implemented in fro bounded find slice"]
fn find_iregex_todo() {}

#[test]
#[ignore = "TODO: find -regextype not yet implemented in fro bounded find slice"]
fn find_regextype_todo() {}

#[test]
#[ignore = "TODO: find -lname not yet implemented in fro bounded find slice"]
fn find_lname_todo() {}

#[test]
#[ignore = "TODO: find -ilname not yet implemented in fro bounded find slice"]
fn find_ilname_todo() {}

#[test]
#[ignore = "TODO: find -wholename not yet implemented in fro bounded find slice"]
fn find_wholename_todo() {}

#[test]
#[ignore = "TODO: find -iwholename not yet implemented in fro bounded find slice"]
fn find_iwholename_todo() {}

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
#[ignore = "TODO: find -depth not yet implemented in fro bounded find slice"]
fn find_depth_todo() {}

#[test]
#[ignore = "TODO: find -mount / -xdev not yet implemented in fro bounded find slice"]
fn find_mount_xdev_todo() {}

#[test]
#[ignore = "TODO: find -xdev not yet implemented in fro bounded find slice"]
fn find_xdev_todo() {}

#[test]
#[ignore = "TODO: find -noleaf not yet implemented in fro bounded find slice"]
fn find_noleaf_todo() {}

#[test]
#[ignore = "TODO: find -follow not yet implemented in fro bounded find slice"]
fn find_follow_todo() {}

#[test]
#[ignore = "TODO: find -ignore_readdir_race not yet implemented in fro bounded find slice"]
fn find_ignore_readdir_race_todo() {}

#[test]
#[ignore = "TODO: find -noignore_readdir_race not yet implemented in fro bounded find slice"]
fn find_noignore_readdir_race_todo() {}

#[test]
#[ignore = "TODO: find -fstype not yet implemented in fro bounded find slice"]
fn find_fstype_todo() {}

#[test]
#[ignore = "TODO: find -context (SELinux) not yet implemented in fro bounded find slice"]
fn find_context_todo() {}

#[test]
#[ignore = "TODO: find -xtype not yet implemented in fro bounded find slice"]
fn find_xtype_todo() {}

// ── Action predicates ────────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -exec not yet implemented in fro bounded find slice"]
fn find_exec_todo() {}

#[test]
#[ignore = "TODO: find -execdir not yet implemented in fro bounded find slice"]
fn find_execdir_todo() {}

#[test]
#[ignore = "TODO: find -ok not yet implemented in fro bounded find slice"]
fn find_ok_todo() {}

#[test]
#[ignore = "TODO: find -okdir not yet implemented in fro bounded find slice"]
fn find_okdir_todo() {}

#[test]
#[ignore = "TODO: find -delete not yet implemented in fro bounded find slice"]
fn find_delete_todo() {}

#[test]
#[ignore = "TODO: find -ls not yet implemented in fro bounded find slice"]
fn find_ls_todo() {}

#[test]
#[ignore = "TODO: find -fls not yet implemented in fro bounded find slice"]
fn find_fls_todo() {}

#[test]
#[ignore = "TODO: find -printf not yet implemented in fro bounded find slice"]
fn find_printf_todo() {}

#[test]
#[ignore = "TODO: find -fprintf not yet implemented in fro bounded find slice"]
fn find_fprintf_todo() {}

#[test]
#[ignore = "TODO: find -fprint not yet implemented in fro bounded find slice"]
fn find_fprint_todo() {}

#[test]
#[ignore = "TODO: find -fprint0 not yet implemented in fro bounded find slice"]
fn find_fprint0_todo() {}

#[test]
#[ignore = "TODO: find -prune not yet implemented in fro bounded find slice"]
fn find_prune_todo() {}

#[test]
#[ignore = "TODO: find -quit not yet implemented in fro bounded find slice"]
fn find_quit_todo() {}

// ── Constant predicates ──────────────────────────────────────────────

#[test]
#[ignore = "TODO: find -true not yet implemented in fro bounded find slice"]
fn find_true_todo() {}

#[test]
#[ignore = "TODO: find -false not yet implemented in fro bounded find slice"]
fn find_false_todo() {}
