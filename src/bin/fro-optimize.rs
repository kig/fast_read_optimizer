use std::process::Command;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct MountInfoEntry {
    mount_point: String,
    fstype: String,
    mount_source: String,
    major_minor: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    writable_dir: Option<String>,
}

fn is_disk_backed_mount(e: &MountInfoEntry) -> bool {
    // Heuristic filter: default output should focus on mounts that are likely backed by real disks.
    // Keep this conservative and add an escape hatch via --list-devices-all.
    let fs = e.fstype.as_str();

    // Common virtual/pseudo filesystems and ephemeral mounts.
    const EXCLUDE_FS: &[&str] = &[
        "proc",
        "sysfs",
        "devtmpfs",
        "devpts",
        "tmpfs",
        "ramfs",
        "cgroup",
        "cgroup2",
        "pstore",
        "debugfs",
        "tracefs",
        "securityfs",
        "bpf",
        "mqueue",
        "hugetlbfs",
        "configfs",
        "fusectl",
        "autofs",
        "rpc_pipefs",
        "binfmt_misc",
        "efivarfs",
        // Snap/immutable images.
        "squashfs",
        // Container overlay; often not meaningful for physical device tuning.
        "overlay",
        // Network/remote.
        "nfs",
        "nfs4",
        "cifs",
        "smb3",
        "ceph",
        "glusterfs",
    ];
    if EXCLUDE_FS.contains(&fs) {
        return false;
    }

    let mp = e.mount_point.as_str();
    if mp == "/" {
        return true;
    }

    // Common non-disk mount roots.
    const EXCLUDE_MP_PREFIX: &[&str] = &["/proc", "/sys", "/dev", "/run", "/snap", "/var/lib/snapd"];
    if EXCLUDE_MP_PREFIX.iter().any(|p| mp.starts_with(p)) {
        return false;
    }

    // Most disk-backed mounts have a /dev/* source, but some (e.g. zfs) don't.
    if e.mount_source.starts_with("/dev/") {
        return true;
    }

    matches!(fs, "zfs" | "btrfs")
}

fn is_user_writable_dir(p: &Path) -> bool {
    if !p.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c = match CString::new(p.as_os_str().as_bytes()) {
            Ok(v) => v,
            Err(_) => return false,
        };
        unsafe { libc::access(c.as_ptr(), libc::W_OK | libc::X_OK) == 0 }
    }
    #[cfg(not(unix))]
    {
        // Best-effort fallback.
        fs::metadata(p).is_ok()
    }
}

fn collect_home_targets() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let home = match std::env::var("HOME").ok() {
        Some(h) => PathBuf::from(h),
        None => return out,
    };
    out.push(home.clone());

    if let Ok(rd) = fs::read_dir(&home) {
        for ent in rd.flatten() {
            if let Ok(ft) = ent.file_type() {
                if !ft.is_symlink() {
                    continue;
                }
            }

            let link = match fs::read_link(ent.path()) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let target = if link.is_absolute() {
                link
            } else {
                home.join(link)
            };
            let canon = fs::canonicalize(&target).unwrap_or(target);
            out.push(canon);
        }
    }

    out
}

fn find_writable_dir_for_mount(mount_point: &Path, home_targets: &[PathBuf]) -> Option<PathBuf> {
    // 1) Prefer any $HOME symlink targets that land on this mount.
    for t in home_targets {
        if t.starts_with(mount_point) && is_user_writable_dir(t) {
            return Some(t.clone());
        }
    }

    // 2) Common shallow candidates.
    let user = std::env::var("USER").ok();
    let mut candidates: Vec<PathBuf> = vec![mount_point.to_path_buf()];
    if let Some(u) = user.as_deref() {
        candidates.push(mount_point.join(u));
        candidates.push(mount_point.join("home").join(u));
        candidates.push(mount_point.join("users").join(u));
    }
    candidates.push(mount_point.join("tmp"));
    candidates.push(mount_point.join("scratch"));
    candidates.push(mount_point.join("data"));

    for c in candidates {
        if is_user_writable_dir(&c) {
            return Some(c);
        }
    }

    // 3) Bounded breadth-first search for a writable dir.
    let max_depth: usize = 4;
    let max_dirs: usize = 2000;
    let mut q: std::collections::VecDeque<(PathBuf, usize)> = std::collections::VecDeque::new();
    q.push_back((mount_point.to_path_buf(), 0));

    let mut seen = 0usize;
    while let Some((dir, depth)) = q.pop_front() {
        if depth > max_depth {
            continue;
        }
        if seen >= max_dirs {
            break;
        }
        seen += 1;

        if depth > 0 && is_user_writable_dir(&dir) {
            return Some(dir);
        }

        let rd = match fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for ent in rd.flatten() {
            let ft = match ent.file_type() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if !ft.is_dir() || ft.is_symlink() {
                continue;
            }
            let name = ent.file_name();
            if let Some(s) = name.to_str() {
                if s.starts_with('.') {
                    continue;
                }
                if s == "proc" || s == "sys" || s == "dev" || s == "run" {
                    continue;
                }
            }
            q.push_back((ent.path(), depth + 1));
        }
    }

    None
}

fn read_mountinfo() -> Vec<MountInfoEntry> {
    let data = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mut out = Vec::new();
    for line in data.lines() {
        let (lhs, rhs) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };

        let left_fields: Vec<&str> = lhs.split_whitespace().collect();
        if left_fields.len() < 5 {
            continue;
        }
        let major_minor = left_fields[2].to_string();
        let mount_point = left_fields[4].to_string();

        let right_fields: Vec<&str> = rhs.split_whitespace().collect();
        if right_fields.len() < 3 {
            continue;
        }
        let fstype = right_fields[0].to_string();
        let mount_source = right_fields[1].to_string();

        out.push(MountInfoEntry {
            mount_point,
            fstype,
            mount_source,
            major_minor,
            writable_dir: None,
        });
    }

    out.sort_by(|a, b| a.mount_point.cmp(&b.mount_point));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_devices_filters_pseudo_and_snap_fs() {
        let entries = vec![
            MountInfoEntry {
                mount_point: "/".into(),
                fstype: "ext4".into(),
                mount_source: "/dev/nvme0n1p2".into(),
                major_minor: "259:2".into(),
                writable_dir: None,
            },
            MountInfoEntry {
                mount_point: "/run".into(),
                fstype: "tmpfs".into(),
                mount_source: "tmpfs".into(),
                major_minor: "0:5".into(),
                writable_dir: None,
            },
            MountInfoEntry {
                mount_point: "/snap/core".into(),
                fstype: "squashfs".into(),
                mount_source: "/dev/loop0".into(),
                major_minor: "7:0".into(),
                writable_dir: None,
            },
            MountInfoEntry {
                mount_point: "/var/lib/data".into(),
                fstype: "xfs".into(),
                mount_source: "/dev/md0".into(),
                major_minor: "9:0".into(),
                writable_dir: None,
            },
        ];

        let kept: Vec<_> = entries.into_iter().filter(|e| is_disk_backed_mount(e)).collect();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].mount_point, "/");
        assert_eq!(kept[1].mount_point, "/var/lib/data");
    }

    #[test]
    fn list_devices_allows_zfs_without_dev_mount_source() {
        let z = MountInfoEntry {
            mount_point: "/tank".into(),
            fstype: "zfs".into(),
            mount_source: "tank/dataset".into(),
            major_minor: "0:0".into(),
            writable_dir: None,
        };
        assert!(is_disk_backed_mount(&z));
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--list-devices | --list-devices-all] [--test-dir path] <test_prefix ...>",
            args[0]
        );
        println!("\n--list-devices prints likely disk-backed mountpoints as JSON and exits.");
        println!("--list-devices-all prints all mountpoints from /proc/self/mountinfo as JSON and exits.");
        std::process::exit(0);
    }
    if args.len() > 1 && (args[1] == "--list-devices" || args[1] == "--list-devices-all") {
        let all = args[1] == "--list-devices-all";
        let home_targets = collect_home_targets();

        let entries = read_mountinfo();
        let mut entries: Vec<_> = if all {
            entries
        } else {
            entries.into_iter().filter(|e| is_disk_backed_mount(e)).collect()
        };

        for e in entries.iter_mut() {
            let mp = Path::new(&e.mount_point);
            let wd = find_writable_dir_for_mount(mp, &home_targets);
            e.writable_dir = wd.map(|p| p.display().to_string());
        }

        let out = serde_json::to_string_pretty(&entries).unwrap_or_else(|_| "[]".to_string());
        println!("{}", out);
        return;
    }

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else {
            patterns.push(arg);
        }
    }

    let mut fro_exe = env::current_exe().expect("Failed to get current executable path");
    fro_exe.set_file_name("fro");

    let test_path = std::path::Path::new(test_dir);

    let source_file = test_path.join("fro_bench_tmp_source").display().to_string();
    let target_file_dir = test_path.join("fro_bench_tmp_direct").display().to_string();
    let target_file_cache = test_path.join("fro_bench_tmp_cache").display().to_string();

    // Setup temp files
    if let Ok(f) = fs::File::create(&source_file) { let _ = f.set_len(4 * 1024 * 1024 * 1024); }
    let _ = Command::new(&fro_exe).args(["write", &source_file]).status().expect("Failed to write fro_bench_tmp_source");
    let _ = Command::new(&fro_exe).args(["copy", &source_file, &target_file_dir]).status().expect("Failed to write fro_bench_tmp_direct");
    let _ = Command::new(&fro_exe).args(["copy", &source_file, &target_file_cache]).status().expect("Failed to write fro_bench_tmp_cache");

    let configs = vec![
        vec!["read", "-s", "--direct", "-n", "100", &source_file],
        vec!["read", "-s", "--no-direct", "-n", "100", &source_file],
        vec!["grep", "-s", "--direct", "-n", "100", "needle", &source_file],
        vec!["grep", "-s", "--no-direct", "-n", "100", "needle", &source_file],
        vec!["write", "-s", "--direct", "-n", "20", &target_file_dir],
        vec!["write", "-s", "--no-direct", "-n", "20", &target_file_cache],
        vec!["copy", "-s", "--direct", "-n", "20", &source_file, &target_file_dir],
        vec!["copy", "-s", "--no-direct", "-n", "20", &source_file, &target_file_cache],
        vec!["diff", "-s", "--direct", "-n", "100", &source_file, &target_file_dir],
        vec!["diff", "-s", "--no-direct", "-n", "100", &source_file, &target_file_cache],
        vec!["dual-read-bench", "-s", "--direct", "-n", "100", &source_file, &target_file_dir],
        vec!["dual-read-bench", "-s", "--no-direct", "-n", "100", &source_file, &target_file_cache],
    ];

    println!("Running optimizer for all modes... (This may take a while)");

    for args in configs {
        if patterns.len() > 0 {
            let mut pattern_found = false;
            for p in patterns.iter() {
                if args[0].starts_with(*p) {
                    pattern_found = true;
                    break;
                }
            }
            if !pattern_found { continue; }
        }
        println!("Optimizing: fro {:?}", args);
        let status = Command::new(&fro_exe)
            .args(&args)
            .status()
            .expect("Failed to execute process");

        if !status.success() {
            eprintln!("Warning: Optimization failed for {:?}", args);
        }
    }

    let _ = fs::remove_file(source_file);
    let _ = fs::remove_file(target_file_dir);
    let _ = fs::remove_file(target_file_cache);

    println!("Optimization complete! Results saved to fro.json");
}
