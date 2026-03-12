use std::process::Command;
use std::env;
use std::fs;

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct MountInfoEntry {
    mount_point: String,
    fstype: String,
    mount_source: String,
    major_minor: String,
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

    // Most disk-backed mounts have a /dev/* source; keep others out by default.
    if e.mount_source.starts_with("/dev/") {
        return true;
    }

    false
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
            },
            MountInfoEntry {
                mount_point: "/run".into(),
                fstype: "tmpfs".into(),
                mount_source: "tmpfs".into(),
                major_minor: "0:5".into(),
            },
            MountInfoEntry {
                mount_point: "/snap/core".into(),
                fstype: "squashfs".into(),
                mount_source: "/dev/loop0".into(),
                major_minor: "7:0".into(),
            },
            MountInfoEntry {
                mount_point: "/var/lib/data".into(),
                fstype: "xfs".into(),
                mount_source: "/dev/md0".into(),
                major_minor: "9:0".into(),
            },
        ];

        let kept: Vec<_> = entries.into_iter().filter(|e| is_disk_backed_mount(e)).collect();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].mount_point, "/");
        assert_eq!(kept[1].mount_point, "/var/lib/data");
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
        let entries = read_mountinfo();
        let entries = if all {
            entries
        } else {
            entries
                .into_iter()
                .filter(|e| is_disk_backed_mount(e))
                .collect()
        };
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
