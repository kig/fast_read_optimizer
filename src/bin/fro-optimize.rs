use std::collections::BTreeMap;
use std::process::Command;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct IOParams {
    num_threads: u64,
    block_size: u64,
    qd: usize,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct ModeParams {
    direct: IOParams,
    page_cache: IOParams,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct DeviceDbParams {
    read: ModeParams,
    grep: ModeParams,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct DeviceDb {
    version: u32,
    profiles: Vec<DeviceDbProfile>,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct DeviceDbProfile {
    id: String,
    #[serde(rename = "match")]
    m: DeviceDbMatch,
    params: DeviceDbParams,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(serde::Deserialize, Clone, Debug, Default)]
struct DeviceDbMatch {
    #[serde(default)]
    fstype: Option<String>,
    #[serde(default)]
    dev_kind: Option<String>,
    #[serde(default)]
    dev_model_contains: Option<String>,
    #[serde(default)]
    md_level: Option<String>,
}

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
struct MountInfoEntry {
    mount_point: String,
    fstype: String,
    mount_source: String,
    major_minor: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    writable_dir: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    device_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_serial: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    device_by_id: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pci_bdf: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pci_numa_node: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pci_local_cpulist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pcie_current_link_width: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pcie_current_link_speed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pcie_max_link_width: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pcie_max_link_speed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aer_dev_correctable: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aer_dev_nonfatal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aer_dev_fatal: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    zfs_pool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    zfs_dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    zfs_props: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    zpool_vdevs: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    md_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    md_chunk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    md_members: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    device_db_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_db_read_direct: Option<IOParams>,
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

fn read_sysfs_trimmed(path: &Path) -> Option<String> {
    let s = fs::read_to_string(path).ok()?;
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn is_pci_bdf(s: &str) -> bool {
    // 0000:00:00.0
    let b = s.as_bytes();
    if b.len() != 12 {
        return false;
    }
    let hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F');
    hex(b[0])
        && hex(b[1])
        && hex(b[2])
        && hex(b[3])
        && b[4] == b':'
        && hex(b[5])
        && hex(b[6])
        && b[7] == b':'
        && hex(b[8])
        && hex(b[9])
        && b[10] == b'.'
        && matches!(b[11], b'0'..=b'7')
}

fn pci_bdf_from_sysfs_path(p: &Path) -> Option<String> {
    for anc in p.ancestors() {
        let name = anc.file_name()?.to_string_lossy();
        if is_pci_bdf(&name) {
            return Some(name.to_string());
        }
    }
    None
}

fn base_block_name_from_devpath(devpath: &Path) -> Option<String> {
    let canon = fs::canonicalize(devpath).ok()?;
    let name = canon.file_name()?.to_string_lossy().to_string();

    let sys = Path::new("/sys/class/block").join(&name);
    if sys.join("partition").exists() {
        // Follow to real sysfs path and take parent basename (e.g. nvme0n1p1 -> nvme0n1).
        let real = fs::read_link(&sys).ok()?;
        let real_abs = if real.is_absolute() {
            real
        } else {
            Path::new("/sys/class/block").join(real)
        };
        let parent = real_abs.parent()?;
        return Some(parent.file_name()?.to_string_lossy().to_string());
    }

    Some(name)
}

fn dev_by_id_for_block_name(block: &str) -> Vec<String> {
    let mut out = Vec::new();
    let dir = Path::new("/dev/disk/by-id");
    let rd = match fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return out,
    };

    for ent in rd.flatten() {
        let name = ent.file_name().to_string_lossy().to_string();
        let link = match fs::read_link(ent.path()) {
            Ok(p) => p,
            Err(_) => continue,
        };
        // Resolve ../../nvme0n1 -> /dev/nvme0n1
        let target = if link.is_absolute() {
            link
        } else {
            dir.join(link)
        };
        let canon = match fs::canonicalize(&target) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if canon.file_name().map(|n| n == block).unwrap_or(false) {
            out.push(name);
        }
    }

    out.sort();
    out
}

fn run_cmd_stdout(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout);
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn parse_zfs_get_props(out: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for line in out.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (k, v) = match line.split_once('\t') {
            Some(p) => p,
            None => continue,
        };
        let k = k.trim();
        let v = v.trim();
        if !k.is_empty() && !v.is_empty() {
            m.insert(k.to_string(), v.to_string());
        }
    }
    m
}

fn zfs_get_props(dataset: &str) -> Option<BTreeMap<String, String>> {
    // Keep this small/structured; callers can always run `zfs get all` themselves.
    let props = "recordsize,compression,compressratio,atime,sync,primarycache,secondarycache,logbias";
    let out = run_cmd_stdout("zfs", &["get", "-Hp", "-o", "property,value", props, dataset])?;
    let m = parse_zfs_get_props(&out);
    if m.is_empty() {
        None
    } else {
        Some(m)
    }
}

fn parse_zpool_status_vdevs(out: &str) -> Vec<String> {
    let mut vdevs = Vec::new();
    for line in out.lines() {
        let t = line.trim_start();
        let first = t.split_whitespace().next().unwrap_or("");
        if first.starts_with("/dev/") {
            vdevs.push(first.to_string());
        }
    }
    vdevs.sort();
    vdevs.dedup();
    vdevs
}

fn zpool_get_vdevs(pool: &str) -> Vec<String> {
    let out = match run_cmd_stdout("zpool", &["status", "-P", pool]) {
        Some(s) => s,
        None => return Vec::new(),
    };
    parse_zpool_status_vdevs(&out)
}

fn fill_device_info(e: &mut MountInfoEntry) {
    let src = Path::new(&e.mount_source);
    if !e.mount_source.starts_with("/dev/") {
        // Non-/dev sources are still useful, but we generally can't map them to a single block
        // device without filesystem-specific tooling.
        e.device_kind = Some(e.fstype.clone());
        e.device_name = Some(e.mount_source.clone());

        if e.fstype == "zfs" {
            e.zfs_dataset = Some(e.mount_source.clone());
            e.zfs_pool = e.mount_source.split('/').next().map(|s| s.to_string());
            if let Some(ref ds) = e.zfs_dataset {
                e.zfs_props = zfs_get_props(ds);
            }
            if let Some(ref pool) = e.zfs_pool {
                e.zpool_vdevs = zpool_get_vdevs(pool);
            }
            e.signature = Some(format!("fstype=zfs;dataset={}", e.mount_source));
        } else {
            e.signature = Some(format!("fstype={};source={}", e.fstype, e.mount_source));
        }
        return;
    }

    let base = match base_block_name_from_devpath(src) {
        Some(b) => b,
        None => return,
    };

    e.device_name = Some(base.clone());
    let kind = if base.starts_with("nvme") {
        "nvme"
    } else if base.starts_with("md") {
        "md"
    } else if base.starts_with("dm-") {
        "dm"
    } else if base.starts_with("sd") {
        "sd"
    } else {
        "block"
    };
    e.device_kind = Some(kind.to_string());

    let sys = Path::new("/sys/class/block").join(&base);
    e.device_model = read_sysfs_trimmed(&sys.join("device/model"));
    e.device_serial = read_sysfs_trimmed(&sys.join("device/serial"));
    e.device_by_id = dev_by_id_for_block_name(&base);

    // PCIe/NUMA/AER info (best-effort). For partitions, sys/device points at the same parent chain.
    if let Ok(devlink) = fs::read_link(sys.join("device")) {
        let devpath = if devlink.is_absolute() {
            devlink
        } else {
            sys.join(devlink)
        };
        let devpath = fs::canonicalize(devpath).unwrap_or_else(|_| sys.join("device"));
        if let Some(bdf) = pci_bdf_from_sysfs_path(&devpath) {
            e.pci_bdf = Some(bdf.clone());
            let pcidir = Path::new("/sys/bus/pci/devices").join(&bdf);
            e.pci_numa_node = read_sysfs_trimmed(&pcidir.join("numa_node")).and_then(|s| s.parse().ok());
            e.pci_local_cpulist = read_sysfs_trimmed(&pcidir.join("local_cpulist"));
            e.pcie_current_link_width = read_sysfs_trimmed(&pcidir.join("current_link_width"));
            e.pcie_current_link_speed = read_sysfs_trimmed(&pcidir.join("current_link_speed"));
            e.pcie_max_link_width = read_sysfs_trimmed(&pcidir.join("max_link_width"));
            e.pcie_max_link_speed = read_sysfs_trimmed(&pcidir.join("max_link_speed"));
            e.aer_dev_correctable = read_sysfs_trimmed(&pcidir.join("aer_dev_correctable"));
            e.aer_dev_nonfatal = read_sysfs_trimmed(&pcidir.join("aer_dev_nonfatal"));
            e.aer_dev_fatal = read_sysfs_trimmed(&pcidir.join("aer_dev_fatal"));
        }
    }

    if kind == "md" {
        e.md_level = read_sysfs_trimmed(&sys.join("md/level"));
        e.md_chunk_bytes = read_sysfs_trimmed(&sys.join("md/chunk_size")).and_then(|s| s.parse().ok());

        let slaves_dir = sys.join("slaves");
        if let Ok(rd) = fs::read_dir(slaves_dir) {
            for ent in rd.flatten() {
                let slave = ent.file_name().to_string_lossy().to_string();
                e.md_members.push(slave);
            }
        }
        e.md_members.sort();
    }

    // Human-readable signature used for device-db matching.
    let model = e.device_model.clone().unwrap_or_else(|| "unknown".into());
    let md_level = e.md_level.clone().unwrap_or_else(|| "".into());
    let sig = match kind {
        "md" if !md_level.is_empty() => format!("fstype={};dev=md;level={};model={}", e.fstype, md_level, model),
        _ => format!("fstype={};dev={};model={}", e.fstype, kind, model),
    };
    e.signature = Some(sig);
}

fn load_device_db() -> Option<DeviceDb> {
    let p = std::env::var("FRO_DEVICE_DB").ok().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("fro-device-db.json"));
    let data = fs::read_to_string(p).ok()?;
    let db: DeviceDb = serde_json::from_str(&data).ok()?;
    if db.version != 1 {
        return None;
    }
    Some(db)
}

fn device_db_match<'a>(db: &'a DeviceDb, e: &MountInfoEntry) -> Option<&'a DeviceDbProfile> {
    for p in &db.profiles {
        if let Some(ref f) = p.m.fstype {
            if &e.fstype != f {
                continue;
            }
        }
        if let Some(ref k) = p.m.dev_kind {
            if e.device_kind.as_deref() != Some(k.as_str()) {
                continue;
            }
        }
        if let Some(ref ml) = p.m.md_level {
            if e.md_level.as_deref() != Some(ml.as_str()) {
                continue;
            }
        }
        if let Some(ref mc) = p.m.dev_model_contains {
            if !e.device_model.as_deref().unwrap_or("").contains(mc) {
                continue;
            }
        }
        return Some(p);
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
            device_kind: None,
            device_name: None,
            device_model: None,
            device_serial: None,
            device_by_id: Vec::new(),
            pci_bdf: None,
            pci_numa_node: None,
            pci_local_cpulist: None,
            pcie_current_link_width: None,
            pcie_current_link_speed: None,
            pcie_max_link_width: None,
            pcie_max_link_speed: None,
            aer_dev_correctable: None,
            aer_dev_nonfatal: None,
            aer_dev_fatal: None,
            zfs_pool: None,
            zfs_dataset: None,
            zfs_props: None,
            zpool_vdevs: Vec::new(),
            md_level: None,
            md_chunk_bytes: None,
            md_members: Vec::new(),
            signature: None,
            device_db_profile: None,
            device_db_read_direct: None,
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
                device_kind: None,
                device_name: None,
                device_model: None,
                device_serial: None,
                device_by_id: Vec::new(),
                pci_bdf: None,
                pci_numa_node: None,
                pci_local_cpulist: None,
                pcie_current_link_width: None,
                pcie_current_link_speed: None,
                pcie_max_link_width: None,
                pcie_max_link_speed: None,
                aer_dev_correctable: None,
                aer_dev_nonfatal: None,
                aer_dev_fatal: None,
                zfs_pool: None,
                zfs_dataset: None,
                zfs_props: None,
                zpool_vdevs: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                signature: None,
                device_db_profile: None,
                device_db_read_direct: None,
            },
            MountInfoEntry {
                mount_point: "/run".into(),
                fstype: "tmpfs".into(),
                mount_source: "tmpfs".into(),
                major_minor: "0:5".into(),
                writable_dir: None,
                device_kind: None,
                device_name: None,
                device_model: None,
                device_serial: None,
                device_by_id: Vec::new(),
                pci_bdf: None,
                pci_numa_node: None,
                pci_local_cpulist: None,
                pcie_current_link_width: None,
                pcie_current_link_speed: None,
                pcie_max_link_width: None,
                pcie_max_link_speed: None,
                aer_dev_correctable: None,
                aer_dev_nonfatal: None,
                aer_dev_fatal: None,
                zfs_pool: None,
                zfs_dataset: None,
                zfs_props: None,
                zpool_vdevs: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                signature: None,
                device_db_profile: None,
                device_db_read_direct: None,
            },
            MountInfoEntry {
                mount_point: "/snap/core".into(),
                fstype: "squashfs".into(),
                mount_source: "/dev/loop0".into(),
                major_minor: "7:0".into(),
                writable_dir: None,
                device_kind: None,
                device_name: None,
                device_model: None,
                device_serial: None,
                device_by_id: Vec::new(),
                pci_bdf: None,
                pci_numa_node: None,
                pci_local_cpulist: None,
                pcie_current_link_width: None,
                pcie_current_link_speed: None,
                pcie_max_link_width: None,
                pcie_max_link_speed: None,
                aer_dev_correctable: None,
                aer_dev_nonfatal: None,
                aer_dev_fatal: None,
                zfs_pool: None,
                zfs_dataset: None,
                zfs_props: None,
                zpool_vdevs: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                signature: None,
                device_db_profile: None,
                device_db_read_direct: None,
            },
            MountInfoEntry {
                mount_point: "/var/lib/data".into(),
                fstype: "xfs".into(),
                mount_source: "/dev/md0".into(),
                major_minor: "9:0".into(),
                writable_dir: None,
                device_kind: None,
                device_name: None,
                device_model: None,
                device_serial: None,
                device_by_id: Vec::new(),
                pci_bdf: None,
                pci_numa_node: None,
                pci_local_cpulist: None,
                pcie_current_link_width: None,
                pcie_current_link_speed: None,
                pcie_max_link_width: None,
                pcie_max_link_speed: None,
                aer_dev_correctable: None,
                aer_dev_nonfatal: None,
                aer_dev_fatal: None,
                zfs_pool: None,
                zfs_dataset: None,
                zfs_props: None,
                zpool_vdevs: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                signature: None,
                device_db_profile: None,
                device_db_read_direct: None,
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
            device_kind: None,
            device_name: None,
            device_model: None,
            device_serial: None,
            device_by_id: Vec::new(),
            pci_bdf: None,
            pci_numa_node: None,
            pci_local_cpulist: None,
            pcie_current_link_width: None,
            pcie_current_link_speed: None,
            pcie_max_link_width: None,
            pcie_max_link_speed: None,
            aer_dev_correctable: None,
            aer_dev_nonfatal: None,
            aer_dev_fatal: None,
            zfs_pool: None,
            zfs_dataset: None,
            zfs_props: None,
            zpool_vdevs: Vec::new(),
            md_level: None,
            md_chunk_bytes: None,
            md_members: Vec::new(),
            signature: None,
            device_db_profile: None,
            device_db_read_direct: None,
        };
        assert!(is_disk_backed_mount(&z));
    }

    #[test]
    fn device_db_matches_ext4_mdraid0() {
        let db: DeviceDb = serde_json::from_str(include_str!("../../fro-device-db.json")).unwrap();
        let mut e = MountInfoEntry {
            mount_point: "/data".into(),
            fstype: "ext4".into(),
            mount_source: "/dev/md127".into(),
            major_minor: "9:127".into(),
            writable_dir: None,
            device_kind: Some("md".into()),
            device_name: Some("md127".into()),
            device_model: Some("unknown".into()),
            device_serial: None,
            device_by_id: Vec::new(),
            pci_bdf: None,
            pci_numa_node: None,
            pci_local_cpulist: None,
            pcie_current_link_width: None,
            pcie_current_link_speed: None,
            pcie_max_link_width: None,
            pcie_max_link_speed: None,
            aer_dev_correctable: None,
            aer_dev_nonfatal: None,
            aer_dev_fatal: None,
            zfs_pool: None,
            zfs_dataset: None,
            zfs_props: None,
            zpool_vdevs: Vec::new(),
            md_level: Some("raid0".into()),
            md_chunk_bytes: None,
            md_members: Vec::new(),
            signature: Some("fstype=ext4;dev=md;level=raid0;model=unknown".into()),
            device_db_profile: None,
            device_db_read_direct: None,
        };

        let p = device_db_match(&db, &e).unwrap();
        assert_eq!(p.id, "example-ext4-mdraid0");
        e.device_db_profile = Some(p.id.clone());
        e.device_db_read_direct = Some(p.params.read.direct.clone());
        assert_eq!(e.device_db_read_direct.unwrap().block_size, 3145728);
    }

    #[test]
    fn parse_zfs_get_props_parses_tab_separated_pairs() {
        let out = "recordsize\t128K\ncompression\toff\ncompressratio\t1.00x\n";
        let m = parse_zfs_get_props(out);
        assert_eq!(m.get("recordsize").unwrap(), "128K");
        assert_eq!(m.get("compression").unwrap(), "off");
        assert_eq!(m.get("compressratio").unwrap(), "1.00x");
    }

    #[test]
    fn parse_zpool_status_vdevs_extracts_dev_paths() {
        let out = r#"
  pool: tank
 state: ONLINE
config:

	NAME	STATE	READ	WRITE	CKSUM
	tank	ONLINE	0	0	0
	  mirror-0	ONLINE	0	0	0
	    /dev/disk/by-id/nvme-SAMSUNG_PM1733_ABC	ONLINE	0	0	0
	    /dev/nvme1n1	ONLINE	0	0	0
"#;
        let v = parse_zpool_status_vdevs(out);
        assert_eq!(v, vec!["/dev/disk/by-id/nvme-SAMSUNG_PM1733_ABC", "/dev/nvme1n1"]);
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
        let db = load_device_db();

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

            fill_device_info(e);
            if let Some(ref db) = db {
                if let Some(p) = device_db_match(db, e) {
                    e.device_db_profile = Some(p.id.clone());
                    e.device_db_read_direct = Some(p.params.read.direct.clone());
                }
            }
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
