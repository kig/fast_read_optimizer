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
    #[allow(dead_code)]
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

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq, Default)]
struct DeviceLeafInfo {
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    devnode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    by_id_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    serial: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    by_id: Vec<String>,

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

    // ZFS-only fields (when parsed from `zpool status`)
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    was: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    vdev_path: Vec<String>,
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
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    zpool_vdevs_info: Vec<DeviceLeafInfo>,

    #[serde(skip_serializing_if = "Option::is_none")]
    md_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    md_chunk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    md_members: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    md_members_info: Vec<DeviceLeafInfo>,

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

fn dev_by_id_for_devnode(devnode: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let canon_dev = match fs::canonicalize(devnode) {
        Ok(p) => p,
        Err(_) => return out,
    };
    let dev_basename = match canon_dev.file_name() {
        Some(n) => n.to_string_lossy().to_string(),
        None => return out,
    };

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
        let target = if link.is_absolute() { link } else { dir.join(link) };
        let canon = match fs::canonicalize(&target) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if canon
            .file_name()
            .map(|n| n == std::ffi::OsStr::new(&dev_basename))
            .unwrap_or(false)
        {
            out.push(name);
        }
    }

    out.sort();
    out
}

fn enrich_device_leaf_from_devnode(leaf: &mut DeviceLeafInfo, devnode: &Path) {
    leaf.devnode = Some(devnode.display().to_string());
    leaf.by_id = dev_by_id_for_devnode(devnode);

    // `model`/`serial` live on the base block device.
    if let Some(base) = base_block_name_from_devpath(devnode) {
        let sys = Path::new("/sys/class/block").join(&base);
        leaf.model = read_sysfs_trimmed(&sys.join("device/model"));
        leaf.serial = read_sysfs_trimmed(&sys.join("device/serial"));

        if let Ok(devlink) = fs::read_link(sys.join("device")) {
            let devpath = if devlink.is_absolute() { devlink } else { sys.join(devlink) };
            let devpath = fs::canonicalize(devpath).unwrap_or_else(|_| sys.join("device"));
            if let Some(bdf) = pci_bdf_from_sysfs_path(&devpath) {
                leaf.pci_bdf = Some(bdf.clone());
                let pcidir = Path::new("/sys/bus/pci/devices").join(&bdf);
                leaf.pci_numa_node = read_sysfs_trimmed(&pcidir.join("numa_node")).and_then(|s| s.parse().ok());
                leaf.pci_local_cpulist = read_sysfs_trimmed(&pcidir.join("local_cpulist"));
                leaf.pcie_current_link_width = read_sysfs_trimmed(&pcidir.join("current_link_width"));
                leaf.pcie_current_link_speed = read_sysfs_trimmed(&pcidir.join("current_link_speed"));
            }
        }
    }
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

fn is_zpool_group_name(name: &str) -> bool {
    name.starts_with("mirror-")
        || name.starts_with("raidz")
        || name == "logs"
        || name == "log"
        || name == "cache"
        || name == "spares"
        || name.starts_with("spare-")
        || name == "special"
        || name.starts_with("replacing")
}

fn parse_zpool_status_leaves(out: &str) -> Vec<DeviceLeafInfo> {
    let mut in_config = false;
    let mut in_table = false;

    let mut pool_name: Option<String> = None;
    let mut stack: Vec<(usize, String)> = Vec::new(); // (indent, name)

    let mut leaves = Vec::new();

    for line in out.lines() {
        let t = line.trim();
        if t == "config:" {
            in_config = true;
            continue;
        }
        if !in_config {
            continue;
        }

        if t.starts_with("errors:") {
            break;
        }

        // Table header: NAME STATE READ WRITE CKSUM
        if t.starts_with("NAME") && t.contains("STATE") {
            in_table = true;
            continue;
        }
        if !in_table {
            continue;
        }
        if t.is_empty() {
            continue;
        }

        let indent = line.chars().take_while(|c| c.is_whitespace()).count();
        let mut parts = t.split_whitespace();
        let name = match parts.next() {
            Some(v) => v.to_string(),
            None => continue,
        };
        let state = parts.next().map(|s| s.to_string());

        let was = line.split(" was ").nth(1).map(|s| s.trim().to_string());

        while let Some((last_indent, _)) = stack.last() {
            if *last_indent >= indent {
                stack.pop();
            } else {
                break;
            }
        }

        if pool_name.is_none() {
            pool_name = Some(name.clone());
            stack.push((indent, name));
            continue;
        }

        if is_zpool_group_name(&name) {
            stack.push((indent, name));
            continue;
        }

        let vdev_path: Vec<String> = stack.iter().skip(1).map(|(_, n)| n.clone()).collect();

        leaves.push(DeviceLeafInfo {
            name,
            state,
            was,
            vdev_path,
            ..Default::default()
        });
    }

    leaves
}

fn resolve_zpool_leaf_devnode(leaf: &DeviceLeafInfo) -> (Option<String>, Option<PathBuf>) {
    let candidates = [leaf.was.as_deref(), Some(leaf.name.as_str())];

    for c in candidates.into_iter().flatten() {
        if c.starts_with("/dev/") {
            let p = PathBuf::from(c);
            let by_id_path = if c.starts_with("/dev/disk/by-id/") {
                Some(c.to_string())
            } else {
                None
            };
            let devnode = fs::canonicalize(&p).unwrap_or(p);
            return (by_id_path, Some(devnode));
        }

        let by_id = Path::new("/dev/disk/by-id").join(c);
        if by_id.exists() {
            let devnode = fs::canonicalize(&by_id).unwrap_or(by_id.clone());
            return (Some(by_id.display().to_string()), Some(devnode));
        }
    }

    (None, None)
}

fn zpool_get_vdevs_and_info(pool: &str) -> (Vec<String>, Vec<DeviceLeafInfo>) {
    let out = match run_cmd_stdout("zpool", &["status", "-P", pool]) {
        Some(s) => s,
        None => return (Vec::new(), Vec::new()),
    };

    let mut leaves = parse_zpool_status_leaves(&out);
    for l in leaves.iter_mut() {
        let (by_id_path, devnode) = resolve_zpool_leaf_devnode(l);
        l.by_id_path = by_id_path;
        if let Some(devnode) = devnode {
            enrich_device_leaf_from_devnode(l, &devnode);
        }
    }

    let vdevs: Vec<String> = leaves
        .iter()
        .map(|l| {
            l.by_id_path
                .clone()
                .or_else(|| l.devnode.clone())
                .unwrap_or_else(|| l.name.clone())
        })
        .collect();

    (vdevs, leaves)
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
                let (vdevs, info) = zpool_get_vdevs_and_info(pool);
                e.zpool_vdevs = vdevs;
                e.zpool_vdevs_info = info;
            }

            let topo = if e.zpool_vdevs_info.iter().any(|l| l.vdev_path.iter().any(|p| p.starts_with("mirror-"))) {
                "mirror"
            } else if e
                .zpool_vdevs_info
                .iter()
                .any(|l| l.vdev_path.iter().any(|p| p.starts_with("raidz")))
            {
                "raidz"
            } else {
                "stripe"
            };

            let mut models: Vec<String> = e
                .zpool_vdevs_info
                .iter()
                .filter_map(|l| l.model.clone())
                .collect();
            models.sort();
            models.dedup();
            let models = models.join(",");

            let mut devs: Vec<String> = e
                .zpool_vdevs_info
                .iter()
                .map(|l| {
                    let bdf = l.pci_bdf.clone().unwrap_or_else(|| "?".into());
                    let w = l.pcie_current_link_width.clone().unwrap_or_else(|| "?".into());
                    let s = l.pcie_current_link_speed.clone().unwrap_or_else(|| "?".into());
                    format!("{}@{}({}@{})", l.name, bdf, w, s)
                })
                .collect();
            devs.sort();
            let devs = devs.join(",");

            let pool = e.zfs_pool.clone().unwrap_or_else(|| "unknown".into());
            e.signature = Some(format!("fstype=zfs;pool={};topology={};models={};devs={}", pool, topo, models, devs));
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

        for member in &e.md_members {
            let mut leaf = DeviceLeafInfo {
                name: member.clone(),
                ..Default::default()
            };
            let devnode = Path::new("/dev").join(member);
            if devnode.exists() {
                enrich_device_leaf_from_devnode(&mut leaf, &devnode);
            } else {
                // Fall back to sysfs-only info.
                let msys = Path::new("/sys/class/block").join(member);
                leaf.model = read_sysfs_trimmed(&msys.join("device/model"));
                leaf.serial = read_sysfs_trimmed(&msys.join("device/serial"));
                leaf.by_id = dev_by_id_for_block_name(member);
            }
            e.md_members_info.push(leaf);
        }
    }

    // Human-readable signature used for device-db matching / debugging.
    // The device-db matcher does NOT parse this today; it matches on structured fields.
    let model = e.device_model.clone().unwrap_or_else(|| "unknown".into());
    let md_level = e.md_level.clone().unwrap_or_else(|| "".into());
    let sig = match kind {
        "md" if !md_level.is_empty() => {
            let mut models: Vec<String> = e
                .md_members_info
                .iter()
                .filter_map(|l| l.model.clone())
                .collect();
            models.sort();
            models.dedup();
            let models = models.join(",");

            let mut devs: Vec<String> = e
                .md_members_info
                .iter()
                .map(|l| {
                    let bdf = l.pci_bdf.clone().unwrap_or_else(|| "?".into());
                    let w = l.pcie_current_link_width.clone().unwrap_or_else(|| "?".into());
                    let s = l.pcie_current_link_speed.clone().unwrap_or_else(|| "?".into());
                    format!("{}@{}({}@{})", l.name, bdf, w, s)
                })
                .collect();
            devs.sort();
            let devs = devs.join(",");

            format!(
                "fstype={};dev=md;level={};models={};devs={}",
                e.fstype, md_level, models, devs
            )
        }
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
            zpool_vdevs_info: Vec::new(),
            md_level: None,
            md_chunk_bytes: None,
            md_members: Vec::new(),
            md_members_info: Vec::new(),
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
                zpool_vdevs_info: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                md_members_info: Vec::new(),
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
                zpool_vdevs_info: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                md_members_info: Vec::new(),
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
                zpool_vdevs_info: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                md_members_info: Vec::new(),
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
                zpool_vdevs_info: Vec::new(),
                md_level: None,
                md_chunk_bytes: None,
                md_members: Vec::new(),
                md_members_info: Vec::new(),
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
            zpool_vdevs_info: Vec::new(),
            md_level: None,
            md_chunk_bytes: None,
            md_members: Vec::new(),
            md_members_info: Vec::new(),
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
            zpool_vdevs_info: Vec::new(),
            md_level: Some("raid0".into()),
            md_chunk_bytes: None,
            md_members: Vec::new(),
            md_members_info: Vec::new(),
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
    fn parse_zpool_status_leaves_extracts_vdev_path_and_was() {
        let out = r#"
  pool: tank
 state: DEGRADED
config:

        NAME                                        STATE     READ WRITE CKSUM
        tank                                        DEGRADED     0     0     0
          mirror-0                                  DEGRADED     0     0     0
            11842916200294856737                    UNAVAIL      0     0     0  was /dev/disk/by-id/nvme-KCD61LUL7T68_6140A14ST4Z8_1-part2
            nvme-KCD61LUL7T68_6150A0DXT4Z8_1-part2  ONLINE       0     0     0

errors: No known data errors
"#;
        let leaves = parse_zpool_status_leaves(out);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].vdev_path, vec!["mirror-0"]);
        assert_eq!(leaves[0].state.as_deref(), Some("UNAVAIL"));
        assert_eq!(
            leaves[0].was.as_deref(),
            Some("/dev/disk/by-id/nvme-KCD61LUL7T68_6140A14ST4Z8_1-part2")
        );
        assert_eq!(leaves[1].vdev_path, vec!["mirror-0"]);
        assert_eq!(leaves[1].state.as_deref(), Some("ONLINE"));
    }

    #[test]
    fn parse_size_accepts_common_suffixes() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1KiB"), Some(1024));
        assert_eq!(parse_size("2MiB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_size("4GiB"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1g"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("nope"), None);
    }

}

fn fs_stats_for_path(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) } != 0 {
        return None;
    }

    let frsize = if vfs.f_frsize == 0 { vfs.f_bsize } else { vfs.f_frsize } as u64;
    let total = frsize.saturating_mul(vfs.f_blocks as u64);
    let avail = frsize.saturating_mul(vfs.f_bavail as u64);
    Some((total, avail))
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or_else(|| s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);

    let num: u64 = num_str.parse().ok()?;
    let mult: u64 = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return None,
    };

    num.checked_mul(mult)
}

fn align_down(bytes: u64, align: u64) -> u64 {
    if align == 0 {
        bytes
    } else {
        bytes / align * align
    }
}

fn fmt_gib(bytes: u64) -> String {
    format!("{:.2} GiB", (bytes as f64) / (1024.0 * 1024.0 * 1024.0))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut patterns = vec![];

    let mut test_dir = ".";

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!(
            "USAGE: {} [--list-devices | --list-devices-all] [--plan] [--test-dir path] [--test-size <size>] [--max-drive-writes <fraction>] <test_prefix ...>",
            args[0]
        );
        println!(
            "\nAuto sizing (default): chooses a temp file size based on free space and a wear budget.\n\
             - --plan                  (print suggested test size + write load and exit)\n\
             - --test-size 1GiB         (force fixed size)\n\
             - --max-drive-writes 0.05  (cap total user-data writes per run to ~5% of FS capacity)"
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

    let mut test_size: Option<u64> = None;
    let mut min_test_size: u64 = 256 * 1024 * 1024;
    let mut max_test_size: u64 = 1024 * 1024 * 1024;
    let mut max_drive_writes: f64 = 0.05;
    let mut plan = false;

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        i += 1;
        if arg == "--test-dir" {
            test_dir = &args[i];
            i += 1;
        } else if arg == "--test-size" {
            let v = &args[i];
            i += 1;
            test_size = parse_size(v);
            if test_size.is_none() {
                eprintln!("Invalid --test-size: {}", v);
                std::process::exit(2);
            }
        } else if arg == "--min-test-size" {
            let v = &args[i];
            i += 1;
            min_test_size = parse_size(v).unwrap_or_else(|| {
                eprintln!("Invalid --min-test-size: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--max-test-size" {
            let v = &args[i];
            i += 1;
            max_test_size = parse_size(v).unwrap_or_else(|| {
                eprintln!("Invalid --max-test-size: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--max-drive-writes" {
            let v = &args[i];
            i += 1;
            max_drive_writes = v.parse().unwrap_or_else(|_| {
                eprintln!("Invalid --max-drive-writes: {}", v);
                std::process::exit(2);
            });
        } else if arg == "--plan" {
            plan = true;
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

    macro_rules! argsv {
        ($($s:expr),* $(,)?) => {
            vec![$($s.to_string()),*]
        };
    }

    let configs: Vec<Vec<String>> = vec![
        argsv!("read", "-s", "--direct", "-n", "100", &source_file),
        argsv!("read", "-s", "--no-direct", "-n", "100", &source_file),
        argsv!("grep", "-s", "--direct", "-n", "100", "needle", &source_file),
        argsv!("grep", "-s", "--no-direct", "-n", "100", "needle", &source_file),
        argsv!("write", "-s", "--direct", "-n", "20", &target_file_dir),
        argsv!("write", "-s", "--no-direct", "-n", "20", &target_file_cache),
        argsv!("copy", "-s", "--direct", "-n", "20", &source_file, &target_file_dir),
        argsv!("copy", "-s", "--no-direct", "-n", "20", &source_file, &target_file_cache),
        argsv!("diff", "-s", "--direct", "-n", "100", &source_file, &target_file_dir),
        argsv!("diff", "-s", "--no-direct", "-n", "100", &source_file, &target_file_cache),
        argsv!(
            "dual-read-bench",
            "-s",
            "--direct",
            "-n",
            "100",
            &source_file,
            &target_file_dir
        ),
        argsv!(
            "dual-read-bench",
            "-s",
            "--no-direct",
            "-n",
            "100",
            &source_file,
            &target_file_cache
        ),
    ];

    let mut selected = Vec::new();
    for cfg in configs {
        if patterns.is_empty() {
            selected.push(cfg);
            continue;
        }
        let mut ok = false;
        for p in patterns.iter() {
            if cfg[0].starts_with(p.as_str()) {
                ok = true;
                break;
            }
        }
        if ok {
            selected.push(cfg);
        }
    }

    if selected.is_empty() {
        eprintln!("No optimizer modes selected.");
        return;
    }

    let mut need_source = false;
    let mut need_target_dir = false;
    let mut need_target_cache = false;
    let mut need_target_dir_matching = false;
    let mut need_target_cache_matching = false;

    let mut num_full_writes: u64 = 0;
    for cfg in &selected {
        let op = cfg[0].as_str();

        if cfg.iter().any(|s| s == &source_file) {
            need_source = true;
        }
        if cfg.iter().any(|s| s == &target_file_dir) {
            need_target_dir = true;
        }
        if cfg.iter().any(|s| s == &target_file_cache) {
            need_target_cache = true;
        }

        if (op == "diff" || op == "dual-read-bench") && cfg.iter().any(|s| s == &target_file_dir) {
            need_target_dir_matching = true;
        }
        if (op == "diff" || op == "dual-read-bench") && cfg.iter().any(|s| s == &target_file_cache) {
            need_target_cache_matching = true;
        }

        if op == "write" || op == "copy" {
            let mut n = 1_u64;
            let mut j = 0;
            while j + 1 < cfg.len() {
                if cfg[j] == "-n" {
                    n = cfg[j + 1].parse::<u64>().unwrap_or(1);
                    break;
                }
                j += 1;
            }
            num_full_writes = num_full_writes.saturating_add(n);
        }
    }

    if need_target_dir || need_target_cache {
        need_source = true;
    }

    let file_count = (need_source as u64) + (need_target_dir as u64) + (need_target_cache as u64);
    let setup_writes = (need_source as u64)
        + (need_target_dir_matching as u64)
        + (need_target_cache_matching as u64);
    num_full_writes = num_full_writes.saturating_add(setup_writes);

    let size = if let Some(s) = test_size {
        align_down(s, 4096).max(4096)
    } else if let Some((total, avail)) = fs_stats_for_path(test_path) {
        let space_cap = ((avail as f64) * 0.60 / (file_count as f64)) as u64;
        let wear_cap = if num_full_writes == 0 {
            u64::MAX
        } else {
            ((total as f64) * max_drive_writes / (num_full_writes as f64)) as u64
        };

        let mut size = max_test_size.min(space_cap).min(wear_cap);
        size = align_down(size, 4096).max(4096);

        if size < min_test_size {
            eprintln!(
                "Warning: wear/space cap suggests a small test file: {} (min requested: {})",
                fmt_gib(size),
                fmt_gib(min_test_size)
            );
        }

        size
    } else {
        eprintln!("Warning: statvfs failed for --test-dir; falling back to 4GiB");
        4 * 1024 * 1024 * 1024
    };

    let est_user_writes = size.saturating_mul(num_full_writes);
    let alloc = size.saturating_mul(file_count);

    if test_size.is_none() {
        eprintln!(
            "Auto-sized test file: {} (alloc={} across {} files; est_writes={} => est_user_writes={})",
            fmt_gib(size),
            fmt_gib(alloc),
            file_count,
            num_full_writes,
            fmt_gib(est_user_writes),
        );
    }

    if plan {
        println!("fro-optimize plan");
        println!("  test_dir: {}", test_dir);
        println!("  test_size: {}", fmt_gib(size));
        println!("  file_count: {} (source={} direct_target={} cache_target={})", file_count, need_source, need_target_dir, need_target_cache);
        println!("  alloc_total: {}", fmt_gib(alloc));
        println!("  est_full_writes: {}", num_full_writes);
        println!("  est_user_writes: {}", fmt_gib(est_user_writes));
        println!("  max_drive_writes: {}", max_drive_writes);
        println!("  selected_modes:");
        for cfg in &selected {
            println!("    {}", cfg.join(" "));
        }
        return;
    }

    // Setup temp files (only the ones we actually need).
    if need_source {
        if let Ok(f) = fs::File::create(&source_file) {
            let _ = f.set_len(size);
        }
        let _ = Command::new(&fro_exe)
            .args(["write", &source_file])
            .status()
            .expect("Failed to write fro_bench_tmp_source");
    }
    if need_target_dir {
        if let Ok(f) = fs::File::create(&target_file_dir) {
            let _ = f.set_len(size);
        }
        if need_target_dir_matching {
            let _ = Command::new(&fro_exe)
                .args(["copy", &source_file, &target_file_dir])
                .status()
                .expect("Failed to prepare fro_bench_tmp_direct");
        }
    }
    if need_target_cache {
        if let Ok(f) = fs::File::create(&target_file_cache) {
            let _ = f.set_len(size);
        }
        if need_target_cache_matching {
            let _ = Command::new(&fro_exe)
                .args(["copy", &source_file, &target_file_cache])
                .status()
                .expect("Failed to prepare fro_bench_tmp_cache");
        }
    }

    println!("Running optimizer for all modes... (This may take a while)");

    for args in selected {
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
