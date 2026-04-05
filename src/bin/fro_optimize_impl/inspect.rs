use super::*;

pub(super) fn is_disk_backed_mount(e: &MountInfoEntry) -> bool {
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
    const EXCLUDE_MP_PREFIX: &[&str] =
        &["/proc", "/sys", "/dev", "/run", "/snap", "/var/lib/snapd"];
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

pub(super) fn collect_home_targets() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let home = match std::env::var("HOME").ok() {
        Some(h) => PathBuf::from(h),
        None => return out,
    };
    out.push(home.clone());

    if let Ok(rd) = fs::read_dir(&home) {
        for ent in rd.flatten() {
            let ft = match ent.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if !ft.is_symlink() {
                continue;
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

pub(super) fn find_writable_dir_for_mount(mount_point: &Path, home_targets: &[PathBuf]) -> Option<PathBuf> {
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

// Returns all /dev/disk/by-id symlink names whose canonical target has the same basename as
// the canonical form of `devpath`. Works for both a block name (e.g. "nvme0n1") and a devnode
// path (e.g. /dev/nvme0n1 or /dev/nvme0n1p1).
fn dev_by_id_for_devpath(devpath: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let canon_dev = match fs::canonicalize(devpath) {
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
        let target = if link.is_absolute() {
            link
        } else {
            dir.join(link)
        };
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

fn dev_by_id_for_block_name(block: &str) -> Vec<String> {
    dev_by_id_for_devpath(Path::new("/dev").join(block).as_path())
}

fn enrich_device_leaf_from_devnode(leaf: &mut DeviceLeafInfo, devnode: &Path) {
    leaf.devnode = Some(devnode.display().to_string());
    leaf.by_id = dev_by_id_for_devpath(devnode);

    // `model`/`serial` live on the base block device.
    if let Some(base) = base_block_name_from_devpath(devnode) {
        let sys = Path::new("/sys/class/block").join(&base);
        leaf.model = read_sysfs_trimmed(&sys.join("device/model"));
        leaf.serial = read_sysfs_trimmed(&sys.join("device/serial"));

        if let Ok(devlink) = fs::read_link(sys.join("device")) {
            let devpath = if devlink.is_absolute() {
                devlink
            } else {
                sys.join(devlink)
            };
            let devpath = fs::canonicalize(devpath).unwrap_or_else(|_| sys.join("device"));
            if let Some(bdf) = pci_bdf_from_sysfs_path(&devpath) {
                leaf.pci_bdf = Some(bdf.clone());
                let pcidir = Path::new("/sys/bus/pci/devices").join(&bdf);
                leaf.pci_numa_node =
                    read_sysfs_trimmed(&pcidir.join("numa_node")).and_then(|s| s.parse().ok());
                leaf.pci_local_cpulist = read_sysfs_trimmed(&pcidir.join("local_cpulist"));
                leaf.pcie_current_link_width =
                    read_sysfs_trimmed(&pcidir.join("current_link_width"));
                leaf.pcie_current_link_speed =
                    read_sysfs_trimmed(&pcidir.join("current_link_speed"));
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

pub(super) fn parse_zfs_get_props(out: &str) -> BTreeMap<String, String> {
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
    let props =
        "recordsize,compression,compressratio,atime,sync,primarycache,secondarycache,logbias";
    let out = run_cmd_stdout(
        "zfs",
        &["get", "-Hp", "-o", "property,value", props, dataset],
    )?;
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

pub(super) fn parse_zpool_status_leaves(out: &str) -> Vec<DeviceLeafInfo> {
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

pub(super) fn fill_device_info(e: &mut MountInfoEntry) {
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

            let topo = if e
                .zpool_vdevs_info
                .iter()
                .any(|l| l.vdev_path.iter().any(|p| p.starts_with("mirror-")))
            {
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
                    let w = l
                        .pcie_current_link_width
                        .clone()
                        .unwrap_or_else(|| "?".into());
                    let s = l
                        .pcie_current_link_speed
                        .clone()
                        .unwrap_or_else(|| "?".into());
                    format!("{}@{}({}@{})", l.name, bdf, w, s)
                })
                .collect();
            devs.sort();
            let devs = devs.join(",");

            let pool = e.zfs_pool.clone().unwrap_or_else(|| "unknown".into());
            e.signature = Some(format!(
                "fstype=zfs;pool={};topology={};models={};devs={}",
                pool, topo, models, devs
            ));
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
            e.pci_numa_node =
                read_sysfs_trimmed(&pcidir.join("numa_node")).and_then(|s| s.parse().ok());
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
        e.md_chunk_bytes =
            read_sysfs_trimmed(&sys.join("md/chunk_size")).and_then(|s| s.parse().ok());

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
                    let w = l
                        .pcie_current_link_width
                        .clone()
                        .unwrap_or_else(|| "?".into());
                    let s = l
                        .pcie_current_link_speed
                        .clone()
                        .unwrap_or_else(|| "?".into());
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

pub(super) fn load_device_db() -> Option<DeviceDb> {
    let p = std::env::var("FRO_DEVICE_DB")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fro-device-db.json"));
    let data = fs::read_to_string(p).ok()?;
    let db: DeviceDb = serde_json::from_str(&data).ok()?;
    if db.version != 1 {
        return None;
    }
    Some(db)
}

pub(super) fn device_db_match<'a>(db: &'a DeviceDb, e: &MountInfoEntry) -> Option<&'a DeviceDbProfile> {
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

pub(super) fn read_mountinfo() -> Vec<MountInfoEntry> {
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
            ..Default::default()
        });
    }

    out.sort_by(|a, b| a.mount_point.cmp(&b.mount_point));
    out
}

