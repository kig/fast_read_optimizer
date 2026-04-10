use super::*;

static MOUNTINFO_CACHE: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static DEVICE_SIGNATURE_CACHE: OnceLock<Mutex<HashMap<String, Option<DeviceSignature>>>> =
    OnceLock::new();

fn mountinfo_cache() -> &'static Mutex<Option<String>> {
    MOUNTINFO_CACHE.get_or_init(|| Mutex::new(None))
}

fn device_signature_cache() -> &'static Mutex<HashMap<String, Option<DeviceSignature>>> {
    DEVICE_SIGNATURE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(super) fn clear_mountinfo_cache_for_tests() {
    *mountinfo_cache().lock().unwrap() = None;
}

#[cfg(test)]
pub(super) fn clear_device_signature_cache_for_tests() {
    device_signature_cache().lock().unwrap().clear();
}

pub(super) fn device_db_match<'a>(
    db: &'a DeviceDb,
    mount: &MountInfo,
    device: Option<&DeviceSignature>,
) -> Option<&'a DeviceDbProfile> {
    let context = DeviceDbMatchContext::from_mount_and_device(mount, device);
    db.profiles
        .iter()
        .find(|profile| profile.matches_context(&context))
}

#[derive(Default)]
struct DeviceDbMatchContext {
    fstype: String,
    dev_kind: Option<String>,
    dev_model: Option<String>,
    md_level: Option<String>,
}

impl DeviceDbMatchContext {
    fn from_mount_and_device(mount: &MountInfo, device: Option<&DeviceSignature>) -> Self {
        let mut ctx = Self {
            fstype: mount.fstype.clone(),
            dev_kind: Some(device_kind_for_mount_source(&mount.mount_source)),
            ..Self::default()
        };

        if let Some(device) = device {
            if let Some(block) = device.block_device.as_ref() {
                ctx.dev_kind = Some(device_kind_for_kernel_name(&block.kernel_name));
                ctx.dev_model = block.model.clone();
                ctx.md_level = block.md_level.clone();
            }
        }

        ctx
    }
}

impl DeviceDbProfile {
    fn matches_context(&self, ctx: &DeviceDbMatchContext) -> bool {
        if let Some(ref fstype) = self.match_fields.fstype {
            if &ctx.fstype != fstype {
                return false;
            }
        }
        if let Some(ref dev_kind) = self.match_fields.dev_kind {
            if ctx.dev_kind.as_deref() != Some(dev_kind.as_str()) {
                return false;
            }
        }
        if let Some(ref md_level) = self.match_fields.md_level {
            if ctx.md_level.as_deref() != Some(md_level.as_str()) {
                return false;
            }
        }
        if let Some(ref contains) = self.match_fields.dev_model_contains {
            if !ctx.dev_model.as_deref().unwrap_or("").contains(contains) {
                return false;
            }
        }
        true
    }
}

fn device_kind_for_mount_source(source: &str) -> String {
    if source.starts_with("/dev/") {
        let kernel_name = source.rsplit('/').next().unwrap_or(source);
        return device_kind_for_kernel_name(kernel_name);
    }
    source.to_string()
}

fn device_kind_for_kernel_name(kernel_name: &str) -> String {
    if kernel_name.starts_with("nvme") {
        "nvme".to_string()
    } else if kernel_name.starts_with("md") {
        "md".to_string()
    } else if kernel_name.starts_with("dm-") {
        "dm".to_string()
    } else if kernel_name.starts_with("sd") {
        "sd".to_string()
    } else {
        "block".to_string()
    }
}

pub(super) fn mountpoint_for_path(path: &str) -> Option<String> {
    mount_info_for_path(path).map(|info| info.mount_point)
}

pub(super) fn normalize_path(path: &str) -> PathBuf {
    let path = Path::new(path);
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

pub fn mount_info_for_path(path: &str) -> Option<MountInfo> {
    let normalized = normalize_path(path);
    let path = normalized.to_string_lossy();

    let mut cache = mountinfo_cache().lock().unwrap();
    let data = match cache.as_ref() {
        Some(data) => data.clone(),
        None => match fs::read_to_string("/proc/self/mountinfo") {
            Ok(data) => {
                *cache = Some(data.clone());
                data
            }
            Err(err) => {
                MOUNTINFO_WARNING.call_once(|| {
                    eprintln!(
                        "Warning: could not read /proc/self/mountinfo ({}); mount-specific config overrides are disabled",
                        err
                    );
                });
                return None;
            }
        },
    };
    drop(cache);

    mount_info_for_path_from_data(&path, &data)
}

#[allow(dead_code)]
pub fn device_signature_for_path(path: &str) -> Option<DeviceSignature> {
    let mount = mount_info_for_path(path)?;
    device_signature_from_mount_info(&mount)
}

pub(super) fn device_signature_from_mount_info(mount: &MountInfo) -> Option<DeviceSignature> {
    let roots = DeviceProbeRoots::default();
    device_signature_from_mount_info_with_roots(mount, &roots)
}

pub(super) fn device_signature_from_mount_info_with_roots(
    mount: &MountInfo,
    roots: &DeviceProbeRoots,
) -> Option<DeviceSignature> {
    let cache_key = device_signature_cache_key(mount, roots);
    if let Some(cached) = device_signature_cache()
        .lock()
        .unwrap()
        .get(&cache_key)
        .cloned()
    {
        return cached;
    }

    let signature = if mount.mount_source.is_empty() {
        None
    } else {
        let canonical_source = canonical_mount_source(&mount.mount_source, roots);
        let block = block_device_signature_for_mount_source(&canonical_source, roots);
        let match_keys = device_match_keys(&mount.mount_source, &canonical_source, block.as_ref());

        Some(DeviceSignature {
            mount_source: mount.mount_source.clone(),
            canonical_source,
            match_keys,
            block_device: block,
        })
    };

    device_signature_cache()
        .lock()
        .unwrap()
        .insert(cache_key, signature.clone());
    signature
}

fn device_signature_cache_key(mount: &MountInfo, roots: &DeviceProbeRoots) -> String {
    format!(
        "{}\n{}\n{}",
        roots.dev_root.display(),
        roots.sys_class_block_root.display(),
        mount.mount_source
    )
}

fn canonical_mount_source(source: &str, roots: &DeviceProbeRoots) -> String {
    if !source.starts_with('/') {
        return source.to_string();
    }
    match canonicalize_dev_path_in_root(source, &roots.dev_root) {
        Some(path) => path.to_string_lossy().into_owned(),
        None => source.to_string(),
    }
}

fn block_device_signature_for_mount_source(
    canonical_source: &str,
    roots: &DeviceProbeRoots,
) -> Option<BlockDeviceSignature> {
    let devnode = if canonical_source.starts_with('/') {
        canonical_source.to_string()
    } else {
        return None;
    };
    let kernel_name = devnode
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())?
        .to_string();
    block_device_signature_for_kernel_name(&kernel_name, &devnode, roots)
}

fn block_device_signature_for_kernel_name(
    kernel_name: &str,
    devnode: &str,
    roots: &DeviceProbeRoots,
) -> Option<BlockDeviceSignature> {
    let sysfs_dir = roots.sys_class_block_root.join(kernel_name);
    if !sysfs_dir.exists() {
        return None;
    }

    let vendor = read_trimmed(sysfs_dir.join("device/vendor"));
    let model = read_trimmed(sysfs_dir.join("device/model"));
    let rotational =
        read_trimmed(sysfs_dir.join("queue/rotational")).and_then(|value| match value.as_str() {
            "0" => Some(false),
            "1" => Some(true),
            _ => None,
        });
    let dm_name = read_trimmed(sysfs_dir.join("dm/name"));
    let md_level = read_trimmed(sysfs_dir.join("md/level"));
    let by_id = dev_disk_by_id_matches(devnode, roots);
    let slaves = block_device_slaves(&sysfs_dir, roots);

    Some(BlockDeviceSignature {
        kernel_name: kernel_name.to_string(),
        devnode: devnode.to_string(),
        by_id,
        vendor,
        model,
        rotational,
        dm_name,
        md_level,
        slaves,
    })
}

fn block_device_slaves(sysfs_dir: &Path, roots: &DeviceProbeRoots) -> Vec<BlockDeviceSignature> {
    let slaves_dir = sysfs_dir.join("slaves");
    let mut entries: Vec<String> = match fs::read_dir(slaves_dir) {
        Ok(read_dir) => read_dir
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect(),
        Err(_) => return Vec::new(),
    };
    entries.sort();
    entries
        .into_iter()
        .filter_map(|kernel_name| {
            let devnode = roots
                .dev_root
                .join(&kernel_name)
                .to_string_lossy()
                .into_owned();
            block_device_signature_for_kernel_name(&kernel_name, &devnode, roots)
        })
        .collect()
}

fn dev_disk_by_id_matches(devnode: &str, roots: &DeviceProbeRoots) -> Vec<String> {
    let by_id_dir = roots.dev_root.join("disk").join("by-id");
    let mut matches = Vec::new();

    let target_real = normalize_existing_path_in_root(Path::new(devnode), &roots.dev_root);

    let read_dir = match fs::read_dir(&by_id_dir) {
        Ok(read_dir) => read_dir,
        Err(_) => return matches,
    };

    for entry in read_dir.filter_map(|entry| entry.ok()) {
        let file_name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let link_target = match fs::read_link(entry.path()) {
            Ok(target) => target,
            Err(_) => continue,
        };
        let joined = if link_target.is_absolute() {
            link_target
        } else {
            by_id_dir.join(link_target)
        };
        let candidate = normalize_existing_path(&joined);
        if candidate == target_real {
            matches.push(file_name);
        }
    }

    matches.sort();
    matches
}

fn device_match_keys(
    mount_source: &str,
    canonical_source: &str,
    block: Option<&BlockDeviceSignature>,
) -> Vec<String> {
    let mut keys = Vec::new();
    push_unique(&mut keys, format!("mount_source={mount_source}"));
    if canonical_source != mount_source {
        push_unique(&mut keys, format!("canonical_source={canonical_source}"));
    }
    if let Some(block) = block {
        push_unique(&mut keys, format!("devnode={}", block.devnode));
        push_unique(&mut keys, format!("kernel={}", block.kernel_name));
        push_unique(
            &mut keys,
            format!("kind={}", device_kind_for_kernel_name(&block.kernel_name)),
        );
        if let Some(dm_name) = &block.dm_name {
            push_unique(&mut keys, format!("dm_name={dm_name}"));
        }
        if let Some(md_level) = &block.md_level {
            push_unique(&mut keys, format!("md_level={md_level}"));
        }
        if let Some(vendor) = &block.vendor {
            push_unique(&mut keys, format!("vendor={vendor}"));
        }
        if let Some(model) = &block.model {
            push_unique(&mut keys, format!("model={model}"));
        }
        if let Some(rotational) = block.rotational {
            push_unique(
                &mut keys,
                format!("rotational={}", if rotational { 1 } else { 0 }),
            );
        }
        for by_id in &block.by_id {
            push_unique(&mut keys, format!("by-id={by_id}"));
        }
        for slave in &block.slaves {
            extend_match_keys_for_child(&mut keys, "slave", slave);
        }
        extend_composite_match_keys(&mut keys, block);
    }
    keys
}

fn extend_match_keys_for_child(keys: &mut Vec<String>, prefix: &str, block: &BlockDeviceSignature) {
    push_unique(keys, format!("{prefix}.kernel={}", block.kernel_name));
    push_unique(keys, format!("{prefix}.devnode={}", block.devnode));
    push_unique(
        keys,
        format!(
            "{prefix}.kind={}",
            device_kind_for_kernel_name(&block.kernel_name)
        ),
    );
    if let Some(dm_name) = &block.dm_name {
        push_unique(keys, format!("{prefix}.dm_name={dm_name}"));
    }
    if let Some(md_level) = &block.md_level {
        push_unique(keys, format!("{prefix}.md_level={md_level}"));
    }
    if let Some(vendor) = &block.vendor {
        push_unique(keys, format!("{prefix}.vendor={vendor}"));
    }
    if let Some(model) = &block.model {
        push_unique(keys, format!("{prefix}.model={model}"));
    }
    if let Some(rotational) = block.rotational {
        push_unique(
            keys,
            format!("{prefix}.rotational={}", if rotational { 1 } else { 0 }),
        );
    }
    for by_id in &block.by_id {
        push_unique(keys, format!("{prefix}.by-id={by_id}"));
    }
    for child in &block.slaves {
        extend_match_keys_for_child(keys, &format!("{prefix}.slave"), child);
    }
}

fn extend_composite_match_keys(keys: &mut Vec<String>, block: &BlockDeviceSignature) {
    let mut stack = Vec::new();
    collect_composite_match_keys(keys, block, &mut stack);
}

fn collect_composite_match_keys(
    keys: &mut Vec<String>,
    block: &BlockDeviceSignature,
    stack: &mut Vec<String>,
) {
    let kind = device_kind_for_kernel_name(&block.kernel_name);
    push_unique(keys, format!("component.kind={kind}"));
    for by_id in &block.by_id {
        push_unique(keys, format!("component.by-id={by_id}"));
    }
    if let Some(dm_name) = &block.dm_name {
        push_unique(keys, format!("component.dm_name={dm_name}"));
    }
    if let Some(md_level) = &block.md_level {
        push_unique(keys, format!("component.md_level={md_level}"));
    }

    stack.push(kind.clone());
    push_unique(keys, format!("stack={}", stack.join(">")));

    if block.slaves.is_empty() {
        push_unique(keys, format!("leaf.kind={kind}"));
        for by_id in &block.by_id {
            push_unique(keys, format!("leaf.by-id={by_id}"));
        }
        if let Some(vendor) = &block.vendor {
            push_unique(keys, format!("leaf.vendor={vendor}"));
        }
        if let Some(model) = &block.model {
            push_unique(keys, format!("leaf.model={model}"));
        }
        if let Some(rotational) = block.rotational {
            push_unique(
                keys,
                format!("leaf.rotational={}", if rotational { 1 } else { 0 }),
            );
        }
    } else {
        for child in &block.slaves {
            collect_composite_match_keys(keys, child, stack);
        }
    }

    stack.pop();
}

fn push_unique(keys: &mut Vec<String>, value: String) {
    if !keys.iter().any(|existing| existing == &value) {
        keys.push(value);
    }
}

fn normalize_existing_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn normalize_existing_path_in_root(path: &Path, root: &Path) -> PathBuf {
    if path.is_absolute() {
        if let Ok(relative) = path.strip_prefix("/dev") {
            return normalize_existing_path(&root.join(relative));
        }
    }
    normalize_existing_path(path)
}

fn canonicalize_dev_path_in_root(path: &str, root: &Path) -> Option<PathBuf> {
    let relative = path.strip_prefix("/dev/")?;
    let canonical = fs::canonicalize(root.join(relative)).ok()?;
    if canonical.starts_with(root) {
        let relative = canonical.strip_prefix(root).ok()?;
        Some(Path::new("/dev").join(relative))
    } else {
        Some(canonical)
    }
}

fn read_trimmed(path: PathBuf) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

impl Default for DeviceProbeRoots {
    fn default() -> Self {
        Self {
            dev_root: PathBuf::from("/dev"),
            sys_class_block_root: PathBuf::from("/sys/class/block"),
        }
    }
}

fn path_starts_with_mount(path: &str, mount_point: &str) -> bool {
    if mount_point == "/" {
        return path.starts_with('/');
    }
    if path == mount_point {
        return true;
    }
    if let Some(rest) = path.strip_prefix(mount_point) {
        return rest.starts_with('/');
    }
    false
}

pub(super) fn mount_info_for_path_from_data(path: &str, data: &str) -> Option<MountInfo> {
    let mut best: Option<MountInfo> = None;
    let mut best_len = 0usize;

    for line in data.lines() {
        let (lhs, rhs) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };
        let left_fields: Vec<&str> = lhs.split_whitespace().collect();
        if left_fields.len() < 5 {
            continue;
        }

        let right_fields: Vec<&str> = rhs.split_whitespace().collect();
        if right_fields.is_empty() {
            continue;
        }

        let mount_point = decode_mountinfo_field(left_fields[4]);
        if !path_starts_with_mount(path, &mount_point) {
            continue;
        }
        if mount_point.len() > best_len {
            best_len = mount_point.len();
            best = Some(MountInfo {
                mount_point,
                fstype: decode_mountinfo_field(right_fields[0]),
                mount_source: right_fields
                    .get(1)
                    .map(|value| decode_mountinfo_field(value))
                    .unwrap_or_default(),
            });
        }
    }

    best
}

fn decode_mountinfo_field(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = String::with_capacity(value.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let octal = &bytes[i + 1..i + 4];
            if octal.iter().all(|b| (b'0'..=b'7').contains(b)) {
                let code = ((octal[0] - b'0') << 6) | ((octal[1] - b'0') << 3) | (octal[2] - b'0');
                decoded.push(char::from(code));
                i += 4;
                continue;
            }
        }
        decoded.push(bytes[i] as char);
        i += 1;
    }
    decoded
}
