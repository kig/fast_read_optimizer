use super::*;

#[derive(Clone, Copy)]
pub(super) struct ResolvedCopyExecution {
    pub(super) copy_strategy: CopyStrategy,
    pub(super) io_mode_read: common::IOMode,
    pub(super) io_mode_write: common::IOMode,
    pub(super) diff_overwrite: bool,
    pub(super) full_rewrite: bool,
    pub(super) path_label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HeuristicCopyPlan {
    DiffOverwrite,
    CachedReadDirectWrite,
    DirectReadDirectWrite,
    CopyFileRangeSingle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StorageRedundancy {
    Redundant,
    NonRedundant,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MountInfoBrief {
    pub(super) mount_point: String,
    pub(super) fstype: String,
    pub(super) mount_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ZpoolLeafState {
    pub(super) state: Option<String>,
    pub(super) vdev_path: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyRewriteMode {
    Auto,
    Diff,
    Full,
}

pub(super) fn resolve_copy_execution(
    config: &config::LoadedConfig,
    source_path: &str,
    path: &str,
    requested_strategy: CopyStrategy,
    rewrite_mode: CopyRewriteMode,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
) -> io::Result<ResolvedCopyExecution> {
    if rewrite_mode == CopyRewriteMode::Diff {
        return Ok(if direct_writer_supported(path)? {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::Direct,
                diff_overwrite: true,
                full_rewrite: false,
                path_label: "forced diff-overwrite",
            }
        } else {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::CopyFileRangeSingle,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::PageCache,
                diff_overwrite: false,
                full_rewrite: false,
                path_label: "forced diff copy_file_range single fallback",
            }
        });
    }

    if requested_strategy != CopyStrategy::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: requested_strategy,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: match requested_strategy {
                CopyStrategy::Auto => "auto",
                CopyStrategy::Threaded => "threaded",
                CopyStrategy::CopyFileRange => "copy_file_range",
                CopyStrategy::CopyFileRangeSingle => "copy_file_range single",
                CopyStrategy::Reflink => "reflink",
            },
        });
    }

    if io_mode_read != common::IOMode::Auto || io_mode_write != common::IOMode::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: "explicit io-mode threaded",
        });
    }

    let resolved = match config.get_copy_auto_mode_for_path(path) {
        CopyAutoMode::PageCache => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config page-cache threaded",
        },
        CopyAutoMode::Direct => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::Direct,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config direct threaded",
        },
        CopyAutoMode::CopyFileRange => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::CopyFileRange,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config copy_file_range",
        },
        CopyAutoMode::Heuristic => {
            let (source_cached, target_cached, source_len, target_len) =
                inspect_copy_auto_state(source_path, path);
            let (redundancy, reflink_possible) = detect_copy_storage_policy(source_path, path);
            if rewrite_mode == CopyRewriteMode::Auto
                && redundancy == StorageRedundancy::Redundant
                && reflink_possible
            {
                return Ok(ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Reflink,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto redundant reflink",
                });
            }
            if redundancy == StorageRedundancy::NonRedundant {
                return Ok(choose_nonredundant_full_copy_plan(
                    source_cached,
                    source_len,
                    target_len,
                ));
            }
            let plan = if rewrite_mode != CopyRewriteMode::Full
                && should_prefer_cached_diff_overwrite(
                    source_cached,
                    target_cached,
                    source_len,
                    target_len,
                ) {
                if direct_writer_supported(path)? {
                    HeuristicCopyPlan::DiffOverwrite
                } else {
                    HeuristicCopyPlan::CopyFileRangeSingle
                }
            } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len)
            {
                HeuristicCopyPlan::CachedReadDirectWrite
            } else {
                HeuristicCopyPlan::DirectReadDirectWrite
            };

            match plan {
                HeuristicCopyPlan::DiffOverwrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: true,
                    full_rewrite: false,
                    path_label: "auto diff-overwrite",
                },
                HeuristicCopyPlan::CachedReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto cached-read direct-write",
                },
                HeuristicCopyPlan::DirectReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::Direct,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto direct threaded",
                },
                HeuristicCopyPlan::CopyFileRangeSingle => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::CopyFileRangeSingle,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto copy_file_range single fallback",
                },
            }
        }
    };

    Ok(resolved)
}

pub(super) fn inspect_copy_auto_state(
    source_path: &str,
    target_path: &str,
) -> (bool, bool, Option<u64>, Option<u64>) {
    let source_cached = Ok(true) == is_first_page_resident(source_path);
    let target_cached = Ok(true) == is_first_page_resident(target_path);
    let source_len = std::fs::metadata(source_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    let target_len = std::fs::metadata(target_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    (source_cached, target_cached, source_len, target_len)
}

pub(super) fn target_is_similar_size(source_len: Option<u64>, target_len: Option<u64>) -> bool {
    const TARGET_SIZE_THRESHOLD_PERCENT: u64 = 70;

    let (Some(source_len), Some(target_len)) = (source_len, target_len) else {
        return false;
    };

    if source_len == 0 {
        return target_len == 0;
    }

    target_len.saturating_mul(100).saturating_div(source_len) >= TARGET_SIZE_THRESHOLD_PERCENT
}

#[cfg(kani)]
mod kani_proofs {
    use super::target_is_similar_size;

    #[kani::proof]
    fn zero_length_source_is_only_similar_to_zero_length_target() {
        let target_len: Option<u64> = kani::any();
        let expected = matches!(target_len, Some(0));
        assert_eq!(target_is_similar_size(Some(0), target_len), expected);
    }

    #[kani::proof]
    fn missing_sizes_are_never_similar() {
        let source_len: Option<u64> = kani::any();
        let target_len: Option<u64> = kani::any();
        kani::assume(source_len.is_none() || target_len.is_none());
        assert!(!target_is_similar_size(source_len, target_len));
    }
}

pub(super) fn should_prefer_cached_diff_overwrite(
    source_cached: bool,
    target_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_cached && target_is_similar_size(source_len, target_len)
}

pub(super) fn should_prefer_cached_read_direct_write(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_is_similar_size(source_len, target_len)
}

pub(super) fn path_starts_with_mount(path: &str, mount_point: &str) -> bool {
    if mount_point == "/" {
        return path.starts_with('/');
    }
    if path == mount_point {
        return true;
    }
    path.strip_prefix(mount_point)
        .is_some_and(|rest| rest.starts_with('/'))
}

pub(super) fn path_for_mount_lookup(path: &Path) -> Option<String> {
    let absolute = if path.exists() {
        fs::canonicalize(path).ok()?
    } else if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    Some(absolute.to_string_lossy().into_owned())
}

pub(super) fn mount_info_for_path(path: &Path) -> Option<MountInfoBrief> {
    let path = path_for_mount_lookup(path)?;
    let data = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<MountInfoBrief> = None;
    let mut best_len = 0usize;
    for line in data.lines() {
        let (lhs, rhs) = line.split_once(" - ")?;
        let left_fields: Vec<&str> = lhs.split_whitespace().collect();
        if left_fields.len() < 5 {
            continue;
        }
        let mount_point = left_fields[4];
        if !path_starts_with_mount(&path, mount_point) {
            continue;
        }
        let right_fields: Vec<&str> = rhs.split_whitespace().collect();
        if right_fields.len() < 2 {
            continue;
        }
        if mount_point.len() > best_len {
            best_len = mount_point.len();
            best = Some(MountInfoBrief {
                mount_point: mount_point.to_string(),
                fstype: right_fields[0].to_string(),
                mount_source: right_fields[1].to_string(),
            });
        }
    }
    best
}

pub(super) fn filesystem_supports_reflink(fstype: &str) -> bool {
    matches!(fstype, "btrfs" | "xfs" | "ocfs2" | "bcachefs")
}

pub(super) fn base_block_name_from_devpath(devpath: &Path) -> Option<String> {
    let canon = fs::canonicalize(devpath).ok()?;
    let name = canon.file_name()?.to_string_lossy().to_string();
    let sys = Path::new("/sys/class/block").join(&name);
    if sys.join("partition").exists() {
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

pub(super) fn read_sysfs_trimmed(path: &Path) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub(super) fn md_storage_redundancy(base_block: &str) -> StorageRedundancy {
    let md = Path::new("/sys/class/block").join(base_block).join("md");
    let Some(level) = read_sysfs_trimmed(&md.join("level")) else {
        return StorageRedundancy::Unknown;
    };
    let degraded = read_sysfs_trimmed(&md.join("degraded"))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let redundant = match level.as_str() {
        "raid0" | "linear" => return StorageRedundancy::NonRedundant,
        "raid1" | "raid4" | "raid5" | "raid6" | "raid10" => true,
        _ => false,
    };
    if !redundant {
        return StorageRedundancy::Unknown;
    }
    if degraded == 0 {
        StorageRedundancy::Redundant
    } else {
        StorageRedundancy::NonRedundant
    }
}

pub(super) fn is_zpool_group_name(name: &str) -> bool {
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

pub(super) fn parse_zpool_status_leaves(out: &str) -> Vec<ZpoolLeafState> {
    let mut in_config = false;
    let mut in_table = false;
    let mut saw_pool = false;
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut leaves = Vec::new();

    for line in out.lines() {
        let trimmed = line.trim();
        if trimmed == "config:" {
            in_config = true;
            continue;
        }
        if !in_config {
            continue;
        }
        if trimmed.starts_with("errors:") {
            break;
        }
        if trimmed.starts_with("NAME") && trimmed.contains("STATE") {
            in_table = true;
            continue;
        }
        if !in_table || trimmed.is_empty() {
            continue;
        }

        let indent = line.chars().take_while(|c| c.is_whitespace()).count();
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next().map(str::to_string) else {
            continue;
        };
        let state = parts.next().map(str::to_string);

        while let Some((last_indent, _)) = stack.last() {
            if *last_indent >= indent {
                stack.pop();
            } else {
                break;
            }
        }

        if !saw_pool {
            saw_pool = true;
            stack.push((indent, name));
            continue;
        }

        if is_zpool_group_name(&name) {
            stack.push((indent, name));
            continue;
        }

        leaves.push(ZpoolLeafState {
            state,
            vdev_path: stack.iter().skip(1).map(|(_, name)| name.clone()).collect(),
        });
    }

    leaves
}

pub(super) fn zfs_storage_redundancy_from_status(out: &str) -> StorageRedundancy {
    let leaves = parse_zpool_status_leaves(out);
    if leaves.is_empty() {
        return StorageRedundancy::Unknown;
    }
    let has_mirror = leaves.iter().any(|leaf| {
        leaf.vdev_path
            .iter()
            .any(|name| name.starts_with("mirror-"))
    });
    if has_mirror {
        return if leaves.iter().any(|leaf| {
            leaf.vdev_path
                .iter()
                .any(|name| name.starts_with("mirror-"))
                && leaf.state.as_deref() != Some("ONLINE")
        }) {
            StorageRedundancy::NonRedundant
        } else {
            StorageRedundancy::Redundant
        };
    }

    let has_raidz = leaves
        .iter()
        .any(|leaf| leaf.vdev_path.iter().any(|name| name.starts_with("raidz")));
    if has_raidz {
        return if leaves.iter().any(|leaf| {
            leaf.vdev_path.iter().any(|name| name.starts_with("raidz"))
                && leaf.state.as_deref() != Some("ONLINE")
        }) {
            StorageRedundancy::NonRedundant
        } else {
            StorageRedundancy::Redundant
        };
    }

    StorageRedundancy::NonRedundant
}

pub(super) fn zfs_storage_redundancy(dataset: &str) -> StorageRedundancy {
    let pool = dataset.split('/').next().unwrap_or(dataset);
    let output = Command::new("zpool")
        .args(["status", "-P", pool])
        .output()
        .ok();
    let Some(output) = output else {
        return StorageRedundancy::Unknown;
    };
    if !output.status.success() {
        return StorageRedundancy::Unknown;
    }
    zfs_storage_redundancy_from_status(&String::from_utf8_lossy(&output.stdout))
}

pub(super) fn mount_storage_redundancy(info: &MountInfoBrief) -> StorageRedundancy {
    if info.fstype == "zfs" {
        return zfs_storage_redundancy(&info.mount_source);
    }
    if info.mount_source.starts_with("/dev/") {
        if let Some(base) = base_block_name_from_devpath(Path::new(&info.mount_source)) {
            if base.starts_with("md") {
                return md_storage_redundancy(&base);
            }
        }
    }
    StorageRedundancy::Unknown
}

pub(super) fn detect_copy_storage_policy(
    source_path: &str,
    target_path: &str,
) -> (StorageRedundancy, bool) {
    let source_mount = mount_info_for_path(Path::new(source_path));
    let target_mount = mount_info_for_path(Path::new(target_path));
    let redundancy = target_mount
        .as_ref()
        .map(mount_storage_redundancy)
        .unwrap_or(StorageRedundancy::Unknown);
    let reflink_possible = match (source_mount.as_ref(), target_mount.as_ref()) {
        (Some(source), Some(target))
            if source.mount_point == target.mount_point
                && source.fstype == target.fstype
                && filesystem_supports_reflink(&target.fstype) =>
        {
            true
        }
        _ => false,
    };
    (redundancy, reflink_possible)
}

pub(super) fn choose_nonredundant_full_copy_plan(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> ResolvedCopyExecution {
    if should_prefer_cached_read_direct_write(source_cached, source_len, target_len) {
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: true,
            path_label: "auto nonredundant cached-read direct-write",
        }
    } else {
        ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::Direct,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: true,
            path_label: "auto nonredundant direct threaded",
        }
    }
}

pub(super) fn io_mode_label(io_mode: common::IOMode) -> &'static str {
    match io_mode {
        common::IOMode::Auto => "auto",
        common::IOMode::PageCache => "page-cache",
        common::IOMode::Direct => "direct",
    }
}

pub(super) fn describe_copy_path(
    resolved_copy: ResolvedCopyExecution,
    via_memory: bool,
    keep_target_size: bool,
) -> String {
    if via_memory {
        return format!(
            "copy path: via-memory [read={}, write={}]",
            io_mode_label(resolved_copy.io_mode_read),
            io_mode_label(resolved_copy.io_mode_write)
        );
    }

    let mut details = vec![
        format!("strategy={}", resolved_copy.path_label),
        format!("read={}", io_mode_label(resolved_copy.io_mode_read)),
        format!("write={}", io_mode_label(resolved_copy.io_mode_write)),
    ];
    if resolved_copy.full_rewrite {
        details.push("rewrite=full".to_string());
    }
    if resolved_copy.diff_overwrite && !keep_target_size {
        details.push("delta=changed-chunks".to_string());
    }
    if keep_target_size {
        details.push("target=keep-size".to_string());
    }
    format!("copy path: {}", details.join(", "))
}
