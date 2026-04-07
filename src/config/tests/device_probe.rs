use super::*;

fn write_trimmed_file(path: &Path, value: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, format!("{value}\n")).unwrap();
}

#[test]
fn mount_info_parsing_prefers_longest_boundary_match_from_fixture() {
    let mountinfo = concat!(
        "11 1 0:1 / / rw,relatime - overlay overlay rw\n",
        "12 11 0:2 / /mnt/data rw,relatime - ext4 /dev/md0 rw\n",
        "13 12 0:3 / /mnt/data/archive\\040space rw,relatime - xfs /dev/mapper/archive\\040vg rw\n",
        "14 11 0:4 / /mnt/data-archive rw,relatime - ext4 /dev/sdb1 rw\n",
        "malformed line without separator\n",
        "15 11 0:5 /missing-fields - ext4 /dev/ignored rw\n",
    );

    struct Case<'a> {
        path: &'a str,
        expected: Option<(&'a str, &'a str, &'a str)>,
    }

    let cases = [
        Case {
            path: "/mnt/data/file.bin",
            expected: Some(("/mnt/data", "ext4", "/dev/md0")),
        },
        Case {
            path: "/mnt/data/archive space/log.txt",
            expected: Some(("/mnt/data/archive space", "xfs", "/dev/mapper/archive vg")),
        },
        Case {
            path: "/mnt/data-archive/snapshot",
            expected: Some(("/mnt/data-archive", "ext4", "/dev/sdb1")),
        },
        Case {
            path: "/mnt/datax/not-a-child",
            expected: Some(("/", "overlay", "overlay")),
        },
    ];

    for case in cases {
        let actual = mount_info_for_path_from_data(case.path, mountinfo);
        let expected = case
            .expected
            .map(|(mount_point, fstype, mount_source)| MountInfo {
                mount_point: mount_point.into(),
                fstype: fstype.into(),
                mount_source: mount_source.into(),
            });
        assert_eq!(actual, expected, "path {}", case.path);
    }
}

#[test]
fn mount_info_parsing_defaults_missing_mount_source_and_skips_non_matches() {
    let mountinfo = concat!(
        "21 1 0:10 / /cache rw,relatime - tmpfs\n",
        "22 1 0:11 / /srv\\040share rw,relatime - nfs server:/export\\040share rw\n",
    );

    let cache = mount_info_for_path_from_data("/cache/tmp.txt", mountinfo).unwrap();
    assert_eq!(cache.mount_point, "/cache");
    assert_eq!(cache.fstype, "tmpfs");
    assert_eq!(cache.mount_source, "");

    let share = mount_info_for_path_from_data("/srv share/file.txt", mountinfo).unwrap();
    assert_eq!(share.mount_point, "/srv share");
    assert_eq!(share.mount_source, "server:/export share");

    assert!(mount_info_for_path_from_data("/unmounted/path", mountinfo).is_none());
}

#[test]
fn device_signature_for_non_block_mount_source_keeps_minimal_match_keys() {
    let mount = MountInfo {
        mount_point: "/run/cache".into(),
        fstype: "tmpfs".into(),
        mount_source: "tmpfs".into(),
    };

    let signature =
        device_signature_from_mount_info_with_roots(&mount, &DeviceProbeRoots::default()).unwrap();
    assert_eq!(signature.mount_source, "tmpfs");
    assert_eq!(signature.canonical_source, "tmpfs");
    assert_eq!(signature.match_keys, vec!["mount_source=tmpfs".to_string()]);
    assert!(signature.block_device.is_none());
}

#[test]
fn device_signature_extracts_leaf_device_metadata_and_by_id() {
    let tmp = unique_temp_dir("fro-device-signature-leaf");
    let dev_root = tmp.join("dev");
    let sys_root = tmp.join("sys/class/block");
    std::fs::create_dir_all(dev_root.join("disk/by-id")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/queue")).unwrap();
    std::fs::write(dev_root.join("nvme0n1"), b"").unwrap();
    std::os::unix::fs::symlink("../../nvme0n1", dev_root.join("disk/by-id/nvme-fastdisk")).unwrap();
    write_trimmed_file(&sys_root.join("nvme0n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme0n1/device/model"), "Turbo");
    write_trimmed_file(&sys_root.join("nvme0n1/queue/rotational"), "0");

    let mount = MountInfo {
        mount_point: "/mnt/data".into(),
        fstype: "ext4".into(),
        mount_source: "/dev/disk/by-id/nvme-fastdisk".into(),
    };
    let roots = DeviceProbeRoots {
        dev_root: dev_root.clone(),
        sys_class_block_root: sys_root.clone(),
    };

    let signature = device_signature_from_mount_info_with_roots(&mount, &roots).unwrap();
    assert_eq!(signature.mount_source, "/dev/disk/by-id/nvme-fastdisk");
    assert_eq!(signature.canonical_source, "/dev/nvme0n1");
    assert!(signature
        .match_keys
        .contains(&"canonical_source=/dev/nvme0n1".to_string()));
    assert!(signature
        .match_keys
        .contains(&"by-id=nvme-fastdisk".to_string()));

    let block = signature.block_device.unwrap();
    assert_eq!(block.kernel_name, "nvme0n1");
    assert_eq!(block.devnode, "/dev/nvme0n1");
    assert_eq!(block.by_id, vec!["nvme-fastdisk".to_string()]);
    assert_eq!(block.vendor.as_deref(), Some("ACME"));
    assert_eq!(block.model.as_deref(), Some("Turbo"));
    assert_eq!(block.rotational, Some(false));
    assert!(block.slaves.is_empty());
}

#[test]
fn device_signature_extracts_composite_dm_and_md_slaves() {
    let tmp = unique_temp_dir("fro-device-signature-composite");
    let dev_root = tmp.join("dev");
    let sys_root = tmp.join("sys/class/block");
    std::fs::create_dir_all(dev_root.join("disk/by-id")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/dm")).unwrap();
    std::fs::create_dir_all(sys_root.join("dm-0/slaves")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/md")).unwrap();
    std::fs::create_dir_all(sys_root.join("md0/slaves")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme0n1/queue")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme1n1/device")).unwrap();
    std::fs::create_dir_all(sys_root.join("nvme1n1/queue")).unwrap();
    std::fs::write(dev_root.join("dm-0"), b"").unwrap();
    std::fs::write(dev_root.join("md0"), b"").unwrap();
    std::fs::write(dev_root.join("nvme0n1"), b"").unwrap();
    std::fs::write(dev_root.join("nvme1n1"), b"").unwrap();
    std::os::unix::fs::symlink("../../dm-0", dev_root.join("disk/by-id/dm-uuid-crypt")).unwrap();
    std::os::unix::fs::symlink("../../md0", dev_root.join("disk/by-id/md-array")).unwrap();
    std::os::unix::fs::symlink("../../nvme0n1", dev_root.join("disk/by-id/nvme-disk-a")).unwrap();
    std::os::unix::fs::symlink("../../nvme1n1", dev_root.join("disk/by-id/nvme-disk-b")).unwrap();
    std::os::unix::fs::symlink("../md0", sys_root.join("dm-0/slaves/md0")).unwrap();
    std::os::unix::fs::symlink("../nvme0n1", sys_root.join("md0/slaves/nvme0n1")).unwrap();
    std::os::unix::fs::symlink("../nvme1n1", sys_root.join("md0/slaves/nvme1n1")).unwrap();
    write_trimmed_file(&sys_root.join("dm-0/dm/name"), "cryptroot");
    write_trimmed_file(&sys_root.join("dm-0/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("md0/md/level"), "raid0");
    write_trimmed_file(&sys_root.join("md0/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("nvme0n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme0n1/device/model"), "Fast A");
    write_trimmed_file(&sys_root.join("nvme0n1/queue/rotational"), "0");
    write_trimmed_file(&sys_root.join("nvme1n1/device/vendor"), "ACME");
    write_trimmed_file(&sys_root.join("nvme1n1/device/model"), "Fast B");
    write_trimmed_file(&sys_root.join("nvme1n1/queue/rotational"), "0");

    let mount = MountInfo {
        mount_point: "/mnt/crypt".into(),
        fstype: "xfs".into(),
        mount_source: "/dev/mapper/cryptroot".into(),
    };
    let roots = DeviceProbeRoots {
        dev_root: dev_root.clone(),
        sys_class_block_root: sys_root.clone(),
    };

    std::fs::create_dir_all(dev_root.join("mapper")).unwrap();
    std::os::unix::fs::symlink("../dm-0", dev_root.join("mapper/cryptroot")).unwrap();

    let signature = device_signature_from_mount_info_with_roots(&mount, &roots).unwrap();
    assert_eq!(signature.canonical_source, "/dev/dm-0");
    let block = signature.block_device.unwrap();
    assert_eq!(block.kernel_name, "dm-0");
    assert_eq!(block.dm_name.as_deref(), Some("cryptroot"));
    assert_eq!(block.by_id, vec!["dm-uuid-crypt".to_string()]);
    assert_eq!(block.slaves.len(), 1);
    let md = &block.slaves[0];
    assert_eq!(md.kernel_name, "md0");
    assert_eq!(md.md_level.as_deref(), Some("raid0"));
    assert_eq!(md.by_id, vec!["md-array".to_string()]);
    assert_eq!(md.slaves.len(), 2);
    assert!(signature
        .match_keys
        .contains(&"dm_name=cryptroot".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.md_level=raid0".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.slave.by-id=nvme-disk-a".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.slave.by-id=nvme-disk-b".to_string()));
    assert!(signature.match_keys.contains(&"kind=dm".to_string()));
    assert!(signature.match_keys.contains(&"slave.kind=md".to_string()));
    assert!(signature
        .match_keys
        .contains(&"slave.slave.kind=nvme".to_string()));
    assert!(signature
        .match_keys
        .contains(&"component.kind=dm".to_string()));
    assert!(signature
        .match_keys
        .contains(&"component.kind=md".to_string()));
    assert!(signature
        .match_keys
        .contains(&"component.by-id=md-array".to_string()));
    assert!(signature.match_keys.contains(&"stack=dm".to_string()));
    assert!(signature.match_keys.contains(&"stack=dm>md".to_string()));
    assert!(signature
        .match_keys
        .contains(&"stack=dm>md>nvme".to_string()));
    assert!(signature.match_keys.contains(&"leaf.kind=nvme".to_string()));
    assert!(signature
        .match_keys
        .contains(&"leaf.by-id=nvme-disk-a".to_string()));
    assert!(signature
        .match_keys
        .contains(&"leaf.by-id=nvme-disk-b".to_string()));
    assert!(signature
        .match_keys
        .contains(&"leaf.model=Fast A".to_string()));
    assert!(signature
        .match_keys
        .contains(&"leaf.model=Fast B".to_string()));
}
