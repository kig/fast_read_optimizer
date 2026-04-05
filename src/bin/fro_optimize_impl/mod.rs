use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq, Default)]
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


mod inspect;
#[cfg(test)]
mod tests;
mod cli_utils;
mod run;


pub(super) fn main_impl() {
    run::main_impl();
}
