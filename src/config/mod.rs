use crate::common::{CopyAutoMode, ReadAutoStrategy, ReadPathKind};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Once;

mod device;
mod storage;
#[cfg(test)]
mod tests;

pub use self::storage::{load_config, resolve_default_config_path};

use self::device::{device_db_match, mountpoint_for_path, normalize_path};
pub use self::device::{device_signature_for_path, mount_info_for_path};
use self::storage::{
    default_bundle_v1, default_compute_mode_config, default_copy_auto_mode,
    default_copy_range_params, default_hash_mode_config, default_read_auto_strategy,
    default_read_to_memory_mode_config, default_recursive_small_file_threads,
    default_verify_mode_config,
};

static MOUNTINFO_WARNING: Once = Once::new();
const _: fn() -> PathBuf = resolve_default_config_path;

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct MountInfo {
    pub mount_point: String,
    pub fstype: String,
    pub mount_source: String,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct DeviceSignature {
    pub mount_source: String,
    pub canonical_source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_device: Option<BlockDeviceSignature>,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct BlockDeviceSignature {
    pub kernel_name: String,
    pub devnode: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub by_id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotational: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dm_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md_level: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slaves: Vec<BlockDeviceSignature>,
}

#[derive(Clone, Debug)]
struct DeviceProbeRoots {
    dev_root: PathBuf,
    sys_class_block_root: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IOParams {
    pub num_threads: u64,
    pub block_size: u64,
    pub qd: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ModeConfig {
    pub direct: IOParams,
    pub page_cache: IOParams,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub read: ModeConfig,
    #[serde(default = "default_read_to_memory_mode_config")]
    pub read_to_memory: ModeConfig,
    pub write: ModeConfig,
    pub copy: ModeConfig,
    #[serde(default = "default_copy_range_params")]
    pub copy_range: IOParams,
    #[serde(default = "default_copy_auto_mode")]
    pub copy_auto_mode: CopyAutoMode,
    #[serde(default = "default_read_auto_strategy")]
    pub read_auto_strategy: ReadAutoStrategy,
    #[serde(default = "default_recursive_small_file_threads")]
    pub recursive_small_file_threads: RecursiveSmallFileThreads,
    pub grep: ModeConfig,
    pub diff: ModeConfig,
    pub dual_read_bench: ModeConfig,
    #[serde(default = "default_compute_mode_config")]
    pub compute: ModeConfig,
    #[serde(default = "default_hash_mode_config")]
    pub hash: ModeConfig,
    #[serde(default = "default_verify_mode_config")]
    pub verify: ModeConfig,
}

// New config wrapper. For now it primarily wraps the existing AppConfig shape,
// while letting us grow into mount/device selection without breaking schema.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConfigBundleV1 {
    pub version: u32,
    pub defaults: AppConfig,

    #[serde(default)]
    pub mount_overrides: MountOverrides,

    #[serde(default)]
    pub device_db: DeviceDbConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct MountOverrides {
    #[serde(default)]
    pub by_mountpoint: HashMap<String, AppConfigPatch>,
}

// Sparse overrides (only fill what you want to override).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AppConfigPatch {
    pub read: Option<ModeConfigPatch>,
    pub read_to_memory: Option<ModeConfigPatch>,
    pub write: Option<ModeConfigPatch>,
    pub copy: Option<ModeConfigPatch>,
    pub copy_range: Option<IOParams>,
    pub copy_auto_mode: Option<CopyAutoMode>,
    pub read_auto_strategy: Option<ReadAutoStrategy>,
    pub recursive_small_file_threads: Option<RecursiveSmallFileThreads>,
    pub grep: Option<ModeConfigPatch>,
    pub diff: Option<ModeConfigPatch>,
    pub dual_read_bench: Option<ModeConfigPatch>,
    pub compute: Option<ModeConfigPatch>,
    pub hash: Option<ModeConfigPatch>,
    pub verify: Option<ModeConfigPatch>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ModeConfigPatch {
    pub direct: Option<IOParams>,
    pub page_cache: Option<IOParams>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DeviceDbConfig {
    #[serde(default)]
    pub paths: Vec<String>,

    #[serde(default)]
    pub allow_online_update: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DeviceDb {
    version: u32,
    profiles: Vec<DeviceDbProfile>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DeviceDbProfile {
    id: String,
    #[serde(rename = "match")]
    match_fields: DeviceDbMatch,
    params: AppConfigPatch,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
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

#[derive(Clone, Debug)]
struct DeviceDbSelection {
    source_path: String,
    profile: DeviceDbProfile,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecursiveSmallFileThreads {
    pub hot: u64,
    pub cold: u64,
}

#[derive(Clone, Debug)]
pub enum LoadedConfig {
    Legacy {
        path: PathBuf,
        config: AppConfig,
    },
    BundleV1 {
        path: PathBuf,
        bundle: ConfigBundleV1,
    },
}

impl LoadedConfig {
    fn promote_legacy_to_bundle(&mut self) {
        let LoadedConfig::Legacy { path, config } = self else {
            return;
        };
        let path = path.clone();
        let config = config.clone();
        *self = LoadedConfig::BundleV1 {
            path,
            bundle: ConfigBundleV1 {
                version: 1,
                defaults: config,
                mount_overrides: MountOverrides::default(),
                device_db: default_bundle_v1().device_db,
            },
        };
    }

    fn defaults_ref(&self) -> &AppConfig {
        match self {
            LoadedConfig::Legacy { config, .. } => config,
            LoadedConfig::BundleV1 { bundle, .. } => &bundle.defaults,
        }
    }

    fn mount_patch_for_mount(&self, mount: Option<&MountInfo>) -> Option<&AppConfigPatch> {
        let LoadedConfig::BundleV1 { bundle, .. } = self else {
            return None;
        };
        let mount = mount?;
        bundle.mount_overrides.by_mountpoint.get(&mount.mount_point)
    }

    fn device_db_selection_for_context(
        &self,
        mount: Option<&MountInfo>,
        device: Option<&DeviceSignature>,
    ) -> Option<DeviceDbSelection> {
        let LoadedConfig::BundleV1 { bundle, .. } = self else {
            return None;
        };
        let mount = mount?;
        for db_path in &bundle.device_db.paths {
            let data = match fs::read_to_string(db_path) {
                Ok(data) => data,
                Err(_) => continue,
            };
            let db = match serde_json::from_str::<DeviceDb>(&data) {
                Ok(db) if db.version == 1 => db,
                _ => continue,
            };
            if let Some(profile) = device_db_match(&db, mount, device) {
                return Some(DeviceDbSelection {
                    source_path: db_path.clone(),
                    profile: profile.clone(),
                });
            }
        }
        None
    }

    fn effective_config_for_context(
        &self,
        mount: Option<&MountInfo>,
        device: Option<&DeviceSignature>,
    ) -> AppConfig {
        let mut effective = self.defaults_ref().clone();
        if let Some(selection) = self.device_db_selection_for_context(mount, device) {
            selection.profile.params.apply_to(&mut effective);
        }
        if let Some(patch) = self.mount_patch_for_mount(mount) {
            patch.apply_to(&mut effective);
        }
        effective
    }

    fn mount_patch_for_path_mut(&mut self, path: &str) -> Option<&mut AppConfigPatch> {
        let LoadedConfig::BundleV1 { bundle, .. } = self else {
            return None;
        };
        let mp = mountpoint_for_path(path).unwrap_or_else(|| "/".to_string());
        Some(
            bundle
                .mount_overrides
                .by_mountpoint
                .entry(mp)
                .or_insert_with(AppConfigPatch::default),
        )
    }

    #[allow(dead_code)]
    pub fn defaults_mut(&mut self) -> &mut AppConfig {
        match self {
            LoadedConfig::Legacy { config, .. } => config,
            LoadedConfig::BundleV1 { bundle, .. } => &mut bundle.defaults,
        }
    }

    pub fn get_params(&self, mode: &str, direct: bool) -> IOParams {
        self.defaults_ref().get_params(mode, direct)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn get_copy_range_params(&self) -> IOParams {
        self.defaults_ref().copy_range.clone()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn get_copy_auto_mode(&self) -> CopyAutoMode {
        self.defaults_ref().copy_auto_mode
    }

    pub fn get_read_auto_strategy(&self) -> ReadAutoStrategy {
        self.defaults_ref().read_auto_strategy
    }
    pub fn get_params_for_path(&self, mode: &str, direct: bool, path: &str) -> IOParams {
        self.effective_config_for_path(path)
            .get_params(mode, direct)
    }

    pub fn get_copy_range_params_for_path(&self, path: &str) -> IOParams {
        self.effective_config_for_path(path).copy_range
    }

    pub fn get_copy_auto_mode_for_path(&self, path: &str) -> CopyAutoMode {
        self.effective_config_for_path(path).copy_auto_mode
    }

    pub fn get_read_auto_strategy_for_path(&self, path: &str) -> ReadAutoStrategy {
        self.effective_config_for_path(path).read_auto_strategy
    }

    pub fn get_recursive_small_file_threads_for_path(
        &self,
        path: &str,
    ) -> RecursiveSmallFileThreads {
        self.effective_config_for_path(path)
            .recursive_small_file_threads
    }

    pub fn mount_info_for_path(&self, path: &str) -> Option<MountInfo> {
        mount_info_for_path(path)
    }

    pub fn device_signature_for_path(&self, path: &str) -> Option<DeviceSignature> {
        device_signature_for_path(path)
    }

    pub fn config_path(&self) -> &Path {
        match self {
            LoadedConfig::Legacy { path, .. } | LoadedConfig::BundleV1 { path, .. } => {
                path.as_path()
            }
        }
    }

    pub fn format_name(&self) -> &'static str {
        match self {
            LoadedConfig::Legacy { .. } => "legacy",
            LoadedConfig::BundleV1 { .. } => "bundle_v1",
        }
    }

    pub fn effective_config_for_path(&self, path: &str) -> AppConfig {
        let mount = self.mount_info_for_path(path);
        let device = self.device_signature_for_path(path);
        self.effective_config_for_context(mount.as_ref(), device.as_ref())
    }

    pub fn explain_for_path(&self, path: &str) -> Value {
        let normalized_path = normalize_path(path);
        let mount_info = self.mount_info_for_path(path);
        let device = self.device_signature_for_path(path);
        let mount_override = self.mount_patch_for_mount(mount_info.as_ref()).cloned();
        let device_db_match =
            self.device_db_selection_for_context(mount_info.as_ref(), device.as_ref());
        let effective = self.effective_config_for_context(mount_info.as_ref(), device.as_ref());
        let device_db = match self {
            LoadedConfig::Legacy { .. } => None,
            LoadedConfig::BundleV1 { bundle, .. } => Some(bundle.device_db.clone()),
        };

        json!({
            "config_path": self.config_path(),
            "config_format": self.format_name(),
            "requested_path": path,
            "normalized_path": normalized_path,
            "mount": mount_info,
            "device": device,
            "defaults": self.defaults_ref(),
            "device_db_match": device_db_match.as_ref().map(|selection| json!({
                "source_path": selection.source_path,
                "profile_id": selection.profile.id,
                "match": selection.profile.match_fields,
                "params": selection.profile.params,
                "notes": selection.profile.notes,
            })),
            "mount_override": mount_override,
            "effective": effective,
            "device_db": device_db,
        })
    }

    pub fn to_pretty_json(&self) -> io::Result<String> {
        let rendered = match self {
            LoadedConfig::Legacy { config, .. } => serde_json::to_string_pretty(config),
            LoadedConfig::BundleV1 { bundle, .. } => serde_json::to_string_pretty(bundle),
        };
        rendered.map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }

    #[allow(dead_code)]
    pub fn update_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        self.defaults_mut().update_params(mode, direct, params)
    }

    #[allow(dead_code)]
    pub fn update_copy_range_params(&mut self, params: IOParams) {
        self.defaults_mut().copy_range = params;
    }

    #[allow(dead_code)]
    pub fn update_copy_auto_mode(&mut self, mode: CopyAutoMode) {
        self.defaults_mut().copy_auto_mode = mode;
    }

    #[allow(dead_code)]
    pub fn update_read_auto_strategy(&mut self, strategy: ReadAutoStrategy) {
        self.defaults_mut().read_auto_strategy = strategy;
    }

    #[allow(dead_code)]
    pub fn update_recursive_small_file_threads(&mut self, threads: RecursiveSmallFileThreads) {
        self.defaults_mut().recursive_small_file_threads = threads;
    }

    pub fn update_params_for_path(
        &mut self,
        mode: &str,
        direct: bool,
        path: &str,
        params: IOParams,
    ) {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            self.promote_legacy_to_bundle();
        }
        if let Some(entry) = self.mount_patch_for_path_mut(path) {
            entry.set_mode_params(mode, direct, params);
        } else if let LoadedConfig::Legacy { config, .. } = self {
            config.update_params(mode, direct, params);
        }
    }

    pub fn update_copy_range_params_for_path(&mut self, path: &str, params: IOParams) {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            self.promote_legacy_to_bundle();
        }
        if let Some(entry) = self.mount_patch_for_path_mut(path) {
            entry.copy_range = Some(params);
        } else if let LoadedConfig::Legacy { config, .. } = self {
            config.copy_range = params;
        }
    }

    #[allow(dead_code)]
    pub fn update_copy_auto_mode_for_path(&mut self, path: &str, mode: CopyAutoMode) {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            self.promote_legacy_to_bundle();
        }
        if let Some(entry) = self.mount_patch_for_path_mut(path) {
            entry.copy_auto_mode = Some(mode);
        } else if let LoadedConfig::Legacy { config, .. } = self {
            config.copy_auto_mode = mode;
        }
    }

    pub fn update_read_auto_strategy_for_path(&mut self, path: &str, strategy: ReadAutoStrategy) {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            self.promote_legacy_to_bundle();
        }
        if let Some(entry) = self.mount_patch_for_path_mut(path) {
            entry.read_auto_strategy = Some(strategy);
        } else if let LoadedConfig::Legacy { config, .. } = self {
            config.read_auto_strategy = strategy;
        }
    }

    pub fn update_recursive_small_file_threads_for_path(
        &mut self,
        path: &str,
        threads: RecursiveSmallFileThreads,
    ) {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            self.promote_legacy_to_bundle();
        }
        if let Some(entry) = self.mount_patch_for_path_mut(path) {
            entry.recursive_small_file_threads = Some(threads);
        } else if let LoadedConfig::Legacy { config, .. } = self {
            config.recursive_small_file_threads = threads;
        }
    }

    #[allow(dead_code)]
    pub fn promote_mount_override_to_defaults_for_path(&mut self, path: &str) -> bool {
        if matches!(self, LoadedConfig::Legacy { .. }) {
            return false;
        }

        let LoadedConfig::BundleV1 { bundle, .. } = self else {
            return false;
        };
        let mount_point = mountpoint_for_path(path).unwrap_or_else(|| "/".to_string());
        let Some(patch) = bundle.mount_overrides.by_mountpoint.remove(&mount_point) else {
            return false;
        };
        patch.apply_to(&mut bundle.defaults);
        true
    }

    pub fn save(&self) {
        match self {
            LoadedConfig::Legacy { path, config } => {
                config.save(path.to_str().unwrap_or("fro.json"))
            }
            LoadedConfig::BundleV1 { path, bundle } => {
                if let Ok(data) = serde_json::to_string_pretty(bundle) {
                    let _ = fs::write(path, data);
                }
            }
        }
    }
}

impl AppConfigPatch {
    fn set_mode_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        let m = match mode {
            "read" => &mut self.read,
            "read-to-memory" | "read_to_memory" => &mut self.read_to_memory,
            "write" => &mut self.write,
            "copy" => &mut self.copy,
            "grep" => &mut self.grep,
            "diff" => &mut self.diff,
            "dual-read-bench" | "dual_read_bench" => &mut self.dual_read_bench,
            "compute" => &mut self.compute,
            "hash" => &mut self.hash,
            "verify" => &mut self.verify,
            _ => return,
        };

        let mp = m.get_or_insert_with(ModeConfigPatch::default);
        if direct {
            mp.direct = Some(params);
        } else {
            mp.page_cache = Some(params);
        }
    }

    fn apply_to(&self, config: &mut AppConfig) {
        if let Some(patch) = &self.read {
            patch.apply_to(&mut config.read);
        }
        if let Some(patch) = &self.read_to_memory {
            patch.apply_to(&mut config.read_to_memory);
        }
        if let Some(patch) = &self.write {
            patch.apply_to(&mut config.write);
        }
        if let Some(patch) = &self.copy {
            patch.apply_to(&mut config.copy);
        }
        if let Some(params) = self.copy_range.clone() {
            config.copy_range = params;
        }
        if let Some(mode) = self.copy_auto_mode {
            config.copy_auto_mode = mode;
        }
        if let Some(strategy) = self.read_auto_strategy {
            config.read_auto_strategy = strategy;
        }
        if let Some(threads) = self.recursive_small_file_threads {
            config.recursive_small_file_threads = threads;
        }
        if let Some(patch) = &self.grep {
            patch.apply_to(&mut config.grep);
        }
        if let Some(patch) = &self.diff {
            patch.apply_to(&mut config.diff);
        }
        if let Some(patch) = &self.dual_read_bench {
            patch.apply_to(&mut config.dual_read_bench);
        }
        if let Some(patch) = &self.compute {
            patch.apply_to(&mut config.compute);
        }
        if let Some(patch) = &self.hash {
            patch.apply_to(&mut config.hash);
        }
        if let Some(patch) = &self.verify {
            patch.apply_to(&mut config.verify);
        }
    }
}

impl ModeConfigPatch {
    fn apply_to(&self, mode: &mut ModeConfig) {
        if let Some(params) = self.direct.clone() {
            mode.direct = params;
        }
        if let Some(params) = self.page_cache.clone() {
            mode.page_cache = params;
        }
    }
}
