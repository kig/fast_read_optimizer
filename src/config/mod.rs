use crate::common::{CopyAutoMode, ReadAutoStrategy, ReadPathKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Once;

static MOUNTINFO_WARNING: Once = Once::new();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountInfo {
    pub mount_point: String,
    pub fstype: String,
    pub mount_source: String,
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

    fn mount_patch_for_path(&self, path: &str) -> Option<&AppConfigPatch> {
        let LoadedConfig::BundleV1 { bundle, .. } = self else {
            return None;
        };
        let mp = mountpoint_for_path(path)?;
        bundle.mount_overrides.by_mountpoint.get(&mp)
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

    pub fn get_copy_range_params(&self) -> IOParams {
        self.defaults_ref().copy_range.clone()
    }

    pub fn get_copy_auto_mode(&self) -> CopyAutoMode {
        self.defaults_ref().copy_auto_mode
    }

    pub fn get_read_auto_strategy(&self) -> ReadAutoStrategy {
        self.defaults_ref().read_auto_strategy
    }

    pub fn get_recursive_small_file_threads(&self) -> RecursiveSmallFileThreads {
        self.defaults_ref().recursive_small_file_threads
    }

    pub fn get_params_for_path(&self, mode: &str, direct: bool, path: &str) -> IOParams {
        let base = self.get_params(mode, direct);
        self.mount_patch_for_path(path)
            .and_then(|patch| patch.get_mode_patch(mode))
            .and_then(|m| {
                if direct {
                    m.direct.clone()
                } else {
                    m.page_cache.clone()
                }
            })
            .unwrap_or(base)
    }

    pub fn get_copy_range_params_for_path(&self, path: &str) -> IOParams {
        self.mount_patch_for_path(path)
            .and_then(|patch| patch.copy_range.clone())
            .unwrap_or_else(|| self.get_copy_range_params())
    }

    pub fn get_copy_auto_mode_for_path(&self, path: &str) -> CopyAutoMode {
        self.mount_patch_for_path(path)
            .and_then(|patch| patch.copy_auto_mode)
            .unwrap_or_else(|| self.get_copy_auto_mode())
    }

    pub fn get_read_auto_strategy_for_path(&self, path: &str) -> ReadAutoStrategy {
        self.mount_patch_for_path(path)
            .and_then(|patch| patch.read_auto_strategy)
            .unwrap_or_else(|| self.get_read_auto_strategy())
    }

    pub fn get_recursive_small_file_threads_for_path(&self, path: &str) -> RecursiveSmallFileThreads {
        self.mount_patch_for_path(path)
            .and_then(|patch| patch.recursive_small_file_threads)
            .unwrap_or_else(|| self.get_recursive_small_file_threads())
    }

    pub fn mount_info_for_path(&self, path: &str) -> Option<MountInfo> {
        mount_info_for_path(path)
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
    fn get_mode_patch(&self, mode: &str) -> Option<&ModeConfigPatch> {
        match mode {
            "read" => self.read.as_ref(),
            "read-to-memory" | "read_to_memory" => self.read_to_memory.as_ref(),
            "write" => self.write.as_ref(),
            "copy" => self.copy.as_ref(),
            "grep" => self.grep.as_ref(),
            "diff" => self.diff.as_ref(),
            "dual-read-bench" | "dual_read_bench" => self.dual_read_bench.as_ref(),
            "compute" => self.compute.as_ref(),
            "hash" => self.hash.as_ref(),
            "verify" => self.verify.as_ref(),
            _ => None,
        }
    }

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
}

fn mountpoint_for_path(path: &str) -> Option<String> {
    mount_info_for_path(path).map(|info| info.mount_point)
}

pub fn mount_info_for_path(path: &str) -> Option<MountInfo> {
    let p = std::path::Path::new(path);
    let canonical = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    let path = canonical.to_string_lossy();

    let data = match fs::read_to_string("/proc/self/mountinfo") {
        Ok(data) => data,
        Err(err) => {
            MOUNTINFO_WARNING.call_once(|| {
                eprintln!(
                    "Warning: could not read /proc/self/mountinfo ({}); mount-specific config overrides are disabled",
                    err
                );
            });
            return None;
        }
    };

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
        let mp = left_fields[4];
        if !path_starts_with_mount(&path, mp) {
            continue;
        }
        if mp.len() > best_len {
            best_len = mp.len();
            best = Some(MountInfo {
                mount_point: mp.to_string(),
                fstype: right_fields[0].to_string(),
                mount_source: right_fields.get(1).unwrap_or(&"").to_string(),
            });
        }
    }

    best
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

pub fn default_user_config_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".fro").join("fro.json"))
}

pub fn default_system_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("FRO_SYSTEM_CONFIG") {
        return PathBuf::from(p);
    }
    PathBuf::from("/etc/fro.json")
}

pub fn resolve_default_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("FRO_CONFIG") {
        return PathBuf::from(p);
    }

    if let Some(p) = default_user_config_path() {
        if p.exists() {
            return p;
        }
    }

    let sys = default_system_config_path();
    if sys.exists() {
        return sys;
    }

    // Default to user path even if it doesn't exist yet.
    default_user_config_path().unwrap_or_else(|| PathBuf::from("fro.json"))
}

pub fn load_config(path: Option<&str>) -> LoadedConfig {
    let path = path
        .map(PathBuf::from)
        .unwrap_or_else(resolve_default_config_path);

    if path.exists() {
        match fs::read_to_string(&path) {
            Ok(data) => {
                if let Ok(bundle) = serde_json::from_str::<ConfigBundleV1>(&data) {
                    if bundle.version == 1 {
                        return LoadedConfig::BundleV1 { path, bundle };
                    }
                }

                if let Ok(config) = serde_json::from_str::<AppConfig>(&data) {
                    return LoadedConfig::Legacy { path, config };
                }

                eprintln!(
                    "Warning: config {} is malformed; using defaults without overwriting it",
                    path.display()
                );
            }
            Err(err) => {
                eprintln!(
                    "Warning: could not read config {} ({}); using defaults without overwriting it",
                    path.display(),
                    err
                );
            }
        }

        return LoadedConfig::BundleV1 {
            path,
            bundle: default_bundle_v1(),
        };
    }

    // Create a default config at the chosen path.
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let bundle = default_bundle_v1();

    if let Ok(data) = serde_json::to_string_pretty(&bundle) {
        let _ = fs::write(&path, data);
    }

    LoadedConfig::BundleV1 { path, bundle }
}

fn default_bundle_v1() -> ConfigBundleV1 {
    let mut db_paths = vec![
        "/etc/fro.d/disk-id.json".into(),
        "/etc/fro.d/fro-device-db.json".into(),
    ];
    if let Ok(home) = std::env::var("HOME") {
        db_paths.push(format!("{}/.config/fro/fro-device-db.json", home));
    }
    ConfigBundleV1 {
        version: 1,
        defaults: AppConfig::default(),
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig {
            paths: db_paths,
            allow_online_update: false,
        },
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        let default_direct = IOParams {
            num_threads: 16,
            block_size: 3 * 1024 * 1024,
            qd: 2,
        };
        let default_cache = IOParams {
            num_threads: 31,
            block_size: 128 * 1024,
            qd: 1,
        };
        let default_write_direct = IOParams {
            num_threads: 4,
            block_size: 256 * 1024,
            qd: 3,
        };
        let default_write = IOParams {
            num_threads: 4,
            block_size: 1024 * 1024,
            qd: 2,
        };
        let default_copy_range = IOParams {
            num_threads: 4,
            block_size: 512 * 1024,
            qd: 4,
        };

        let default_mode = ModeConfig {
            direct: default_direct.clone(),
            page_cache: default_cache.clone(),
        };

        let default_hash = IOParams {
            num_threads: default_cache.num_threads,
            block_size: crate::block_hash::BLOCK_HASH_SIZE,
            qd: default_cache.qd,
        };
        let default_hash_direct = IOParams {
            num_threads: default_direct.num_threads,
            block_size: crate::block_hash::BLOCK_HASH_SIZE,
            qd: default_direct.qd,
        };
        let default_hash_mode = ModeConfig {
            direct: default_hash_direct.clone(),
            page_cache: default_hash.clone(),
        };
        let default_compute_mode = ModeConfig {
            direct: IOParams {
                num_threads: 32,
                block_size: crate::block_hash::BLOCK_HASH_SIZE,
                qd: default_direct.qd,
            },
            page_cache: IOParams {
                num_threads: 32,
                block_size: crate::block_hash::BLOCK_HASH_SIZE,
                qd: default_cache.qd,
            },
        };

        let default_write_mode = ModeConfig {
            direct: default_write_direct.clone(),
            page_cache: default_write.clone(),
        };

        // Custom defaults based on previous tuning
        let mut diff_cache = default_cache.clone();
        diff_cache.num_threads = 4;

        AppConfig {
            read: default_mode.clone(),
            read_to_memory: default_mode.clone(),
            write: default_write_mode.clone(),
            copy: default_write_mode.clone(),
            copy_range: default_copy_range,
            copy_auto_mode: CopyAutoMode::Heuristic,
            read_auto_strategy: default_read_auto_strategy(),
            recursive_small_file_threads: default_recursive_small_file_threads(),
            grep: default_mode.clone(),
            diff: ModeConfig {
                direct: default_direct.clone(),
                page_cache: diff_cache.clone(),
            },
            dual_read_bench: ModeConfig {
                direct: default_direct.clone(),
                page_cache: diff_cache.clone(),
            },
            compute: default_compute_mode,
            hash: default_hash_mode.clone(),
            verify: default_hash_mode,
        }
    }
}

impl AppConfig {
    pub fn save(&self, path: &str) {
        if let Ok(data) = serde_json::to_string_pretty(self) {
            let _ = fs::write(path, data);
        }
    }

    pub fn get_params(&self, mode: &str, direct: bool) -> IOParams {
        let mode_config = match mode {
            "read" => &self.read,
            "read-to-memory" | "read_to_memory" => &self.read_to_memory,
            "write" => &self.write,
            "copy" => &self.copy,
            "grep" => &self.grep,
            "diff" => &self.diff,
            "dual-read-bench" => &self.dual_read_bench,
            "compute" => &self.compute,
            "hash" => &self.hash,
            "verify" => &self.verify,
            "copy_range" => {
                return self.copy_range.clone();
            }
            _ => {
                return if direct {
                    self.read.direct.clone()
                } else {
                    self.read.page_cache.clone()
                }
            }
        };

        if direct {
            mode_config.direct.clone()
        } else {
            mode_config.page_cache.clone()
        }
    }

    pub fn update_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        let mode_config = match mode {
            "read" => &mut self.read,
            "read-to-memory" | "read_to_memory" => &mut self.read_to_memory,
            "write" => &mut self.write,
            "copy" => &mut self.copy,
            "grep" => &mut self.grep,
            "diff" => &mut self.diff,
            "dual-read-bench" => &mut self.dual_read_bench,
            "compute" => &mut self.compute,
            "hash" => &mut self.hash,
            "verify" => &mut self.verify,
            "copy_range" => {
                self.copy_range = params;
                return;
            }
            _ => return,
        };

        if direct {
            mode_config.direct = params;
        } else {
            mode_config.page_cache = params;
        }
    }
}

fn default_read_auto_strategy() -> ReadAutoStrategy {
    ReadAutoStrategy {
        hot_large_min_bytes: 256 * 1024 * 1024,
        cold_large_min_bytes: 256 * 1024 * 1024,
        hot_small_path: ReadPathKind::SimplePageCache,
        hot_large_path: ReadPathKind::ThreadedPageCache,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::ThreadedDirect,
    }
}

fn default_recursive_small_file_threads() -> RecursiveSmallFileThreads {
    RecursiveSmallFileThreads { hot: 32, cold: 32 }
}

fn default_hash_mode_config() -> ModeConfig {
    AppConfig::default().hash
}

fn default_compute_mode_config() -> ModeConfig {
    AppConfig::default().compute
}

fn default_read_to_memory_mode_config() -> ModeConfig {
    AppConfig::default().read_to_memory
}

fn default_verify_mode_config() -> ModeConfig {
    AppConfig::default().verify
}

fn default_copy_range_params() -> IOParams {
    AppConfig::default().copy_range
}

fn default_copy_auto_mode() -> CopyAutoMode {
    CopyAutoMode::Heuristic
}


#[cfg(test)]
mod tests;
