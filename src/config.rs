use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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
    pub write: ModeConfig,
    pub copy: ModeConfig,
    pub grep: ModeConfig,
    pub diff: ModeConfig,
    pub dual_read_bench: ModeConfig,
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
    pub write: Option<ModeConfigPatch>,
    pub copy: Option<ModeConfigPatch>,
    pub grep: Option<ModeConfigPatch>,
    pub diff: Option<ModeConfigPatch>,
    pub dual_read_bench: Option<ModeConfigPatch>,
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

#[derive(Clone, Debug)]
pub enum LoadedConfig {
    Legacy { path: PathBuf, config: AppConfig },
    BundleV1 { path: PathBuf, bundle: ConfigBundleV1 },
}

impl LoadedConfig {
    pub fn path(&self) -> &Path {
        match self {
            LoadedConfig::Legacy { path, .. } => path,
            LoadedConfig::BundleV1 { path, .. } => path,
        }
    }

    pub fn defaults_mut(&mut self) -> &mut AppConfig {
        match self {
            LoadedConfig::Legacy { config, .. } => config,
            LoadedConfig::BundleV1 { bundle, .. } => &mut bundle.defaults,
        }
    }

    pub fn get_params(&self, mode: &str, direct: bool) -> IOParams {
        let cfg = match self {
            LoadedConfig::Legacy { config, .. } => config,
            LoadedConfig::BundleV1 { bundle, .. } => &bundle.defaults,
        };
        cfg.get_params(mode, direct)
    }

    pub fn update_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        self.defaults_mut().update_params(mode, direct, params)
    }

    pub fn save(&self) {
        match self {
            LoadedConfig::Legacy { path, config } => config.save(path.to_str().unwrap_or("fro.json")),
            LoadedConfig::BundleV1 { path, bundle } => {
                if let Ok(data) = serde_json::to_string_pretty(bundle) {
                    let _ = fs::write(path, data);
                }
            }
        }
    }
}

pub fn default_user_config_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".fro").join("fro.json"))
}

pub fn default_system_config_path() -> PathBuf {
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
        let data = fs::read_to_string(&path).unwrap_or_default();

        // Prefer bundle format if it matches.
        if let Ok(bundle) = serde_json::from_str::<ConfigBundleV1>(&data) {
            if bundle.version == 1 {
                return LoadedConfig::BundleV1 { path, bundle };
            }
        }

        if let Ok(config) = serde_json::from_str::<AppConfig>(&data) {
            return LoadedConfig::Legacy { path, config };
        }
    }

    // Create a default config at the chosen path.
    let cfg = AppConfig::default();

    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    // Write as bundle v1 at the default locations.
    let bundle = ConfigBundleV1 {
        version: 1,
        defaults: cfg,
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig {
            paths: vec![
                "/etc/fro.d/disk-id.json".into(),
                "/etc/fro.d/fro-device-db.json".into(),
                "~/.config/fro/fro-device-db.json".into(),
            ],
            allow_online_update: false,
        },
    };

    if let Ok(data) = serde_json::to_string_pretty(&bundle) {
        let _ = fs::write(&path, data);
    }

    LoadedConfig::BundleV1 { path, bundle }
}

impl Default for AppConfig {
    fn default() -> Self {
        let default_direct = IOParams { num_threads: 16, block_size: 3 * 1024 * 1024, qd: 2 };
        let default_cache = IOParams { num_threads: 31, block_size: 128 * 1024, qd: 1 };
        let default_write_direct = IOParams { num_threads: 4, block_size: 256 * 1024, qd: 3 };
        let default_write = IOParams { num_threads: 4, block_size: 1024 * 1024, qd: 2 };
        
        let default_mode = ModeConfig {
            direct: default_direct.clone(),
            page_cache: default_cache.clone(),
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
            write: default_write_mode.clone(),
            copy: default_write_mode.clone(),
            grep: default_mode.clone(),
            diff: ModeConfig { direct: default_direct.clone(), page_cache: diff_cache.clone() },
            dual_read_bench: ModeConfig { direct: default_direct.clone(), page_cache: diff_cache.clone() },
        }
    }
}

impl AppConfig {
    pub fn load(path: &str) -> Self {
        if Path::new(path).exists() {
            let data = fs::read_to_string(path).unwrap_or_default();
            if let Ok(config) = serde_json::from_str(&data) {
                return config;
            }
        }
        let default_config = Self::default();
        default_config.save(path);
        default_config
    }

    pub fn save(&self, path: &str) {
        if let Ok(data) = serde_json::to_string_pretty(self) {
            let _ = fs::write(path, data);
        }
    }
    
    pub fn get_params(&self, mode: &str, direct: bool) -> IOParams {
        let mode_config = match mode {
            "read" => &self.read,
            "write" => &self.write,
            "copy" => &self.copy,
            "grep" => &self.grep,
            "diff" => &self.diff,
            "dual-read-bench" => &self.dual_read_bench,
            _ => return if direct { self.read.direct.clone() } else { self.read.page_cache.clone() },
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
            "write" => &mut self.write,
            "copy" => &mut self.copy,
            "grep" => &mut self.grep,
            "diff" => &mut self.diff,
            "dual-read-bench" => &mut self.dual_read_bench,
            _ => return,
        };
        
        if direct {
            mode_config.direct = params;
        } else {
            mode_config.page_cache = params;
        }
    }
}
