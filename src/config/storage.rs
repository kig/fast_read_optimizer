use super::*;

use std::sync::{Mutex, OnceLock};

#[derive(Clone)]
struct CachedDefaultConfig {
    path: PathBuf,
    loaded: LoadedConfig,
}

static DEFAULT_CONFIG_CACHE: OnceLock<Mutex<Option<CachedDefaultConfig>>> = OnceLock::new();

fn default_config_cache() -> &'static Mutex<Option<CachedDefaultConfig>> {
    DEFAULT_CONFIG_CACHE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(super) fn clear_default_config_cache_for_tests() {
    *default_config_cache().lock().unwrap() = None;
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

fn load_config_from_path(path: PathBuf) -> LoadedConfig {
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

pub fn load_config(path: Option<&str>) -> LoadedConfig {
    let Some(path) = path.map(PathBuf::from) else {
        let path = resolve_default_config_path();
        let mut cache = default_config_cache().lock().unwrap();
        if let Some(cached) = cache.as_ref() {
            if cached.path == path {
                return cached.loaded.clone();
            }
        }

        let loaded = load_config_from_path(path.clone());
        *cache = Some(CachedDefaultConfig {
            path,
            loaded: loaded.clone(),
        });
        return loaded;
    };

    load_config_from_path(path)
}

pub(super) fn refresh_cached_default_config(loaded: &LoadedConfig) {
    let mut cache = default_config_cache().lock().unwrap();
    let Some(cached) = cache.as_mut() else {
        return;
    };
    if cached.path.as_path() == loaded.config_path() {
        cached.loaded = loaded.clone();
    }
}

pub(super) fn default_bundle_v1() -> ConfigBundleV1 {
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

pub(super) fn default_cat_dev_null_backend() -> CatDevNullBackend {
    CatDevNullBackend::Auto
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
            cat_dev_null_backend: default_cat_dev_null_backend(),
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
    pub fn save(&self, path: &str) -> io::Result<()> {
        if let Ok(data) = serde_json::to_string_pretty(self) {
            fs::write(path, data)?;
        }
        Ok(())
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

pub(super) fn default_read_auto_strategy() -> ReadAutoStrategy {
    ReadAutoStrategy {
        hot_large_min_bytes: 256 * 1024 * 1024,
        cold_large_min_bytes: 256 * 1024 * 1024,
        hot_small_path: ReadPathKind::SimplePageCache,
        hot_large_path: ReadPathKind::ThreadedPageCache,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::ThreadedDirect,
    }
}

pub(super) fn default_recursive_small_file_threads() -> RecursiveSmallFileThreads {
    RecursiveSmallFileThreads { hot: 32, cold: 32 }
}

pub(super) fn default_hash_mode_config() -> ModeConfig {
    AppConfig::default().hash
}

pub(super) fn default_compute_mode_config() -> ModeConfig {
    AppConfig::default().compute
}

pub(super) fn default_read_to_memory_mode_config() -> ModeConfig {
    AppConfig::default().read_to_memory
}

pub(super) fn default_verify_mode_config() -> ModeConfig {
    AppConfig::default().verify
}

pub(super) fn default_copy_range_params() -> IOParams {
    AppConfig::default().copy_range
}

pub(super) fn default_copy_auto_mode() -> CopyAutoMode {
    CopyAutoMode::Heuristic
}
