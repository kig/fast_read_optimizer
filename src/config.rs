use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

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
    pub write: Option<ModeConfigPatch>,
    pub copy: Option<ModeConfigPatch>,
    pub grep: Option<ModeConfigPatch>,
    pub diff: Option<ModeConfigPatch>,
    pub dual_read_bench: Option<ModeConfigPatch>,
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
    #[allow(dead_code)]
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

    pub fn get_params_for_path(&self, mode: &str, direct: bool, path: &str) -> IOParams {
        let base = self.get_params(mode, direct);

        let (bundle, mountpoint) = match self {
            LoadedConfig::BundleV1 { bundle, .. } => (bundle, mountpoint_for_path(path)),
            _ => return base,
        };

        let mp = match mountpoint {
            Some(mp) => mp,
            None => return base,
        };

        let patch = match bundle.mount_overrides.by_mountpoint.get(&mp) {
            Some(p) => p,
            None => return base,
        };

        patch
            .get_mode_patch(mode)
            .and_then(|m| {
                if direct {
                    m.direct.clone()
                } else {
                    m.page_cache.clone()
                }
            })
            .unwrap_or(base)
    }

    #[allow(dead_code)]
    pub fn update_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        self.defaults_mut().update_params(mode, direct, params)
    }

    pub fn update_params_for_path(
        &mut self,
        mode: &str,
        direct: bool,
        path: &str,
        params: IOParams,
    ) {
        match self {
            LoadedConfig::BundleV1 { bundle, .. } => {
                let mp = mountpoint_for_path(path).unwrap_or_else(|| "/".to_string());
                let entry = bundle
                    .mount_overrides
                    .by_mountpoint
                    .entry(mp)
                    .or_insert_with(AppConfigPatch::default);
                entry.set_mode_params(mode, direct, params);
            }
            LoadedConfig::Legacy { config, .. } => {
                config.update_params(mode, direct, params);
            }
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
            "write" => self.write.as_ref(),
            "copy" => self.copy.as_ref(),
            "grep" => self.grep.as_ref(),
            "diff" => self.diff.as_ref(),
            "dual-read-bench" | "dual_read_bench" => self.dual_read_bench.as_ref(),
            "hash" => self.hash.as_ref(),
            "verify" => self.verify.as_ref(),
            _ => None,
        }
    }

    fn set_mode_params(&mut self, mode: &str, direct: bool, params: IOParams) {
        let m = match mode {
            "read" => &mut self.read,
            "write" => &mut self.write,
            "copy" => &mut self.copy,
            "grep" => &mut self.grep,
            "diff" => &mut self.diff,
            "dual-read-bench" | "dual_read_bench" => &mut self.dual_read_bench,
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
    let p = std::path::Path::new(path);
    let canonical = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    let path = canonical.to_string_lossy();

    let data = fs::read_to_string("/proc/self/mountinfo").ok()?;

    let mut best: Option<String> = None;
    let mut best_len = 0usize;

    for line in data.lines() {
        let (lhs, _) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };
        let left_fields: Vec<&str> = lhs.split_whitespace().collect();
        if left_fields.len() < 5 {
            continue;
        }

        let mp = left_fields[4];
        if !path_starts_with_mount(&path, mp) {
            continue;
        }
        if mp.len() > best_len {
            best_len = mp.len();
            best = Some(mp.to_string());
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
    let mut db_paths = vec![
        "/etc/fro.d/disk-id.json".into(),
        "/etc/fro.d/fro-device-db.json".into(),
    ];
    if let Ok(home) = std::env::var("HOME") {
        db_paths.push(format!("{}/.config/fro/fro-device-db.json", home));
    }
    let bundle = ConfigBundleV1 {
        version: 1,
        defaults: cfg,
        mount_overrides: MountOverrides::default(),
        device_db: DeviceDbConfig {
            paths: db_paths,
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
            diff: ModeConfig {
                direct: default_direct.clone(),
                page_cache: diff_cache.clone(),
            },
            dual_read_bench: ModeConfig {
                direct: default_direct.clone(),
                page_cache: diff_cache.clone(),
            },
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
            "write" => &self.write,
            "copy" => &self.copy,
            "grep" => &self.grep,
            "diff" => &self.diff,
            "dual-read-bench" => &self.dual_read_bench,
            "hash" => &self.hash,
            "verify" => &self.verify,
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
            "write" => &mut self.write,
            "copy" => &mut self.copy,
            "grep" => &mut self.grep,
            "diff" => &mut self.diff,
            "dual-read-bench" => &mut self.dual_read_bench,
            "hash" => &mut self.hash,
            "verify" => &mut self.verify,
            _ => return,
        };

        if direct {
            mode_config.direct = params;
        } else {
            mode_config.page_cache = params;
        }
    }
}

fn default_hash_mode_config() -> ModeConfig {
    AppConfig::default().hash
}

fn default_verify_mode_config() -> ModeConfig {
    AppConfig::default().verify
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("{}-{}-{}", prefix, pid, nanos));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn set_env_var(key: &str, value: Option<&str>) -> Option<String> {
        let old = std::env::var(key).ok();
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        old
    }

    fn restore_env_var(key: &str, old: Option<String>) {
        match old {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn resolve_default_config_path_prefers_fro_config_env() {
        let _lock = ENV_LOCK.lock().unwrap();

        let tmp = unique_temp_dir("fro-test");
        let cfg_path = tmp.join("cfg.json");

        let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
        let old_home = set_env_var("HOME", None);

        let resolved = resolve_default_config_path();
        assert_eq!(resolved, cfg_path);

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }

    #[test]
    fn resolve_default_config_path_defaults_to_user_path() {
        let _lock = ENV_LOCK.lock().unwrap();

        let home = unique_temp_dir("fro-home");
        let old_fro = set_env_var("FRO_CONFIG", None);
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
        let old_home = set_env_var("HOME", Some(home.to_str().unwrap()));

        let resolved = resolve_default_config_path();
        assert!(resolved.ends_with(Path::new(".fro/fro.json")));
        assert!(resolved.starts_with(&home));

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }

    #[test]
    fn resolve_default_config_path_uses_system_when_user_missing_and_system_exists() {
        let _lock = ENV_LOCK.lock().unwrap();

        let home = unique_temp_dir("fro-home");
        let tmp = unique_temp_dir("fro-sys");
        let sys_cfg = tmp.join("fro.json");
        std::fs::write(&sys_cfg, "{}\n").unwrap();

        let old_fro = set_env_var("FRO_CONFIG", None);
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", Some(sys_cfg.to_str().unwrap()));
        let old_home = set_env_var("HOME", Some(home.to_str().unwrap()));

        let resolved = resolve_default_config_path();
        assert_eq!(resolved, sys_cfg);

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }

    #[test]
    fn load_config_creates_bundle_and_roundtrips_updates() {
        let _lock = ENV_LOCK.lock().unwrap();

        let tmp = unique_temp_dir("fro-test");
        let cfg_path = tmp.join("fro.json");

        let old_fro = set_env_var("FRO_CONFIG", Some(cfg_path.to_str().unwrap()));
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
        let old_home = set_env_var("HOME", None);

        let mut loaded = load_config(None);
        match &loaded {
            LoadedConfig::BundleV1 { path, bundle } => {
                assert_eq!(path, &cfg_path);
                assert_eq!(bundle.version, 1);
            }
            _ => panic!("expected bundle v1"),
        }

        assert!(cfg_path.exists());
        let text = std::fs::read_to_string(&cfg_path).unwrap();
        assert!(text.contains("\"version\": 1"));
        assert!(text.contains("\"defaults\""));

        let p0 = loaded.get_params("read", true);
        assert_eq!(p0.num_threads, 16);

        loaded.update_params(
            "read",
            true,
            IOParams {
                num_threads: 99,
                block_size: 4 * 1024,
                qd: 7,
            },
        );
        loaded.save();

        let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
        let p1 = reloaded.get_params("read", true);
        assert_eq!(p1.num_threads, 99);
        assert_eq!(p1.block_size, 4 * 1024);
        assert_eq!(p1.qd, 7);

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }

    #[test]
    fn load_config_reads_legacy_appconfig() {
        let _lock = ENV_LOCK.lock().unwrap();

        let tmp = unique_temp_dir("fro-test");
        let cfg_path = tmp.join("legacy.json");

        let old_fro = set_env_var("FRO_CONFIG", None);
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
        let old_home = set_env_var("HOME", None);

        let legacy = AppConfig::default();
        std::fs::write(&cfg_path, serde_json::to_string_pretty(&legacy).unwrap()).unwrap();

        let mut loaded = load_config(Some(cfg_path.to_str().unwrap()));
        match loaded {
            LoadedConfig::Legacy { ref path, .. } => assert_eq!(path, &cfg_path),
            _ => panic!("expected legacy config"),
        }

        loaded.update_params(
            "diff",
            false,
            IOParams {
                num_threads: 3,
                block_size: 123,
                qd: 1,
            },
        );
        loaded.save();

        let reloaded = load_config(Some(cfg_path.to_str().unwrap()));
        let p = reloaded.get_params("diff", false);
        assert_eq!(p.num_threads, 3);
        assert_eq!(p.block_size, 123);

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }

    #[test]
    fn load_config_reads_legacy_appconfig_without_hash_verify_fields() {
        let _lock = ENV_LOCK.lock().unwrap();

        let tmp = unique_temp_dir("fro-test");
        let cfg_path = tmp.join("legacy-missing-hash-verify.json");

        let old_fro = set_env_var("FRO_CONFIG", None);
        let old_sys = set_env_var("FRO_SYSTEM_CONFIG", None);
        let old_home = set_env_var("HOME", None);

        let legacy = AppConfig::default();
        let legacy_text = serde_json::json!({
            "read": legacy.read,
            "write": legacy.write,
            "copy": legacy.copy,
            "grep": legacy.grep,
            "diff": legacy.diff,
            "dual_read_bench": legacy.dual_read_bench,
        });
        std::fs::write(&cfg_path, serde_json::to_string_pretty(&legacy_text).unwrap()).unwrap();

        let loaded = load_config(Some(cfg_path.to_str().unwrap()));
        let hash = loaded.get_params("hash", false);
        let verify = loaded.get_params("verify", false);
        assert_eq!(hash.block_size, crate::block_hash::BLOCK_HASH_SIZE);
        assert_eq!(verify.block_size, crate::block_hash::BLOCK_HASH_SIZE);
        assert_eq!(hash.num_threads, 31);
        assert_eq!(verify.qd, 1);

        restore_env_var("FRO_CONFIG", old_fro);
        restore_env_var("FRO_SYSTEM_CONFIG", old_sys);
        restore_env_var("HOME", old_home);
    }
}
