use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

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

impl Default for AppConfig {
    fn default() -> Self {
        let default_direct = IOParams { num_threads: 16, block_size: 3 * 1024 * 1024, qd: 2 };
        let default_cache = IOParams { num_threads: 12, block_size: 128 * 1024, qd: 1 };
        
        let default_mode = ModeConfig {
            direct: default_direct.clone(),
            page_cache: default_cache.clone(),
        };

        // Custom defaults based on previous tuning
        let mut diff_cache = default_cache.clone();
        diff_cache.num_threads = 4;
        
        AppConfig {
            read: default_mode.clone(),
            write: default_mode.clone(),
            copy: default_mode.clone(),
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
