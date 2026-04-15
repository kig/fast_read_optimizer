use crate::common::{CopyStrategy, IOMode};
use crate::config::{self, LoadedConfig};

fn io_params(params: &[u64], offset: usize) -> config::IOParams {
    config::IOParams {
        num_threads: params[offset],
        block_size: params[offset + 1],
        qd: params[offset + 2] as usize,
    }
}

pub(super) fn save_single_run_params(
    config: &mut LoadedConfig,
    io_mode: IOMode,
    config_mode: &str,
    context_path: &str,
    params: &[u64],
) {
    if io_mode == IOMode::Auto {
        return;
    }

    let direct = io_mode == IOMode::Direct;
    let offset = if direct { 3 } else { 0 };
    config.update_params_for_path(config_mode, direct, context_path, io_params(params, offset));
    config.save();
}

pub(super) fn save_best_params(
    config: &mut LoadedConfig,
    mode: &str,
    copy_strategy: CopyStrategy,
    io_mode: IOMode,
    config_mode: &str,
    context_path: &str,
    params: &[u64],
) {
    match (mode, copy_strategy, io_mode) {
        ("copy", CopyStrategy::CopyFileRange, _) => {
            config.update_copy_range_params_for_path(context_path, io_params(params, 6));
            config.save();
        }
        (_, _, IOMode::Auto) => {}
        _ => {
            let direct = io_mode == IOMode::Direct;
            let offset = if direct { 3 } else { 0 };
            config.update_params_for_path(
                config_mode,
                direct,
                context_path,
                io_params(params, offset),
            );
            config.save();
        }
    }
}
