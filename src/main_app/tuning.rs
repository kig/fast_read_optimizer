use super::*;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ManualReadOverrides {
    pub(super) threads: Option<u64>,
    pub(super) block_size: Option<u64>,
    pub(super) qd: Option<usize>,
}

impl ManualReadOverrides {
    pub(super) fn any(self) -> bool {
        self.threads.is_some() || self.block_size.is_some() || self.qd.is_some()
    }
}

pub(super) fn mark_optimizer_params(mask: &mut [bool], indices: &[usize], include_block_size: bool) {
    for &index in indices {
        if include_block_size || index % 3 != 1 {
            mask[index] = true;
        }
    }
}

pub(super) fn active_optimizer_param_mask(
    mode: &str,
    io_mode: common::IOMode,
    io_mode_write: common::IOMode,
    via_memory: bool,
    copy_strategy: CopyStrategy,
) -> Vec<bool> {
    let mut mask = [false; 9];
    match mode {
        "read" | "grep" | "hash" | "diff" | "dual-read-bench" | "recursive-read-bench"
        | "file-list-read-bench" | "file-list-read-uring-bench"
        | "file-list-read-open-read-close-sweep"
        | "bench-recursive-small-file-threads" => {
            match io_mode {
                common::IOMode::Direct => {
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
                common::IOMode::PageCache => {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                }
                common::IOMode::Auto => {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
            }
        }
        "verify" | "recover" => match io_mode {
            common::IOMode::Direct => {
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, false);
            }
            common::IOMode::PageCache => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, false);
            }
            common::IOMode::Auto => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, false);
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, false);
            }
        },
        "write" => match io_mode_write {
            common::IOMode::PageCache => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
            }
            common::IOMode::Direct | common::IOMode::Auto => {
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
            }
        },
        "copy" => match copy_strategy {
            CopyStrategy::CopyFileRange => {
                mark_optimizer_params(&mut mask, &COPY_RANGE_PARAM_INDICES, true);
            }
            CopyStrategy::CopyFileRangeSingle | CopyStrategy::Reflink => {}
            CopyStrategy::Auto => {
                mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                mark_optimizer_params(&mut mask, &COPY_RANGE_PARAM_INDICES, true);
            }
            CopyStrategy::Threaded => {
                if io_mode_write == common::IOMode::PageCache
                    || io_mode == common::IOMode::PageCache
                {
                    mark_optimizer_params(&mut mask, &PAGE_CACHE_PARAM_INDICES, true);
                }
                if !(io_mode_write == common::IOMode::PageCache
                    && io_mode == common::IOMode::PageCache)
                {
                    mark_optimizer_params(&mut mask, &DIRECT_PARAM_INDICES, true);
                }
            }
        },
        _ => mask.fill(true),
    }
    if mode == "copy"
        && (via_memory
            || matches!(
                copy_strategy,
                CopyStrategy::CopyFileRangeSingle | CopyStrategy::Reflink
            ))
    {
        mask.fill(false);
    }
    mask.to_vec()
}

pub(super) fn freeze_read_override(
    start_params: &mut [u64],
    params_steps: &mut [u64],
    optimizer_mask: &mut [bool],
    indices: [usize; 2],
    value: u64,
) {
    for index in indices {
        start_params[index] = value;
        params_steps[index] = 1;
        optimizer_mask[index] = false;
    }
}

pub(super) fn apply_manual_read_overrides(
    start_params: &mut [u64],
    params_steps: &mut [u64],
    optimizer_mask: &mut [bool],
    overrides: ManualReadOverrides,
) {
    if let Some(threads) = overrides.threads {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[0], DIRECT_PARAM_INDICES[0]],
            threads,
        );
    }
    if let Some(block_size) = overrides.block_size {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[1], DIRECT_PARAM_INDICES[1]],
            block_size,
        );
    }
    if let Some(qd) = overrides.qd {
        freeze_read_override(
            start_params,
            params_steps,
            optimizer_mask,
            [PAGE_CACHE_PARAM_INDICES[2], DIRECT_PARAM_INDICES[2]],
            qd as u64,
        );
    }
}
