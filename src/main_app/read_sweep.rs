use super::*;
use crate::main_app::util::{
    format_bytes_compact, format_phase_duration, unique_temp_file, write_sweep_fixture,
};
use crate::reader;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadSweepCacheState {
    Cold,
    Hot,
}

impl ReadSweepCacheState {
    fn label(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Hot => "hot",
        }
    }
}

pub(super) fn prepare_read_sweep_cache(
    path: &Path,
    variant: ReadBenchmarkVariant,
    cache: ReadSweepCacheState,
) {
    let path_str = path.to_str().unwrap();
    match (variant, cache) {
        (ReadBenchmarkVariant::SingleThreadDirect, _) => {
            let _ = reader::evict_file_cache(path_str);
        }
        (_, ReadSweepCacheState::Cold) => {
            let _ = reader::evict_file_cache(path_str);
        }
        (_, ReadSweepCacheState::Hot) => {
            let _ = reader::warm_file_page_cache(path_str);
        }
    }
}

#[derive(Clone)]
pub(super) struct ReadSweepRow {
    cache_state: ReadSweepCacheState,
    size: u64,
    variant: ReadBenchmarkVariant,
    gbps: f64,
    elapsed: f64,
    params: reader::ResolvedReadParams,
    phase_timings: reader::ReadPhaseTimings,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SmallFileThreadCacheState {
    Hot,
    Cold,
}

pub(super) fn variant_to_path_kind(
    cache_state: ReadSweepCacheState,
    variant: ReadBenchmarkVariant,
) -> ReadPathKind {
    match (cache_state, variant) {
        (_, ReadBenchmarkVariant::SingleThreadPageCache) => ReadPathKind::SimplePageCache,
        (_, ReadBenchmarkVariant::SingleThreadDirect) => ReadPathKind::SimpleDirect,
        (_, ReadBenchmarkVariant::SingleThreadIoUring) => ReadPathKind::IoUringPageCache,
        (_, ReadBenchmarkVariant::QuickProbePageCache) => ReadPathKind::SimplePageCache,
        (ReadSweepCacheState::Hot, ReadBenchmarkVariant::MultiThreadCurrent) => {
            ReadPathKind::ThreadedPageCache
        }
        (ReadSweepCacheState::Cold, ReadBenchmarkVariant::MultiThreadCurrent) => {
            ReadPathKind::ThreadedDirect
        }
    }
}

pub(super) fn path_kind_label(path: ReadPathKind) -> &'static str {
    match path {
        ReadPathKind::SimplePageCache => "simple-page-cache",
        ReadPathKind::SimpleDirect => "simple-direct",
        ReadPathKind::IoUringPageCache => "io-uring-page-cache",
        ReadPathKind::ThreadedPageCache => "threaded-page-cache",
        ReadPathKind::ThreadedDirect => "threaded-direct",
    }
}

pub(super) fn infer_cache_strategy(
    rows: &[ReadSweepRow],
    cache_state: ReadSweepCacheState,
) -> (u64, ReadPathKind, ReadPathKind) {
    let mut by_size = std::collections::BTreeMap::<u64, Vec<&ReadSweepRow>>::new();
    for row in rows.iter().filter(|row| row.cache_state == cache_state) {
        by_size.entry(row.size).or_default().push(row);
    }
    let sizes = by_size.keys().copied().collect::<Vec<_>>();
    if sizes.is_empty() {
        return (
            0,
            ReadPathKind::SimplePageCache,
            ReadPathKind::SimplePageCache,
        );
    }

    let variants = [
        ReadPathKind::SimplePageCache,
        ReadPathKind::SimpleDirect,
        ReadPathKind::IoUringPageCache,
        match cache_state {
            ReadSweepCacheState::Hot => ReadPathKind::ThreadedPageCache,
            ReadSweepCacheState::Cold => ReadPathKind::ThreadedDirect,
        },
    ];

    let avg_for = |segment: &[u64], path_kind: ReadPathKind| -> f64 {
        if segment.is_empty() {
            return f64::NEG_INFINITY;
        }
        let mut total = 0.0;
        for size in segment {
            let row = by_size[size]
                .iter()
                .find(|row| variant_to_path_kind(cache_state, row.variant) == path_kind)
                .expect("path kind row should exist for each sweep size");
            total += row.gbps;
        }
        total / segment.len() as f64
    };

    let mut best_score = f64::NEG_INFINITY;
    let mut best = (
        sizes[0],
        variant_to_path_kind(cache_state, by_size[&sizes[0]][0].variant),
        variant_to_path_kind(
            cache_state,
            by_size[sizes.last().expect("sizes non-empty")][0].variant,
        ),
    );

    for split in 0..=sizes.len() {
        let small_sizes = &sizes[..split];
        let large_sizes = &sizes[split..];
        let small_path = variants
            .iter()
            .copied()
            .max_by(|a, b| {
                avg_for(small_sizes, *a)
                    .partial_cmp(&avg_for(small_sizes, *b))
                    .unwrap()
            })
            .unwrap_or(variants[0]);
        let large_path = variants
            .iter()
            .copied()
            .max_by(|a, b| {
                avg_for(large_sizes, *a)
                    .partial_cmp(&avg_for(large_sizes, *b))
                    .unwrap()
            })
            .unwrap_or(variants[0]);
        let mut score = 0.0;
        for size in small_sizes {
            score += avg_for(&[*size], small_path);
        }
        for size in large_sizes {
            score += avg_for(&[*size], large_path);
        }
        if score > best_score {
            let cutoff = if split >= sizes.len() {
                *sizes.last().unwrap()
            } else {
                sizes[split]
            };
            best_score = score;
            best = (cutoff, small_path, large_path);
        }
    }

    best
}

pub(super) fn print_read_sweep_summary(rows: &[ReadSweepRow]) {
    println!();
    println!("summary\tcache\tsize\tfastest-variant\tfastest-path\tgbps");
    for cache_state in [ReadSweepCacheState::Cold, ReadSweepCacheState::Hot] {
        let mut sizes = rows
            .iter()
            .filter(|row| row.cache_state == cache_state)
            .map(|row| row.size)
            .collect::<Vec<_>>();
        sizes.sort_unstable();
        sizes.dedup();
        for size in sizes {
            if let Some(best) = rows
                .iter()
                .filter(|row| row.cache_state == cache_state && row.size == size)
                .max_by(|a, b| a.gbps.partial_cmp(&b.gbps).unwrap())
            {
                println!(
                    "summary\t{}\t{}\t{}\t{}\t{:.6}",
                    cache_state.label(),
                    size,
                    best.variant.label(),
                    path_kind_label(variant_to_path_kind(cache_state, best.variant)),
                    best.gbps
                );
            }
        }
    }
}

pub(super) fn print_read_sweep_strategy(strategy: ReadAutoStrategy) {
    println!();
    println!("strategy\tstate\tsmall-path\tlarge-path\tcutoff-bytes");
    println!(
        "strategy\thot\t{}\t{}\t{}",
        path_kind_label(strategy.hot_small_path),
        path_kind_label(strategy.hot_large_path),
        strategy.hot_large_min_bytes
    );
    println!(
        "strategy\tcold\t{}\t{}\t{}",
        path_kind_label(strategy.cold_small_path),
        path_kind_label(strategy.cold_large_path),
        strategy.cold_large_min_bytes
    );
}

pub(super) fn zfs_direct_read_strategy() -> ReadAutoStrategy {
    ReadAutoStrategy {
        hot_large_min_bytes: 1,
        cold_large_min_bytes: 1,
        hot_small_path: ReadPathKind::SimpleDirect,
        hot_large_path: ReadPathKind::SimpleDirect,
        cold_small_path: ReadPathKind::SimpleDirect,
        cold_large_path: ReadPathKind::SimpleDirect,
    }
}

pub(super) fn print_read_sweep_table(rows: &[ReadSweepRow]) {
    let mut rendered = vec![vec![
        "cache".to_string(),
        "size".to_string(),
        "variant".to_string(),
        "bytes".to_string(),
        "elapsed_s".to_string(),
        "gbps".to_string(),
        "threads".to_string(),
        "block".to_string(),
        "qd".to_string(),
        "direct".to_string(),
    ]];
    for row in rows {
        rendered.push(vec![
            row.cache_state.label().to_string(),
            format_bytes_compact(row.size),
            row.variant.label().to_string(),
            row.size.to_string(),
            format!("{:.6}", row.elapsed),
            format!("{:.6}", row.gbps),
            row.params.num_threads.to_string(),
            format_bytes_compact(row.params.block_size),
            row.params.qd.to_string(),
            row.params.use_direct.to_string(),
        ]);
    }
    let widths = (0..rendered[0].len())
        .map(|col| rendered.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    for row in rendered {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

pub(super) fn run_bench_read_sweep(config: &mut config::LoadedConfig) -> io::Result<()> {
    let sizes = [
        4 * 1024_u64,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
        16 * 1024 * 1024,
        32 * 1024 * 1024,
        64 * 1024 * 1024,
        80 * 1024 * 1024,
        256 * 1024 * 1024,
    ];
    let variants = [
        ReadBenchmarkVariant::SingleThreadPageCache,
        ReadBenchmarkVariant::SingleThreadDirect,
        ReadBenchmarkVariant::SingleThreadIoUring,
        ReadBenchmarkVariant::QuickProbePageCache,
        ReadBenchmarkVariant::MultiThreadCurrent,
    ];
    let cache_states = [ReadSweepCacheState::Cold, ReadSweepCacheState::Hot];
    let page_cache = config.get_params("read", false);
    let direct = config.get_params("read", true);
    let strategy_path = std::env::temp_dir().to_string_lossy().to_string();
    let mount_info = config.mount_info_for_path(&strategy_path);
    let strategy = if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
        zfs_direct_read_strategy()
    } else {
        config.get_read_auto_strategy()
    };

    let path = unique_temp_file("fro-read-sweep");
    let mut created = false;
    let mut previous_size = 0_u64;
    let mut rows = Vec::new();
    let result = (|| -> io::Result<()> {
        for size in sizes {
            if !created || size != previous_size {
                write_sweep_fixture(&path, size as usize)?;
                created = true;
                previous_size = size;
            }
            for cache_state in cache_states {
                for variant in variants {
                    prepare_read_sweep_cache(&path, variant, cache_state);
                    let result = benchmark_read_variant(
                        path.to_str().unwrap(),
                        variant,
                        match cache_state {
                            ReadSweepCacheState::Cold => ReadBenchmarkCacheState::Cold,
                            ReadSweepCacheState::Hot => ReadBenchmarkCacheState::Hot,
                        },
                        strategy,
                        mount_info.as_ref(),
                        page_cache.clone(),
                        direct.clone(),
                    )?;
                    let elapsed = result.elapsed.as_secs_f64();
                    let gbps = if elapsed > 0.0 {
                        result.bytes_read as f64 / elapsed / 1e9
                    } else {
                        0.0
                    };
                    rows.push(ReadSweepRow {
                        cache_state,
                        size,
                        variant,
                        gbps,
                        elapsed,
                        params: result.params,
                        phase_timings: result.phase_timings,
                    });
                    println!(
                        "result\t{}\t{}\t{}\t{:.6}\t{:.6}\t{}\t{}\t{}\t{}",
                        cache_state.label(),
                        size,
                        variant.label(),
                        elapsed,
                        gbps,
                        result.params.num_threads,
                        result.params.block_size,
                        result.params.qd,
                        result.params.use_direct
                    );
                }
            }
        }
        rows.sort_by(|a, b| {
            a.cache_state
                .label()
                .cmp(b.cache_state.label())
                .then(a.size.cmp(&b.size))
                .then(a.variant.label().cmp(b.variant.label()))
        });
        println!();
        print_read_sweep_table(&rows);
        if rows.iter().any(|row| row.phase_timings.enabled()) {
            println!();
            println!(
                "phases\tcache\tsize\tvariant\tthreads-created\tfirst-submit\tfirst-completion\twrapup-start\tjoin-done"
            );
            for row in rows.iter().filter(|row| row.phase_timings.enabled()) {
                let timings = row.phase_timings;
                println!(
                    "phases\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    row.cache_state.label(),
                    row.size,
                    row.variant.label(),
                    format_phase_duration(timings.call_to_threads_created),
                    format_phase_duration(timings.call_to_first_submit),
                    format_phase_duration(timings.call_to_first_completion),
                    format_phase_duration(timings.call_to_wrapup_start),
                    format_phase_duration(timings.call_to_join_done),
                );
            }
        }
        print_read_sweep_summary(&rows);
        let (hot_cutoff, hot_small, hot_large) =
            infer_cache_strategy(&rows, ReadSweepCacheState::Hot);
        let (cold_cutoff, cold_small, cold_large) =
            infer_cache_strategy(&rows, ReadSweepCacheState::Cold);
        let strategy = ReadAutoStrategy {
            hot_large_min_bytes: hot_cutoff,
            cold_large_min_bytes: cold_cutoff,
            hot_small_path: hot_small,
            hot_large_path: hot_large,
            cold_small_path: cold_small,
            cold_large_path: cold_large,
        };
        let strategy = if mount_info.as_ref().is_some_and(|info| info.fstype == "zfs") {
            zfs_direct_read_strategy()
        } else {
            strategy
        };
        print_read_sweep_strategy(strategy);
        config.update_read_auto_strategy_for_path(&strategy_path, strategy);
        config.save();
        Ok(())
    })();
    let _ = fs::remove_file(&path);
    result
}
