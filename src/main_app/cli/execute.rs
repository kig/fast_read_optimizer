use super::*;
use super::args::ParsedArgs;
use crate::main_app::bench_tar_archive;
use crate::main_app::copy_plan::{describe_copy_path, resolve_copy_execution};
use crate::main_app::recursive::bench::{bench_recursive_read, bench_recursive_small_file_threads};
use crate::main_app::recursive::paths::resolve_recursive_copy_root;
use crate::main_app::recursive::{run_recursive_copy, run_split_manifest_recursive_copy};

pub(super) fn run(parsed: ParsedArgs) -> io::Result<i32> {
    let ParsedArgs {
        mode,
        io_mode,
        io_mode_write,
        to_memory,
        auto_lift,
        to_memory_mode,
        to_memory_options,
        manual_read_overrides,
        via_memory,
        verify_copy,
        verify_copy_diff,
        recursive_copy,
        persist_verification_hashes,
        quiet,
        no_lock,
        keep_target_size,
        force_diff_copy,
        force_full_copy,
        force_copy_file_range,
        force_copy_file_range_single,
        force_threaded_copy,
        force_reflink,
        verbose,
        source,
        pattern,
        filename,
        extra_paths,
        hash_base,
        recover_mode,
        hash_type,
        hash_only,
        create_size,
        iterations,
        save_config,
        config_path,
        overlap_large_file,
        small_file_thread_cache_state,
    } = parsed;

    let mode = mode;
    let filename = filename;
    let pattern = pattern;
    let context_path = filename.as_str();

    let mut config = config::load_config(config_path.as_deref());
    let config_mode = match mode.as_str() {
        "read" if to_memory => "read_to_memory",
        "recursive-read-bench"
        | "file-list-read-bench"
        | "file-list-read-uring-bench"
        | "file-list-read-open-read-close-sweep"
        | "manifest-recursive-copy-bench"
        | "bench-recursive-small-file-threads" => "read",
        "recover" => "verify",
        "hash" | "verify" => mode.as_str(),
        _ => mode.as_str(),
    };

    let params_page_cache = config.get_params_for_path(config_mode, false, context_path);
    let params_direct = config.get_params_for_path(config_mode, true, context_path);
    let params_copy_range = config.get_copy_range_params_for_path(context_path);

    let mut start_params = vec![
        params_page_cache.num_threads,
        params_page_cache.block_size / (4 * 1024),
        params_page_cache.qd as u64,
        params_direct.num_threads,
        params_direct.block_size / (256 * 1024),
        params_direct.qd as u64,
        params_copy_range.num_threads,
        params_copy_range.block_size / (256 * 1024),
        params_copy_range.qd as u64,
    ];
    let mut params_steps = vec![1, 4 * 1024, 1, 1, 256 * 1024, 1, 1, 256 * 1024, 1];

    let copy_strategy = if force_copy_file_range {
        CopyStrategy::CopyFileRange
    } else if force_copy_file_range_single {
        CopyStrategy::CopyFileRangeSingle
    } else if force_reflink {
        CopyStrategy::Reflink
    } else if force_threaded_copy || via_memory {
        CopyStrategy::Threaded
    } else {
        CopyStrategy::Auto
    };
    let copy_rewrite_mode = if force_diff_copy {
        CopyRewriteMode::Diff
    } else if force_full_copy {
        CopyRewriteMode::Full
    } else {
        CopyRewriteMode::Auto
    };
    let mut optimizer_mask =
        active_optimizer_param_mask(mode.as_str(), io_mode, io_mode_write, via_memory, copy_strategy);
    if matches!(
        mode.as_str(),
        "read"
            | "recursive-read-bench"
            | "file-list-read-bench"
            | "file-list-read-uring-bench"
            | "file-list-read-open-read-close-sweep"
            | "manifest-recursive-copy-bench"
            | "bench-recursive-small-file-threads"
    ) {
        apply_manual_read_overrides(
            &mut start_params,
            &mut params_steps,
            &mut optimizer_mask,
            manual_read_overrides,
        );
    }
    let verbose = verbose || mode == "read" || mode == "write";
    if verbose {
        eprintln!("Opening file {} for {}", filename, mode);
    }

    let mut exit_code = 0;
    let hash_base_owned = hash_base;
    let extra_paths_owned = extra_paths;
    let read_auto_strategy = config.get_read_auto_strategy_for_path(context_path);
    let read_mount_info = config.mount_info_for_path(context_path);
    let params_page_cache_for_path = config.get_params_for_path(config_mode, false, context_path);
    let params_direct_for_path = config.get_params_for_path(config_mode, true, context_path);

    let mode_callback = |p: &[u64]| {
        if mode == "read" && to_memory {
            measure_file_load_to_memory(
                &filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                to_memory_mode,
                to_memory_options,
            )
        } else if mode == "read" || mode == "grep" {
            if auto_lift {
                read_file_auto_with_strategy(
                    &pattern,
                    &filename,
                    read_auto_strategy,
                    read_mount_info.as_ref(),
                    params_page_cache_for_path.clone(),
                    params_direct_for_path.clone(),
                )
            } else {
                read_file(
                    &pattern,
                    &filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )
            }
        } else if mode == "recursive-read-bench" {
            bench_recursive_read(
                &config,
                &filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                verbose,
                manual_read_overrides.threads,
            )
        } else if mode == "file-list-read-bench" {
            bench_file_list_read(
                &config,
                &filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                verbose,
            )
        } else if mode == "file-list-read-uring-bench" {
            bench_file_list_read_uring(&config, &filename, io_mode, verbose)
        } else if mode == "file-list-read-open-read-close-sweep" {
            bench_file_list_read_open_read_close_sweep(&config, &filename, io_mode, verbose)
        } else if mode == "manifest-recursive-copy-bench" {
            bench_manifest_recursive_copy(
                &filename,
                extra_paths_owned[0].as_str(),
                extra_paths_owned[1].as_str(),
                overlap_large_file.as_deref(),
                verbose,
            )
        } else if mode == "bench-recursive-small-file-threads" {
            bench_recursive_small_file_threads(
                &mut config,
                &filename,
                io_mode,
                verbose,
                save_config,
                small_file_thread_cache_state,
            )
        } else if mode == "bench-tar-archive" {
            let target = extra_paths_owned.get(1).map(Path::new);
            bench_tar_archive(&filename, Path::new(extra_paths_owned[0].as_str()), target, io_mode, io_mode_write)
        } else if mode == "hash" {
            if hash_only || iterations > 1 {
                let manifest = hash_file_blocks(
                    &filename,
                    hash_type,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )?;
                if iterations == 1 {
                    println!("{}  {}", manifest.hash_of_hashes, filename);
                }
                Ok(manifest.bytes_hashed)
            } else {
                let manifest = hash_file_to_replicas(
                    &filename,
                    hash_base_owned.as_deref(),
                    hash_type,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                )?;
                println!(
                    "wrote {} {:?} hash blocks ({} bytes each) to {}.[0-2].json",
                    manifest.block_hashes.len(),
                    manifest.hash_type,
                    manifest.block_size,
                    hash_base_owned
                        .as_deref()
                        .unwrap_or(&default_hash_base(&filename))
                );
                Ok(manifest.bytes_hashed)
            }
        } else if mode == "verify" {
            let report = verify_file_with_replicas(
                &filename,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )?;
            if iterations == 1 {
                print_verify_report(&report);
            }
            if iterations == 1 && !report.bad_blocks.is_empty() {
                exit_code = 1;
            }
            Ok(report.bytes_hashed)
        } else if mode == "recover" {
            let report = recover_file_with_copies(
                &filename,
                &extra_paths_owned,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                recover_mode,
            )?;
            println!(
                "recover: repaired_blocks={}, repaired_files={}, sidecars_refreshed={}, failed_blocks={}, used_fast_path={}, fell_back_to_full_scan={}",
                report.repaired_blocks,
                report.repaired_files,
                report.sidecars_refreshed,
                report.failed_blocks.len(),
                report.used_fast_path,
                report.fell_back_to_full_scan
            );
            for issue in &report.failed_blocks {
                println!(
                    "file {} ({}), block {}: {}",
                    issue.file_index,
                    issue.file_path,
                    issue.block_index,
                    issue.decision.status_message()
                );
            }
            if !report.failed_blocks.is_empty() {
                println!(
                    "recover could not fully repair all requested files; add more clean replicas or inspect the failed block reasons above"
                );
                exit_code = 1;
            } else if report.repaired_blocks == 0 {
                println!("recover: no block writes were needed");
            }
            Ok(report.bytes_hashed)
        } else if mode == "write" {
            write_file(
                &filename,
                create_size,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode_write,
            )
        } else if mode == "copy" || mode == "split-manifest-recursive-copy-bench" {
            if let Some(src) = source.as_deref() {
                if recursive_copy || mode == "split-manifest-recursive-copy-bench" {
                    let source_root = PathBuf::from(src);
                    let source_metadata = fs::symlink_metadata(&source_root)?;
                    if !source_metadata.file_type().is_dir() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            if mode == "copy" {
                                "copy --recursive requires a directory source"
                            } else {
                                "split-manifest-recursive-copy-bench requires a directory source"
                            },
                        ));
                    }
                    let target_root = resolve_recursive_copy_root(&source_root, Path::new(&filename))?;
                    let optimizer_params = std::array::from_fn(|index| p[index]);
                    let recursive_ctx = RecursiveCopyContext {
                        config: config.clone(),
                        source_root,
                        target_root,
                        optimizer_params,
                        requested_strategy: copy_strategy,
                        rewrite_mode: copy_rewrite_mode,
                        io_mode_read: io_mode,
                        io_mode_write,
                        keep_target_size,
                        use_lock: !no_lock,
                        relative_copy_method: RelativeCopyMethod::CopyFileRange,
                    };
                    if mode == "split-manifest-recursive-copy-bench" {
                        run_split_manifest_recursive_copy(recursive_ctx, verbose)
                    } else {
                        run_recursive_copy(recursive_ctx, verbose)
                    }
                } else {
                    let resolved_copy = resolve_copy_execution(
                        &config,
                        src,
                        &filename,
                        copy_strategy,
                        copy_rewrite_mode,
                        io_mode,
                        io_mode_write,
                    )?;
                    if verbose {
                        eprintln!("{}", describe_copy_path(resolved_copy, via_memory, keep_target_size));
                    }
                    if verify_copy {
                        let target_hash_base_owned = if persist_verification_hashes {
                            Some(
                                hash_base_owned
                                    .clone()
                                    .unwrap_or_else(|| default_hash_base(&filename)),
                            )
                        } else {
                            None
                        };
                        let report = copy_file_verified_with_options_and_lock(
                            src,
                            &filename,
                            resolved_copy.io_mode_read,
                            resolved_copy.io_mode_write,
                            hash_type,
                            via_memory,
                            target_hash_base_owned.as_deref(),
                            resolved_copy.copy_strategy,
                            !no_lock,
                        )?;
                        if !quiet {
                            eprintln!(
                                "copy verify: success; verified_blocks={}, repaired_blocks={}, used_recovery={}, hash_type={:?}, sidecars_written={}",
                                report.verified_blocks,
                                report.repaired_blocks,
                                report.used_recovery,
                                report.hash_type,
                                report.hashes_persisted
                            );
                        }
                        Ok(report.bytes_copied)
                    } else if verify_copy_diff {
                        let guard = CopyOperationGuard::new(src, &filename, !no_lock)?;
                        let copied = if via_memory {
                            let read_page_cache = config.get_params_for_path("read_to_memory", false, src);
                            let read_direct = config.get_params_for_path("read_to_memory", true, src);
                            let loaded = load_file_to_memory(
                                src,
                                read_page_cache.num_threads,
                                read_page_cache.block_size,
                                read_page_cache.qd,
                                read_direct.num_threads,
                                read_direct.block_size,
                                read_direct.qd,
                                io_mode,
                            )?;
                            let write_page_cache = config.get_params_for_path("write", false, &filename);
                            let write_direct = config.get_params_for_path("write", true, &filename);
                            write_buffer(
                                &filename,
                                &loaded.data,
                                write_page_cache.num_threads,
                                write_page_cache.block_size,
                                write_page_cache.qd,
                                write_direct.num_threads,
                                write_direct.block_size,
                                write_direct.qd,
                                resolved_copy.io_mode_write,
                            )?
                        } else {
                            copy_file_with_strategy(
                                src,
                                &filename,
                                p[0],
                                p[1],
                                p[2] as usize,
                                p[3],
                                p[4],
                                p[5] as usize,
                                p[6],
                                p[7],
                                p[8] as usize,
                                resolved_copy.io_mode_read,
                                resolved_copy.io_mode_write,
                                resolved_copy.copy_strategy,
                            )?
                        };
                        sync_path(&filename)?;
                        guard.ensure_source_unchanged()?;
                        let diff_page_cache = config.get_params_for_path("diff", false, &filename);
                        let diff_direct = config.get_params_for_path("diff", true, &filename);
                        let diff_res = diff_files(
                            src,
                            &filename,
                            diff_page_cache.num_threads,
                            diff_page_cache.block_size,
                            diff_page_cache.qd,
                            diff_direct.num_threads,
                            diff_direct.block_size,
                            diff_direct.qd,
                            io_mode,
                            false,
                            true,
                        )?;
                        if diff_res != 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("copy verify-diff found a mismatch at byte offset {}", diff_res),
                            ));
                        }
                        guard.ensure_source_unchanged()?;
                        if !quiet {
                            eprintln!("copy verify-diff: success");
                        }
                        Ok(copied)
                    } else if via_memory {
                        let guard = CopyOperationGuard::new(src, &filename, !no_lock)?;
                        let read_page_cache = config.get_params_for_path("read_to_memory", false, src);
                        let read_direct = config.get_params_for_path("read_to_memory", true, src);
                        let loaded = load_file_to_memory(
                            src,
                            read_page_cache.num_threads,
                            read_page_cache.block_size,
                            read_page_cache.qd,
                            read_direct.num_threads,
                            read_direct.block_size,
                            read_direct.qd,
                            io_mode,
                        )?;
                        let write_page_cache = config.get_params_for_path("write", false, &filename);
                        let write_direct = config.get_params_for_path("write", true, &filename);
                        let copied = write_buffer(
                            &filename,
                            &loaded.data,
                            write_page_cache.num_threads,
                            write_page_cache.block_size,
                            write_page_cache.qd,
                            write_direct.num_threads,
                            write_direct.block_size,
                            write_direct.qd,
                            resolved_copy.io_mode_write,
                        )?;
                        guard.ensure_source_unchanged()?;
                        Ok(copied)
                    } else {
                        let guard = CopyOperationGuard::new(src, &filename, !no_lock)?;
                        let copied = if resolved_copy.diff_overwrite && !keep_target_size {
                            let diff_scan = config.get_params_for_path("diff", false, &filename);
                            overwrite_changed_chunks_direct(
                                src,
                                &filename,
                                diff_scan.num_threads,
                                diff_scan.block_size,
                                diff_scan.qd,
                                p[3],
                                p[4],
                                p[5] as usize,
                            )?
                        } else {
                            copy_file_with_strategy_and_truncate(
                                src,
                                &filename,
                                p[0],
                                p[1],
                                p[2] as usize,
                                p[3],
                                p[4],
                                p[5] as usize,
                                p[6],
                                p[7],
                                p[8] as usize,
                                resolved_copy.io_mode_read,
                                resolved_copy.io_mode_write,
                                resolved_copy.copy_strategy,
                                !keep_target_size,
                            )?
                        };
                        guard.ensure_source_unchanged()?;
                        Ok(copied)
                    }
                }
            } else {
                eprintln!("Copy is missing a destination path.");
                Ok(1)
            }
        } else if mode == "diff" || mode == "dual-read-bench" {
            let src = source.as_deref().unwrap();
            let s1 = std::fs::metadata(src)?.len();
            let s2 = std::fs::metadata(&filename)?.len();
            if s1 != s2 {
                if verbose {
                    eprintln!("Files have different sizes: {} != {}", s1, s2);
                }
                if mode == "diff" {
                    exit_code = 1;
                }
            }
            if exit_code == 0 {
                let bench_only = mode == "dual-read-bench";
                let size = std::fs::File::open(&filename)?.metadata()?.len();
                let res = diff_files(
                    src,
                    &filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                    bench_only,
                    true,
                )?;
                if res != 0 && mode == "diff" {
                    exit_code = 1;
                }
                Ok(size * 2)
            } else {
                Ok(0)
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid mode {}", mode),
            ))
        }
    };

    let effective_to_memory_mode = if mode == "read" && to_memory {
        Some(resolve_to_memory_mode(&filename, io_mode, to_memory_mode))
    } else {
        None
    };

    if mode == "read"
        && to_memory
        && iterations == 1
        && effective_to_memory_mode.is_some_and(|mode| {
            matches!(mode, ReadToMemoryMode::Mmap | ReadToMemoryMode::MmapReadPages)
        })
        && !to_memory_options.measure_unmap_time
    {
        let single_run_params = start_params
            .iter()
            .zip(params_steps.iter())
            .map(|(value, scale)| value * scale)
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        let prepared = prepare_file_load_to_memory(
            &filename,
            single_run_params[0],
            single_run_params[1],
            single_run_params[2] as usize,
            single_run_params[3],
            single_run_params[4],
            single_run_params[5] as usize,
            io_mode,
            to_memory_mode,
            to_memory_options,
        )?;
        let bytes = std::fs::metadata(&filename)?.len();
        let elapsed = start.elapsed().as_secs_f64();
        eprintln!(
            "{} {} bytes in {:.4} s, {:.1} GB/s, {:?}",
            mode,
            bytes,
            elapsed,
            bytes as f64 / elapsed / 1e9,
            &single_run_params[..6]
        );
        drop(prepared);
        if save_config {
            match io_mode {
                common::IOMode::Auto => {}
                _ => {
                    let direct = io_mode == common::IOMode::Direct;
                    let off = if direct { 3 } else { 0 };
                    config.update_params_for_path(
                        config_mode,
                        direct,
                        context_path,
                        config::IOParams {
                            num_threads: single_run_params[off],
                            block_size: single_run_params[off + 1],
                            qd: single_run_params[off + 2] as usize,
                        },
                    );
                    config.save();
                }
            }
        }
        return Ok(0);
    }

    let best_params = run_optimizer(
        mode.as_str(),
        start_params,
        params_steps,
        optimizer_mask,
        usize::try_from(iterations).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "iterations overflow usize"))?,
        verbose,
        mode_callback,
    )?;

    if mode == "verify" && iterations > 1 {
        let report = verify_file_with_replicas(
            &filename,
            hash_base_owned.as_deref(),
            best_params[0],
            best_params[1],
            best_params[2] as usize,
            best_params[3],
            best_params[4],
            best_params[5] as usize,
            io_mode,
        )?;
        print_verify_report(&report);
        if !report.bad_blocks.is_empty() {
            exit_code = 1;
        }
    }

    if save_config {
        match (mode.as_str(), copy_strategy, io_mode) {
            ("copy", CopyStrategy::CopyFileRange, _) => {
                config.update_copy_range_params_for_path(
                    context_path,
                    config::IOParams {
                        num_threads: best_params[6],
                        block_size: best_params[7],
                        qd: best_params[8] as usize,
                    },
                );
                config.save();
            }
            (_, _, common::IOMode::Auto) => {}
            _ => {
                let direct = io_mode == common::IOMode::Direct;
                let off = if direct { 3 } else { 0 };
                config.update_params_for_path(
                    config_mode,
                    direct,
                    context_path,
                    config::IOParams {
                        num_threads: best_params[off],
                        block_size: best_params[off + 1],
                        qd: best_params[off + 2] as usize,
                    },
                );
                config.save();
            }
        }
    }

    if exit_code != 0 {
        return Ok(exit_code);
    }
    Ok(0)
}
