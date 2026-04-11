use super::*;
use crate::writer;

pub(super) struct ParsedArgs {
    pub(super) mode: String,
    pub(super) config_subcommand: Option<String>,
    pub(super) config_target: Option<String>,
    pub(super) io_mode: common::IOMode,
    pub(super) io_mode_write: common::IOMode,
    pub(super) to_memory: bool,
    pub(super) auto_lift: bool,
    pub(super) to_memory_mode: ReadToMemoryMode,
    pub(super) to_memory_options: ReadToMemoryOptions,
    pub(super) manual_read_overrides: ManualReadOverrides,
    pub(super) via_memory: bool,
    pub(super) verify_copy: bool,
    pub(super) verify_copy_diff: bool,
    pub(super) recursive_copy: bool,
    pub(super) persist_verification_hashes: bool,
    pub(super) quiet: bool,
    pub(super) no_lock: bool,
    pub(super) keep_target_size: bool,
    pub(super) force_diff_copy: bool,
    pub(super) force_full_copy: bool,
    pub(super) force_copy_file_range: bool,
    pub(super) force_copy_file_range_single: bool,
    pub(super) force_threaded_copy: bool,
    pub(super) force_reflink: bool,
    pub(super) cp_compat: bool,
    pub(super) cp_no_clobber: bool,
    pub(super) cp_target_directory: Option<String>,
    pub(super) cp_no_target_directory: bool,
    pub(super) cp_update: bool,
    pub(super) cp_preserve: bool,
    pub(super) cp_no_dereference: bool,
    pub(super) verbose: bool,
    pub(super) source: Option<String>,
    pub(super) pattern: String,
    pub(super) filename: String,
    pub(super) extra_paths: Vec<String>,
    pub(super) hash_base: Option<String>,
    pub(super) recover_mode: RecoverMode,
    pub(super) hash_type: BlockHashAlgorithm,
    pub(super) hash_only: bool,
    pub(super) create_size: Option<u64>,
    pub(super) iterations: u64,
    pub(super) save_config: bool,
    pub(super) config_path: Option<String>,
    pub(super) overlap_large_file: Option<String>,
    pub(super) small_file_thread_cache_state: Option<SmallFileThreadCacheState>,
}

pub(super) enum ParseOutcome {
    Early(i32),
    Parsed(ParsedArgs),
}

fn rebuild_cp_fallback_args(raw_args: &[String]) -> Vec<String> {
    if raw_args.get(1).is_some_and(|arg| arg == "cp") {
        let mut rebuilt = Vec::with_capacity(raw_args.len().saturating_sub(1));
        rebuilt.push("cp".to_string());
        rebuilt.extend(
            raw_args
                .iter()
                .skip(2)
                .filter(|arg| arg.as_str() != "--no-fallback")
                .cloned(),
        );
        rebuilt
    } else if raw_args.get(1).is_some_and(|arg| arg == "copy") {
        let mut rebuilt = Vec::with_capacity(raw_args.len().saturating_sub(1));
        rebuilt.push("cp".to_string());
        rebuilt.extend(
            raw_args
                .iter()
                .skip(2)
                .filter(|arg| arg.as_str() != "--cp-compat" && arg.as_str() != "--no-fallback")
                .cloned(),
        );
        rebuilt
    } else {
        raw_args.to_vec()
    }
}

#[allow(dead_code)]
pub(super) fn parse_cli() -> io::Result<ParseOutcome> {
    parse_cli_from(env::args().collect())
}

fn wrapper_multicall_config_path(raw_args: &[String]) -> Option<&str> {
    let mut config_path: Option<&str> = None;
    let mut idx = 1usize;
    while idx < raw_args.len() {
        match raw_args[idx].as_str() {
            "--no-fallback" => {
                idx += 1;
            }
            "-c" | "--config" => {
                idx += 1;
                config_path = raw_args.get(idx).map(String::as_str);
                idx += 1;
            }
            "cp" => return config_path,
            other if coreutils::is_coreutils_command(other) => return config_path,
            _ => return None,
        }
    }
    None
}

pub(super) fn parse_cli_from(raw_args: Vec<String>) -> io::Result<ParseOutcome> {
    if let Some(path) = wrapper_multicall_config_path(&raw_args) {
        std::env::set_var("FRO_CONFIG", path);
    }
    if let Some(code) = coreutils::try_run_multicall(&raw_args)? {
        return Ok(ParseOutcome::Early(code));
    }
    if raw_args
        .get(1)
        .is_some_and(|arg| is_version_flag(arg.as_str()))
    {
        print_version(raw_args[0].as_str());
        return Ok(ParseOutcome::Early(0));
    }
    let args = coreutils::rewrite_subcommand_alias(coreutils::rewrite_alias_args(raw_args.clone()));
    if args.len() < 2 || is_help_flag(args[1].as_str()) {
        print_general_help(args[0].as_str());
        return Ok(ParseOutcome::Early(0));
    }
    let subcommand_short_help_is_real_flag = args.get(1).is_some_and(|command| command == "sort")
        && args.get(2).is_some_and(|arg| arg == "-h");
    if args.len() >= 3 && is_help_flag(args[2].as_str()) && !subcommand_short_help_is_real_flag {
        if let Some(help) = command_help(args[1].as_str()) {
            print_command_help(args[0].as_str(), help);
        } else {
            fro::cio_eprintln!("Unknown command: {}", args[1]);
            fro::cio_println!();
            print_general_help(args[0].as_str());
        }
        return Ok(ParseOutcome::Early(0));
    }
    if let Some(code) =
        coreutils::try_run_subcommand(args[0].as_str(), args[1].as_str(), &args[2..])?
    {
        return Ok(ParseOutcome::Early(code));
    }
    let legacy_copy_via_memory = args[1] == "copy-via-memory";
    let mode = if legacy_copy_via_memory {
        "copy"
    } else {
        args[1].as_str()
    };
    let mut config_subcommand: Option<String> = None;
    let mut config_target: Option<String> = None;
    let mut io_mode = common::IOMode::Auto;
    let mut io_mode_write = common::IOMode::Auto;
    let mut to_memory = false;
    let mut auto_lift = false;
    let mut to_memory_mode = ReadToMemoryMode::Auto;
    let mut to_memory_options = ReadToMemoryOptions::default();
    let mut manual_read_overrides = ManualReadOverrides::default();
    let mut via_memory = legacy_copy_via_memory;
    let mut verify_copy = false;
    let mut verify_copy_diff = false;
    let mut recursive_copy = false;
    let mut persist_verification_hashes = false;
    let mut quiet = false;
    let mut no_lock = false;
    let mut keep_target_size = false;
    let mut force_diff_copy = false;
    let mut force_full_copy = false;
    let mut force_copy_file_range = false;
    let mut force_copy_file_range_single = false;
    let mut force_threaded_copy = false;
    let mut force_reflink = false;
    let mut cp_compat = false;
    let mut cp_no_clobber = false;
    let mut cp_target_directory: Option<String> = None;
    let mut cp_no_target_directory = false;
    let mut cp_update = false;
    let mut cp_preserve = false;
    let mut cp_no_dereference = false;
    let mut verbose = false;
    let mut source: Option<String> = None;
    let mut pattern = String::new();
    let mut filename = String::new();
    let mut extra_paths: Vec<String> = Vec::new();
    let mut hash_base: Option<String> = None;
    let mut recover_mode = RecoverMode::Standard;
    let mut hash_type = BlockHashAlgorithm::Xxh3;
    let mut hash_only = false;
    let mut recover_fast_requested = false;
    let mut recover_in_place_all_requested = false;
    let mut create_size: Option<u64> = None;
    let mut iterations = if mode == "read" {
        1000
    } else if mode == "bench-base64-encode"
        || mode == "bench-base64-decode"
        || mode == "bench-base64-decode-detect-fallback"
        || mode == "bench-base64-wrapped-encode"
        || mode == "bench-base64-wrapped-decode"
    {
        1_000_000
    } else {
        1
    };
    let mut save_config = false;
    let mut config_path: Option<String> = None;
    let mut bench_size: Option<u64> = None;
    let mut bench_threads: Option<usize> = None;
    let mut base64_kernel = coreutils::Base64EncodeKernel::Auto;
    let mut base64_decode_kernel = coreutils::Base64DecodeKernel::Auto;
    let mut base64_wrap_cols: usize = 76;
    let mut base64_ignore_garbage = false;
    let mut small_file_thread_cache_state: Option<SmallFileThreadCacheState> = None;
    let mut overlap_large_file: Option<String> = None;

    let mut i = 2;
    let mut end_flags = false;
    while i < args.len() {
        let is_flag = !end_flags && args[i].starts_with('-');
        if is_flag {
            if args[i] == "--" {
                end_flags = true;
            } else if args[i] == "--no-fallback" {
                i += 1;
                continue;
            } else if args[i] == "--help" {
                if let Some(help) = command_help(args[1].as_str()) {
                    print_command_help(args[0].as_str(), help);
                } else {
                    fro::cio_eprintln!("Unknown command: {}", args[1]);
                    fro::cio_println!();
                    print_general_help(args[0].as_str());
                }
                return Ok(ParseOutcome::Early(0));
            } else if args[i] == "-c" || args[i] == "--config" {
                i += 1;
                if i < args.len() {
                    config_path = Some(args[i].clone());
                }
            } else if args[i] == "--for" {
                i += 1;
                if i < args.len() {
                    config_target = Some(args[i].clone());
                }
            } else if args[i] == "--hash-base" {
                i += 1;
                if i < args.len() {
                    hash_base = Some(args[i].clone());
                }
            } else if args[i] == "--overlap-large-file" {
                i += 1;
                if i < args.len() {
                    overlap_large_file = Some(args[i].clone());
                }
            } else if args[i] == "--size" {
                i += 1;
                if i < args.len() {
                    bench_size = parse_size(args[i].as_str()).or_else(|| {
                        fro::cio_eprintln!("Invalid --size: {}", args[i]);
                        None
                    });
                    if bench_size.is_none() {
                        return Ok(ParseOutcome::Early(1));
                    }
                }
            } else if args[i] == "--variant" {
                i += 1;
                if i < args.len() {
                    if mode == "bench-base64-encode" {
                        base64_kernel = coreutils::parse_base64_encode_kernel(args[i].as_str())?;
                    } else if mode == "bench-base64-decode"
                        || mode == "bench-base64-decode-detect-fallback"
                    {
                        base64_decode_kernel =
                            coreutils::parse_base64_decode_kernel(args[i].as_str())?;
                    } else {
                        fro::cio_eprintln!(
                            "--variant is only supported for bench-base64-encode/decode"
                        );
                        return Ok(ParseOutcome::Early(1));
                    }
                }
            } else if args[i] == "--wrap" {
                i += 1;
                if i < args.len() {
                    base64_wrap_cols = args[i].parse().map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid wrap size: {}", err),
                        )
                    })?;
                }
            } else if args[i] == "--ignore-garbage" {
                base64_ignore_garbage = true;
            } else if args[i] == "--hot" {
                small_file_thread_cache_state = Some(SmallFileThreadCacheState::Hot);
            } else if args[i] == "--cold" {
                small_file_thread_cache_state = Some(SmallFileThreadCacheState::Cold);
            } else if args[i] == "--threads" {
                i += 1;
                if i < args.len() {
                    if mode == "bench-memcpy" {
                        bench_threads = Some(args[i].parse().map_err(|err| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid thread count: {}", err),
                            )
                        })?);
                    } else {
                        manual_read_overrides.threads = Some(args[i].parse().map_err(|err| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid thread count: {}", err),
                            )
                        })?);
                    }
                }
            } else if args[i] == "--qd" {
                i += 1;
                if i < args.len() {
                    manual_read_overrides.qd = Some(args[i].parse().map_err(|err| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("invalid qd: {}", err))
                    })?);
                }
            } else if args[i] == "--blocksize" {
                i += 1;
                if i < args.len() {
                    manual_read_overrides.block_size = parse_size(args[i].as_str()).or_else(|| {
                        fro::cio_eprintln!("Invalid --blocksize: {}", args[i]);
                        None
                    });
                    if manual_read_overrides.block_size.is_none() {
                        return Ok(ParseOutcome::Early(1));
                    }
                }
            } else if args[i] == "--create" {
                i += 1;
                if i < args.len() {
                    create_size = parse_size(args[i].as_str()).or_else(|| {
                        fro::cio_eprintln!("Invalid --create size: {}", args[i]);
                        None
                    });
                    if create_size.is_none() {
                        return Ok(ParseOutcome::Early(1));
                    }
                }
            } else if args[i] == "--fast" {
                recover_fast_requested = true;
                recover_mode = RecoverMode::Fast;
            } else if args[i] == "--in-place-all" {
                recover_in_place_all_requested = true;
                recover_mode = RecoverMode::InPlaceAll;
            } else if args[i] == "--sha256" {
                hash_type = BlockHashAlgorithm::Sha256;
            } else if args[i] == "--xxh3" {
                hash_type = BlockHashAlgorithm::Xxh3;
            } else if args[i] == "--hash-only" {
                hash_only = true;
            } else if args[i] == "--direct" {
                io_mode = common::IOMode::Direct;
                io_mode_write = common::IOMode::Direct;
            } else if args[i] == "--no-direct" {
                io_mode = common::IOMode::PageCache;
                io_mode_write = common::IOMode::PageCache;
            } else if args[i] == "--auto" {
                io_mode = common::IOMode::Auto;
                io_mode_write = common::IOMode::Auto;
            } else if args[i] == "--direct-write" {
                io_mode_write = common::IOMode::Direct;
            } else if args[i] == "--no-direct-write" {
                io_mode_write = common::IOMode::PageCache;
            } else if args[i] == "--auto-write" {
                io_mode_write = common::IOMode::Auto;
            } else if args[i] == "--to-memory" {
                to_memory = true;
            } else if args[i] == "--auto-lift" {
                auto_lift = true;
            } else if args[i] == "--paged-shared-buffer" {
                to_memory_mode = ReadToMemoryMode::PagedSharedBuffer;
            } else if args[i] == "--mmap" {
                to_memory_mode = ReadToMemoryMode::Mmap;
            } else if args[i] == "--mmap-read-pages" {
                to_memory_mode = ReadToMemoryMode::MmapReadPages;
            } else if args[i] == "--multiple-target-buffers" {
                to_memory_mode = ReadToMemoryMode::MultipleTargetBuffers;
            } else if args[i] == "--disable-hugepages" {
                to_memory_options.hugepages = HugepageAdvice::Disabled;
            } else if args[i] == "--measure-unmap-time" {
                to_memory_options.measure_unmap_time = true;
            } else if args[i] == "--via-memory" {
                via_memory = true;
            } else if args[i] == "-r" || args[i] == "-R" || args[i] == "--recursive" {
                recursive_copy = true;
            } else if args[i] == "--verify" || args[i] == "--verified" {
                verify_copy = true;
            } else if args[i] == "--verify-diff" {
                verify_copy_diff = true;
            } else if args[i] == "--hash" {
                persist_verification_hashes = true;
            } else if args[i] == "--no-lock" {
                no_lock = true;
            } else if args[i] == "--keep-target-size" {
                keep_target_size = true;
            } else if args[i] == "--diff" {
                force_diff_copy = true;
            } else if args[i] == "--full" {
                force_full_copy = true;
            } else if args[i] == "--copy-file-range" {
                force_copy_file_range = true;
            } else if args[i] == "--copy-file-range-single" {
                force_copy_file_range_single = true;
            } else if args[i] == "--threaded-copy" {
                force_threaded_copy = true;
            } else if args[i] == "--reflink" {
                force_reflink = true;
            } else if args[i] == "--cp-compat" {
                cp_compat = true;
            } else if args[i] == "--cp-no-clobber" {
                cp_no_clobber = true;
            } else if args[i] == "--cp-target-directory" {
                i += 1;
                if i >= args.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument for --cp-target-directory",
                    ));
                }
                cp_target_directory = Some(args[i].clone());
            } else if args[i] == "--cp-no-target-directory" {
                cp_no_target_directory = true;
            } else if args[i] == "--cp-update" {
                cp_update = true;
            } else if args[i] == "--cp-preserve" {
                cp_preserve = true;
            } else if args[i] == "--cp-no-dereference" {
                cp_no_dereference = true;
            } else if mode == "copy" && (args[i] == "-a" || args[i] == "--archive") {
                recursive_copy = true;
                cp_preserve = true;
                cp_no_dereference = true;
            } else if args[i] == "-q" || args[i] == "--quiet" {
                quiet = true;
            } else if args[i] == "-v" || args[i] == "--verbose" {
                verbose = true;
            } else if args[i] == "-s" || args[i] == "--save" {
                save_config = true;
            } else if args[i] == "-n" {
                i += 1;
                if i < args.len() {
                    iterations = args[i].parse().map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid number of iterations: {}", err),
                        )
                    })?;
                }
            } else {
                if mode == "copy" && cp_compat && !coreutils::args_request_no_fallback(&raw_args) {
                    let fallback_args = rebuild_cp_fallback_args(&raw_args);
                    if let Some(code) =
                        coreutils::try_external_command_fallback("cp", &fallback_args)?
                    {
                        return Ok(ParseOutcome::Early(code));
                    }
                }
                fro::cio_eprintln!("Unknown flag for {}: {}", args[0], args[i]);
                fro::cio_println!();
                if let Some(help) = command_help(args[0].as_str()) {
                    print_command_help(args[0].as_str(), help);
                }
                return Ok(ParseOutcome::Early(1));
            }
        } else if mode == "config" {
            if config_subcommand.is_none() {
                config_subcommand = Some(args[i].clone());
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "copy"
            || mode == "diff"
            || mode == "dual-read-bench"
            || mode == "split-manifest-recursive-copy-bench"
        {
            if source.is_none() {
                source = Some(args[i].clone());
            } else if mode == "copy" && cp_target_directory.is_some() {
                extra_paths.push(args[i].clone());
            } else if filename.is_empty() {
                filename = args[i].clone();
            } else if mode == "copy" && cp_target_directory.is_some() {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "manifest-recursive-copy-bench" || mode == "bench-tar-archive" {
            if filename.is_empty() {
                filename = args[i].clone();
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "recover" {
            if filename.is_empty() {
                filename = args[i].clone();
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "grep" {
            if pattern.is_empty() {
                pattern = args[i].clone();
            } else {
                filename = args[i].clone();
            }
        } else {
            filename = args[i].clone();
        }
        i += 1;
    }
    if mode == "bench-diff" {
        bench_diff_memory(16, 1024 * 1024);
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-read-sweep" {
        let mut config = config::load_config(config_path.as_deref());
        run_bench_read_sweep(&mut config)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-memcpy" {
        let total_size = bench_size.unwrap_or(4 * 1024 * 1024 * 1024);
        let num_threads = bench_threads.unwrap_or(32);
        if num_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--threads must be greater than zero",
            ));
        }
        let total_size = usize::try_from(total_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bench size does not fit in usize: {}", total_size),
            )
        })?;
        bench_memcpy_memory(num_threads, total_size);
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-base64-encode" {
        coreutils::bench_base64_encode(iterations, base64_kernel)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-base64-decode" {
        coreutils::bench_base64_decode(iterations, base64_decode_kernel)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-base64-decode-detect-fallback" {
        coreutils::bench_base64_decode_detect_fallback(iterations, base64_decode_kernel)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-base64-wrapped-encode" {
        coreutils::bench_base64_wrapped_encode(iterations, base64_wrap_cols)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-base64-wrapped-decode" {
        coreutils::bench_base64_wrapped_decode(iterations, base64_ignore_garbage)?;
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-mmap-write" {
        if filename.is_empty() {
            fro::cio_println!("Filename missing");
            return Ok(ParseOutcome::Early(1));
        }
        writer::bench_mmap_write(&filename);
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-write" {
        if filename.is_empty() {
            fro::cio_println!("Filename missing");
            return Ok(ParseOutcome::Early(1));
        }
        writer::bench_write(&filename);
        return Ok(ParseOutcome::Early(0));
    }
    if mode == "bench-tar-archive" {
        if filename.is_empty() {
            fro::cio_println!("Variant missing");
            return Ok(ParseOutcome::Early(1));
        }
        if extra_paths.is_empty() {
            fro::cio_println!("Source path missing");
            return Ok(ParseOutcome::Early(1));
        }
    }

    if mode == "config" {
        match config_subcommand.as_deref() {
            Some("print") => {
                if config_target.is_some() || !extra_paths.is_empty() {
                    fro::cio_println!(
                        "config print does not take a path; use fro config explain --for <path>"
                    );
                    return Ok(ParseOutcome::Early(1));
                }
            }
            Some("explain") => {
                if config_target.is_none() {
                    fro::cio_println!("config explain requires --for <path>");
                    return Ok(ParseOutcome::Early(1));
                }
                if !extra_paths.is_empty() {
                    fro::cio_println!("config explain accepts only one target path");
                    return Ok(ParseOutcome::Early(1));
                }
            }
            Some(other) => {
                fro::cio_println!("Unknown config subcommand: {other}");
                return Ok(ParseOutcome::Early(1));
            }
            None => {
                if let Some(help) = command_help("config") {
                    print_command_help(args[0].as_str(), help);
                }
                return Ok(ParseOutcome::Early(0));
            }
        }
        return Ok(ParseOutcome::Parsed(ParsedArgs {
            mode: mode.to_string(),
            config_subcommand,
            config_target,
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
            cp_compat,
            cp_no_clobber,
            cp_target_directory,
            cp_no_target_directory,
            cp_update,
            cp_preserve,
            cp_no_dereference,
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
        }));
    }

    if filename.is_empty() && !(mode == "copy" && cp_target_directory.is_some()) {
        fro::cio_println!("Filename missing");
        return Ok(ParseOutcome::Early(1));
    }
    if to_memory && mode != "read" {
        fro::cio_println!("--to-memory is only supported for read");
        return Ok(ParseOutcome::Early(1));
    }
    if auto_lift && mode != "read" && mode != "grep" {
        fro::cio_println!("--auto-lift is only supported for read and grep");
        return Ok(ParseOutcome::Early(1));
    }
    if auto_lift && to_memory {
        fro::cio_println!("--auto-lift is not supported with read --to-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if auto_lift && io_mode != common::IOMode::Auto {
        fro::cio_println!("--auto-lift chooses between direct and page-cache itself; do not combine it with --auto, --no-direct, or --direct");
        return Ok(ParseOutcome::Early(1));
    }
    if to_memory_mode != ReadToMemoryMode::Auto && !to_memory {
        fro::cio_println!(
            "--paged-shared-buffer, --mmap, --mmap-read-pages, and --multiple-target-buffers require read --to-memory"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if matches!(to_memory_options.hugepages, HugepageAdvice::Disabled) && !to_memory {
        fro::cio_println!("--disable-hugepages requires read --to-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if to_memory_options.measure_unmap_time && !to_memory {
        fro::cio_println!("--measure-unmap-time requires read --to-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if matches!(
        to_memory_mode,
        ReadToMemoryMode::Mmap | ReadToMemoryMode::MmapReadPages
    ) && io_mode == common::IOMode::Direct
    {
        fro::cio_println!("--mmap and --mmap-read-pages are not supported with --direct");
        return Ok(ParseOutcome::Early(1));
    }
    if manual_read_overrides.any()
        && mode != "read"
        && mode != "recursive-read-bench"
        && mode != "file-list-read-bench"
        && mode != "file-list-read-uring-bench"
        && mode != "file-list-read-open-read-close-sweep"
        && mode != "manifest-recursive-copy-bench"
    {
        fro::cio_println!("--threads, --qd, and --blocksize overrides are only supported for read-style benchmarks");
        return Ok(ParseOutcome::Early(1));
    }
    if via_memory && mode != "copy" {
        fro::cio_println!("--via-memory is only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if recursive_copy && mode != "copy" {
        fro::cio_println!("--recursive is only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if (force_copy_file_range
        || force_copy_file_range_single
        || force_threaded_copy
        || force_reflink)
        && mode != "copy"
    {
        fro::cio_println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink are only supported for copy"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if (verify_copy || verify_copy_diff) && mode != "copy" {
        fro::cio_println!("--verify and --verify-diff are only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if usize::from(force_copy_file_range)
        + usize::from(force_copy_file_range_single)
        + usize::from(force_threaded_copy)
        + usize::from(force_reflink)
        > 1
    {
        fro::cio_println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink cannot be combined"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if no_lock && mode != "copy" {
        fro::cio_println!("--no-lock is only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if keep_target_size && mode != "copy" {
        fro::cio_println!("--keep-target-size is only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if (force_diff_copy || force_full_copy) && mode != "copy" {
        fro::cio_println!("--diff and --full are only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if force_diff_copy && force_full_copy {
        fro::cio_println!("copy --diff and --full cannot be combined");
        return Ok(ParseOutcome::Early(1));
    }
    if verify_copy && verify_copy_diff {
        fro::cio_println!("--verify and --verify-diff cannot be used together");
        return Ok(ParseOutcome::Early(1));
    }
    if persist_verification_hashes && !verify_copy {
        fro::cio_println!("--hash is only supported for copy --verify");
        return Ok(ParseOutcome::Early(1));
    }
    if via_memory && save_config {
        fro::cio_println!(
            "copy --via-memory does not support --save; tune read and write separately"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if keep_target_size && (via_memory || verify_copy || verify_copy_diff) {
        fro::cio_println!("copy --keep-target-size is only supported for plain streaming copy");
        return Ok(ParseOutcome::Early(1));
    }
    if force_diff_copy && (via_memory || verify_copy || verify_copy_diff) {
        fro::cio_println!("copy --diff is only supported for plain streaming copy");
        return Ok(ParseOutcome::Early(1));
    }
    if force_diff_copy && (force_copy_file_range || force_copy_file_range_single || force_reflink) {
        fro::cio_println!("copy --diff cannot be combined with --copy-file-range, --copy-file-range-single, or --reflink");
        return Ok(ParseOutcome::Early(1));
    }
    if force_copy_file_range && via_memory {
        fro::cio_println!("copy --copy-file-range cannot be used with --via-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if force_copy_file_range_single && via_memory {
        fro::cio_println!("copy --copy-file-range-single cannot be used with --via-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if force_threaded_copy && via_memory {
        fro::cio_println!("copy --threaded-copy cannot be used with --via-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if force_reflink && via_memory {
        fro::cio_println!("copy --reflink cannot be used with --via-memory");
        return Ok(ParseOutcome::Early(1));
    }
    if (verify_copy || verify_copy_diff) && save_config {
        fro::cio_println!(
            "copy verification modes do not support --save; tune copy and verification separately"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if force_copy_file_range_single && save_config {
        fro::cio_println!(
            "copy --copy-file-range-single does not support --save; benchmark it with -n 1"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if force_diff_copy && save_config {
        fro::cio_println!("copy --diff does not support --save; benchmark it with -n 1");
        return Ok(ParseOutcome::Early(1));
    }
    if force_reflink && save_config {
        fro::cio_println!("copy --reflink does not support --save; benchmark it with -n 1");
        return Ok(ParseOutcome::Early(1));
    }
    if (verify_copy || verify_copy_diff) && iterations > 1 {
        fro::cio_println!("copy verification modes require -n 1");
        return Ok(ParseOutcome::Early(1));
    }
    if recursive_copy && iterations > 1 {
        fro::cio_println!("copy --recursive currently requires -n 1");
        return Ok(ParseOutcome::Early(1));
    }
    if force_reflink && iterations > 1 {
        fro::cio_println!("copy --reflink requires -n 1");
        return Ok(ParseOutcome::Early(1));
    }
    if force_copy_file_range_single && iterations > 1 {
        fro::cio_println!("copy --copy-file-range-single benchmarks the fixed one-call path; use --copy-file-range to optimize the tunable multi-call mode");
        return Ok(ParseOutcome::Early(1));
    }
    if verify_copy_diff && hash_base.is_some() {
        fro::cio_println!("copy --verify-diff does not use --hash-base");
        return Ok(ParseOutcome::Early(1));
    }
    if (force_copy_file_range || force_copy_file_range_single)
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        fro::cio_println!("copy --copy-file-range and --copy-file-range-single do not support direct read/write modes");
        return Ok(ParseOutcome::Early(1));
    }
    if force_reflink
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        fro::cio_println!("copy --reflink does not support direct read/write modes");
        return Ok(ParseOutcome::Early(1));
    }
    if recursive_copy && via_memory {
        fro::cio_println!("copy --recursive does not support --via-memory yet");
        return Ok(ParseOutcome::Early(1));
    }
    if recursive_copy && (verify_copy || verify_copy_diff) {
        fro::cio_println!("copy --recursive does not support verification modes yet");
        return Ok(ParseOutcome::Early(1));
    }
    if recursive_copy && save_config {
        fro::cio_println!("copy --recursive does not support --save yet");
        return Ok(ParseOutcome::Early(1));
    }
    if (cp_no_clobber
        || cp_target_directory.is_some()
        || cp_no_target_directory
        || cp_update
        || cp_preserve
        || cp_no_dereference)
        && mode != "copy"
    {
        fro::cio_println!("cp compatibility flags are only supported for copy");
        return Ok(ParseOutcome::Early(1));
    }
    if cp_target_directory.is_some() && cp_no_target_directory {
        fro::cio_eprintln!(
            "cp: cannot combine --target-directory (-t) and --no-target-directory (-T)"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if mode == "copy" && cp_target_directory.is_some() && source.is_none() {
        fro::cio_eprintln!("cp: missing file operand");
        fro::cio_eprintln!("Try 'cp --help' for more information.");
        return Ok(ParseOutcome::Early(1));
    }
    if mode == "recover" && extra_paths.is_empty() {
        fro::cio_println!("At least one recovery copy is required");
        return Ok(ParseOutcome::Early(1));
    }
    if mode == "manifest-recursive-copy-bench" && extra_paths.len() != 2 {
        fro::cio_println!(
            "manifest-recursive-copy-bench requires <manifest> <source_root> <target_root>"
        );
        return Ok(ParseOutcome::Early(1));
    }
    if mode != "write" && create_size.is_some() {
        fro::cio_println!("--create is only supported for write");
        return Ok(ParseOutcome::Early(1));
    }
    if mode == "recover" && recover_fast_requested && recover_in_place_all_requested {
        fro::cio_println!("--fast and --in-place-all cannot be used together");
        return Ok(ParseOutcome::Early(1));
    }

    Ok(ParseOutcome::Parsed(ParsedArgs {
        mode: mode.to_string(),
        config_subcommand,
        config_target,
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
        cp_compat,
        cp_no_clobber,
        cp_target_directory,
        cp_no_target_directory,
        cp_update,
        cp_preserve,
        cp_no_dereference,
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
    }))
}

#[cfg(test)]
mod tests;

