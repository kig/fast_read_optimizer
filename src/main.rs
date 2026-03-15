use std::env;

mod block_hash;
mod common;
mod config;
mod differ;
mod mincore;
mod optimizer;
mod reader;
mod writer;

use block_hash::{
    default_hash_base, hash_file_to_replicas, recover_file_with_copies, verify_file_with_replicas,
    RecoverMode,
};
use differ::{bench_diff_memory, diff_files};
use optimizer::run_optimizer;
use reader::read_file;
use writer::write_file;

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);
    let num: u64 = num_str.parse().ok()?;
    let mult = match suffix.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return None,
    };
    num.checked_mul(mult)
}

#[derive(Clone, Copy)]
struct CommandHelp {
    name: &'static str,
    usage: &'static str,
    summary: &'static str,
    notes: &'static [&'static str],
    examples: &'static [(&'static str, &'static str)],
}

fn is_help_flag(arg: &str) -> bool {
    arg == "--help" || arg == "-h"
}

fn command_help(name: &str) -> Option<CommandHelp> {
    match name {
        "read" => Some(CommandHelp {
            name: "read",
            usage: "read [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Striped multi-threaded file read for measuring raw throughput on one file.",
            notes: &[
                "Use -n 1 for one measured run with the current tuned parameters.",
                "Use -s together with --direct or --no-direct to save the best result back to config.",
            ],
            examples: &[(
                "Measure direct-IO read throughput once",
                "read --direct -n 1 /mnt/fast/bigfile.dat",
            )],
        }),
        "grep" => Some(CommandHelp {
            name: "grep",
            usage: "grep [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <pattern> <filename>",
            summary: "Read plus literal byte-substring search over one file.",
            notes: &[
                "This is a literal substring search, not a regex engine.",
                "Matches are printed as offset:pattern.",
            ],
            examples: &[(
                "Scan a file in page cache for a literal marker string",
                "grep --no-direct -n 1 needle /mnt/fast/bigfile.dat",
            )],
        }),
        "write" => Some(CommandHelp {
            name: "write",
            usage: "write [--create <size>] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Write a file using the tuned pipeline, optionally creating and sizing it first.",
            notes: &[
                "Without --create, the existing file size is used.",
                "--direct/--no-direct control the read-side planner mode; --direct-write/--no-direct-write control the write side.",
            ],
            examples: &[
                (
                    "Create a fresh 64 MiB file and fill it through the write path",
                    "write --create 64MiB --no-direct -n 1 out.bin",
                ),
                (
                    "Rewrite an existing file with direct writes",
                    "write --direct-write -n 1 out.bin",
                ),
            ],
        }),
        "copy" => Some(CommandHelp {
            name: "copy",
            usage: "copy [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <source> <target>",
            summary: "Copy one file to another using the same tuned read/write pipeline.",
            notes: &[
                "--direct/--no-direct/--auto control source reads.",
                "--direct-write/--no-direct-write/--auto-write control destination writes.",
            ],
            examples: &[
                (
                    "Do one copy run with current defaults",
                    "copy -n 1 in.bin out.bin",
                ),
                (
                    "Read through page cache but force direct writes to the destination",
                    "copy --no-direct --direct-write -n 1 in.bin out.bin",
                ),
            ],
        }),
        "diff" => Some(CommandHelp {
            name: "diff",
            usage: "diff [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Compare two files and report the first mismatch.",
            notes: &["Exits nonzero on mismatch."],
            examples: &[(
                "Check whether two large files are byte-identical",
                "diff --direct -n 1 a.bin b.bin",
            )],
        }),
        "dual-read-bench" => Some(CommandHelp {
            name: "dual-read-bench",
            usage: "dual-read-bench [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Read two files like diff, but treat it as a throughput benchmark rather than a mismatch-reporting tool.",
            notes: &["Useful when you want the read pressure of diff without stopping to explain a mismatch."],
            examples: &[(
                "Benchmark reading two files from page cache",
                "dual-read-bench --no-direct -n 1 a.bin b.bin",
            )],
        }),
        "hash" => Some(CommandHelp {
            name: "hash",
            usage: "hash [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Hash a file into fixed 1 MiB blocks and write three JSON sidecar replicas.",
            notes: &[
                "Default sidecar base is <file>.fro-hash.",
                "Default -n for hash is 1.",
            ],
            examples: &[(
                "Create block-hash sidecars for one large file",
                "hash --no-direct -n 1 bigfile.dat",
            )],
        }),
        "verify" => Some(CommandHelp {
            name: "verify",
            usage: "verify [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Re-hash a file, compare it to its sidecars, and report bad blocks.",
            notes: &[
                "Read-only command.",
                "On a clean file with intact sidecars, verify hashes the file once and stops.",
            ],
            examples: &[(
                "Scrub one file against its block-hash sidecars",
                "verify --no-direct -n 1 bigfile.dat",
            )],
        }),
        "recover" => Some(CommandHelp {
            name: "recover",
            usage: "recover [--auto|--no-direct|--direct] [--fast] [--in-place-all] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <target> <copy1> [copy2 ...]",
            summary: "Repair corrupted 1 MiB blocks using one or more full-file replicas.",
            notes: &[
                "Default recover rewrites only the first file; later files are read-only sources.",
                "--fast behaves like verify on the first file unless corruption forces a full multi-file scan.",
                "--in-place-all attempts to repair every input file and refresh broken sidecars.",
            ],
            examples: &[
                (
                    "Repair a target file from one clean copy",
                    "recover --no-direct -n 1 target.bin backup.bin",
                ),
                (
                    "Use verify-like fast scrub behavior and only fall back to full recovery if needed",
                    "recover --fast --no-direct -n 1 target.bin backup.bin",
                ),
            ],
        }),
        "bench-diff" => Some(CommandHelp {
            name: "bench-diff",
            usage: "bench-diff",
            summary: "In-memory diff microbenchmark used by the benchmark harness.",
            notes: &["This is mainly for development and regression tracking."],
            examples: &[("Run the in-memory diff microbenchmark", "bench-diff")],
        }),
        "bench-mmap-write" => Some(CommandHelp {
            name: "bench-mmap-write",
            usage: "bench-mmap-write <filename>",
            summary: "Memory-mapped write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the mmap write microbenchmark against an existing file",
                "bench-mmap-write out.bin",
            )],
        }),
        "bench-write" => Some(CommandHelp {
            name: "bench-write",
            usage: "bench-write <filename>",
            summary: "Plain write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the plain write microbenchmark against an existing file",
                "bench-write out.bin",
            )],
        }),
        _ => None,
    }
}

fn print_command_help(program: &str, help: CommandHelp) {
    println!("{} - {}", help.name, help.summary);
    println!();
    println!("USAGE:");
    println!("  {} {}", program, help.usage);
    if !help.notes.is_empty() {
        println!();
        println!("NOTES:");
        for note in help.notes {
            println!("  - {}", note);
        }
    }
    if !help.examples.is_empty() {
        println!();
        println!("EXAMPLES:");
        for (description, command) in help.examples {
            println!("  {}", description);
            println!("    {} {}", program, command);
        }
    }
}

fn print_general_help(program: &str) {
    println!("fast_read_optimizer (fro)");
    println!("High-throughput Linux file IO utilities with companion benchmark and optimizer tooling.");
    println!();
    println!("USAGE:");
    println!("  {} <command> [options]", program);
    println!("  {} <command> --help", program);
    println!();
    println!("Utilities:");
    for (name, summary) in [
        ("grep", "search for a literal byte substring while reading"),
        ("write", "rewrite or create a file through the tuned write path"),
        ("copy", "copy one file to another with tuned read/write settings"),
        ("diff", "compare two files and report the first mismatch"),
        ("hash", "write 1 MiB block-hash sidecars"),
        ("verify", "scrub a file against its block-hash sidecars"),
        ("recover", "repair corrupted blocks from one or more replicas"),
    ] {
        println!("  {:<16} {}", name, summary);
    }
    println!();
    println!("Benchmarks:");
    println!("  read               measure striped file read throughput");
    println!("  dual-read-bench    benchmark the read pressure of diff");
    println!("  fro-optimize       tune configs for one or more commands / mounts");
    println!("  fro-benchmark      run the regression benchmark suite");
    println!("  bench-diff         in-memory diff microbenchmark");
    println!("  bench-mmap-write   mmap write microbenchmark");
    println!("  bench-write        plain write microbenchmark");
    println!();
    println!("Common flags:");
    println!("  --auto | --no-direct | --direct");
    println!("  --auto-write | --no-direct-write | --direct-write");
    println!("  -n <iterations>    use -n 1 for one measured run with current tuned params");
    println!("  -s, --save         save tuned params when forcing --direct or --no-direct");
    println!("  -c, --config PATH  override config path");
    println!("  -v, --verbose      print more about the current run");
    println!();
    println!("Related tools:");
    println!("  ./target/release/fro-optimize --help");
    println!("  ./target/release/fro-benchmark --help");
    println!();
    println!(
        "Config resolution (when -c is not provided): $FRO_CONFIG, then ~/.fro/fro.json, then /etc/fro.json"
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || is_help_flag(args[1].as_str()) {
        print_general_help(args[0].as_str());
        return;
    }
    if args.len() >= 3 && is_help_flag(args[2].as_str()) {
        if let Some(help) = command_help(args[1].as_str()) {
            print_command_help(args[0].as_str(), help);
        } else {
            eprintln!("Unknown command: {}", args[1]);
            println!();
            print_general_help(args[0].as_str());
        }
        return;
    }
    let mode = args[1].as_str();
    let mut io_mode = common::IOMode::Auto;
    let mut io_mode_write = common::IOMode::Auto;
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut extra_paths: Vec<String> = Vec::new();
    let mut hash_base: Option<&str> = None;
    let mut recover_mode = RecoverMode::Standard;
    let mut recover_fast_requested = false;
    let mut recover_in_place_all_requested = false;
    let mut create_size: Option<u64> = None;
    let mut iterations = if mode == "read" { 1000 } else { 1 };
    let mut save_config = false;
    let mut config_path: Option<&str> = None;

    let mut i = 2;
    while i < args.len() {
        if args[i] == "-c" || args[i] == "--config" {
            i += 1;
            if i < args.len() {
                config_path = Some(args[i].as_str());
            }
        } else if args[i] == "--hash-base" {
            i += 1;
            if i < args.len() {
                hash_base = Some(args[i].as_str());
            }
        } else if args[i] == "--create" {
            i += 1;
            if i < args.len() {
                create_size = parse_size(args[i].as_str()).or_else(|| {
                    eprintln!("Invalid --create size: {}", args[i]);
                    None
                });
                if create_size.is_none() {
                    return;
                }
            }
        } else if args[i] == "--fast" {
            recover_fast_requested = true;
            recover_mode = RecoverMode::Fast;
        } else if args[i] == "--in-place-all" {
            recover_in_place_all_requested = true;
            recover_mode = RecoverMode::InPlaceAll;
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
        } else if args[i] == "-v" || args[i] == "--verbose" {
            verbose = true;
        } else if args[i] == "-s" || args[i] == "--save" {
            save_config = true;
        } else if args[i] == "-n" {
            i += 1;
            if i < args.len() {
                iterations = args[i].parse().expect("Invalid number of iterations");
            }
        } else if mode == "copy" || mode == "diff" || mode == "dual-read-bench" {
            if source.is_none() {
                source = Some(args[i].as_str());
            } else if filename == "" {
                filename = args[i].as_str();
            }
        } else if mode == "recover" {
            if filename == "" {
                filename = args[i].as_str();
            } else {
                extra_paths.push(args[i].clone());
            }
        } else if mode == "grep" {
            if pattern == "" {
                pattern = args[i].as_str();
            } else {
                filename = args[i].as_str();
            }
        } else {
            filename = args[i].as_str();
        }
        i += 1;
    }
    if mode == "bench-diff" {
        bench_diff_memory(16, 1024 * 1024);
        return;
    }
    if mode == "bench-mmap-write" {
        if filename == "" {
            println!("Filename missing");
            return;
        }
        writer::bench_mmap_write(filename);
        return;
    }
    if mode == "bench-write" {
        if filename == "" {
            println!("Filename missing");
            return;
        }
        writer::bench_write(filename);
        return;
    }

    if filename == "" {
        println!("Filename missing");
        return;
    }

    if mode == "recover" && extra_paths.is_empty() {
        println!("At least one recovery copy is required");
        return;
    }
    if mode != "write" && create_size.is_some() {
        println!("--create is only supported for write");
        return;
    }
    if mode == "recover" && recover_fast_requested && recover_in_place_all_requested {
        println!("--fast and --in-place-all cannot be used together");
        return;
    }

    let mut config = config::load_config(config_path);
    let config_mode = match mode {
        "recover" => "verify",
        "hash" | "verify" => mode,
        _ => mode,
    };

    let context_path = filename;

    let params_page_cache = config.get_params_for_path(config_mode, false, context_path);
    let params_direct = config.get_params_for_path(config_mode, true, context_path);

    let num_threads_pc = params_page_cache.num_threads;
    let qd_pc = params_page_cache.qd;

    let num_threads_direct = params_direct.num_threads;
    let qd_direct = params_direct.qd;

    // We reverse the scaling factor logic here since run_optimizer multiplies by bsf*1024
    let base_block_size_pc = params_page_cache.block_size / (4 * 1024);
    let base_block_size_direct = params_direct.block_size / (256 * 1024);

    let start_params = vec![
        num_threads_pc,
        base_block_size_pc,
        qd_pc as u64,
        num_threads_direct,
        base_block_size_direct,
        qd_direct as u64,
    ];
    let params_steps = vec![1, 4 * 1024, 1, 1, 256 * 1024, 1];

    let mode_name = mode;
    let verbose = verbose
        || mode == "read"
        || mode == "write"
        || mode == "hash"
        || mode == "verify"
        || mode == "recover";
    if verbose {
        eprintln!("Opening file {} for {}", filename, mode);
    }

    let mut exit_code = 0;
    let hash_base_owned = hash_base.map(|s| s.to_string());
    let extra_paths_owned = extra_paths;

    let mode_callback = |p: &[u64]| {
        if mode == "read" || mode == "grep" {
            read_file(
                pattern,
                filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )
        } else if mode == "hash" {
            let manifest = hash_file_to_replicas(
                filename,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )
            .expect("Failed to hash file");
            println!(
                "wrote {} hash blocks ({} bytes each) to {}[0-2].json",
                manifest.block_hashes.len(),
                manifest.block_size,
                hash_base_owned
                    .as_deref()
                    .unwrap_or(&default_hash_base(filename))
            );
            manifest.bytes_hashed
        } else if mode == "verify" {
            let report = verify_file_with_replicas(
                filename,
                hash_base_owned.as_deref(),
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )
            .expect("Failed to verify file");
            println!(
                "verify: loaded {}/3 hash replicas, ok_blocks={}, bad_blocks={}",
                report.loaded_manifests,
                report.ok_blocks,
                report.bad_blocks.len()
            );
            for issue in &report.bad_blocks {
                println!(
                    "block {}: {}",
                    issue.block_index,
                    issue.decision.status_message()
                );
            }
            if !report.bad_blocks.is_empty() {
                exit_code = 1;
            }
            report.bytes_hashed
        } else if mode == "recover" {
            let report = recover_file_with_copies(
                filename,
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
            )
            .expect("Failed to recover file");
            println!(
                "recover: repaired_blocks={}, repaired_files={}, sidecars_refreshed={}, failed_blocks={}, used_fast_path={}, fell_back_to_full_scan={}",
                report.repaired_blocks,
                report.repaired_files,
                report.sidecars_refreshed,
                report.failed_blocks.len()
                ,
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
            report.bytes_hashed
        } else if mode == "write" {
            write_file(
                source,
                filename,
                create_size,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                io_mode_write,
            )
        } else if mode == "copy" {
            write_file(
                source,
                filename,
                create_size,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
                io_mode_write,
            )
        } else if mode == "diff" || mode == "dual-read-bench" {
            let s1 = std::fs::metadata(source.unwrap())
                .expect("Could not read file 1")
                .len();
            let s2 = std::fs::metadata(filename)
                .expect("Could not read file 2")
                .len();
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
                let size = std::fs::File::open(filename)
                    .unwrap()
                    .metadata()
                    .unwrap()
                    .len();
                let res = diff_files(
                    source.unwrap(),
                    filename,
                    p[0],
                    p[1],
                    p[2] as usize,
                    p[3],
                    p[4],
                    p[5] as usize,
                    io_mode,
                    bench_only,
                );
                if res != 0 && mode == "diff" {
                    exit_code = 1;
                }
                size * 2
            } else {
                0
            }
        } else {
            panic!("Invalid mode {}", mode)
        }
    };

    let best_params = run_optimizer(
        mode_name,
        start_params,
        params_steps,
        iterations,
        verbose,
        mode_callback,
    );

    if save_config && io_mode != common::IOMode::Auto {
        let direct = io_mode == common::IOMode::Direct;
        let off = if direct { 3 } else { 0 };
        config.update_params_for_path(
            config_mode,
            direct,
            context_path,
            config::IOParams {
                num_threads: best_params[off + 0],
                block_size: best_params[off + 1],
                qd: best_params[off + 2] as usize,
            },
        );
        config.save();
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
