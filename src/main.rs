use std::env;

mod block_hash;
mod common;
mod config;
mod coreutils;
mod differ;
mod io_util;
mod mincore;
mod optimizer;
mod reader;
mod verified_copy;
mod writer;

use block_hash::{
    default_hash_base, hash_file_blocks, hash_file_to_replicas, recover_file_with_copies,
    verify_file_with_replicas, BlockHashAlgorithm, RecoverMode,
};
use common::{CopyAutoMode, CopyStrategy};
use differ::{bench_diff_memory, bench_memcpy_memory, diff_files};
use io_util::{direct_writer_supported, sync_path, CopyOperationGuard};
use mincore::is_first_page_resident;
use optimizer::run_optimizer;
use reader::{load_file_to_memory, read_file};
use std::io::{self, Write};
use verified_copy::copy_file_verified_with_options_and_lock;
use writer::{
    copy_file_with_strategy, copy_file_with_strategy_and_truncate, overwrite_changed_chunks_direct,
    write_buffer, write_file,
};

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

const PAGE_CACHE_PARAM_INDICES: [usize; 3] = [0, 1, 2];
const DIRECT_PARAM_INDICES: [usize; 3] = [3, 4, 5];
const COPY_RANGE_PARAM_INDICES: [usize; 3] = [6, 7, 8];

fn mark_optimizer_params(mask: &mut [bool], indices: &[usize], include_block_size: bool) {
    for &index in indices {
        if include_block_size || index % 3 != 1 {
            mask[index] = true;
        }
    }
}

fn active_optimizer_param_mask(
    mode: &str,
    io_mode: common::IOMode,
    io_mode_write: common::IOMode,
    via_memory: bool,
    copy_strategy: CopyStrategy,
) -> Vec<bool> {
    let mut mask = [false; 9];
    match mode {
        "read" | "grep" | "hash" | "diff" | "dual-read-bench" => match io_mode {
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
        },
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

#[derive(Clone, Copy)]
struct ResolvedCopyExecution {
    copy_strategy: CopyStrategy,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
    diff_overwrite: bool,
    full_rewrite: bool,
    path_label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeuristicCopyPlan {
    DiffOverwrite,
    CachedReadDirectWrite,
    DirectReadDirectWrite,
    CopyFileRangeSingle,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CopyRewriteMode {
    Auto,
    Diff,
    Full,
}

fn resolve_copy_execution(
    config: &config::LoadedConfig,
    source_path: &str,
    path: &str,
    requested_strategy: CopyStrategy,
    rewrite_mode: CopyRewriteMode,
    io_mode_read: common::IOMode,
    io_mode_write: common::IOMode,
) -> io::Result<ResolvedCopyExecution> {
    if rewrite_mode == CopyRewriteMode::Diff {
        return Ok(if direct_writer_supported(path)? {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::Direct,
                diff_overwrite: true,
                full_rewrite: false,
                path_label: "forced diff-overwrite",
            }
        } else {
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::CopyFileRangeSingle,
                io_mode_read: common::IOMode::PageCache,
                io_mode_write: common::IOMode::PageCache,
                diff_overwrite: false,
                full_rewrite: false,
                path_label: "forced diff copy_file_range single fallback",
            }
        });
    }

    if requested_strategy != CopyStrategy::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: requested_strategy,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: match requested_strategy {
                CopyStrategy::Auto => "auto",
                CopyStrategy::Threaded => "threaded",
                CopyStrategy::CopyFileRange => "copy_file_range",
                CopyStrategy::CopyFileRangeSingle => "copy_file_range single",
                CopyStrategy::Reflink => "reflink",
            },
        });
    }

    if io_mode_read != common::IOMode::Auto || io_mode_write != common::IOMode::Auto {
        return Ok(ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read,
            io_mode_write,
            diff_overwrite: false,
            full_rewrite: rewrite_mode == CopyRewriteMode::Full,
            path_label: "explicit io-mode threaded",
        });
    }

    let resolved = match config.get_copy_auto_mode_for_path(path) {
        CopyAutoMode::PageCache => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config page-cache threaded",
        },
        CopyAutoMode::Direct => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::Threaded,
            io_mode_read: common::IOMode::Direct,
            io_mode_write: common::IOMode::Direct,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config direct threaded",
        },
        CopyAutoMode::CopyFileRange => ResolvedCopyExecution {
            copy_strategy: CopyStrategy::CopyFileRange,
            io_mode_read: common::IOMode::PageCache,
            io_mode_write: common::IOMode::PageCache,
            diff_overwrite: false,
            full_rewrite: false,
            path_label: "auto config copy_file_range",
        },
        CopyAutoMode::Heuristic => {
            let (source_cached, target_cached, source_len, target_len) =
                inspect_copy_auto_state(source_path, path);
            let plan = if rewrite_mode != CopyRewriteMode::Full
                && should_prefer_cached_diff_overwrite(
                source_cached,
                target_cached,
                source_len,
                target_len,
            ) {
                if direct_writer_supported(path)? {
                    HeuristicCopyPlan::DiffOverwrite
                } else {
                    HeuristicCopyPlan::CopyFileRangeSingle
                }
            } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len)
            {
                HeuristicCopyPlan::CachedReadDirectWrite
            } else {
                HeuristicCopyPlan::DirectReadDirectWrite
            };

            match plan {
                HeuristicCopyPlan::DiffOverwrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: true,
                    full_rewrite: false,
                    path_label: "auto diff-overwrite",
                },
                HeuristicCopyPlan::CachedReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto cached-read direct-write",
                },
                HeuristicCopyPlan::DirectReadDirectWrite => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::Threaded,
                    io_mode_read: common::IOMode::Direct,
                    io_mode_write: common::IOMode::Direct,
                    diff_overwrite: false,
                    full_rewrite: rewrite_mode == CopyRewriteMode::Full,
                    path_label: "auto direct threaded",
                },
                HeuristicCopyPlan::CopyFileRangeSingle => ResolvedCopyExecution {
                    copy_strategy: CopyStrategy::CopyFileRangeSingle,
                    io_mode_read: common::IOMode::PageCache,
                    io_mode_write: common::IOMode::PageCache,
                    diff_overwrite: false,
                    full_rewrite: false,
                    path_label: "auto copy_file_range single fallback",
                },
            }
        }
    };

    Ok(resolved)
}

fn inspect_copy_auto_state(
    source_path: &str,
    target_path: &str,
) -> (bool, bool, Option<u64>, Option<u64>) {
    let source_cached = Ok(true) == is_first_page_resident(source_path);
    let target_cached = Ok(true) == is_first_page_resident(target_path);
    let source_len = std::fs::metadata(source_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    let target_len = std::fs::metadata(target_path)
        .ok()
        .filter(|meta| meta.file_type().is_file())
        .map(|meta| meta.len());
    (source_cached, target_cached, source_len, target_len)
}

fn target_is_similar_size(source_len: Option<u64>, target_len: Option<u64>) -> bool {
    const TARGET_SIZE_THRESHOLD_PERCENT: u64 = 70;

    let (Some(source_len), Some(target_len)) = (source_len, target_len) else {
        return false;
    };

    if source_len == 0 {
        return target_len == 0;
    }

    target_len
        .saturating_mul(100)
        .saturating_div(source_len)
        >= TARGET_SIZE_THRESHOLD_PERCENT
}

#[cfg(kani)]
mod kani_proofs {
    use super::target_is_similar_size;

    #[kani::proof]
    fn zero_length_source_is_only_similar_to_zero_length_target() {
        let target_len: Option<u64> = kani::any();
        let expected = matches!(target_len, Some(0));
        assert_eq!(target_is_similar_size(Some(0), target_len), expected);
    }

    #[kani::proof]
    fn missing_sizes_are_never_similar() {
        let source_len: Option<u64> = kani::any();
        let target_len: Option<u64> = kani::any();
        kani::assume(source_len.is_none() || target_len.is_none());
        assert!(!target_is_similar_size(source_len, target_len));
    }
}

fn should_prefer_cached_diff_overwrite(
    source_cached: bool,
    target_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_cached && target_is_similar_size(source_len, target_len)
}

fn should_prefer_cached_read_direct_write(
    source_cached: bool,
    source_len: Option<u64>,
    target_len: Option<u64>,
) -> bool {
    source_cached && target_is_similar_size(source_len, target_len)
}

fn io_mode_label(io_mode: common::IOMode) -> &'static str {
    match io_mode {
        common::IOMode::Auto => "auto",
        common::IOMode::PageCache => "page-cache",
        common::IOMode::Direct => "direct",
    }
}

fn describe_copy_path(
    resolved_copy: ResolvedCopyExecution,
    via_memory: bool,
    keep_target_size: bool,
) -> String {
    if via_memory {
        return format!(
            "copy path: via-memory [read={}, write={}]",
            io_mode_label(resolved_copy.io_mode_read),
            io_mode_label(resolved_copy.io_mode_write)
        );
    }

    let mut details = vec![
        format!("strategy={}", resolved_copy.path_label),
        format!("read={}", io_mode_label(resolved_copy.io_mode_read)),
        format!("write={}", io_mode_label(resolved_copy.io_mode_write)),
    ];
    if resolved_copy.full_rewrite {
        details.push("rewrite=full".to_string());
    }
    if resolved_copy.diff_overwrite && !keep_target_size {
        details.push("delta=changed-chunks".to_string());
    }
    if keep_target_size {
        details.push("target=keep-size".to_string());
    }
    format!("copy path: {}", details.join(", "))
}

fn print_verify_report(report: &block_hash::VerifyReport) {
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
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some(message)
    } else if let Some(message) = payload.downcast_ref::<String>() {
        Some(message.as_str())
    } else {
        None
    }
}

fn is_broken_pipe_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}

fn is_broken_pipe_panic(payload: &(dyn std::any::Any + Send)) -> bool {
    panic_payload_message(payload).is_some_and(|message| message.contains("Broken pipe"))
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
            usage: "read [--to-memory] [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Striped multi-threaded file read for measuring raw throughput on one file.",
            notes: &[
                "Use -n 1 for one measured run with the current tuned parameters.",
                "Use -s together with --direct or --no-direct to save the best result back to config.",
                "--to-memory loads the whole file into RAM instead of only measuring the streaming read path.",
            ],
            examples: &[
                (
                    "Measure direct-IO read throughput once",
                    "read --direct -n 1 /mnt/fast/bigfile.dat",
                ),
                (
                    "Load a hot file all the way into memory",
                    "read --to-memory --no-direct -n 1 /mnt/fast/bigfile.dat",
                ),
            ],
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
        "copy" | "copy-via-memory" => Some(CommandHelp {
            name: "copy",
            usage: "copy [--via-memory] [--keep-target-size] [--diff|--full] [--copy-file-range|--copy-file-range-single|--threaded-copy|--reflink] [--no-lock] [--verify|--verify-diff] [--hash] [--xxh3|--sha256] [--hash-base path] [-q|--quiet] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <source> <target>",
            summary: "Copy one file to another using the same tuned read/write pipeline.",
            notes: &[
                "--direct/--no-direct/--auto control source reads.",
                "--direct-write/--no-direct-write/--auto-write control destination writes.",
                "--copy-file-range uses the tunable multi-call copy_file_range(2) strategy with its own optimizer params.",
                "--copy-file-range-single forces the one-call copy_file_range(2) baseline for benchmarking.",
                "--threaded-copy forces the existing tuned striped io_uring copy path.",
                "--reflink requests a CoW clone/reflink when the filesystem supports it; this is fast but does not promise physically independent storage blocks.",
                "--diff forces chunked diff-and-overwrite copy when supported; --full disables diffing and always rewrites the full file.",
                "Without either flag, plain copy uses copy auto mode: cold/default paths prefer direct threaded copy; hot similar-size cached files can diff-and-overwrite only changed chunks with direct writes, and otherwise a hot source plus a near-sized target can switch to page-cache read with direct write.",
                "--keep-target-size preserves an already-sized destination instead of re-truncating/re-preallocating it; this is mainly useful for best-case benchmarking.",
                "Copy takes an advisory shared lock on the source and an advisory exclusive lock on the destination by default; use --no-lock to skip that cooperative locking.",
                "--via-memory loads the whole source file into RAM first, then writes that buffer to the destination.",
                "--verify hashes the source, copies into a temporary sibling, fsyncs and verifies that file, then renames it into place without leaving sidecars by default.",
                "--hash with --verify leaves durable sidecars at the destination and at the source if the source did not already have sidecars, and syncs their parent directories too.",
                "--verify-diff fsyncs the destination and then runs a diff pass instead of block-hash verification.",
                "For non-verified copy modes, fro also checks whether the source file's size/mtime/ctime changed during the operation and fails if it did.",
                "When using --via-memory, tune read and write separately instead of saving copy params.",
                "Verification success is reported to stderr unless --quiet is used.",
            ],
            examples: &[
                (
                    "Copy in.bin to out.bin",
                    "copy in.bin out.bin",
                ),
                (
                    "Read through page cache but force direct writes to the destination",
                    "copy --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Benchmark the kernel copy_file_range syscall path directly",
                    "copy --copy-file-range-single --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Optimize the chunked copy_file_range path separately",
                    "copy --copy-file-range --no-direct -n 32 -s in.bin out.bin",
                ),
                (
                    "Force the existing striped io_uring copy path for comparison",
                    "copy --threaded-copy --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Request a CoW reflink/soft copy when the filesystem supports it",
                    "copy --reflink --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Load the whole source into RAM, then flush it with direct writes",
                    "copy --via-memory --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Skip advisory locking when cooperating lock semantics would get in the way",
                    "copy --no-lock --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file through a verified temp target swap without leaving sidecars",
                    "copy --verify --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file, verify it, and leave source/destination sidecars",
                    "copy --verify --hash --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file and run a diff pass after fsync",
                    "copy --verify-diff --no-direct in.bin out.bin",
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
                "diff --direct a.bin b.bin",
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
            usage: "hash [--auto|--no-direct|--direct] [--xxh3|--sha256] [--hash-only] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Hash a file in parallel 1 MiB blocks, hash the hashes, and write three JSON sidecar replicas.",
            notes: &[
                "Default sidecar base is <file>.fro-hash.",
                "Use --xxh3 or --sha256 to choose the sidecar digest algorithm. (NB: this is not sha256sum-compatible.)",
                "Use --hash-only to only print the filename and hash of hashes.",
                "Default -n for hash is 1.",
            ],
            examples: &[(
                "Create block-hash sidecars for one large file",
                "hash --no-direct bigfile.dat",
            ),
            (
                "Get a SHA256 hash of block hashes for easy file comparisons",
                "hash --sha256 --hash-only bigfile.dat",
            )],
        }),
        "verify" => Some(CommandHelp {
            name: "verify",
            usage: "verify [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Re-hash a file, compare it to its sidecars, and report bad blocks.",
            notes: &[
                "Read-only command.",
                "verify follows the hash type stored in the sidecar manifest.",
                "On a clean file with intact sidecars, verify hashes the file once and stops.",
            ],
            examples: &[(
                "Scrub one file against its block-hash sidecars",
                "verify --no-direct bigfile.dat",
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
                "recover follows each file's stored sidecar hash type.",
            ],
            examples: &[
                (
                    "Repair a target file from one clean copy",
                    "recover --no-direct target.bin backup.bin",
                ),
                (
                    "Use verify-like fast scrub behavior and only fall back to full recovery if needed",
                    "recover --fast --no-direct target.bin backup.bin",
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
        "bench-memcpy" => Some(CommandHelp {
            name: "bench-memcpy",
            usage: "bench-memcpy [--size <bytes>] [--threads <count>]",
            summary: "In-memory memcpy microbenchmark for establishing the RAM copy ceiling.",
            notes: &[
                "Defaults to --size 4GiB and --threads 32.",
                "Reports effective bandwidth as source read plus destination write bytes.",
            ],
            examples: &[(
                "Benchmark a 4 GiB to 4 GiB memcpy with 32 threads",
                "bench-memcpy --size 4GiB --threads 32",
            )],
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
    println!(
        "High-throughput Linux file IO utilities with companion benchmark and optimizer tooling."
    );
    println!();
    println!("USAGE:");
    println!("  {} <command> [options]", program);
    println!("  {} <command> --help", program);
    println!();
    println!("Utilities:");
    for (name, summary) in [
        ("grep", "search for a literal byte substring while reading"),
        (
            "write",
            "rewrite or create a file through the tuned write path",
        ),
        (
            "copy",
            "copy one file to another with tuned read/write settings",
        ),
        ("diff", "compare two files and report the first mismatch"),
        ("hash", "write 1 MiB block-hash sidecars"),
        ("verify", "scrub a file against its block-hash sidecars"),
        (
            "recover",
            "repair corrupted blocks from one or more replicas",
        ),
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
    println!("  bench-memcpy       in-memory memcpy microbenchmark");
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
    println!("Multicall aliases:");
    println!("  cp cmp fgrep cat tac wc sum cksum b3sum sha224sum sha256sum sha384sum sha512sum shred");
    println!("  (invoke via argv[0], e.g. by symlinking `fro` to one of those names)");
    println!();
    println!("Related tools:");
    println!("  ./target/release/fro-optimize --help");
    println!("  ./target/release/fro-benchmark --help");
    println!();
    println!(
        "Config resolution (when -c is not provided): $FRO_CONFIG, then ~/.fro/fro.json, then /etc/fro.json"
    );
}

fn try_main() -> io::Result<i32> {
    let raw_args: Vec<String> = env::args().collect();
    if let Some(code) = coreutils::try_run_multicall(&raw_args)? {
        return Ok(code);
    }
    let args = coreutils::rewrite_alias_args(raw_args);
    if args.len() < 2 || is_help_flag(args[1].as_str()) {
        print_general_help(args[0].as_str());
        return Ok(0);
    }
    if args.len() >= 3 && is_help_flag(args[2].as_str()) {
        if let Some(help) = command_help(args[1].as_str()) {
            print_command_help(args[0].as_str(), help);
        } else {
            eprintln!("Unknown command: {}", args[1]);
            println!();
            print_general_help(args[0].as_str());
        }
        return Ok(0);
    }
    let legacy_copy_via_memory = args[1] == "copy-via-memory";
    let mode = if legacy_copy_via_memory {
        "copy"
    } else {
        args[1].as_str()
    };
    let mut io_mode = common::IOMode::Auto;
    let mut io_mode_write = common::IOMode::Auto;
    let mut to_memory = false;
    let mut via_memory = legacy_copy_via_memory;
    let mut verify_copy = false;
    let mut verify_copy_diff = false;
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
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut extra_paths: Vec<String> = Vec::new();
    let mut hash_base: Option<&str> = None;
    let mut recover_mode = RecoverMode::Standard;
    let mut hash_type = BlockHashAlgorithm::Xxh3;
    let mut hash_only = false;
    let mut recover_fast_requested = false;
    let mut recover_in_place_all_requested = false;
    let mut create_size: Option<u64> = None;
    let mut iterations = if mode == "read" { 1000 } else { 1 };
    let mut save_config = false;
    let mut config_path: Option<&str> = None;
    let mut bench_size: Option<u64> = None;
    let mut bench_threads: Option<usize> = None;

    let mut i = 2;
    let mut end_flags = false;
    while i < args.len() {
        let is_flag = !end_flags && args[i].starts_with("-");
        if is_flag {
            if args[i] == "--" {
                end_flags = true;
            } else if args[i] == "--help" {
                if let Some(help) = command_help(args[1].as_str()) {
                    print_command_help(args[0].as_str(), help);
                } else {
                    eprintln!("Unknown command: {}", args[1]);
                    println!();
                    print_general_help(args[0].as_str());
                }
                return Ok(0);
            } else if args[i] == "-c" || args[i] == "--config" {
                i += 1;
                if i < args.len() {
                    config_path = Some(args[i].as_str());
                }
            } else if args[i] == "--hash-base" {
                i += 1;
                if i < args.len() {
                    hash_base = Some(args[i].as_str());
                }
            } else if args[i] == "--size" {
                i += 1;
                if i < args.len() {
                    bench_size = parse_size(args[i].as_str()).or_else(|| {
                        eprintln!("Invalid --size: {}", args[i]);
                        None
                    });
                    if bench_size.is_none() {
                        return Ok(1);
                    }
                }
            } else if args[i] == "--threads" {
                i += 1;
                if i < args.len() {
                    bench_threads = Some(args[i].parse().map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid thread count: {}", err),
                        )
                    })?);
                }
            } else if args[i] == "--create" {
                i += 1;
                if i < args.len() {
                    create_size = parse_size(args[i].as_str()).or_else(|| {
                        eprintln!("Invalid --create size: {}", args[i]);
                        None
                    });
                    if create_size.is_none() {
                        return Ok(1);
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
            } else if args[i] == "--via-memory" {
                via_memory = true;
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
                eprintln!("Unknown flag for {}: {}", args[0], args[i]);
                println!();
                if let Some(help) = command_help(args[0].as_str()) {
                    print_command_help(args[0].as_str(), help);
                }
                return Ok(0);
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
        return Ok(0);
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
        return Ok(0);
    }
    if mode == "bench-mmap-write" {
        if filename == "" {
            println!("Filename missing");
            return Ok(1);
        }
        writer::bench_mmap_write(filename);
        return Ok(0);
    }
    if mode == "bench-write" {
        if filename == "" {
            println!("Filename missing");
            return Ok(1);
        }
        writer::bench_write(filename);
        return Ok(0);
    }

    if filename == "" {
        println!("Filename missing");
        return Ok(1);
    }
    if to_memory && mode != "read" {
        println!("--to-memory is only supported for read");
        return Ok(1);
    }
    if via_memory && mode != "copy" {
        println!("--via-memory is only supported for copy");
        return Ok(1);
    }
    if (force_copy_file_range
        || force_copy_file_range_single
        || force_threaded_copy
        || force_reflink)
        && mode != "copy"
    {
        println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink are only supported for copy"
        );
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && mode != "copy" {
        println!("--verify and --verify-diff are only supported for copy");
        return Ok(1);
    }
    if usize::from(force_copy_file_range)
        + usize::from(force_copy_file_range_single)
        + usize::from(force_threaded_copy)
        + usize::from(force_reflink)
        > 1
    {
        println!(
            "--copy-file-range, --copy-file-range-single, --threaded-copy, and --reflink cannot be combined"
        );
        return Ok(1);
    }
    if no_lock && mode != "copy" {
        println!("--no-lock is only supported for copy");
        return Ok(1);
    }
    if keep_target_size && mode != "copy" {
        println!("--keep-target-size is only supported for copy");
        return Ok(1);
    }
    if (force_diff_copy || force_full_copy) && mode != "copy" {
        println!("--diff and --full are only supported for copy");
        return Ok(1);
    }
    if force_diff_copy && force_full_copy {
        println!("copy --diff and --full cannot be combined");
        return Ok(1);
    }
    if verify_copy && verify_copy_diff {
        println!("--verify and --verify-diff cannot be used together");
        return Ok(1);
    }
    if persist_verification_hashes && !verify_copy {
        println!("--hash is only supported for copy --verify");
        return Ok(1);
    }
    if via_memory && save_config {
        println!("copy --via-memory does not support --save; tune read and write separately");
        return Ok(1);
    }
    if keep_target_size && (via_memory || verify_copy || verify_copy_diff) {
        println!("copy --keep-target-size is only supported for plain streaming copy");
        return Ok(1);
    }
    if force_diff_copy && (via_memory || verify_copy || verify_copy_diff) {
        println!("copy --diff is only supported for plain streaming copy");
        return Ok(1);
    }
    if force_diff_copy && (force_copy_file_range || force_copy_file_range_single || force_reflink) {
        println!("copy --diff cannot be combined with --copy-file-range, --copy-file-range-single, or --reflink");
        return Ok(1);
    }
    if force_copy_file_range && via_memory {
        println!("copy --copy-file-range cannot be used with --via-memory");
        return Ok(1);
    }
    if force_copy_file_range_single && via_memory {
        println!("copy --copy-file-range-single cannot be used with --via-memory");
        return Ok(1);
    }
    if force_threaded_copy && via_memory {
        println!("copy --threaded-copy cannot be used with --via-memory");
        return Ok(1);
    }
    if force_reflink && via_memory {
        println!("copy --reflink cannot be used with --via-memory");
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && save_config {
        println!(
            "copy verification modes do not support --save; tune copy and verification separately"
        );
        return Ok(1);
    }
    if force_copy_file_range_single && save_config {
        println!("copy --copy-file-range-single does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if force_diff_copy && save_config {
        println!("copy --diff does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if force_reflink && save_config {
        println!("copy --reflink does not support --save; benchmark it with -n 1");
        return Ok(1);
    }
    if (verify_copy || verify_copy_diff) && iterations > 1 {
        println!("copy verification modes require -n 1");
        return Ok(1);
    }
    if force_reflink && iterations > 1 {
        println!("copy --reflink requires -n 1");
        return Ok(1);
    }
    if force_copy_file_range_single && iterations > 1 {
        println!("copy --copy-file-range-single benchmarks the fixed one-call path; use --copy-file-range to optimize the tunable multi-call mode");
        return Ok(1);
    }
    if verify_copy_diff && hash_base.is_some() {
        println!("copy --verify-diff does not use --hash-base");
        return Ok(1);
    }
    if (force_copy_file_range || force_copy_file_range_single)
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        println!("copy --copy-file-range and --copy-file-range-single do not support direct read/write modes");
        return Ok(1);
    }
    if force_reflink
        && (io_mode == common::IOMode::Direct || io_mode_write == common::IOMode::Direct)
    {
        println!("copy --reflink does not support direct read/write modes");
        return Ok(1);
    }

    if mode == "recover" && extra_paths.is_empty() {
        println!("At least one recovery copy is required");
        return Ok(1);
    }
    if mode != "write" && create_size.is_some() {
        println!("--create is only supported for write");
        return Ok(1);
    }
    if mode == "recover" && recover_fast_requested && recover_in_place_all_requested {
        println!("--fast and --in-place-all cannot be used together");
        return Ok(1);
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
    let params_copy_range = config.get_copy_range_params_for_path(context_path);

    let num_threads_pc = params_page_cache.num_threads;
    let qd_pc = params_page_cache.qd;

    let num_threads_direct = params_direct.num_threads;
    let qd_direct = params_direct.qd;

    // We reverse the scaling factor logic here since run_optimizer multiplies by bsf*1024
    let base_block_size_pc = params_page_cache.block_size / (4 * 1024);
    let base_block_size_direct = params_direct.block_size / (256 * 1024);
    let base_block_size_copy_range = params_copy_range.block_size / (256 * 1024);

    let start_params = vec![
        num_threads_pc,
        base_block_size_pc,
        qd_pc as u64,
        num_threads_direct,
        base_block_size_direct,
        qd_direct as u64,
        params_copy_range.num_threads,
        base_block_size_copy_range,
        params_copy_range.qd as u64,
    ];
    let params_steps = vec![1, 4 * 1024, 1, 1, 256 * 1024, 1, 1, 256 * 1024, 1];

    let mode_name = mode;
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
    let optimizer_mask =
        active_optimizer_param_mask(mode, io_mode, io_mode_write, via_memory, copy_strategy);
    let verbose = verbose || mode == "read" || mode == "write";
    if verbose {
        eprintln!("Opening file {} for {}", filename, mode);
    }

    let mut exit_code = 0;
    let hash_base_owned = hash_base.map(|s| s.to_string());
    let extra_paths_owned = extra_paths;

    let mode_callback = |p: &[u64]| {
        if mode == "read" && to_memory {
            let loaded = load_file_to_memory(
                filename,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode,
            )?;
            Ok(loaded.bytes_read)
        } else if mode == "read" || mode == "grep" {
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
            if hash_only || iterations > 1 {
                let manifest = hash_file_blocks(
                    filename,
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
                    filename,
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
                        .unwrap_or(&default_hash_base(filename))
                );
                Ok(manifest.bytes_hashed)
            }
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
            )?;
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
            Ok(report.bytes_hashed)
        } else if mode == "write" {
            write_file(
                filename,
                create_size,
                p[0],
                p[1],
                p[2] as usize,
                p[3],
                p[4],
                p[5] as usize,
                io_mode_write,
            )
        } else if mode == "copy" {
            if let Some(src) = source {
                let resolved_copy = resolve_copy_execution(
                    &config,
                    src,
                    filename,
                    copy_strategy,
                    copy_rewrite_mode,
                    io_mode,
                    io_mode_write,
                )?;
                if verbose {
                    eprintln!(
                        "{}",
                        describe_copy_path(resolved_copy, via_memory, keep_target_size)
                    );
                }
                if verify_copy {
                    let target_hash_base_owned = if persist_verification_hashes {
                        Some(
                            hash_base_owned
                                .clone()
                                .unwrap_or_else(|| default_hash_base(filename)),
                        )
                    } else {
                        None
                    };
                    let report = copy_file_verified_with_options_and_lock(
                        src,
                        filename,
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
                    let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                    let copied = if via_memory {
                        let read_page_cache = config.get_params_for_path("read", false, src);
                        let read_direct = config.get_params_for_path("read", true, src);
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
                        let write_page_cache = config.get_params_for_path("write", false, filename);
                        let write_direct = config.get_params_for_path("write", true, filename);
                        write_buffer(
                            filename,
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
                            filename,
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
                    sync_path(filename)?;
                    guard.ensure_source_unchanged()?;
                    let diff_page_cache = config.get_params_for_path("diff", false, filename);
                    let diff_direct = config.get_params_for_path("diff", true, filename);
                    let diff_res = diff_files(
                        src,
                        filename,
                        diff_page_cache.num_threads,
                        diff_page_cache.block_size,
                        diff_page_cache.qd,
                        diff_direct.num_threads,
                        diff_direct.block_size,
                        diff_direct.qd,
                        io_mode,
                        false,
                    )?;
                    if diff_res != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "copy verify-diff found a mismatch at byte offset {}",
                                diff_res
                            ),
                        ));
                    }
                    guard.ensure_source_unchanged()?;
                    if !quiet {
                        eprintln!("copy verify-diff: success");
                    }
                    Ok(copied)
                } else if via_memory {
                    let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                    let read_page_cache = config.get_params_for_path("read", false, src);
                    let read_direct = config.get_params_for_path("read", true, src);
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
                    let write_page_cache = config.get_params_for_path("write", false, filename);
                    let write_direct = config.get_params_for_path("write", true, filename);
                    let copied = write_buffer(
                        filename,
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
                    let guard = CopyOperationGuard::new(src, filename, !no_lock)?;
                    let copied = if resolved_copy.diff_overwrite && !keep_target_size {
                        let diff_scan = config.get_params_for_path("diff", false, filename);
                        overwrite_changed_chunks_direct(
                            src,
                            filename,
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
                            filename,
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
            } else {
                eprintln!("Copy is missing a destination path.");
                Ok(1)
            }
        } else if mode == "diff" || mode == "dual-read-bench" {
            let s1 = std::fs::metadata(source.unwrap())?.len();
            let s2 = std::fs::metadata(filename)?.len();
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
                let size = std::fs::File::open(filename)?.metadata()?.len();
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

    let best_params = run_optimizer(
        mode_name,
        start_params,
        params_steps,
        optimizer_mask,
        iterations,
        verbose,
        mode_callback,
    )?;

    if mode == "verify" && iterations > 1 {
        let report = verify_file_with_replicas(
            filename,
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
        match (mode, copy_strategy, io_mode) {
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
                        num_threads: best_params[off + 0],
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

fn main() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if is_broken_pipe_panic(info.payload()) {
            return;
        }
        default_hook(info);
    }));

    match std::panic::catch_unwind(try_main) {
        Ok(Ok(code)) if code == 0 => {}
        Ok(Ok(code)) => std::process::exit(code),
        Ok(Err(err)) => {
            if is_broken_pipe_error(&err) {
                std::process::exit(0);
            }
            let _ = writeln!(io::stderr().lock(), "Error: {}", err);
            std::process::exit(1);
        }
        Err(payload) => {
            if is_broken_pipe_panic(payload.as_ref()) {
                std::process::exit(0);
            }
            std::panic::resume_unwind(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        active_optimizer_param_mask, describe_copy_path, should_prefer_cached_diff_overwrite,
        should_prefer_cached_read_direct_write, target_is_similar_size, HeuristicCopyPlan,
        ResolvedCopyExecution,
    };
    use crate::common::{CopyStrategy, IOMode};

    fn heuristic_plan(
        source_cached: bool,
        target_cached: bool,
        source_len: Option<u64>,
        target_len: Option<u64>,
        direct_write_supported: bool,
    ) -> HeuristicCopyPlan {
        if should_prefer_cached_diff_overwrite(source_cached, target_cached, source_len, target_len)
        {
            if direct_write_supported {
                HeuristicCopyPlan::DiffOverwrite
            } else {
                HeuristicCopyPlan::CopyFileRangeSingle
            }
        } else if should_prefer_cached_read_direct_write(source_cached, source_len, target_len) {
            HeuristicCopyPlan::CachedReadDirectWrite
        } else {
            HeuristicCopyPlan::DirectReadDirectWrite
        }
    }

    #[test]
    fn verify_skips_block_size_mutations() {
        assert_eq!(
            active_optimizer_param_mask(
                "verify",
                IOMode::PageCache,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, false, true, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "verify",
                IOMode::Direct,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, true, false, true, false, false, false]
        );
    }

    #[test]
    fn write_only_mutates_write_side_params() {
        assert_eq!(
            active_optimizer_param_mask(
                "write",
                IOMode::Auto,
                IOMode::PageCache,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, true, true, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Auto,
                false,
                CopyStrategy::Threaded
            ),
            vec![true, true, true, true, true, true, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Auto,
                true,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::Direct,
                true,
                CopyStrategy::Threaded
            ),
            vec![false, false, false, false, false, false, false, false, false]
        );
        assert_eq!(
            active_optimizer_param_mask(
                "copy",
                IOMode::PageCache,
                IOMode::PageCache,
                false,
                CopyStrategy::CopyFileRange
            ),
            vec![false, false, false, false, false, false, true, true, true]
        );
    }

    #[test]
    fn cached_read_direct_write_requires_hot_close_sized_target() {
        assert!(should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(1024)
        ));
        assert!(should_prefer_cached_read_direct_write(true, Some(1024), Some(900)));
        assert!(should_prefer_cached_read_direct_write(true, Some(1024), Some(2048)));
        assert!(should_prefer_cached_read_direct_write(true, Some(0), Some(0)));
        assert!(!should_prefer_cached_read_direct_write(
            false,
            Some(1024),
            Some(1024)
        ));
        assert!(!should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            Some(716)
        ));
        assert!(!should_prefer_cached_read_direct_write(
            true,
            Some(1024),
            None
        ));
    }

    #[test]
    fn cached_diff_overwrite_requires_hot_similar_sized_files() {
        assert!(should_prefer_cached_diff_overwrite(
            true,
            true,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            true,
            false,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            false,
            true,
            Some(1024),
            Some(900)
        ));
        assert!(!should_prefer_cached_diff_overwrite(
            true,
            true,
            Some(1024),
            Some(600)
        ));
    }

    #[test]
    fn zero_length_source_is_only_similar_to_zero_length_target() {
        assert!(target_is_similar_size(Some(0), Some(0)));
        assert!(!target_is_similar_size(Some(0), Some(1)));
    }

    #[test]
    fn heuristic_plan_prefers_diff_overwrite_then_copy_file_range_single_fallback() {
        assert_eq!(
            heuristic_plan(true, true, Some(1024), Some(900), true),
            HeuristicCopyPlan::DiffOverwrite
        );
        assert_eq!(
            heuristic_plan(true, true, Some(1024), Some(900), false),
            HeuristicCopyPlan::CopyFileRangeSingle
        );
        assert_eq!(
            heuristic_plan(true, false, Some(1024), Some(900), true),
            HeuristicCopyPlan::CachedReadDirectWrite
        );
        assert_eq!(
            heuristic_plan(false, false, Some(1024), Some(900), true),
            HeuristicCopyPlan::DirectReadDirectWrite
        );
    }

    #[test]
    fn describe_copy_path_reports_diff_overwrite_details() {
        let path = describe_copy_path(
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: IOMode::PageCache,
                io_mode_write: IOMode::Direct,
                diff_overwrite: true,
                full_rewrite: false,
                path_label: "auto diff-overwrite",
            },
            false,
            false,
        );
        assert!(path.contains("strategy=auto diff-overwrite"));
        assert!(path.contains("read=page-cache"));
        assert!(path.contains("write=direct"));
        assert!(path.contains("delta=changed-chunks"));
    }

    #[test]
    fn describe_copy_path_reports_via_memory_path() {
        let path = describe_copy_path(
            ResolvedCopyExecution {
                copy_strategy: CopyStrategy::Threaded,
                io_mode_read: IOMode::PageCache,
                io_mode_write: IOMode::Direct,
                diff_overwrite: false,
                full_rewrite: false,
                path_label: "auto cached-read direct-write",
            },
            true,
            false,
        );
        assert_eq!(path, "copy path: via-memory [read=page-cache, write=direct]");
    }
}
