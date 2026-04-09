use crate::common::AlignedBuffer;
use crate::config::load_config;
use crate::differ::diff_files_window;
use crate::reader::{
    grep_match_offsets_for_mode, load_file_to_memory_for_mode, map_file_blocks_for_mode, BufReader,
    LoadedFile,
};
use crate::writer::{write_generated_file, BufWriter, GeneratedWritePattern};
use fro::{hash_file, read_file_with_mode, visit_blocks_with_mode, HashAlgorithm, IOMode};
use iou::sqe::SpliceFlags;
use iou::IoUring;
use memchr::{memchr_iter, memmem::Finder};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::{CStr, CString};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};

mod base64;
mod cat;
mod cmp;
mod du;
mod encrypt;
mod fgrep;
mod find;
mod hash;
mod head;
mod io_helpers;
mod mv;
mod pv;
mod rm;
mod shred;
mod sort;
mod tac;
mod tail;
mod tar;
mod wc;
mod work_queue;

pub(crate) use self::io_helpers::*;
pub(crate) use self::work_queue::*;
pub(crate) use base64::{
    parse_base64_decode_kernel, parse_base64_encode_kernel, Base64DecodeKernel, Base64EncodeKernel,
};
const FRO_VERSION: &str = env!("CARGO_PKG_VERSION");
const STREAM_WINDOW_BLOCK_SIZE: usize = 1 << 20;

pub fn is_coreutils_command(name: &str) -> bool {
    matches!(
        name,
        "cat"
            | "base64"
            | "encrypt"
            | "decrypt"
            | "cmp"
            | "dd"
            | "fgrep"
            | "find"
            | "du"
            | "tac"
            | "wc"
            | "cksum"
            | "b3sum"
            | "b2sum"
            | "md5sum"
            | "sha224sum"
            | "sha256sum"
            | "sha384sum"
            | "sha512sum"
            | "head"
            | "tail"
            | "rm"
            | "mv"
            | "tar"
            | "pv"
            | "shred"
            | "sort"
    )
}

pub fn rewrite_alias_args(args: Vec<String>) -> Vec<String> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return args;
    };
    let Some(mode) = (match invoked.as_str() {
        "cp" => Some("copy"),
        _ => None,
    }) else {
        return args;
    };

    let mut rewritten = Vec::with_capacity(args.len() + 1);
    rewritten.push(args[0].clone());
    rewritten.push(mode.to_string());
    if invoked == "cp" {
        rewritten.push("--cp-compat".to_string());
        let tail = args.into_iter().skip(1).collect::<Vec<_>>();
        rewritten.extend(rewrite_cp_command_args(&tail));
    } else {
        rewritten.extend(args.into_iter().skip(1));
    }
    rewritten
}

pub fn rewrite_subcommand_alias(args: Vec<String>) -> Vec<String> {
    if args.get(1).map(String::as_str) == Some("cp") {
        let mut rewritten = Vec::with_capacity(args.len() + 1);
        rewritten.push(args[0].clone());
        rewritten.push("copy".to_string());
        rewritten.push("--cp-compat".to_string());
        rewritten.extend(rewrite_cp_command_args(&args[2..]));
        return rewritten;
    }
    args
}

fn multicall_help_command(name: &str) -> Option<&str> {
    match name {
        "cp" => Some("copy"),
        other if is_coreutils_command(other) => Some(other),
        _ => None,
    }
}

fn is_multicall_help_flag(arg: Option<&String>) -> bool {
    matches!(arg.map(String::as_str), Some("-h" | "--help"))
}

fn is_multicall_version_flag(arg: Option<&String>) -> bool {
    matches!(arg.map(String::as_str), Some("--version"))
}

fn rewrite_cp_command_args(command_args: &[String]) -> Vec<String> {
    let mut rewritten = Vec::with_capacity(command_args.len());
    let mut end_flags = false;
    let mut index = 0;
    while index < command_args.len() {
        let arg = &command_args[index];
        if end_flags || !arg.starts_with('-') || arg == "-" {
            rewritten.push(arg.clone());
            index += 1;
            continue;
        }
        if arg == "--" {
            end_flags = true;
            rewritten.push(arg.clone());
            index += 1;
            continue;
        }
        match arg.as_str() {
            "-t" | "--target-directory" => {
                if let Some(value) = command_args.get(index + 1) {
                    rewritten.push("--cp-target-directory".to_string());
                    rewritten.push(value.clone());
                    index += 2;
                    continue;
                }
                rewritten.push(arg.clone());
            }
            long if long.starts_with("--target-directory=") => {
                rewritten.push("--cp-target-directory".to_string());
                rewritten.push(long["--target-directory=".len()..].to_string());
            }
            "-n" | "--no-clobber" => rewritten.push("--cp-no-clobber".to_string()),
            "-T" | "--no-target-directory" => {
                rewritten.push("--cp-no-target-directory".to_string())
            }
            "-u" | "--update" => rewritten.push("--cp-update".to_string()),
            "-p" | "--preserve" => rewritten.push("--cp-preserve".to_string()),
            "-P" | "--no-dereference" => rewritten.push("--cp-no-dereference".to_string()),
            long if cp_preserve_attr_list_supported(long) => {
                rewritten.push("--cp-preserve".to_string())
            }
            short if short.starts_with('-') && !short.starts_with("--") && short.len() > 2 => {
                if let Some((expanded, consumed_next)) =
                    rewrite_cp_short_flag_cluster(short, command_args.get(index + 1))
                {
                    rewritten.extend(expanded);
                    index += 1 + usize::from(consumed_next);
                    continue;
                } else {
                    rewritten.push(arg.clone());
                }
            }
            _ => rewritten.push(arg.clone()),
        }
        index += 1;
    }
    rewritten
}

fn cp_preserve_attr_list_supported(arg: &str) -> bool {
    let Some(attrs) = arg.strip_prefix("--preserve=") else {
        return false;
    };
    if attrs.is_empty() {
        return false;
    }
    let mut saw_timestamps = false;
    for attr in attrs.split(',') {
        match attr {
            "timestamps" => saw_timestamps = true,
            "mode" | "ownership" => {}
            _ => return false,
        }
    }
    saw_timestamps
}

fn rewrite_cp_short_flag_cluster(
    arg: &str,
    next_arg: Option<&String>,
) -> Option<(Vec<String>, bool)> {
    let mut rewritten = Vec::with_capacity(arg.len() - 1);
    let mut chars = arg[1..].chars().peekable();
    let mut consumed_next = false;
    while let Some(ch) = chars.next() {
        match ch {
            'n' => rewritten.push("--cp-no-clobber".to_string()),
            'T' => rewritten.push("--cp-no-target-directory".to_string()),
            'u' => rewritten.push("--cp-update".to_string()),
            'p' => rewritten.push("--cp-preserve".to_string()),
            'P' => rewritten.push("--cp-no-dereference".to_string()),
            'r' => rewritten.push("-r".to_string()),
            'R' => rewritten.push("-R".to_string()),
            'v' => rewritten.push("-v".to_string()),
            't' => {
                rewritten.push("--cp-target-directory".to_string());
                let remainder = chars.collect::<String>();
                if !remainder.is_empty() {
                    rewritten.push(remainder);
                } else if let Some(value) = next_arg {
                    rewritten.push(value.clone());
                    consumed_next = true;
                } else {
                    return None;
                }
                break;
            }
            _ => return None,
        }
    }
    Some((rewritten, consumed_next))
}

pub fn try_run_multicall(args: &[String]) -> io::Result<Option<i32>> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return Ok(None);
    };
    if let Some(help_name) = multicall_help_command(&invoked) {
        if is_multicall_help_flag(args.get(1)) {
            crate::main_app::print_direct_command_help(&invoked, help_name);
            return Ok(Some(0));
        }
        if is_multicall_version_flag(args.get(1)) {
            print_coreutils_version(&invoked);
            return Ok(Some(0));
        }
    }
    run_named_command(&invoked, args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_cp_supports_timestamp_preserve_attr_lists() {
        let rewritten = rewrite_cp_command_args(&[
            "--preserve=timestamps".to_string(),
            "--preserve=mode,timestamps".to_string(),
            "--preserve=timestamps,ownership".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "--cp-preserve".to_string(),
                "--cp-preserve".to_string(),
                "--cp-preserve".to_string(),
                "src".to_string(),
                "dst".to_string(),
            ]
        );
    }

    #[test]
    fn rewrite_cp_leaves_unsupported_preserve_attr_lists_untouched() {
        let rewritten = rewrite_cp_command_args(&[
            "--preserve=mode".to_string(),
            "--preserve=context".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "--preserve=mode".to_string(),
                "--preserve=context".to_string(),
                "src".to_string(),
                "dst".to_string(),
            ]
        );
    }
}

pub fn try_run_subcommand(
    program: &str,
    command: &str,
    command_args: &[String],
) -> io::Result<Option<i32>> {
    if !is_coreutils_command(command) {
        return Ok(None);
    }
    let mut args = Vec::with_capacity(command_args.len() + 1);
    args.push(format!("{program} {command}"));
    args.extend(command_args.iter().cloned());
    run_named_command(command, &args)
}

pub(crate) fn bench_base64_encode(iterations: u64, kernel: Base64EncodeKernel) -> io::Result<()> {
    base64::bench_base64_encode(iterations, kernel)
}

pub(crate) fn bench_base64_decode(iterations: u64, kernel: Base64DecodeKernel) -> io::Result<()> {
    base64::bench_base64_decode(iterations, kernel)
}

pub(crate) fn bench_base64_decode_detect_fallback(
    iterations: u64,
    kernel: Base64DecodeKernel,
) -> io::Result<()> {
    base64::bench_base64_decode_detect_fallback(iterations, kernel)
}

pub(crate) fn bench_base64_wrapped_encode(iterations: u64, wrap_cols: usize) -> io::Result<()> {
    base64::bench_base64_wrapped_encode(iterations, wrap_cols)
}

pub(crate) fn bench_base64_wrapped_decode(iterations: u64, ignore_garbage: bool) -> io::Result<()> {
    base64::bench_base64_wrapped_decode(iterations, ignore_garbage)
}

fn run_named_command(invoked: &str, args: &[String]) -> io::Result<Option<i32>> {
    if !is_coreutils_command(invoked) {
        return Ok(None);
    }
    if is_multicall_help_flag(args.get(1)) {
        crate::main_app::print_direct_command_help(invoked, invoked);
        return Ok(Some(0));
    }
    if is_multicall_version_flag(args.get(1)) {
        print_coreutils_version(invoked);
        return Ok(Some(0));
    }
    let code = match invoked {
        "cat" => {
            cat::run_cat(args)?;
            0
        }
        "base64" => base64::run_base64(args)?,
        "encrypt" => encrypt::run_encrypt(args)?,
        "decrypt" => encrypt::run_decrypt(args)?,
        "cmp" => cmp::run_cmp(args)?,
        "dd" => {
            fro::dd_tool::run_dd(args)?;
            0
        }
        "fgrep" => fgrep::run_fgrep(args)?,
        "find" => find::run_find(args)?,
        "du" => du::run_du(args)?,
        "tac" => {
            tac::run_tac(args)?;
            0
        }
        "wc" => wc::run_wc(args)?,
        "cksum" => hash::cksum::run_cksum(args)?,
        "b3sum" => hash::run_hash_sum(args, HashAlgorithm::Blake3)?,
        "b2sum" => hash::run_hash_sum(args, HashAlgorithm::Blake2b512)?,
        "md5sum" => hash::run_hash_sum(args, HashAlgorithm::Md5)?,
        "sha224sum" => hash::run_hash_sum(args, HashAlgorithm::Sha224)?,
        "sha256sum" => hash::run_hash_sum(args, HashAlgorithm::Sha256)?,
        "sha384sum" => hash::run_hash_sum(args, HashAlgorithm::Sha384)?,
        "sha512sum" => hash::run_hash_sum(args, HashAlgorithm::Sha512)?,
        "head" => {
            head::run_head(args)?;
            0
        }
        "tail" => {
            tail::run_tail(args)?;
            0
        }
        "pv" => {
            pv::run_pv(args)?;
            0
        }
        "rm" => rm::run_rm(args)?,
        "mv" => mv::run_mv(args)?,
        "tar" => tar::run_tar(args)?,
        "shred" => shred::run_shred(args)?,
        "sort" => sort::run_sort(args)?,
        _ => return Ok(None),
    };
    Ok(Some(code))
}
