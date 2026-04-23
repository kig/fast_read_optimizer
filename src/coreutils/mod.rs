use crate::common::AlignedBuffer;
use crate::config::load_config;
use crate::differ::diff_files_window;
use crate::reader::{
    grep_match_offsets_for_mode, load_file_to_memory_for_mode, map_file_blocks_for_mode, BufReader,
    LoadedFile,
};
use crate::writer::{write_generated_file, BufWriter, GeneratedWritePattern};
use fro::{hash_file, read_file_with_mode, visit_blocks_with_mode, HashAlgorithm, IOMode};
use fro::uring::SpliceFlags;
use fro::uring::IoUring;
use memchr::{memchr_iter, memmem::Finder};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::ffi::{CStr, CString};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};

mod base64;
mod cat;
mod cmp;
mod du;
mod encrypt;
mod fgrep;
mod find;
mod gzip;
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
const NO_FALLBACK_FLAG: &str = "--no-fallback";
const FALLBACK_LOG_ENV: &str = "FRO_LOG_FALLBACKS";
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
            | "gzip"
            | "gunzip"
            | "zcat"
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
        "cp" => Some("cp"),
        other if is_coreutils_command(other) => Some(other),
        _ => None,
    }
}

fn is_multicall_help_flag(arg: Option<&String>) -> bool {
    matches!(arg.map(String::as_str), Some("-h" | "--help"))
}

fn is_multicall_version_flag(invoked: &str, arg: Option<&String>) -> bool {
    matches!(arg.map(String::as_str), Some("--version"))
        || (invoked == "cmp" && matches!(arg.map(String::as_str), Some("-v")))
}

fn multicall_short_help_is_real_flag(invoked: &str, arg: Option<&String>) -> bool {
    invoked == "sort" && matches!(arg.map(String::as_str), Some("-h"))
}

pub(crate) fn args_request_no_fallback(args: &[String]) -> bool {
    args.iter().any(|arg| arg == NO_FALLBACK_FLAG)
}

fn strip_wrapper_only_args(args: &[String]) -> Vec<String> {
    args.iter()
        .filter(|arg| arg.as_str() != NO_FALLBACK_FLAG)
        .cloned()
        .collect()
}

fn fallback_logging_enabled_from_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        !normalized.is_empty()
            && normalized != "0"
            && normalized != "false"
            && normalized != "no"
            && normalized != "off"
    })
}

fn fallback_logging_enabled() -> bool {
    fallback_logging_enabled_from_value(env::var(FALLBACK_LOG_ENV).ok().as_deref())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExternalFallbackCommand {
    Ripgrep,
    UutilsCoreutils,
    SystemCommand,
}

impl ExternalFallbackCommand {
    fn build(self, invoked: &str, args: &[String]) -> (String, Vec<String>) {
        let forwarded = args.iter().skip(1).cloned().collect::<Vec<_>>();
        match self {
            ExternalFallbackCommand::Ripgrep => {
                let mut command_args = vec![
                    "--fixed-strings".to_string(),
                    "--color".to_string(),
                    "never".to_string(),
                    "--no-config".to_string(),
                ];
                command_args.extend(forwarded);
                ("rg".to_string(), command_args)
            }
            ExternalFallbackCommand::UutilsCoreutils => {
                let mut command_args = vec![invoked.to_string()];
                command_args.extend(forwarded);
                ("coreutils".to_string(), command_args)
            }
            ExternalFallbackCommand::SystemCommand => {
                (resolve_system_command_path(invoked), forwarded)
            }
        }
    }
}

fn resolve_system_command_path(invoked: &str) -> String {
    for prefix in ["/usr/bin", "/bin", "/usr/sbin", "/sbin"] {
        let candidate = Path::new(prefix).join(invoked);
        if candidate.is_file() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    invoked.to_string()
}

fn fallback_candidates(invoked: &str) -> &'static [ExternalFallbackCommand] {
    match invoked {
        "fgrep" => &[
            ExternalFallbackCommand::Ripgrep,
            ExternalFallbackCommand::UutilsCoreutils,
            ExternalFallbackCommand::SystemCommand,
        ],
        "cat" | "base64" | "cmp" | "dd" | "du" | "find" | "gzip" | "gunzip" | "zcat" | "tac"
        | "wc" | "cksum" | "b2sum" | "md5sum" | "sha224sum" | "sha256sum" | "sha384sum"
        | "sha512sum" | "head" | "tail" | "rm" | "mv" | "tar" | "shred" | "sort" | "cp" => &[
            ExternalFallbackCommand::UutilsCoreutils,
            ExternalFallbackCommand::SystemCommand,
        ],
        _ => &[],
    }
}

fn should_try_external_fallback(err: &io::Error) -> bool {
    if err.kind() != io::ErrorKind::InvalidInput {
        return false;
    }
    let lowered = err.to_string().to_ascii_lowercase();
    lowered.contains("unsupported")
        || lowered.contains("unknown flag")
        || lowered.contains("unknown option")
        || lowered.contains("invalid option")
}

fn stderr_indicates_option_parse_failure(stderr: &[u8]) -> bool {
    let lowered = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    lowered.contains("unrecognized option")
        || lowered.contains("unknown option")
        || lowered.contains("unsupported option")
        || lowered.contains("invalid option")
        || lowered.contains("unexpected argument")
        || lowered.contains("found argument")
        || lowered.contains("wasn't expected")
        || lowered.contains("unknown subcommand")
        || lowered.contains("unrecognized subcommand")
}

fn run_external_fallback(
    invoked: &str,
    args: &[String],
    failure_reason: Option<&str>,
) -> io::Result<Option<i32>> {
    for candidate in fallback_candidates(invoked) {
        let (program, command_args) = candidate.build(invoked, args);
        let child = match Command::new(&program)
            .args(&command_args)
            .stdin(Stdio::from(fro::command_io::stdin_file()?))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        let output = child.wait_with_output()?;
        if matches!(output.status.code(), Some(1 | 2))
            && stderr_indicates_option_parse_failure(&output.stderr)
        {
            continue;
        }
        if fallback_logging_enabled() {
            match failure_reason {
                Some(reason) => fro::cio_eprintln!(
                    "fro: fallback {} -> {} {:?} ({})",
                    invoked,
                    program,
                    command_args,
                    reason
                ),
                None => fro::cio_eprintln!(
                    "fro: fallback {} -> {} {:?}",
                    invoked,
                    program,
                    command_args
                ),
            }
        }
        fro::command_io::stdout_file()?.write_all(&output.stdout)?;
        fro::command_io::stderr_file()?.write_all(&output.stderr)?;
        return Ok(Some(output.status.code().unwrap_or(1)));
    }
    Ok(None)
}

pub(crate) fn try_external_command_fallback(
    invoked: &str,
    args: &[String],
) -> io::Result<Option<i32>> {
    if args_request_no_fallback(args) {
        return Ok(None);
    }
    let sanitized_args = strip_wrapper_only_args(args);
    run_external_fallback(invoked, &sanitized_args, None)
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
            "-p" | "--preserve" => {
                rewritten.push("--cp-preserve-mode".to_string());
                rewritten.push("--cp-preserve-timestamps".to_string());
            }
            "-P" | "--no-dereference" => rewritten.push("--cp-no-dereference".to_string()),
            "-L" | "--dereference" => rewritten.push("--cp-dereference".to_string()),
            _ => {
                if let Some((preserve_mode, preserve_timestamps)) = cp_preserve_attr_list_flags(arg)
                {
                    if preserve_mode {
                        rewritten.push("--cp-preserve-mode".to_string());
                    }
                    if preserve_timestamps {
                        rewritten.push("--cp-preserve-timestamps".to_string());
                    }
                } else if arg.starts_with('-') && !arg.starts_with("--") && arg.len() > 2 {
                    if let Some((expanded, consumed_next)) =
                        rewrite_cp_short_flag_cluster(arg, command_args.get(index + 1))
                    {
                        rewritten.extend(expanded);
                        index += 1 + usize::from(consumed_next);
                        continue;
                    } else {
                        rewritten.push(arg.clone());
                    }
                } else {
                    rewritten.push(arg.clone());
                }
            }
        }
        index += 1;
    }
    rewritten
}

fn cp_preserve_attr_list_flags(arg: &str) -> Option<(bool, bool)> {
    let Some(attrs) = arg.strip_prefix("--preserve=") else {
        return None;
    };
    if attrs.is_empty() {
        return None;
    }
    if attrs == "all" {
        return Some((true, true));
    }
    let mut preserve_mode = false;
    let mut preserve_timestamps = false;
    let mut saw_timestamps = false;
    for attr in attrs.split(',') {
        match attr {
            "timestamps" => {
                preserve_timestamps = true;
                saw_timestamps = true;
            }
            "mode" => preserve_mode = true,
            "ownership" => {}
            _ => return None,
        }
    }
    if preserve_mode || saw_timestamps {
        Some((preserve_mode, preserve_timestamps))
    } else {
        None
    }
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
            'a' => rewritten.push("-a".to_string()),
            'p' => {
                rewritten.push("--cp-preserve-mode".to_string());
                rewritten.push("--cp-preserve-timestamps".to_string());
            }
            'P' => rewritten.push("--cp-no-dereference".to_string()),
            'L' => rewritten.push("--cp-dereference".to_string()),
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
    let allow_fallback = !args_request_no_fallback(args);
    let sanitized_args = strip_wrapper_only_args(args);
    if let Some(help_name) = multicall_help_command(&invoked) {
        if is_multicall_help_flag(sanitized_args.get(1)) {
            crate::main_app::print_direct_command_help(&invoked, help_name);
            return Ok(Some(0));
        }
        if is_multicall_version_flag(&invoked, sanitized_args.get(1)) {
            print_coreutils_version(&invoked);
            return Ok(Some(0));
        }
    }
    run_named_command(&invoked, &sanitized_args, allow_fallback)
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
            "--preserve=all".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "--cp-preserve-timestamps".to_string(),
                "--cp-preserve-mode".to_string(),
                "--cp-preserve-timestamps".to_string(),
                "--cp-preserve-timestamps".to_string(),
                "--cp-preserve-mode".to_string(),
                "--cp-preserve-timestamps".to_string(),
                "src".to_string(),
                "dst".to_string(),
            ]
        );
    }

    #[test]
    fn rewrite_cp_supports_mode_only_and_dereference_flags() {
        let rewritten = rewrite_cp_command_args(&[
            "--preserve=mode".to_string(),
            "-aL".to_string(),
            "-L".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "--cp-preserve-mode".to_string(),
                "-a".to_string(),
                "--cp-dereference".to_string(),
                "--cp-dereference".to_string(),
                "src".to_string(),
                "dst".to_string(),
            ]
        );
    }

    #[test]
    fn rewrite_cp_leaves_unsupported_preserve_attr_lists_untouched() {
        let rewritten = rewrite_cp_command_args(&[
            "--preserve=context".to_string(),
            "--preserve=links".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]);
        assert_eq!(
            rewritten,
            vec![
                "--preserve=context".to_string(),
                "--preserve=links".to_string(),
                "src".to_string(),
                "dst".to_string(),
            ]
        );
    }

    #[test]
    fn unsupported_invalid_input_errors_trigger_external_fallback() {
        let err = io::Error::new(io::ErrorKind::InvalidInput, "unsupported sort flag --debug");
        assert!(should_try_external_fallback(&err));
    }

    #[test]
    fn non_unsupported_invalid_input_errors_do_not_trigger_external_fallback() {
        let err = io::Error::new(io::ErrorKind::InvalidInput, "missing argument for -n");
        assert!(!should_try_external_fallback(&err));
    }

    #[test]
    fn fgrep_prefers_rg_then_coreutils_then_system() {
        assert_eq!(
            fallback_candidates("fgrep"),
            &[
                ExternalFallbackCommand::Ripgrep,
                ExternalFallbackCommand::UutilsCoreutils,
                ExternalFallbackCommand::SystemCommand,
            ]
        );
    }

    #[test]
    fn strip_wrapper_only_args_removes_no_fallback() {
        assert_eq!(
            strip_wrapper_only_args(&[
                "fro sort".to_string(),
                "--no-fallback".to_string(),
                "-k".to_string(),
                "1,1".to_string(),
            ]),
            vec!["fro sort".to_string(), "-k".to_string(), "1,1".to_string()]
        );
    }

    #[test]
    fn fallback_logging_truthy_values_are_opt_in() {
        assert!(fallback_logging_enabled_from_value(Some("1")));
        assert!(fallback_logging_enabled_from_value(Some("yes")));
        assert!(!fallback_logging_enabled_from_value(Some("0")));
        assert!(!fallback_logging_enabled_from_value(Some("false")));
        assert!(!fallback_logging_enabled_from_value(None));
    }

    #[test]
    fn resolve_system_command_path_leaves_unknown_names_unchanged() {
        assert_eq!(
            resolve_system_command_path("__fro_missing_coreutil__"),
            "__fro_missing_coreutil__"
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
    let allow_fallback = !args_request_no_fallback(command_args);
    let sanitized_command_args = strip_wrapper_only_args(command_args);
    let mut args = Vec::with_capacity(sanitized_command_args.len() + 1);
    args.push(format!("{program} {command}"));
    args.extend(sanitized_command_args);
    run_named_command(command, &args, allow_fallback)
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

fn run_named_command(
    invoked: &str,
    args: &[String],
    allow_fallback: bool,
) -> io::Result<Option<i32>> {
    if !is_coreutils_command(invoked) {
        return Ok(None);
    }
    if is_multicall_help_flag(args.get(1))
        && !multicall_short_help_is_real_flag(invoked, args.get(1))
    {
        crate::main_app::print_direct_command_help(invoked, invoked);
        return Ok(Some(0));
    }
    if is_multicall_version_flag(invoked, args.get(1)) {
        print_coreutils_version(invoked);
        return Ok(Some(0));
    }
    let result = match invoked {
        "cat" => {
            cat::run_cat(args)?;
            Ok(0)
        }
        "base64" => base64::run_base64(args),
        "encrypt" => encrypt::run_encrypt(args),
        "decrypt" => encrypt::run_decrypt(args),
        "cmp" => cmp::run_cmp(args),
        "dd" => {
            fro::dd_tool::run_dd(args)?;
            Ok(0)
        }
        "fgrep" => fgrep::run_fgrep(args),
        "find" => find::run_find(args),
        "du" => du::run_du(args),
        "gzip" | "gunzip" | "zcat" => gzip::run_gzip(invoked, args),
        "tac" => {
            tac::run_tac(args)?;
            Ok(0)
        }
        "wc" => wc::run_wc(args),
        "cksum" => hash::cksum::run_cksum(args),
        "b3sum" => hash::run_hash_sum(args, HashAlgorithm::Blake3),
        "b2sum" => hash::run_hash_sum(args, HashAlgorithm::Blake2b512),
        "md5sum" => hash::run_hash_sum(args, HashAlgorithm::Md5),
        "sha224sum" => hash::run_hash_sum(args, HashAlgorithm::Sha224),
        "sha256sum" => hash::run_hash_sum(args, HashAlgorithm::Sha256),
        "sha384sum" => hash::run_hash_sum(args, HashAlgorithm::Sha384),
        "sha512sum" => hash::run_hash_sum(args, HashAlgorithm::Sha512),
        "head" => {
            head::run_head(args)?;
            Ok(0)
        }
        "tail" => {
            tail::run_tail(args)?;
            Ok(0)
        }
        "pv" => {
            pv::run_pv(args)?;
            Ok(0)
        }
        "rm" => rm::run_rm(args),
        "mv" => mv::run_mv(args),
        "tar" => tar::run_tar(args),
        "shred" => shred::run_shred(args),
        "sort" => sort::run_sort(args),
        _ => return Ok(None),
    };
    match result {
        Ok(code) => Ok(Some(code)),
        Err(err) if allow_fallback && should_try_external_fallback(&err) => {
            if let Some(code) = run_external_fallback(invoked, args, Some(&err.to_string()))? {
                Ok(Some(code))
            } else {
                Err(err)
            }
        }
        Err(err) => Err(err),
    }
}
