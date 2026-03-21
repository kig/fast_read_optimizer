use crate::config::load_config;
use crate::differ::diff_files;
use crate::reader::{
    grep_match_offsets_for_mode, load_file_to_memory_for_mode, map_file_blocks_for_mode, LoadedFile,
};
use crate::writer::{write_generated_file, GeneratedWritePattern};
use fro::{hash_file, read_file_with_mode, visit_blocks_with_mode, HashAlgorithm, IOMode};
use memchr::memchr_iter;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::mpsc;

const DEFAULT_SHRED_PASSES: usize = 1;
pub fn is_coreutils_command(name: &str) -> bool {
    matches!(
        name,
        "cat"
            | "cmp"
            | "fgrep"
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
            | "shred"
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
    rewritten.extend(args.into_iter().skip(1));
    rewritten
}

pub fn rewrite_subcommand_alias(args: Vec<String>) -> Vec<String> {
    if args.get(1).map(String::as_str) == Some("cp") {
        let mut rewritten = args;
        rewritten[1] = "copy".to_string();
        return rewritten;
    }
    args
}

pub fn try_run_multicall(args: &[String]) -> io::Result<Option<i32>> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return Ok(None);
    };
    run_named_command(&invoked, args)
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

fn run_named_command(invoked: &str, args: &[String]) -> io::Result<Option<i32>> {
    let code = match invoked {
        "cat" => {
            run_cat(args)?;
            0
        }
        "cmp" => run_cmp(args)?,
        "fgrep" => run_fgrep(args)?,
        "tac" => {
            run_tac(args)?;
            0
        }
        "wc" => {
            run_wc(args)?;
            0
        }
        "cksum" => {
            run_cksum(args)?;
            0
        }
        "b3sum" => {
            run_hash_sum(args, HashAlgorithm::Blake3)?;
            0
        }
        "b2sum" => {
            run_hash_sum(args, HashAlgorithm::Blake2b512)?;
            0
        }
        "md5sum" => {
            run_hash_sum(args, HashAlgorithm::Md5)?;
            0
        }
        "sha224sum" => {
            run_hash_sum(args, HashAlgorithm::Sha224)?;
            0
        }
        "sha256sum" => {
            run_hash_sum(args, HashAlgorithm::Sha256)?;
            0
        }
        "sha384sum" => {
            run_hash_sum(args, HashAlgorithm::Sha384)?;
            0
        }
        "sha512sum" => {
            run_hash_sum(args, HashAlgorithm::Sha512)?;
            0
        }
        "shred" => {
            run_shred(args)?;
            0
        }
        _ => return Ok(None),
    };
    Ok(Some(code))
}

fn invoked_name(program: &str) -> Option<String> {
    Path::new(program)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn parse_io_mode(args: &[String]) -> io::Result<(IOMode, Vec<String>)> {
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    for arg in args {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
    }
    Ok((io_mode, files))
}

fn ensure_files(program: &str, files: Vec<String>, usage: &str) -> io::Result<Vec<String>> {
    if files.is_empty() {
        eprintln!("Usage: {} {}", program, usage);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing file operand",
        ));
    }
    Ok(files)
}

fn internal_io_mode(io_mode: IOMode) -> crate::common::IOMode {
    match io_mode {
        IOMode::Auto => crate::common::IOMode::Auto,
        IOMode::Direct => crate::common::IOMode::Direct,
        IOMode::PageCache => crate::common::IOMode::PageCache,
    }
}

fn load_file_bytes(path: &str, io_mode: IOMode, mode: &str) -> io::Result<LoadedFile> {
    let config = load_config(None);
    load_file_to_memory_for_mode(&config, mode, path, internal_io_mode(io_mode))
}

fn count_newlines_up_to(
    path: &str,
    io_mode: IOMode,
    mode: &str,
    end_offset: u64,
) -> io::Result<u64> {
    let data = load_file_bytes(path, io_mode, mode)?;
    let bytes = data.data.as_slice();
    let end = usize::try_from(end_offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?
        .min(bytes.len());
    Ok(memchr_iter(b'\n', &bytes[..end]).count() as u64)
}

fn write_matching_lines<W: Write>(
    out: &mut W,
    file: &str,
    data: &[u8],
    matches: &[u64],
    multi_file: bool,
    print_line_numbers: bool,
) -> io::Result<()> {
    let bytes = data;
    let mut next_match = 0usize;
    let mut line_start = 0usize;
    let mut line_no = 1_u64;
    while line_start < bytes.len() {
        let rel_end = bytes[line_start..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|pos| pos + 1)
            .unwrap_or(bytes.len() - line_start);
        let line_end = line_start + rel_end;
        let mut matched = false;
        while next_match < matches.len() && matches[next_match] < line_end as u64 {
            if matches[next_match] >= line_start as u64 {
                matched = true;
            }
            next_match += 1;
        }
        if matched {
            if multi_file {
                write!(out, "{}:", file)?;
            }
            if print_line_numbers {
                write!(out, "{}:", line_no)?;
            }
            out.write_all(&bytes[line_start..line_end])?;
        }
        line_start = line_end;
        line_no += 1;
    }
    Ok(())
}

fn visit_ordered_blocks<F>(path: &str, io_mode: IOMode, mut on_block: F) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>)>();
    let sender = tx.clone();
    let visit_result = visit_blocks_with_mode(path, io_mode, move |block_index, data| {
        sender
            .send((block_index, data.to_vec()))
            .map_err(|_| io::Error::other("failed to queue ordered block"))
    });
    drop(tx);

    let mut next_block = 0usize;
    let mut pending = BTreeMap::<usize, Vec<u8>>::new();
    while let Ok((block_index, data)) = rx.recv() {
        pending.insert(block_index, data);
        while let Some(block) = pending.remove(&next_block) {
            on_block(&block)?;
            next_block += 1;
        }
    }
    if !pending.is_empty() {
        return Err(io::Error::other(
            "missing block data while finalizing ordered visitor",
        ));
    }
    visit_result?;
    Ok(())
}

fn run_cat(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for file in files {
        let data = load_file_bytes(&file, io_mode, "read")?;
        out.write_all(data.data.as_slice())?;
    }
    out.flush()
}

fn run_cmp(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file1> <file2>",
    )?;
    if files.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cmp requires exactly two file operands",
        ));
    }

    let config = load_config(None);
    let diff_page_cache = config.get_params_for_path("diff", false, &files[0]);
    let diff_direct = config.get_params_for_path("diff", true, &files[0]);
    let mismatch = diff_files(
        &files[0],
        &files[1],
        diff_page_cache.num_threads,
        diff_page_cache.block_size,
        diff_page_cache.qd,
        diff_direct.num_threads,
        diff_direct.block_size,
        diff_direct.qd,
        internal_io_mode(io_mode),
        false,
        false,
    )?;
    if mismatch != 0 {
        let index = mismatch as usize - 1;
        let line = 1 + count_newlines_up_to(&files[0], io_mode, "read", mismatch - 1)?;
        println!(
            "{} {} differ: byte {}, line {}",
            files[0],
            files[1],
            index + 1,
            line
        );
        return Ok(1);
    }

    let first_len = fs::metadata(&files[0])?.len();
    let second_len = fs::metadata(&files[1])?.len();
    let shared_len = first_len.min(second_len);
    if first_len != second_len {
        let eof_file = if first_len < second_len {
            &files[0]
        } else {
            &files[1]
        };
        let line = count_newlines_up_to(eof_file, io_mode, "read", shared_len)?;
        eprintln!(
            "cmp: EOF on {} after byte {}, line {}",
            eof_file, shared_len, line
        );
        return Ok(1);
    }

    Ok(0)
}

fn run_fgrep(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut io_mode = IOMode::Auto;
    let mut print_line_numbers = false;
    let mut pattern = None::<String>;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-n" => print_line_numbers = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other if pattern.is_none() => pattern = Some(other.to_string()),
            other => files.push(other.to_string()),
        }
    }
    let pattern = pattern.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep requires a search pattern",
        )
    })?;
    let files = ensure_files(
        program,
        files,
        "[-n] [--auto|--no-direct|--direct] <pattern> <file> [file ...]",
    )?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut matched_any = false;
    let multi_file = files.len() > 1;
    let config = load_config(None);
    for file in files {
        let (matches, _) = grep_match_offsets_for_mode(
            &config,
            "grep",
            &file,
            internal_io_mode(io_mode),
            pattern.as_bytes(),
        )?;
        if matches.is_empty() {
            continue;
        }
        matched_any = true;
        let data = load_file_bytes(&file, io_mode, "read")?;
        write_matching_lines(
            &mut out,
            &file,
            data.data.as_slice(),
            &matches,
            multi_file,
            print_line_numbers,
        )?;
    }
    out.flush()?;
    Ok(if matched_any { 0 } else { 1 })
}

fn run_tac(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for file in &files {
        let data = read_file_with_mode(file, io_mode)?;
        let mut parts = data
            .split_inclusive(|&byte| byte == b'\n')
            .collect::<Vec<_>>();
        if parts.is_empty() && !data.is_empty() {
            parts.push(data.as_slice());
        }
        for part in parts.into_iter().rev() {
            out.write_all(part)?;
        }
    }
    out.flush()
}

#[derive(Debug)]
struct WcBlockCounts {
    lines: u64,
    words: u64,
    bytes: u64,
    starts_in_word: bool,
    ends_in_word: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WcTotals {
    lines: u64,
    words: u64,
    bytes: u64,
}

fn reduce_wc_counts(blocks: &[WcBlockCounts]) -> WcTotals {
    let mut totals = WcTotals {
        lines: 0,
        words: 0,
        bytes: 0,
    };
    let mut previous_ended_in_word = false;
    for block in blocks {
        totals.lines += block.lines;
        totals.words += block.words;
        totals.bytes += block.bytes;
        if previous_ended_in_word && block.starts_in_word {
            totals.words -= 1;
        }
        previous_ended_in_word = block.ends_in_word;
    }
    totals
}

fn run_wc(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let mut print_lines = false;
    let mut print_words = false;
    let mut print_bytes = false;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-l" => print_lines = true,
            "-w" => print_words = true,
            "-c" => print_bytes = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
    }
    let files = ensure_files(
        program,
        files,
        "[-l] [-w] [-c] [--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    if !print_lines && !print_words && !print_bytes {
        print_lines = true;
        print_words = true;
        print_bytes = true;
    }

    let config = load_config(None);
    for file in files {
        let blocks =
            map_file_blocks_for_mode(&config, "read", &file, internal_io_mode(io_mode), |block| {
                let mut words = 0_u64;
                let mut prev_is_whitespace = true;
                for &byte in block.data {
                    let is_whitespace = byte.is_ascii_whitespace();
                    if !is_whitespace && prev_is_whitespace {
                        words += 1;
                    }
                    prev_is_whitespace = is_whitespace;
                }
                Ok::<_, io::Error>(WcBlockCounts {
                    lines: memchr_iter(b'\n', block.data).count() as u64,
                    words,
                    bytes: block.data.len() as u64,
                    starts_in_word: block
                        .data
                        .first()
                        .is_some_and(|byte| !byte.is_ascii_whitespace()),
                    ends_in_word: block
                        .data
                        .last()
                        .is_some_and(|byte| !byte.is_ascii_whitespace()),
                })
            })?;
        let totals = reduce_wc_counts(&blocks.blocks);

        let mut first = true;
        for (enabled, value) in [
            (print_lines, totals.lines),
            (print_words, totals.words),
            (print_bytes, totals.bytes),
        ] {
            if enabled {
                if !first {
                    print!(" ");
                }
                print!("{value}");
                first = false;
            }
        }
        println!(" {}", file);
    }
    Ok(())
}

fn run_hash_sum(args: &[String], algorithm: HashAlgorithm) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    for file in files {
        println!(
            "{}  {}",
            hex_digest(&hash_file(&file, algorithm, io_mode)?),
            file
        );
    }
    Ok(())
}

fn crc32_cksum_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn run_cksum(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(
        program,
        files,
        "[--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    for file in files {
        let mut crc = 0_u32;
        let mut bytes = 0_u64;
        visit_ordered_blocks(&file, io_mode, |block| {
            crc = crc32_cksum_update(crc, block);
            bytes += block.len() as u64;
            Ok(())
        })?;
        let mut length = bytes;
        while length != 0 {
            crc = crc32_cksum_update(crc, &[(length & 0xff) as u8]);
            length >>= 8;
        }
        println!("{} {} {}", !crc, bytes, file);
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn run_shred(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let mut passes = DEFAULT_SHRED_PASSES;
    let mut zero_last = false;
    let mut remove_after = false;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-n" => {
                i += 1;
                let count = args.get(i).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for -n")
                })?;
                passes = count.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid pass count")
                })?;
            }
            "-z" => zero_last = true,
            "-u" => remove_after = true,
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            other => files.push(other.to_string()),
        }
        i += 1;
    }
    let files = ensure_files(
        program,
        files,
        "[-n passes] [-z] [-u] [--auto|--no-direct|--direct] <file> [file ...]",
    )?;
    for file in files {
        let size = fs::metadata(&file)?.len();
        for _ in 0..passes {
            overwrite_with_pattern(&file, size, io_mode, true)?;
        }
        if zero_last {
            overwrite_with_pattern(&file, size, io_mode, false)?;
        }
        if remove_after {
            fs::remove_file(&file)?;
        }
    }
    Ok(())
}

fn overwrite_with_pattern(path: &str, size: u64, io_mode: IOMode, random: bool) -> io::Result<()> {
    if size == 0 {
        return Ok(());
    }
    let config = load_config(None);
    let page_cache = config.get_params_for_path("write", false, path);
    let direct = config.get_params_for_path("write", true, path);
    write_generated_file(
        path,
        size,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        internal_io_mode(io_mode),
        if random {
            GeneratedWritePattern::Random
        } else {
            GeneratedWritePattern::Zero
        },
    )?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()
}

#[cfg(test)]
mod tests {
    use super::{reduce_wc_counts, WcBlockCounts, WcTotals};

    #[test]
    fn reduce_wc_counts_merges_cross_block_words() {
        let blocks = [
            WcBlockCounts {
                lines: 0,
                words: 1,
                bytes: 3,
                starts_in_word: true,
                ends_in_word: true,
            },
            WcBlockCounts {
                lines: 1,
                words: 1,
                bytes: 4,
                starts_in_word: true,
                ends_in_word: false,
            },
        ];

        assert_eq!(
            reduce_wc_counts(&blocks),
            WcTotals {
                lines: 1,
                words: 1,
                bytes: 7,
            }
        );
    }
}
