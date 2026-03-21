use blake3::Hasher as Blake3Hasher;
use fro::{offset_writer_with_options, read_file_with_mode, visit_blocks_with_mode, IOMode};
use rand::RngExt;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::mpsc;

const DEFAULT_SHRED_PASSES: usize = 1;
const SHRED_BLOCK_SIZE: u64 = 1024 * 1024;

pub fn rewrite_alias_args(args: Vec<String>) -> Vec<String> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return args;
    };
    let Some(mode) = (match invoked.as_str() {
        "cp" => Some("copy"),
        "cmp" => Some("diff"),
        "fgrep" => Some("grep"),
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

pub fn try_run_multicall(args: &[String]) -> io::Result<Option<i32>> {
    let Some(invoked) = invoked_name(args.first().map(String::as_str).unwrap_or_default()) else {
        return Ok(None);
    };
    let code = match invoked.as_str() {
        "cat" => {
            run_cat(args)?;
            0
        }
        "tac" => {
            run_tac(args)?;
            0
        }
        "wc" => {
            run_wc(args)?;
            0
        }
        "sum" => {
            run_sum(args)?;
            0
        }
        "cksum" => {
            run_cksum(args)?;
            0
        }
        "b3sum" => {
            run_blake3_sum(args)?;
            0
        }
        "sha224sum" => {
            run_sha2_sum::<Sha224>(args)?;
            0
        }
        "sha256sum" => {
            run_sha2_sum::<Sha256>(args)?;
            0
        }
        "sha384sum" => {
            run_sha2_sum::<Sha384>(args)?;
            0
        }
        "sha512sum" => {
            run_sha2_sum::<Sha512>(args)?;
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
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "missing file operand"));
    }
    Ok(files)
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
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for file in files {
        visit_ordered_blocks(&file, io_mode, |block| out.write_all(block))?;
    }
    out.flush()
}

fn run_tac(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for file in files.iter().rev() {
        let data = read_file_with_mode(file, io_mode)?;
        let mut parts = data.split_inclusive(|&byte| byte == b'\n').collect::<Vec<_>>();
        if parts.is_empty() && !data.is_empty() {
            parts.push(data.as_slice());
        }
        for part in parts.into_iter().rev() {
            out.write_all(part)?;
        }
    }
    out.flush()
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

    for file in files {
        let mut lines = 0_u64;
        let mut words = 0_u64;
        let mut bytes = 0_u64;
        let mut in_word = false;
        visit_ordered_blocks(&file, io_mode, |block| {
            bytes += block.len() as u64;
            for &byte in block {
                if byte == b'\n' {
                    lines += 1;
                }
                if byte.is_ascii_whitespace() {
                    in_word = false;
                } else if !in_word {
                    words += 1;
                    in_word = true;
                }
            }
            Ok(())
        })?;

        let mut first = true;
        for (enabled, value) in [(print_lines, lines), (print_words, words), (print_bytes, bytes)] {
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

fn sha2_digest_hex<D>(path: &str, io_mode: IOMode) -> io::Result<String>
where
    D: Digest + Default,
{
    let mut hasher = D::default();
    visit_ordered_blocks(path, io_mode, |block| {
        hasher.update(block);
        Ok(())
    })?;
    Ok(hex_digest(&hasher.finalize()))
}

fn run_sha2_sum<D>(args: &[String]) -> io::Result<()>
where
    D: Digest + Default,
{
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
    for file in files {
        println!("{}  {}", sha2_digest_hex::<D>(&file, io_mode)?, file);
    }
    Ok(())
}

fn run_blake3_sum(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
    for file in files {
        let mut hasher = Blake3Hasher::new();
        visit_ordered_blocks(&file, io_mode, |block| {
            hasher.update(block);
            Ok(())
        })?;
        println!("{}  {}", hex_digest(hasher.finalize().as_bytes()), file);
    }
    Ok(())
}

fn run_sum(args: &[String]) -> io::Result<()> {
    let program = args[0].as_str();
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
    for file in files {
        let mut checksum = 0_u16;
        let mut bytes = 0_u64;
        visit_ordered_blocks(&file, io_mode, |block| {
            bytes += block.len() as u64;
            for &byte in block {
                checksum = checksum.rotate_right(1).wrapping_add(u16::from(byte));
            }
            Ok(())
        })?;
        println!("{} {} {}", checksum, bytes.div_ceil(1024), file);
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
    let files = ensure_files(program, files, "[--auto|--no-direct|--direct] <file> [file ...]")?;
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
    let writer = offset_writer_with_options(path, size, io_mode, false)?;
    let mut offset = 0_u64;
    while offset < size {
        let len = (size - offset).min(SHRED_BLOCK_SIZE) as usize;
        let mut block = vec![0_u8; len];
        if random {
            rand::rng().fill(&mut block[..]);
        }
        writer.write_at_offset(offset, block)?;
        offset += len as u64;
    }
    writer.finish()?;
    OpenOptions::new().read(true).write(true).open(path)?.sync_all()
}
