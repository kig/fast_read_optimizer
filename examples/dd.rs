use fro::IOMode;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq)]
enum StatusMode {
    Summary,
    None,
    Progress,
}

const DEFAULT_DD_BLOCK_SIZE: u64 = 512;

struct Options {
    input: String,
    output: String,
    block_size: Option<u64>,
    count: Option<u64>,
    skip: u64,
    seek: u64,
    input_mode: IOMode,
    output_mode: IOMode,
    notrunc: bool,
    fsync: bool,
    status: StatusMode,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} if=<input> of=<output> [bs=<size>] [count=<blocks>] [skip=<blocks>] [seek=<blocks>] [iflag=direct] [oflag=direct] [conv=notrunc,fsync] [status=none|progress]",
        program
    );
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("size must not be empty".to_string());
    }

    let s_lc = s.to_ascii_lowercase();
    let split = s_lc
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s_lc.len());
    let (num_str, suffix) = s_lc.split_at(split);
    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid size: {}", s))?;
    let mult = match suffix.trim() {
        "" | "c" => 1,
        "w" => 2,
        "b" => 512,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        other => return Err(format!("unsupported size suffix: {}", other)),
    };
    num.checked_mul(mult)
        .ok_or_else(|| format!("size is too large: {}", s))
}

fn parse_mode(value: &str) -> Result<IOMode, String> {
    match value {
        "direct" => Ok(IOMode::Direct),
        "pagecache" | "page-cache" | "cached" => Ok(IOMode::PageCache),
        "auto" => Ok(IOMode::Auto),
        _ => Err(format!("unsupported flag mode: {}", value)),
    }
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        usage(&args[0]);
        return Err("missing dd arguments".to_string());
    }

    let mut input = None;
    let mut output = None;
    let mut block_size = None;
    let mut count = None;
    let mut skip = 0;
    let mut seek = 0;
    let mut input_mode = IOMode::Auto;
    let mut output_mode = IOMode::Auto;
    let mut notrunc = false;
    let mut fsync = false;
    let mut status = StatusMode::Summary;

    for arg in args.iter().skip(1) {
        if arg == "-h" || arg == "--help" {
            usage(&args[0]);
            std::process::exit(0);
        }

        let Some((key, value)) = arg.split_once('=') else {
            return Err(format!("expected key=value argument, got {}", arg));
        };
        match key {
            "if" => input = Some(value.to_string()),
            "of" => output = Some(value.to_string()),
            "bs" => block_size = Some(parse_size(value)?),
            "count" => count = Some(parse_size(value)?),
            "skip" => skip = parse_size(value)?,
            "seek" => seek = parse_size(value)?,
            "iflag" => input_mode = parse_mode(value)?,
            "oflag" => output_mode = parse_mode(value)?,
            "conv" => {
                for conv in value.split(',').filter(|item| !item.is_empty()) {
                    match conv {
                        "notrunc" => notrunc = true,
                        "fsync" => fsync = true,
                        other => return Err(format!("unsupported conv option: {}", other)),
                    }
                }
            }
            "status" => {
                status = match value {
                    "none" => StatusMode::None,
                    "progress" => StatusMode::Progress,
                    "summary" | "default" => StatusMode::Summary,
                    other => return Err(format!("unsupported status mode: {}", other)),
                };
            }
            other => return Err(format!("unsupported dd option: {}", other)),
        }
    }

    Ok(Options {
        input: input.ok_or_else(|| "missing if=<input>".to_string())?,
        output: output.ok_or_else(|| "missing of=<output>".to_string())?,
        block_size,
        count,
        skip,
        seek,
        input_mode,
        output_mode,
        notrunc,
        fsync,
        status,
    })
}

fn record_counts(bytes: u64, block_size: u64) -> (u64, u64) {
    let full = bytes / block_size;
    let partial = u64::from(bytes % block_size != 0);
    (full, partial)
}

fn print_summary(bytes: u64, block_size: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let (full_records, partial_records) = record_counts(bytes, block_size);
    eprintln!("{}+{} records in", full_records, partial_records);
    eprintln!("{}+{} records out", full_records, partial_records);
    eprintln!(
        "{} bytes copied in {:.4} s, {:.1} GB/s",
        bytes,
        secs,
        if secs == 0.0 {
            f64::INFINITY
        } else {
            bytes as f64 / secs / 1e9
        }
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(io::Error::other)?;
    let start = Instant::now();

    if opts.block_size.is_none()
        && opts.count.is_none()
        && opts.skip == 0
        && opts.seek == 0
        && !opts.notrunc
    {
        let record_block_size = DEFAULT_DD_BLOCK_SIZE;
        let bytes = fro::copy_file_with_modes(
            &opts.input,
            &opts.output,
            opts.input_mode,
            opts.output_mode,
        )?;
        if opts.fsync {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&opts.output)?
                .sync_all()?;
        }
        if opts.status != StatusMode::None {
            print_summary(bytes, record_block_size, start.elapsed());
        }
        return Ok(());
    }

    let input = fro::open_with_mode(&opts.input, opts.input_mode)?;
    let block_size = opts.block_size.unwrap_or(DEFAULT_DD_BLOCK_SIZE);
    let input_size = input.len()?;
    let input_offset = opts.skip.saturating_mul(block_size);
    let available = input_size.saturating_sub(input_offset);
    let requested = opts
        .count
        .map(|blocks| blocks.saturating_mul(block_size))
        .unwrap_or(available);
    let copy_len = available.min(requested);
    let output_offset = opts.seek.saturating_mul(block_size);
    let job_count = if copy_len == 0 {
        0
    } else {
        copy_len.div_ceil(block_size) as usize
    };
    let copied = Arc::new(AtomicU64::new(0));
    let copied_progress = copied.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_progress = done.clone();
    let start_block = opts.skip;
    let end_block = start_block + job_count as u64;
    let progress_thread = if opts.status == StatusMode::Progress {
        Some(thread::spawn(move || {
            while !done_progress.load(Ordering::Relaxed) {
                eprintln!("{} bytes copied", copied_progress.load(Ordering::Relaxed));
                thread::sleep(Duration::from_millis(250));
            }
        }))
    } else {
        None
    };

    let is_dev_null = opts.output == "/dev/null";
    let copy_result = if is_dev_null {
        input.foreach_block_parallel(block_size, move |block_index, data| {
            let block_index = block_index as u64;
            if block_index < start_block || block_index >= end_block {
                return Ok(());
            }
            copied.fetch_add(data.len() as u64, Ordering::Relaxed);
            Ok(())
        })?;
        Ok(copy_len)
    } else {
        fro::copy_file_range_with_modes(
            &opts.input,
            &opts.output,
            input_offset,
            output_offset,
            copy_len,
            !opts.notrunc,
            opts.input_mode,
            opts.output_mode,
        )
    };
    done.store(true, Ordering::Relaxed);
    if let Some(progress_thread) = progress_thread {
        progress_thread
            .join()
            .map_err(|_| io::Error::other("progress thread panicked"))?;
    }
    let bytes_copied = copy_result?;

    if opts.fsync {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&opts.output)?
            .sync_all()?;
    }
    if opts.status != StatusMode::None {
        print_summary(bytes_copied, block_size, start.elapsed());
    }
    Ok(())
}
