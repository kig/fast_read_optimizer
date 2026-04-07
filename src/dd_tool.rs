use crate::config::load_config;
use crate::io_util::CopyOperationGuard;
use crate::{CopyStrategy, IOMode};
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
const DD_COPY_FILE_RANGE_SINGLE_MAX: u64 = 1 << 20;
const DD_COPY_FILE_RANGE_CHUNKED_MAX: u64 = 16 << 20;

struct Options {
    input: String,
    output: String,
    block_size: Option<u64>,
    count: Option<u64>,
    skip: u64,
    seek: u64,
    input_mode: IOMode,
    output_mode: IOMode,
    count_bytes: bool,
    skip_bytes: bool,
    seek_bytes: bool,
    notrunc: bool,
    fsync: bool,
    status: StatusMode,
}

pub fn usage(program: &str) {
    eprintln!(
        "USAGE: {} if=<input> of=<output> [bs=<size>] [count=<blocks>] [skip=<blocks>] [seek=<blocks>] [iflag=direct,count_bytes,skip_bytes] [oflag=direct,seek_bytes] [conv=notrunc,fsync] [status=none|progress]",
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

fn parse_input_flags(
    value: &str,
    input_mode: &mut IOMode,
    count_bytes: &mut bool,
    skip_bytes: &mut bool,
) -> Result<(), String> {
    for flag in value.split(',').filter(|item| !item.is_empty()) {
        match flag {
            "count_bytes" => *count_bytes = true,
            "skip_bytes" => *skip_bytes = true,
            "direct" | "pagecache" | "page-cache" | "cached" | "auto" => {
                *input_mode = parse_mode(flag)?;
            }
            other => return Err(format!("unsupported iflag option: {}", other)),
        }
    }
    Ok(())
}

fn parse_output_flags(
    value: &str,
    output_mode: &mut IOMode,
    seek_bytes: &mut bool,
) -> Result<(), String> {
    for flag in value.split(',').filter(|item| !item.is_empty()) {
        match flag {
            "seek_bytes" => *seek_bytes = true,
            "direct" | "pagecache" | "page-cache" | "cached" | "auto" => {
                *output_mode = parse_mode(flag)?;
            }
            other => return Err(format!("unsupported oflag option: {}", other)),
        }
    }
    Ok(())
}

fn parse_args(args: &[String]) -> Result<Options, String> {
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
    let mut count_bytes = false;
    let mut skip_bytes = false;
    let mut seek_bytes = false;
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
            "iflag" => {
                parse_input_flags(value, &mut input_mode, &mut count_bytes, &mut skip_bytes)?
            }
            "oflag" => parse_output_flags(value, &mut output_mode, &mut seek_bytes)?,
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
        count_bytes,
        skip_bytes,
        seek_bytes,
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

fn dd_small_medium_copy_strategy(
    copy_len: u64,
    input_mode: IOMode,
    output_mode: IOMode,
) -> Option<CopyStrategy> {
    if copy_len == 0 || input_mode == IOMode::Direct || output_mode == IOMode::Direct {
        return None;
    }
    if copy_len <= DD_COPY_FILE_RANGE_SINGLE_MAX {
        return Some(CopyStrategy::CopyFileRangeSingle);
    }
    if copy_len <= DD_COPY_FILE_RANGE_CHUNKED_MAX {
        return Some(CopyStrategy::CopyFileRange);
    }
    None
}

fn dd_copy_file_range_eligible(input: &str, output: &str) -> io::Result<bool> {
    if input == output || output == "/dev/null" {
        return Ok(false);
    }
    if !std::fs::metadata(input)?.file_type().is_file() {
        return Ok(false);
    }
    match std::fs::metadata(output) {
        Ok(metadata) => Ok(metadata.file_type().is_file()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

fn copy_file_range_with_dd_strategy(
    source: &str,
    target: &str,
    source_offset: u64,
    dest_offset: u64,
    len: u64,
    truncate_target: bool,
    io_mode_read: IOMode,
    io_mode_write: IOMode,
    copy_strategy: CopyStrategy,
) -> io::Result<u64> {
    let config = load_config(None);
    let guard = CopyOperationGuard::new(source, target, true)?;
    let page_cache = config.get_params_for_path("copy", false, target);
    let direct = config.get_params_for_path("copy", true, target);
    let copy_range = config.get_copy_range_params_for_path(target);
    let copied = crate::writer::copy_file_range_with_strategy(
        source,
        target,
        source_offset,
        dest_offset,
        len,
        truncate_target,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        copy_range.num_threads,
        copy_range.block_size,
        copy_range.qd,
        io_mode_read,
        io_mode_write,
        copy_strategy,
    )?;
    guard.ensure_source_unchanged()?;
    Ok(copied)
}

fn prepare_zero_count_output(path: &str, output_offset: u64, notrunc: bool) -> io::Result<()> {
    let file = OpenOptions::new().create(true).write(true).open(path)?;
    if !notrunc && file.metadata()?.file_type().is_file() {
        file.set_len(output_offset)?;
    }
    Ok(())
}

pub fn run_dd(args: &[String]) -> io::Result<()> {
    let opts = parse_args(args).map_err(io::Error::other)?;
    let start = Instant::now();
    let block_size = opts.block_size.unwrap_or(DEFAULT_DD_BLOCK_SIZE);

    if opts.count == Some(0) {
        std::fs::File::open(&opts.input)?;
        let output_offset = if opts.seek_bytes {
            opts.seek
        } else {
            opts.seek.saturating_mul(block_size)
        };
        prepare_zero_count_output(&opts.output, output_offset, opts.notrunc)?;
        if opts.fsync {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&opts.output)?
                .sync_all()?;
        }
        if opts.status != StatusMode::None {
            print_summary(0, block_size, start.elapsed());
        }
        return Ok(());
    }

    if opts.block_size.is_none()
        && opts.count.is_none()
        && opts.skip == 0
        && opts.seek == 0
        && !opts.notrunc
    {
        let record_block_size = DEFAULT_DD_BLOCK_SIZE;
        let bytes = if dd_copy_file_range_eligible(&opts.input, &opts.output)? {
            match dd_small_medium_copy_strategy(
                std::fs::metadata(&opts.input)?.len(),
                opts.input_mode,
                opts.output_mode,
            ) {
                Some(copy_strategy) => copy_file_range_with_dd_strategy(
                    &opts.input,
                    &opts.output,
                    0,
                    0,
                    u64::MAX,
                    true,
                    opts.input_mode,
                    opts.output_mode,
                    copy_strategy,
                )?,
                None => crate::copy_file_with_modes(
                    &opts.input,
                    &opts.output,
                    opts.input_mode,
                    opts.output_mode,
                )?,
            }
        } else {
            crate::copy_file_with_modes(
                &opts.input,
                &opts.output,
                opts.input_mode,
                opts.output_mode,
            )?
        };
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

    let input = crate::open_with_mode(&opts.input, opts.input_mode)?;
    let input_size = input.len()?;
    let input_offset = if opts.skip_bytes {
        opts.skip
    } else {
        opts.skip.saturating_mul(block_size)
    };
    let available = input_size.saturating_sub(input_offset);
    let requested = opts
        .count
        .map(|count| {
            if opts.count_bytes {
                count
            } else {
                count.saturating_mul(block_size)
            }
        })
        .unwrap_or(available);
    let copy_len = available.min(requested);
    let output_offset = if opts.seek_bytes {
        opts.seek
    } else {
        opts.seek.saturating_mul(block_size)
    };
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
    let copy_strategy = if dd_copy_file_range_eligible(&opts.input, &opts.output)? {
        dd_small_medium_copy_strategy(copy_len, opts.input_mode, opts.output_mode)
    } else {
        None
    };
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
    } else if let Some(copy_strategy) = copy_strategy {
        copy_file_range_with_dd_strategy(
            &opts.input,
            &opts.output,
            input_offset,
            output_offset,
            copy_len,
            !opts.notrunc,
            opts.input_mode,
            opts.output_mode,
            copy_strategy,
        )
    } else {
        crate::copy_file_range_with_modes(
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

pub fn run_dd_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    run_dd(&args)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dd_prefers_copy_file_range_paths_for_small_and_medium_page_cache_transfers() {
        assert_eq!(
            dd_small_medium_copy_strategy(64 * 1024, IOMode::Auto, IOMode::Auto),
            Some(CopyStrategy::CopyFileRangeSingle)
        );
        assert_eq!(
            dd_small_medium_copy_strategy(
                DD_COPY_FILE_RANGE_SINGLE_MAX,
                IOMode::PageCache,
                IOMode::Auto
            ),
            Some(CopyStrategy::CopyFileRangeSingle)
        );
        assert_eq!(
            dd_small_medium_copy_strategy(4 * 1024 * 1024, IOMode::Auto, IOMode::PageCache),
            Some(CopyStrategy::CopyFileRange)
        );
        assert_eq!(
            dd_small_medium_copy_strategy(
                DD_COPY_FILE_RANGE_CHUNKED_MAX,
                IOMode::Auto,
                IOMode::Auto
            ),
            Some(CopyStrategy::CopyFileRange)
        );
    }

    #[test]
    fn dd_keeps_threaded_path_for_direct_or_large_transfers() {
        assert_eq!(
            dd_small_medium_copy_strategy(64 * 1024, IOMode::Direct, IOMode::Auto),
            None
        );
        assert_eq!(
            dd_small_medium_copy_strategy(64 * 1024, IOMode::Auto, IOMode::Direct),
            None
        );
        assert_eq!(
            dd_small_medium_copy_strategy(
                DD_COPY_FILE_RANGE_CHUNKED_MAX + 1,
                IOMode::Auto,
                IOMode::Auto
            ),
            None
        );
    }

    #[test]
    fn dd_parse_iflag_and_oflag_byte_modes() {
        let opts = parse_args(&[
            "dd".to_string(),
            "if=input.bin".to_string(),
            "of=output.bin".to_string(),
            "iflag=skip_bytes,count_bytes,direct".to_string(),
            "oflag=seek_bytes,pagecache".to_string(),
        ])
        .expect("parse dd args");

        assert!(matches!(opts.input_mode, IOMode::Direct));
        assert!(matches!(opts.output_mode, IOMode::PageCache));
        assert!(opts.skip_bytes);
        assert!(opts.count_bytes);
        assert!(opts.seek_bytes);
    }

    #[test]
    fn zero_count_output_truncates_only_without_notrunc() {
        let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fro-dd-zero-count-{}", std::process::id()));
        std::fs::create_dir_all(tmp.parent().expect("tmp parent")).expect("create temp dir");
        let path = tmp;
        std::fs::write(&path, b"abcdefghij").expect("seed output");

        prepare_zero_count_output(path.to_str().unwrap(), 5, false).expect("truncate output");
        assert_eq!(std::fs::read(&path).expect("read output"), b"abcde");

        std::fs::write(&path, b"abcdefghij").expect("reseed output");
        prepare_zero_count_output(path.to_str().unwrap(), 12, true).expect("preserve output");
        assert_eq!(std::fs::read(&path).expect("read preserved"), b"abcdefghij");

        std::fs::remove_file(path).ok();
    }
}
