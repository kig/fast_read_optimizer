use fro::config::load_config;
use fro::reader::resolve_reader_params_for_mode;
use fro::stream::{BlockRange, ParallelFile, ParallelWriter};
use fro::writer::SequentialWriter;
use fro::IOMode;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Instant;
use zstd::bulk::{compress, decompress};

const FOOTER_MAGIC: [u8; 8] = *b"FROZSTD1";
const DEFAULT_ZSTD_BLOCK_SIZE: u64 = 1024 * 1024;
const FOOTER_SIZE: u64 = 8 + (4 * 8);

struct Options {
    config_path: Option<String>,
    io_mode: IOMode,
    compression_level: i32,
    block_size: Option<u64>,
    decompress: bool,
    input: String,
    output: String,
}

struct ContainerMetadata {
    original_size: u64,
    block_size: u64,
    block_count: usize,
    block_ranges: Vec<BlockRange>,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct|--direct] [--decompress] [-c config.json] [--level N] [--block-size bytes] <input> <output>",
        program
    );
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        usage(&args[0]);
        return Err("missing input/output".to_string());
    }

    let mut config_path = None;
    let mut io_mode = IOMode::Auto;
    let mut compression_level = 3;
    let mut block_size = None;
    let mut decompress = false;
    let mut positional = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--config" => {
                i += 1;
                if i >= args.len() {
                    return Err("missing value for --config".to_string());
                }
                config_path = Some(args[i].clone());
            }
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--decompress" => decompress = true,
            "--level" => {
                i += 1;
                if i >= args.len() {
                    return Err("missing value for --level".to_string());
                }
                compression_level = args[i]
                    .parse()
                    .map_err(|_| format!("invalid --level: {}", args[i]))?;
            }
            "--block-size" => {
                i += 1;
                if i >= args.len() {
                    return Err("missing value for --block-size".to_string());
                }
                block_size = Some(
                    args[i]
                        .parse()
                        .map_err(|_| format!("invalid --block-size: {}", args[i]))?,
                );
            }
            "-h" | "--help" => {
                usage(&args[0]);
                std::process::exit(0);
            }
            other => positional.push(other.to_string()),
        }
        i += 1;
    }

    if positional.len() != 2 {
        return Err("expected <input> <output>".to_string());
    }

    Ok(Options {
        config_path,
        io_mode,
        compression_level,
        block_size,
        decompress,
        input: positional.remove(0),
        output: positional.remove(0),
    })
}

fn write_footer(
    out: &mut SequentialWriter,
    original_size: u64,
    block_size: u64,
    block_count: u64,
    offsets_start: u64,
) -> std::io::Result<()> {
    out.append(&FOOTER_MAGIC)?;
    out.append(&original_size.to_le_bytes())?;
    out.append(&block_size.to_le_bytes())?;
    out.append(&block_count.to_le_bytes())?;
    out.append(&offsets_start.to_le_bytes())?;
    Ok(())
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    u64::from_le_bytes(array)
}

fn read_container_metadata(path: &str) -> std::io::Result<ContainerMetadata> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    if file_size < FOOTER_SIZE {
        return Err(std::io::Error::other(
            "file too small to be a fro zstd container",
        ));
    }

    let mut footer = vec![0u8; FOOTER_SIZE as usize];
    file.read_at(&mut footer, (file_size - FOOTER_SIZE) as u64)?;
    if footer[..8] != FOOTER_MAGIC {
        return Err(std::io::Error::other("invalid fro zstd footer magic"));
    }

    let original_size = read_u64_le(&footer[8..16]);
    let block_size = read_u64_le(&footer[16..24]);
    let block_count = read_u64_le(&footer[24..32]) as usize;
    let offsets_start = read_u64_le(&footer[32..40]);
    let offsets_len = block_count as u64 * 16;
    if offsets_start + offsets_len + FOOTER_SIZE != file_size {
        return Err(std::io::Error::other(
            "invalid offsets table placement in fro zstd container",
        ));
    }

    let mut offsets = vec![0u8; offsets_len as usize];
    file.read_at(&mut offsets, offsets_start)?;
    let mut block_ranges = Vec::with_capacity(block_count);
    for index in 0..block_count {
        let start = index * 16;
        block_ranges.push(BlockRange {
            offset: read_u64_le(&offsets[start..start + 8]),
            len: read_u64_le(&offsets[start + 8..start + 16]),
        });
    }

    Ok(ContainerMetadata {
        original_size,
        block_size,
        block_count,
        block_ranges,
    })
}

fn compress_file(
    opts: &Options,
    config: &fro::config::LoadedConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_file = ParallelFile::open(config, "read", &opts.input, opts.io_mode)?;
    let block_size = opts.block_size.unwrap_or(DEFAULT_ZSTD_BLOCK_SIZE);
    if block_size == 0 {
        return Err(std::io::Error::other("block size must be greater than zero").into());
    }
    let block_count = input_file.block_count(block_size)?;
    let output_stream =
        ParallelWriter::indexed(config, "write", &opts.output, opts.io_mode, block_count)?;

    let compression_level = opts.compression_level;
    let start = Instant::now();
    let output_stream_for_blocks = output_stream.clone();
    let read_report =
        input_file.foreach_block_parallel(block_size, move |chunk_index, raw_bytes| {
            let compressed_data =
                compress(raw_bytes, compression_level).map_err(std::io::Error::other)?;
            output_stream_for_blocks.write_at_index(chunk_index, compressed_data)
        })?;
    let write_report = output_stream.finish()?;

    let mut footer_writer = SequentialWriter::open_append(
        &opts.output,
        write_report.write_params.qd,
        write_report.write_params.block_size,
        opts.io_mode,
    )?;
    let offsets_start = write_report.bytes_written;
    for range in &write_report.block_ranges {
        footer_writer.append(&range.offset.to_le_bytes())?;
        footer_writer.append(&range.len.to_le_bytes())?;
    }
    write_footer(
        &mut footer_writer,
        read_report.file_size,
        block_size,
        write_report.block_ranges.len() as u64,
        offsets_start,
    )?;
    footer_writer.flush()?;

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "parallel_zstd compress e2e processed {} bytes in {:.4} s, {:.1} GB/s",
        read_report.bytes_read,
        elapsed,
        read_report.bytes_read as f64 / elapsed / 1e9
    );
    println!(
        "params: read_direct={} read_threads={} read_block_size={} read_qd={} write_direct={} write_block_size={} write_qd={} zstd_block_size={} blocks={}",
        read_report.params.use_direct,
        read_report.params.num_threads,
        read_report.params.block_size,
        read_report.params.qd,
        write_report.write_params.use_direct,
        write_report.write_params.block_size,
        write_report.write_params.qd,
        block_size,
        write_report.block_ranges.len()
    );
    println!(
        "output={} compressed_bytes={} ratio={:.3} offsets_start={}",
        opts.output,
        write_report.bytes_written,
        write_report.bytes_written as f64 / read_report.file_size.max(1) as f64,
        offsets_start
    );

    Ok(())
}

fn decompress_file(
    opts: &Options,
    config: &fro::config::LoadedConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let compressed_file = ParallelFile::open(config, "read", &opts.input, opts.io_mode)?;
    let metadata = Arc::new(read_container_metadata(&opts.input)?);
    let read_params = resolve_reader_params_for_mode(config, "read", &opts.input, opts.io_mode)?;
    let writer = ParallelWriter::fixed_size(
        config,
        "write",
        &opts.output,
        opts.io_mode,
        metadata.original_size,
    )?;
    let writer_for_jobs = writer.clone();
    let compressed_file_for_jobs = compressed_file.clone();
    let metadata_for_jobs = metadata.clone();
    let writer_for_closure = writer_for_jobs.clone();

    let start = Instant::now();
    compressed_file.foreach_index_parallel(metadata.block_count, move |i| {
        let entry = metadata_for_jobs.block_ranges[i];
        let compressed_bytes =
            compressed_file_for_jobs.read_range(entry.offset, entry.len as usize)?;
        let remaining = metadata_for_jobs.original_size - (i as u64 * metadata_for_jobs.block_size);
        let output_bytes = decompress(
            &compressed_bytes,
            remaining.min(metadata_for_jobs.block_size) as usize,
        )
        .map_err(std::io::Error::other)?;
        let target_offset = i as u64 * metadata_for_jobs.block_size;
        writer_for_closure.write_at_offset(target_offset, output_bytes)
    })?;
    drop(writer_for_jobs);

    let write_report = writer.finish()?;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "parallel_zstd decompress e2e processed {} bytes in {:.4} s, {:.1} GB/s",
        metadata.original_size,
        elapsed,
        metadata.original_size as f64 / elapsed / 1e9
    );
    println!(
        "params: read_direct={} read_threads={} read_block_size={} read_qd={} write_direct={} write_block_size={} write_qd={} zstd_block_size={} blocks={}",
        read_params.use_direct,
        read_params.num_threads,
        read_params.block_size,
        read_params.qd,
        write_report.write_params.use_direct,
        write_report.write_params.block_size,
        write_report.write_params.qd,
        metadata.block_size,
        metadata.block_count
    );
    println!(
        "output={} decompressed_bytes={}",
        opts.output, write_report.bytes_written
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(std::io::Error::other)?;
    let config = load_config(opts.config_path.as_deref());

    if opts.decompress {
        decompress_file(&opts, &config)
    } else {
        compress_file(&opts, &config)
    }
}
