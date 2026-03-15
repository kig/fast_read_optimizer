use fro::config::load_config;
use fro::reader::load_file_to_memory_for_mode;
use fro::IOMode;
use std::time::Instant;

struct Options {
    config_path: Option<String>,
    io_mode: IOMode,
    filename: String,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct|--direct] [-c config.json] <model-file>",
        program
    );
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        return Err("missing model file".to_string());
    }

    let mut config_path = None;
    let mut io_mode = IOMode::Auto;
    let mut filename = None;

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
            "-h" | "--help" => {
                usage(&args[0]);
                std::process::exit(0);
            }
            other if filename.is_none() => filename = Some(other.to_string()),
            other => return Err(format!("unexpected argument: {}", other)),
        }
        i += 1;
    }

    Ok(Options {
        config_path,
        io_mode,
        filename: filename.ok_or_else(|| "missing model file".to_string())?,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(std::io::Error::other)?;
    let config = load_config(opts.config_path.as_deref());

    let start = Instant::now();
    let loaded = load_file_to_memory_for_mode(&config, "read", &opts.filename, opts.io_mode)?;
    let elapsed = start.elapsed().as_secs_f64();

    let page_checksum = loaded
        .data
        .iter()
        .step_by(4096)
        .fold(0u64, |acc, byte| acc.wrapping_mul(131).wrapping_add(*byte as u64));

    println!(
        "model_loader loaded {} bytes in {:.4} s, {:.1} GB/s",
        loaded.bytes_read,
        elapsed,
        loaded.bytes_read as f64 / elapsed / 1e9
    );
    println!(
        "params: direct={} threads={} block_size={} qd={}",
        loaded.params.use_direct,
        loaded.params.num_threads,
        loaded.params.block_size,
        loaded.params.qd
    );
    println!("page_checksum={:016x}", page_checksum);

    Ok(())
}
