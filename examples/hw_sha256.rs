use fro::config::load_config;
use fro::reader::map_file_blocks_for_mode;
use fro::IOMode;
use libc::{c_uchar, size_t};
use std::time::Instant;

#[link(name = "crypto")]
unsafe extern "C" {
    fn SHA256(data: *const c_uchar, len: size_t, md: *mut c_uchar) -> *mut c_uchar;
}

struct Options {
    config_path: Option<String>,
    io_mode: IOMode,
    filename: String,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct|--direct] [-c config.json] <file>",
        program
    );
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        return Err("missing file".to_string());
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
        filename: filename.ok_or_else(|| "missing file".to_string())?,
    })
}

fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut digest = [0u8; 32];
    let ret = unsafe { SHA256(data.as_ptr(), data.len(), digest.as_mut_ptr()) };
    assert!(!ret.is_null(), "SHA256 returned null");
    digest
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(std::io::Error::other)?;
    let config = load_config(opts.config_path.as_deref());

    let start = Instant::now();
    let mapped =
        map_file_blocks_for_mode(&config, "hash", &opts.filename, opts.io_mode, |block| {
            Ok::<_, std::io::Error>(sha256_digest(block.data))
        })?;
    let elapsed = start.elapsed().as_secs_f64();

    let digest_summary = mapped.blocks.iter().fold(0u64, |acc, digest| {
        let head = u64::from_le_bytes(digest[..8].try_into().unwrap());
        acc.rotate_left(7) ^ head
    });

    println!(
        "hw_sha256 processed {} bytes in {:.4} s, {:.1} GB/s",
        mapped.bytes_read,
        elapsed,
        mapped.bytes_read as f64 / elapsed / 1e9
    );
    println!(
        "params: direct={} threads={} block_size={} qd={} blocks={}",
        mapped.params.use_direct,
        mapped.params.num_threads,
        mapped.params.block_size,
        mapped.params.qd,
        mapped.blocks.len()
    );
    println!("digest_summary={:016x}", digest_summary);

    Ok(())
}
