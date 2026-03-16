use blake3::Hasher;
use fro::config::load_config;
use fro::reader::visit_file_blocks_for_mode;
use fro::IOMode;
use std::collections::BTreeMap;
use std::io;
use std::sync::mpsc;

struct Options {
    config_path: Option<String>,
    io_mode: IOMode,
    filenames: Vec<String>,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct|--direct] [-c config.json] <file> [file ...]",
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
    let mut filenames = Vec::new();

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
            other => filenames.push(other.to_string()),
        }
        i += 1;
    }

    if filenames.is_empty() {
        return Err("missing file".to_string());
    }

    Ok(Options {
        config_path,
        io_mode,
        filenames,
    })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn hash_file(
    path: &str,
    config: &fro::config::LoadedConfig,
    io_mode: IOMode,
) -> io::Result<blake3::Hash> {
    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>)>();

    let hash_thread = std::thread::spawn(move || -> io::Result<blake3::Hash> {
        let mut hasher = Hasher::new();
        let mut next_block = 0usize;
        let mut pending = BTreeMap::<usize, Vec<u8>>::new();

        while let Ok((block_index, data)) = rx.recv() {
            pending.insert(block_index, data);
            while let Some(block) = pending.remove(&next_block) {
                hasher.update(&block);
                next_block += 1;
            }
        }

        if !pending.is_empty() {
            return Err(io::Error::other(
                "missing block data while finalizing digest",
            ));
        }

        Ok(hasher.finalize())
    });

    let sender = tx.clone();
    let visit_result = visit_file_blocks_for_mode(config, "hash", path, io_mode, move |block| {
        sender
            .send((block.block_index, block.data.to_vec()))
            .map_err(|_| io::Error::other("failed to queue block for hashing"))
    });
    drop(tx);
    let digest = hash_thread
        .join()
        .map_err(|_| io::Error::other("hash worker thread panicked"))??;
    visit_result?;
    Ok(digest)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(io::Error::other)?;
    let config = load_config(opts.config_path.as_deref());

    for filename in &opts.filenames {
        let digest = hash_file(filename, &config, opts.io_mode)?;
        println!("{}  {}", hex_digest(digest.as_bytes()), filename);
    }

    Ok(())
}
