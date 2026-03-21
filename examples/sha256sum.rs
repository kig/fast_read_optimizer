use fro::{hash_file, HashAlgorithm, IOMode};
use std::io;

struct Options {
    io_mode: IOMode,
    filenames: Vec<String>,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct|--direct] <file> [file ...]",
        program
    );
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        return Err("missing file".to_string());
    }

    let mut io_mode = IOMode::Auto;
    let mut filenames = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
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

    Ok(Options { io_mode, filenames })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(io::Error::other)?;

    for filename in &opts.filenames {
        let digest = hash_file(filename, HashAlgorithm::Sha256, opts.io_mode)?;
        println!("{}  {}", hex_digest(&digest), filename);
    }

    Ok(())
}
