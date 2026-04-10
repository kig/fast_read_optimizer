use fro::{hash_file, HashAlgorithm, IOMode};
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

struct Options {
    io_mode: IOMode,
    filenames: Vec<String>,
}

fn program_name(argv0: &str, fallback: &str) -> String {
    Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback)
        .to_string()
}

fn usage(program: &str) {
    println!("Usage: {program} [--auto|--no-direct|--direct] [--] <file> [file ...]");
    println!();
    println!("Hash each file with {}.", digest_label(program));
    println!();
    println!("Options:");
    println!("      --auto       choose direct I/O automatically");
    println!("      --direct     force direct I/O where alignment allows");
    println!("      --no-direct  force page-cache I/O");
    println!("  -h, --help       display this help and exit");
    println!("      --version    output version information and exit");
}

fn version(program: &str) {
    println!("{program} (fro dedicated) {}", env!("CARGO_PKG_VERSION"));
}

fn digest_label(program: &str) -> &'static str {
    match program {
        "md5sum" => "MD5",
        "sha256sum" => "SHA-256",
        _ => "the requested digest",
    }
}

fn parse_args(program: &str) -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    let mut io_mode = IOMode::Auto;
    let mut filenames = Vec::new();
    let mut parse_flags = true;

    for arg in &args[1..] {
        if parse_flags {
            match arg.as_str() {
                "--auto" => {
                    io_mode = IOMode::Auto;
                    continue;
                }
                "--direct" => {
                    io_mode = IOMode::Direct;
                    continue;
                }
                "--no-direct" => {
                    io_mode = IOMode::PageCache;
                    continue;
                }
                "-h" | "--help" => {
                    usage(program);
                    return Err(String::new());
                }
                "--version" => {
                    version(program);
                    return Err(String::new());
                }
                "--" => {
                    parse_flags = false;
                    continue;
                }
                "-" => {}
                other if other.starts_with('-') => {
                    return Err(format!("unsupported flag for {program}: {other}"));
                }
                _ => {}
            }
        }
        filenames.push(arg.clone());
    }

    if filenames.is_empty() {
        usage(program);
        return Err("missing file operand".to_string());
    }

    Ok(Options { io_mode, filenames })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub fn run(algorithm: HashAlgorithm, fallback_program: &str) -> ExitCode {
    let argv0 = std::env::args()
        .next()
        .unwrap_or_else(|| fallback_program.to_string());
    let program = program_name(&argv0, fallback_program);
    let options = match parse_args(&program) {
        Ok(options) => options,
        Err(message) if message.is_empty() => return ExitCode::SUCCESS,
        Err(message) => {
            let _ = writeln!(io::stderr(), "{program}: {message}");
            return ExitCode::from(1);
        }
    };

    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    let mut failed = false;

    for filename in options.filenames {
        match hash_file(&filename, algorithm, options.io_mode) {
            Ok(digest) => {
                if writeln!(out, "{}  {}", hex_digest(&digest), filename).is_err() {
                    return ExitCode::from(1);
                }
            }
            Err(err) => {
                failed = true;
                let _ = writeln!(io::stderr(), "{program}: {filename}: {err}");
            }
        }
    }

    if out.flush().is_err() {
        return ExitCode::from(1);
    }

    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
