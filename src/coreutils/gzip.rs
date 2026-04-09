use super::*;
use flate2::read::MultiGzDecoder;
use gzp::{deflate::Mgzip, par::decompress::ParDecompressBuilder, Compression, ZBuilder};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

const GZIP_COPY_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone)]
struct GzipOptions {
    decompress: bool,
    stdout: bool,
    keep: bool,
    force: bool,
    report_gbps: bool,
    io_mode: IOMode,
    threads: Option<usize>,
    level: u32,
    input: StreamInput,
    output: Option<String>,
}

enum DecompressSink {
    FroFile(BufWriter),
    Stream(File),
}

impl DecompressSink {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            Self::FroFile(writer) => writer.write_all(buf),
            Self::Stream(writer) => writer.write_all(buf),
        }
    }

    fn finish(self) -> io::Result<()> {
        match self {
            Self::FroFile(writer) => writer.into_inner(),
            Self::Stream(mut writer) => writer.flush(),
        }
    }
}

pub(super) fn run_gzip(invoked: &str, args: &[String]) -> io::Result<i32> {
    let options = parse_gzip_options(invoked, args)?;
    let started_at = Instant::now();
    let output_path = resolved_output_path(&options)?;
    let remove_input_on_success = matches!(options.input, StreamInput::File(_))
        && output_path.is_some()
        && options.output.is_none()
        && !options.stdout
        && !options.keep;

    if options.decompress {
        let bytes = run_decompress(&options, output_path.as_deref())?;
        if options.report_gbps {
            report_gbps(invoked, bytes, started_at);
        }
    } else {
        let bytes = run_compress(&options, output_path.as_deref())?;
        if options.report_gbps {
            report_gbps(invoked, bytes, started_at);
        }
    }

    if remove_input_on_success {
        if let StreamInput::File(path) = &options.input {
            fs::remove_file(path)?;
        }
    }

    Ok(0)
}

fn parse_gzip_options(invoked: &str, args: &[String]) -> io::Result<GzipOptions> {
    let mut decompress = matches!(invoked, "gunzip" | "zcat");
    let mut stdout = invoked == "zcat";
    let mut keep = invoked == "zcat";
    let mut force = false;
    let mut report_gbps = false;
    let mut io_mode = IOMode::Auto;
    let mut threads = None;
    let mut level = 3_u32;
    let mut output = None;
    let mut files = Vec::new();
    let mut end_flags = false;

    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
            i += 1;
            continue;
        }

        if !end_flags && arg.starts_with("--") && arg.len() > 2 {
            match arg.as_str() {
                "--decompress" => decompress = true,
                "--stdout" | "--to-stdout" => stdout = true,
                "--keep" => keep = true,
                "--force" => force = true,
                "--report-gbps" => report_gbps = true,
                "--auto" => io_mode = IOMode::Auto,
                "--direct" => io_mode = IOMode::Direct,
                "--no-direct" => io_mode = IOMode::PageCache,
                "--threads" => {
                    i += 1;
                    let value = args.get(i).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "gzip: option '--threads' requires an argument",
                        )
                    })?;
                    threads = Some(parse_threads(value)?);
                }
                "--output" => {
                    i += 1;
                    let value = args.get(i).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "gzip: option '--output' requires an argument",
                        )
                    })?;
                    output = Some(value.clone());
                }
                other if other.starts_with("--threads=") => {
                    threads = Some(parse_threads(&other["--threads=".len()..])?);
                }
                other if other.starts_with("--output=") => {
                    output = Some(other["--output=".len()..].to_string());
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported gzip option '{other}'"),
                    ))
                }
            }
            i += 1;
            continue;
        }

        if !end_flags && arg.starts_with('-') && arg != "-" {
            parse_short_gzip_flags(
                arg,
                args,
                &mut i,
                &mut decompress,
                &mut stdout,
                &mut keep,
                &mut force,
                &mut report_gbps,
                &mut output,
                &mut level,
            )?;
            i += 1;
            continue;
        }

        files.push(arg.clone());
        i += 1;
    }

    if stdout && output.as_deref() == Some("-") {
        output = None;
    }
    if stdout && output.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gzip: cannot combine --stdout with --output",
        ));
    }
    if files.len() > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("gzip: extra operand '{}'", files[1]),
        ));
    }

    let input = parse_stream_inputs(files)
        .into_iter()
        .next()
        .unwrap_or(StreamInput::Stdin { label: None });
    Ok(GzipOptions {
        decompress,
        stdout,
        keep,
        force,
        report_gbps,
        io_mode,
        threads,
        level,
        input,
        output,
    })
}

#[allow(clippy::too_many_arguments)]
fn parse_short_gzip_flags(
    arg: &str,
    args: &[String],
    index: &mut usize,
    decompress: &mut bool,
    stdout: &mut bool,
    keep: &mut bool,
    force: &mut bool,
    report_gbps: &mut bool,
    output: &mut Option<String>,
    level: &mut u32,
) -> io::Result<()> {
    let chars = arg[1..].chars().collect::<Vec<_>>();
    let mut pos = 0usize;
    while pos < chars.len() {
        match chars[pos] {
            'd' => *decompress = true,
            'c' => *stdout = true,
            'k' => *keep = true,
            'f' => *force = true,
            '1'..='9' => *level = chars[pos].to_digit(10).unwrap(),
            'o' => {
                if pos + 1 < chars.len() {
                    *output = Some(chars[pos + 1..].iter().collect());
                    return Ok(());
                }
                *index += 1;
                *output = Some(
                    args.get(*index)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "gzip: option requires an argument -- 'o'",
                            )
                        })?
                        .clone(),
                );
                return Ok(());
            }
            'R' => *report_gbps = true,
            flag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported gzip flag -{flag}"),
                ))
            }
        }
        pos += 1;
    }
    Ok(())
}

fn parse_threads(value: &str) -> io::Result<usize> {
    let threads = value.parse::<usize>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("gzip: invalid thread count '{value}': {err}"),
        )
    })?;
    if threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gzip: thread count must be greater than zero",
        ));
    }
    Ok(threads)
}

fn resolved_output_path(options: &GzipOptions) -> io::Result<Option<PathBuf>> {
    if options.stdout {
        return Ok(None);
    }
    if let Some(output) = &options.output {
        if output == "-" {
            return Ok(None);
        }
        let output = PathBuf::from(output);
        if matches!(&options.input, StreamInput::File(path) if output == Path::new(path)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gzip: input and output paths must differ",
            ));
        }
        return Ok(Some(output));
    }
    match &options.input {
        StreamInput::File(path) => {
            let output = if options.decompress {
                default_decompressed_path(path)?
            } else {
                PathBuf::from(format!("{path}.gz"))
            };
            if output == Path::new(path) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "gzip: input and output paths must differ",
                ));
            }
            Ok(Some(output))
        }
        StreamInput::Stdin { .. } => Ok(None),
    }
}

fn default_decompressed_path(path: &str) -> io::Result<PathBuf> {
    if let Some(stripped) = path.strip_suffix(".gz") {
        return Ok(PathBuf::from(stripped));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("gzip: cannot infer decompressed output name for '{path}'"),
    ))
}

fn run_compress(options: &GzipOptions, output_path: Option<&Path>) -> io::Result<u64> {
    let mut output = open_output_file(output_path, options.force)?;
    let mut builder =
        ZBuilder::<Mgzip, File>::new().compression_level(Compression::new(options.level));
    if let Some(threads) = options.threads {
        builder = builder.num_threads(threads);
    }
    let mut writer = builder.from_writer(output);
    let bytes = visit_ordered_input_counted(&options.input, options.io_mode, |block| {
        writer.write_all(block).map_err(gzp_error)
    })?;
    output = writer.finish().map_err(gzp_error)?;
    output.flush()?;
    Ok(bytes)
}

fn run_decompress(options: &GzipOptions, output_path: Option<&Path>) -> io::Result<u64> {
    let mut reader = match &options.input {
        StreamInput::File(path) => {
            open_gzip_reader_from_stream(File::open(path)?, options.threads)?
        }
        StreamInput::Stdin { .. } => open_gzip_reader_from_stream(stdin_file()?, options.threads)?,
    };

    let mut sink = open_decompress_sink(output_path, options.force)?;
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; GZIP_COPY_BUFFER_SIZE];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        sink.write_all(&buffer[..read])?;
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("gzip decompressed byte count overflow"))?;
    }
    sink.finish()?;
    Ok(total)
}

fn open_output_file(path: Option<&Path>, force: bool) -> io::Result<File> {
    match path {
        Some(path) => {
            let mut options = OpenOptions::new();
            options.write(true);
            if force {
                options.create(true).truncate(true);
            } else {
                options.create_new(true);
            }
            options.open(path)
        }
        None => stdout_file(),
    }
}

fn open_decompress_sink(path: Option<&Path>, force: bool) -> io::Result<DecompressSink> {
    match path {
        Some(path) => {
            let file = open_output_file(Some(path), force)?;
            let config = load_config(None);
            let params = config.get_params("write", false);
            Ok(DecompressSink::FroFile(BufWriter::new(
                file,
                params.qd,
                params.block_size,
            )?))
        }
        None => Ok(DecompressSink::Stream(stdout_file()?)),
    }
}

fn stdout_file() -> io::Result<File> {
    let fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn stdin_file() -> io::Result<File> {
    let fd = unsafe { libc::dup(libc::STDIN_FILENO) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn gzp_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("mgzip error: {err}"))
}

fn open_gzip_reader_from_stream(reader: File, threads: Option<usize>) -> io::Result<Box<dyn Read>> {
    let (prefix, reader) = probe_gzip_prefix(reader)?;
    let is_mgzip = is_mgzip_prefix(&prefix);
    let replay = std::io::Cursor::new(prefix).chain(reader);
    if is_mgzip {
        let reader = match threads {
            Some(threads) => ParDecompressBuilder::<Mgzip>::new()
                .num_threads(threads)
                .map_err(gzp_error)?
                .from_reader(replay),
            None => ParDecompressBuilder::<Mgzip>::new().from_reader(replay),
        };
        Ok(Box::new(reader))
    } else {
        Ok(Box::new(MultiGzDecoder::new(replay)))
    }
}

fn probe_gzip_prefix(mut reader: File) -> io::Result<(Vec<u8>, File)> {
    let mut prefix = vec![0_u8; 20];
    let read = reader.read(&mut prefix)?;
    prefix.truncate(read);
    Ok((prefix, reader))
}

fn is_mgzip_prefix(prefix: &[u8]) -> bool {
    prefix.len() >= 14
        && prefix[0] == 0x1f
        && prefix[1] == 0x8b
        && prefix[2] == 8
        && prefix[3] & 4 == 4
        && prefix[12] == b'I'
        && prefix[13] == b'G'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gzip_test_temp_file(name: &str) -> PathBuf {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn default_decompressed_path_requires_gz_suffix() {
        let err = default_decompressed_path("archive").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn parse_gzip_aliases_apply_expected_defaults() {
        let gunzip = parse_gzip_options("gunzip", &["gunzip".to_string()]).unwrap();
        assert!(gunzip.decompress);
        assert!(!gunzip.stdout);

        let zcat = parse_gzip_options("zcat", &["zcat".to_string()]).unwrap();
        assert!(zcat.decompress);
        assert!(zcat.stdout);
        assert!(zcat.keep);
    }

    #[test]
    fn gzip_roundtrip_mgzip_uses_parallel_mgzip_decode_path() {
        let input = gzip_test_temp_file("fro-gzip-input");
        let compressed = gzip_test_temp_file("fro-gzip-output.gz");
        let restored = gzip_test_temp_file("fro-gzip-restored");
        let bytes = (0..(512 * 1024 + 137))
            .map(|i| ((i * 29 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&input, &bytes).unwrap();

        let compress_args = vec![
            "gzip".to_string(),
            "-k".to_string(),
            "-o".to_string(),
            compressed.display().to_string(),
            input.display().to_string(),
        ];
        assert_eq!(run_gzip("gzip", &compress_args).unwrap(), 0);

        let decompress_args = vec![
            "gunzip".to_string(),
            "-k".to_string(),
            "-o".to_string(),
            restored.display().to_string(),
            compressed.display().to_string(),
        ];
        assert_eq!(run_gzip("gunzip", &decompress_args).unwrap(), 0);
        assert_eq!(fs::read(&restored).unwrap(), bytes);

        let _ = fs::remove_file(input);
        let _ = fs::remove_file(compressed);
        let _ = fs::remove_file(restored);
    }
}
