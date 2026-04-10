use super::*;
use crate::common::IOMode;
use crate::config::load_config;
use crate::stream::transform::{
    run_mapper_transform_with_specs, MapperTransformOutput, TransformInputSpec, TransformOutputSpec,
};
use crate::stream::{ParallelFile, ParallelWriter};
use openssl::pkcs5::pbkdf2_hmac;
use openssl::rand::rand_bytes;
use openssl::symm::{Cipher, Crypter, Mode};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

const ENCRYPT_BLOCK_SIZE: usize = 512 * 1024;
const ENCRYPT_STAGED_FILE_TO_FILE_LIMIT: u64 = 2 * ENCRYPT_BLOCK_SIZE as u64;
const OPENSSL_MAGIC: &[u8; 8] = b"Salted__";
const OPENSSL_SALT_LEN: usize = 8;
const OPENSSL_HEADER_LEN: usize = OPENSSL_MAGIC.len() + OPENSSL_SALT_LEN;
const PBKDF2_ITERATIONS: usize = 10_000;
const AES_256_CTR_NAME: &str = "aes-256-ctr";

#[derive(Clone, Debug)]
struct EncryptOptions {
    passphrase_file: String,
    input: StreamInput,
    output: Option<String>,
    cipher: String,
}

#[derive(Clone, Copy)]
struct CipherSpec {
    cipher: Cipher,
    name: &'static str,
}

pub(super) fn run_encrypt(args: &[String]) -> io::Result<i32> {
    let options = match parse_encrypt_options(args, false)? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };
    run_encrypt_in_process(&options)?;
    Ok(0)
}

pub(super) fn run_decrypt(args: &[String]) -> io::Result<i32> {
    let options = match parse_encrypt_options(args, true)? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };
    run_decrypt_in_process(&options)?;
    Ok(0)
}

fn print_encrypt_help(decrypt: bool) {
    let command = if decrypt { "decrypt" } else { "encrypt" };
    let action = if decrypt { "Decrypt" } else { "Encrypt" };
    println!(
        "Usage: {command} --passphrase-file PATH [--cipher {AES_256_CTR_NAME}] [-o FILE] [INPUT]"
    );
    println!("{action} data using OpenSSL-compatible `enc -aes-256-ctr -pbkdf2 -salt` output.");
    println!();
    println!("  --passphrase-file PATH  read the passphrase from PATH");
    println!("  --cipher NAME           only {AES_256_CTR_NAME} is supported");
    println!("  -o, --output FILE       write to FILE instead of standard output");
    println!("      --help              display this help and exit");
    println!("      --version           output version information and exit");
    println!();
    println!("Notes:");
    println!("  - Regular-file inputs are processed in 512 KiB blocks with parallel workers.");
    println!("  - fro derives the OpenSSL key/IV with PBKDF2-HMAC-SHA256 (10,000 iterations).");
    println!("  - Output begins with the standard `Salted__` header plus the 8-byte salt.");
    println!("  - No trailing b3sum is emitted: appending bytes would change the ciphertext and break `openssl enc` compatibility.");
    println!("  - aes-256-ctr is unauthenticated, so a wrong passphrase may yield garbage plaintext without an explicit error.");
    println!("  - Input defaults to standard input when INPUT is omitted or is -.");
}

fn parse_encrypt_options(
    args: &[String],
    decrypt: bool,
) -> io::Result<Result<EncryptOptions, i32>> {
    let command = if decrypt { "decrypt" } else { "encrypt" };
    let mut cipher = AES_256_CTR_NAME.to_string();
    let mut passphrase_file: Option<String> = None;
    let mut output: Option<String> = None;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--help" => {
                print_encrypt_help(decrypt);
                return Ok(Err(0));
            }
            "--version" => {
                println!("{command} {}", env!("CARGO_PKG_VERSION"));
                return Ok(Err(0));
            }
            "--passphrase-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("{command}: option requires an argument -- 'passphrase-file'");
                    eprintln!("Try '{command} --help' for more information.");
                    return Ok(Err(1));
                }
                passphrase_file = Some(args[i].clone());
            }
            "--cipher" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("{command}: option requires an argument -- 'cipher'");
                    eprintln!("Try '{command} --help' for more information.");
                    return Ok(Err(1));
                }
                cipher = args[i].clone();
            }
            "-o" | "--output" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("{command}: option requires an argument -- 'output'");
                    eprintln!("Try '{command} --help' for more information.");
                    return Ok(Err(1));
                }
                output = Some(args[i].clone());
            }
            "--auto" | "--direct" | "--no-direct" => {
                eprintln!("{command}: I/O mode flags are not supported for encrypt/decrypt");
                eprintln!("Try '{command} --help' for more information.");
                return Ok(Err(1));
            }
            "-" => files.push(args[i].clone()),
            other if other.starts_with('-') => {
                eprintln!("{command}: unrecognized option '{other}'");
                eprintln!("Try '{command} --help' for more information.");
                return Ok(Err(1));
            }
            _ => files.push(args[i].clone()),
        }
        i += 1;
    }

    if files.len() > 1 {
        eprintln!("{command}: extra operand ‘{}’", files[1]);
        eprintln!("Try '{command} --help' for more information.");
        return Ok(Err(1));
    }

    let Some(passphrase_file) = passphrase_file else {
        eprintln!("{command}: missing required option '--passphrase-file'");
        eprintln!("Try '{command} --help' for more information.");
        return Ok(Err(1));
    };

    let input = parse_stream_inputs(files)
        .into_iter()
        .next()
        .unwrap_or(StreamInput::Stdin { label: None });
    Ok(Ok(EncryptOptions {
        passphrase_file,
        input,
        output,
        cipher,
    }))
}

fn run_encrypt_in_process(options: &EncryptOptions) -> io::Result<()> {
    let cipher = resolve_cipher_spec(&options.cipher)?;
    let passphrase = load_passphrase(&options.passphrase_file)?;
    let salt = random_salt()?;
    let header = openssl_header(&salt);

    run_mapper_transform_with_specs(
        transform_input_spec(&options.input),
        transform_output_spec(options.output.as_deref()),
        |input_path, output| {
            encrypt_regular_path(&input_path, output, cipher, &passphrase, salt, header)
        },
        |mut input, mut output| {
            output.as_file_mut().write_all(&header)?;
            encrypt_reader(
                &mut input,
                output.as_file_mut(),
                cipher,
                &passphrase,
                salt,
                0,
            )?;
            output.as_file_mut().flush()
        },
    )
}

fn run_decrypt_in_process(options: &EncryptOptions) -> io::Result<()> {
    let cipher = resolve_cipher_spec(&options.cipher)?;
    let passphrase = load_passphrase(&options.passphrase_file)?;

    run_mapper_transform_with_specs(
        transform_input_spec(&options.input),
        transform_output_spec(options.output.as_deref()),
        |input_path, output| decrypt_regular_path(&input_path, output, cipher, &passphrase),
        |mut input, mut output| {
            let salt = read_openssl_header(&mut input)?;
            decrypt_reader(
                &mut input,
                output.as_file_mut(),
                cipher,
                &passphrase,
                salt,
                0,
            )?;
            output.as_file_mut().flush()
        },
    )
}

fn resolve_cipher_spec(name: &str) -> io::Result<CipherSpec> {
    if name == AES_256_CTR_NAME {
        Ok(CipherSpec {
            cipher: Cipher::aes_256_ctr(),
            name: AES_256_CTR_NAME,
        })
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported cipher '{name}' (supported: {AES_256_CTR_NAME})"),
        ))
    }
}

fn random_salt() -> io::Result<[u8; OPENSSL_SALT_LEN]> {
    let mut salt = [0u8; OPENSSL_SALT_LEN];
    rand_bytes(&mut salt).map_err(io::Error::other)?;
    Ok(salt)
}

fn openssl_header(salt: &[u8; OPENSSL_SALT_LEN]) -> [u8; OPENSSL_HEADER_LEN] {
    let mut header = [0u8; OPENSSL_HEADER_LEN];
    header[..OPENSSL_MAGIC.len()].copy_from_slice(OPENSSL_MAGIC);
    header[OPENSSL_MAGIC.len()..].copy_from_slice(salt);
    header
}

fn read_openssl_header<R: Read>(reader: &mut R) -> io::Result<[u8; OPENSSL_SALT_LEN]> {
    let mut header = [0u8; OPENSSL_HEADER_LEN];
    reader.read_exact(&mut header)?;
    parse_openssl_header(&header)
}

fn parse_openssl_header(header: &[u8; OPENSSL_HEADER_LEN]) -> io::Result<[u8; OPENSSL_SALT_LEN]> {
    if &header[..OPENSSL_MAGIC.len()] != OPENSSL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported ciphertext format: expected OpenSSL salted header",
        ));
    }
    let mut salt = [0u8; OPENSSL_SALT_LEN];
    salt.copy_from_slice(&header[OPENSSL_MAGIC.len()..]);
    Ok(salt)
}

fn load_passphrase(path: &str) -> io::Result<Vec<u8>> {
    let mut passphrase = fs::read(path)?;
    while matches!(passphrase.last(), Some(b'\n' | b'\r')) {
        passphrase.pop();
    }
    if passphrase.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "passphrase file must not be empty",
        ));
    }
    Ok(passphrase)
}

fn transform_input_spec(input: &StreamInput) -> TransformInputSpec<'_> {
    match input {
        StreamInput::File(path) => TransformInputSpec::Path(path),
        StreamInput::Stdin { .. } => TransformInputSpec::Stdin,
    }
}

fn transform_output_spec(output: Option<&str>) -> TransformOutputSpec<'_> {
    match output.filter(|path| *path != "-") {
        Some(path) => TransformOutputSpec::Path(path),
        None => TransformOutputSpec::Stdout,
    }
}

fn derive_key_iv(
    cipher: CipherSpec,
    passphrase: &[u8],
    salt: &[u8; OPENSSL_SALT_LEN],
) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let key_len = cipher.cipher.key_len();
    let iv_len = cipher.cipher.iv_len().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "cipher '{}' requires no IV and is unsupported here",
                cipher.name
            ),
        )
    })?;
    let mut material = vec![0u8; key_len + iv_len];
    pbkdf2_hmac(
        passphrase,
        salt,
        PBKDF2_ITERATIONS,
        openssl::hash::MessageDigest::sha256(),
        &mut material,
    )
    .map_err(io::Error::other)?;
    Ok((material[..key_len].to_vec(), material[key_len..].to_vec()))
}

fn encrypt_regular_path(
    path: &str,
    output: MapperTransformOutput,
    cipher: CipherSpec,
    passphrase: &[u8],
    salt: [u8; OPENSSL_SALT_LEN],
    header: [u8; OPENSSL_HEADER_LEN],
) -> io::Result<()> {
    let config = parallel_config_for_path(path);
    let input = ParallelFile::open(&config, "compute", path, IOMode::Auto)?;
    let block_count = input.block_count(ENCRYPT_BLOCK_SIZE as u64)?;
    let total_blocks = block_count
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "block count overflow"))?;
    let material = Arc::new(derive_key_iv(cipher, passphrase, &salt)?);

    match output {
        MapperTransformOutput::RegularFile(file) => {
            if should_use_staged_encrypt_file_to_file(path)? {
                return encrypt_small_regular_path(path, file, cipher, material.as_ref(), header);
            }
            let writer =
                ParallelWriter::indexed_file(&config, "write", &file, IOMode::Auto, total_blocks)?;
            writer.write_at_index(0, header.to_vec())?;
            let writer_for_blocks = writer.clone();
            let material_for_blocks = Arc::clone(&material);
            let read_result = input.foreach_block_parallel(
                ENCRYPT_BLOCK_SIZE as u64,
                move |chunk_index, raw_bytes| {
                    let ciphertext = crypt_parallel_chunk(
                        cipher,
                        Mode::Encrypt,
                        material_for_blocks.as_ref(),
                        chunk_index,
                        raw_bytes,
                    )?;
                    writer_for_blocks.write_at_index(chunk_index + 1, ciphertext)
                },
            );
            let finish_result = writer.finish();
            read_result?;
            finish_result?;
        }
        MapperTransformOutput::Stream(file) => {
            let writer = ParallelWriter::indexed_pipe(
                file.as_raw_fd(),
                total_blocks,
                ENCRYPT_BLOCK_SIZE as u64,
            )?;
            writer.write_at_index(0, header.to_vec())?;
            let writer_for_blocks = writer.clone();
            let material_for_blocks = Arc::clone(&material);
            let read_result = input.foreach_block_parallel(
                ENCRYPT_BLOCK_SIZE as u64,
                move |chunk_index, raw_bytes| {
                    let ciphertext = crypt_parallel_chunk(
                        cipher,
                        Mode::Encrypt,
                        material_for_blocks.as_ref(),
                        chunk_index,
                        raw_bytes,
                    )?;
                    writer_for_blocks.write_at_index(chunk_index + 1, ciphertext)
                },
            );
            let finish_result = writer.finish();
            read_result?;
            finish_result?;
        }
    }
    Ok(())
}

fn decrypt_regular_path(
    path: &str,
    output: MapperTransformOutput,
    cipher: CipherSpec,
    passphrase: &[u8],
) -> io::Result<()> {
    match output {
        MapperTransformOutput::RegularFile(file) => {
            if should_use_staged_decrypt_file_to_file(path)? {
                return decrypt_small_regular_path(path, file, cipher, passphrase);
            }
            decrypt_parallel_path(
                path,
                MapperTransformOutput::RegularFile(file),
                cipher,
                passphrase,
            )
        }
        MapperTransformOutput::Stream(file) => decrypt_parallel_path(
            path,
            MapperTransformOutput::Stream(file),
            cipher,
            passphrase,
        ),
    }
}

fn should_use_staged_encrypt_file_to_file(path: &str) -> io::Result<bool> {
    staged_file_to_file_len(path, ENCRYPT_STAGED_FILE_TO_FILE_LIMIT)
}

fn should_use_staged_decrypt_file_to_file(path: &str) -> io::Result<bool> {
    staged_file_to_file_len(
        path,
        ENCRYPT_STAGED_FILE_TO_FILE_LIMIT + OPENSSL_HEADER_LEN as u64,
    )
}

fn staged_file_to_file_len(path: &str, limit: u64) -> io::Result<bool> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.is_file() && metadata.len() <= limit)
}

fn encrypt_small_regular_path(
    path: &str,
    mut dest: File,
    cipher: CipherSpec,
    material: &(Vec<u8>, Vec<u8>),
    header: [u8; OPENSSL_HEADER_LEN],
) -> io::Result<()> {
    let mut reader = File::open(path)?;
    dest.write_all(&header)?;
    process_reader_stream(&mut reader, &mut dest, cipher, material, Mode::Encrypt, 0)?;
    dest.flush()
}

fn decrypt_small_regular_path(
    path: &str,
    mut dest: File,
    cipher: CipherSpec,
    passphrase: &[u8],
) -> io::Result<()> {
    let mut file = File::open(path)?;
    let salt = read_openssl_header(&mut file)?;
    let material = derive_key_iv(cipher, passphrase, &salt)?;
    process_reader_stream(&mut file, &mut dest, cipher, &material, Mode::Decrypt, 0)?;
    dest.flush()
}

fn decrypt_parallel_path(
    path: &str,
    output: MapperTransformOutput,
    cipher: CipherSpec,
    passphrase: &[u8],
) -> io::Result<()> {
    let mut file = File::open(path)?;
    let salt = read_openssl_header(&mut file)?;
    let ciphertext_offset = OPENSSL_HEADER_LEN as u64;
    let ciphertext_len = file.metadata()?.len().saturating_sub(ciphertext_offset);
    let block_count = if ciphertext_len == 0 {
        0
    } else {
        ciphertext_len.div_ceil(ENCRYPT_BLOCK_SIZE as u64) as usize
    };
    let config = parallel_config_for_path(path);
    let input = ParallelFile::open(&config, "compute", path, IOMode::Auto)?;
    let material = Arc::new(derive_key_iv(cipher, passphrase, &salt)?);

    match output {
        MapperTransformOutput::RegularFile(file) => {
            let writer =
                ParallelWriter::indexed_file(&config, "write", &file, IOMode::Auto, block_count)?;
            let writer_for_blocks = writer.clone();
            let input_for_blocks = input.clone();
            let material_for_blocks = Arc::clone(&material);
            let read_result = input.foreach_index_parallel(block_count, move |index| {
                let range = ciphertext_range(index, ciphertext_len)?;
                let ciphertext =
                    input_for_blocks.read_range(ciphertext_offset + range.0, range.1)?;
                let plaintext = crypt_parallel_chunk(
                    cipher,
                    Mode::Decrypt,
                    material_for_blocks.as_ref(),
                    index,
                    &ciphertext,
                )?;
                writer_for_blocks.write_at_index(index, plaintext)
            });
            let finish_result = writer.finish();
            read_result?;
            finish_result?;
        }
        MapperTransformOutput::Stream(file) => {
            let writer = ParallelWriter::indexed_pipe(
                file.as_raw_fd(),
                block_count,
                ENCRYPT_BLOCK_SIZE as u64,
            )?;
            let writer_for_blocks = writer.clone();
            let input_for_blocks = input.clone();
            let material_for_blocks = Arc::clone(&material);
            let read_result = input.foreach_index_parallel(block_count, move |index| {
                let range = ciphertext_range(index, ciphertext_len)?;
                let ciphertext =
                    input_for_blocks.read_range(ciphertext_offset + range.0, range.1)?;
                let plaintext = crypt_parallel_chunk(
                    cipher,
                    Mode::Decrypt,
                    material_for_blocks.as_ref(),
                    index,
                    &ciphertext,
                )?;
                writer_for_blocks.write_at_index(index, plaintext)
            });
            let finish_result = writer.finish();
            read_result?;
            finish_result?;
        }
    }
    Ok(())
}

fn ciphertext_range(index: usize, ciphertext_len: u64) -> io::Result<(u64, usize)> {
    let offset = (index as u64)
        .checked_mul(ENCRYPT_BLOCK_SIZE as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ciphertext offset overflow"))?;
    if offset > ciphertext_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "ciphertext range begins past end of file",
        ));
    }
    let remaining = (ciphertext_len - offset) as usize;
    let len = remaining.min(ENCRYPT_BLOCK_SIZE);
    Ok((offset, len))
}

fn encrypt_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    cipher: CipherSpec,
    passphrase: &[u8],
    salt: [u8; OPENSSL_SALT_LEN],
    starting_block_index: usize,
) -> io::Result<()> {
    let material = derive_key_iv(cipher, passphrase, &salt)?;
    process_reader_stream(
        reader,
        writer,
        cipher,
        &material,
        Mode::Encrypt,
        starting_block_index,
    )
}

fn decrypt_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    cipher: CipherSpec,
    passphrase: &[u8],
    salt: [u8; OPENSSL_SALT_LEN],
    starting_block_index: usize,
) -> io::Result<()> {
    let material = derive_key_iv(cipher, passphrase, &salt)?;
    process_reader_stream(
        reader,
        writer,
        cipher,
        &material,
        Mode::Decrypt,
        starting_block_index,
    )
}

fn process_reader_stream<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    cipher: CipherSpec,
    material: &(Vec<u8>, Vec<u8>),
    mode: Mode,
    starting_block_index: usize,
) -> io::Result<()> {
    let mut buffer = vec![0u8; ENCRYPT_BLOCK_SIZE];
    let mut block_index = starting_block_index;
    loop {
        let read = read_chunk(reader, &mut buffer)?;
        if read.is_empty() {
            break;
        }
        let output = crypt_parallel_chunk(cipher, mode, material, block_index, read)?;
        writer.write_all(&output)?;
        block_index += 1;
    }
    Ok(())
}

fn read_chunk<'a, R: Read>(reader: &mut R, buffer: &'a mut [u8]) -> io::Result<&'a [u8]> {
    let mut filled = 0usize;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(&buffer[..filled])
}

fn crypt_parallel_chunk(
    cipher: CipherSpec,
    mode: Mode,
    material: &(Vec<u8>, Vec<u8>),
    chunk_index: usize,
    input: &[u8],
) -> io::Result<Vec<u8>> {
    let iv = ctr_iv_for_chunk(&material.1, chunk_index, ENCRYPT_BLOCK_SIZE)?;
    crypt_block(cipher, mode, &material.0, &iv, input)
}

fn ctr_iv_for_chunk(iv_seed: &[u8], chunk_index: usize, chunk_size: usize) -> io::Result<Vec<u8>> {
    if iv_seed.len() != 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "aes-256-ctr requires a 16-byte IV",
        ));
    }
    let block_advance = (chunk_index as u128)
        .checked_mul((chunk_size / 16) as u128)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ctr block index overflow"))?;
    let base = u128::from_be_bytes(iv_seed.try_into().unwrap());
    let counter = base.wrapping_add(block_advance);
    Ok(counter.to_be_bytes().to_vec())
}

fn crypt_block(
    cipher: CipherSpec,
    mode: Mode,
    key: &[u8],
    iv: &[u8],
    input: &[u8],
) -> io::Result<Vec<u8>> {
    let mut crypter = Crypter::new(cipher.cipher, mode, key, Some(iv)).map_err(io::Error::other)?;
    crypter.pad(false);
    let mut output = vec![0u8; input.len() + cipher.cipher.block_size()];
    let mut written = crypter
        .update(input, &mut output)
        .map_err(io::Error::other)?;
    written += crypter
        .finalize(&mut output[written..])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decrypt failed: corrupt input"))?;
    output.truncate(written);
    Ok(output)
}

fn parallel_config_for_path(path: &str) -> crate::config::LoadedConfig {
    let mut config = load_config(None);
    let threads = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get() as u64)
        .unwrap_or(1);
    for direct in [false, true] {
        let mut params = config.get_params_for_path("compute", direct, path);
        params.num_threads = threads;
        params.block_size = ENCRYPT_BLOCK_SIZE as u64;
        config.update_params_for_path("compute", direct, path, params);
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct PartialReader {
        inner: Cursor<Vec<u8>>,
        max_chunk: usize,
    }

    impl PartialReader {
        fn new(bytes: Vec<u8>, max_chunk: usize) -> Self {
            Self {
                inner: Cursor::new(bytes),
                max_chunk,
            }
        }
    }

    impl Read for PartialReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let len = buf.len().min(self.max_chunk);
            self.inner.read(&mut buf[..len])
        }
    }

    #[test]
    fn reader_stream_handles_partial_reads_across_encrypt_blocks() {
        let plaintext = (0..(ENCRYPT_BLOCK_SIZE + 12345))
            .map(|i| ((i * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>();
        let mut encrypted = Vec::new();
        let mut decrypted = Vec::new();
        let salt = [7u8; OPENSSL_SALT_LEN];
        let passphrase = b"partial-read-regression";
        let mut encrypt_reader_src = PartialReader::new(plaintext.clone(), 4093);

        encrypt_reader(
            &mut encrypt_reader_src,
            &mut encrypted,
            resolve_cipher_spec(AES_256_CTR_NAME).unwrap(),
            passphrase,
            salt,
            0,
        )
        .unwrap();

        let mut decrypt_reader_src = PartialReader::new(encrypted, 3079);
        decrypt_reader(
            &mut decrypt_reader_src,
            &mut decrypted,
            resolve_cipher_spec(AES_256_CTR_NAME).unwrap(),
            passphrase,
            salt,
            0,
        )
        .unwrap();

        assert_eq!(decrypted, plaintext);
    }
}
