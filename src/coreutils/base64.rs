use super::*;
use crate::stream::ParallelStream;
use crate::writer::BufWriter;
use std::fs::File;
use std::hint::black_box;
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::time::Instant;

const BASE64_ENCODE: [u8; 64] =
    *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const BASE64_DEFAULT_WRAP: usize = 76;
const BASE64_ENCODE_INPUT_ALIGN: u64 = 3 * 4096;
const BASE64_BENCH_INPUT_SIZE: usize = 12 * 1024;
const BASE64_BENCH_OUTPUT_SIZE: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Base64EncodeKernel {
    Auto,
    Scalar,
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    Avx2Spmd,
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    Avx2Shuffle,
}

impl Base64EncodeKernel {
    fn bench_name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Scalar => "scalar",
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Avx2Spmd => "avx2-spmd",
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Avx2Shuffle => "avx2-shuffle",
        }
    }
}

#[cfg(target_arch = "x86")]
type M128i = std::arch::x86::__m128i;
#[cfg(target_arch = "x86")]
type M256i = std::arch::x86::__m256i;
#[cfg(target_arch = "x86_64")]
type M128i = std::arch::x86_64::__m128i;
#[cfg(target_arch = "x86_64")]
type M256i = std::arch::x86_64::__m256i;

#[derive(Clone)]
struct Base64Options {
    decode: bool,
    ignore_garbage: bool,
    wrap_cols: usize,
    io_mode: IOMode,
    input: StreamInput,
}

fn set_stdout_direct() -> io::Result<bool> {
    let fd = io::stdout().as_raw_fd();

    unsafe {
        // 1. Check if stdout is a regular file
        let mut stat: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut stat) != 0 {
            return Err(io::Error::last_os_error());
        }

        // S_IFMT is the bit mask for the file type bit fields
        if (stat.st_mode & libc::S_IFMT) == libc::S_IFREG {
            // 2. Get current flags
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags == -1 {
                return Err(io::Error::last_os_error());
            }

            // 3. Set O_DIRECT flag
            if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_DIRECT) == -1 {
                return Err(io::Error::last_os_error());
            }
            return Ok(true);
        } else {
            return Ok(false);
        }
    }
}

fn parse_base64_options(args: &[String]) -> io::Result<Result<Base64Options, i32>> {
    let mut decode = false;
    let mut ignore_garbage = false;
    let mut wrap_cols = BASE64_DEFAULT_WRAP;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-d" | "--decode" => decode = true,
            "-i" | "--ignore-garbage" => ignore_garbage = true,
            "-w" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("base64: option requires an argument -- 'w'");
                    eprintln!("Try 'base64 --help' for more information.");
                    return Ok(Err(1));
                }
                wrap_cols = parse_base64_wrap(&args[i]).map_err(|message| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("base64: {message}"))
                })?;
            }
            other if other.starts_with("--wrap=") => {
                wrap_cols = parse_base64_wrap(&other["--wrap=".len()..]).map_err(|message| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("base64: {message}"))
                })?;
            }
            "-" => files.push(args[i].clone()),
            other if other.starts_with('-') => {
                eprintln!("base64: unrecognized option '{other}'");
                eprintln!("Try 'base64 --help' for more information.");
                return Ok(Err(1));
            }
            _ => files.push(args[i].clone()),
        }
        i += 1;
    }

    if files.len() > 1 {
        eprintln!("base64: extra operand ‘{}’", files[1]);
        eprintln!("Try 'base64 --help' for more information.");
        return Ok(Err(1));
    }

    let input = parse_stream_inputs(files)
        .into_iter()
        .next()
        .unwrap_or(StreamInput::Stdin { label: None });
    Ok(Ok(Base64Options {
        decode,
        ignore_garbage,
        wrap_cols,
        io_mode,
        input,
    }))
}

fn parse_base64_wrap(value: &str) -> Result<usize, &'static str> {
    value.parse::<usize>().map_err(|_| "invalid wrap size")
}

fn base64_decode_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn base64_is_ignored_decode_byte(byte: u8) -> bool {
    matches!(byte, b'\n' | b'\r')
}

fn base64_decoded_len_from_padding(padding: u8) -> Option<usize> {
    match padding {
        0 => Some(3),
        1 => Some(2),
        2 => Some(1),
        _ => None,
    }
}

fn base64_parallel_encode_block_size(block_size: u64) -> u64 {
    let rounded = (block_size / BASE64_ENCODE_INPUT_ALIGN) * BASE64_ENCODE_INPUT_ALIGN;
    if rounded == 0 {
        BASE64_ENCODE_INPUT_ALIGN
    } else {
        rounded
    }
}

fn encoded_base64_len(input_len: usize) -> usize {
    input_len.div_ceil(3) * 4
}

pub(crate) fn parse_base64_encode_kernel(value: &str) -> io::Result<Base64EncodeKernel> {
    match value {
        "auto" => Ok(Base64EncodeKernel::Auto),
        "scalar" => Ok(Base64EncodeKernel::Scalar),
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        "spmd" | "avx2-spmd" => Ok(Base64EncodeKernel::Avx2Spmd),
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        "shuffle" | "avx2-shuffle" => Ok(Base64EncodeKernel::Avx2Shuffle),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown base64 kernel variant '{value}'"),
        )),
    }
}

fn encode_base64_kernel_for_block(
    bytes_len: usize,
    requested: Base64EncodeKernel,
) -> Base64EncodeKernel {
    if bytes_len < 24 {
        return Base64EncodeKernel::Scalar;
    }
    match requested {
        Base64EncodeKernel::Auto => {
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            if std::arch::is_x86_feature_detected!("avx2") {
                return Base64EncodeKernel::Avx2Shuffle;
            }
            Base64EncodeKernel::Scalar
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64EncodeKernel::Avx2Spmd | Base64EncodeKernel::Avx2Shuffle => {
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            if std::arch::is_x86_feature_detected!("avx2") {
                return requested;
            }
            Base64EncodeKernel::Scalar
        }
        Base64EncodeKernel::Scalar => Base64EncodeKernel::Scalar,
    }
}

#[cfg(any(test, kani))]
fn base64_encode_ascii_scalar_glsl(sextet: u8) -> u8 {
    let mut off = 65_i16;
    if sextet > 25 {
        off = 71;
    }
    if sextet > 51 {
        off = -4;
    }
    if sextet == 62 {
        off = 43 - i16::from(sextet);
    }
    if sextet == 63 {
        off = 47 - i16::from(sextet);
    }
    (i16::from(sextet) + off) as u8
}

fn encode_base64_block(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
    let written = encode_base64_block_into(bytes, &mut out);
    out.truncate(written);
    out
}

fn encode_base64_block_into(bytes: &[u8], out: &mut [u8]) -> usize {
    encode_base64_block_into_with_kernel(bytes, out, Base64EncodeKernel::Auto)
}

fn encode_base64_block_into_with_kernel(
    bytes: &[u8],
    out: &mut [u8],
    requested: Base64EncodeKernel,
) -> usize {
    let requested = encode_base64_kernel_for_block(bytes.len(), requested);
    let expected = encoded_base64_len(bytes.len());
    assert!(out.len() >= expected);
    match encode_base64_kernel_for_block(bytes.len(), requested) {
        Base64EncodeKernel::Scalar | Base64EncodeKernel::Auto => {
            encode_base64_block_into_scalar(bytes, out)
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64EncodeKernel::Avx2Spmd => {
            // SAFETY: guarded by runtime AVX2 detection.
            unsafe { encode_base64_block_into_avx2_spmd(bytes, out) }
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64EncodeKernel::Avx2Shuffle => {
            // SAFETY: guarded by runtime AVX2 detection.
            unsafe { encode_base64_block_into_avx2_shuffle(bytes, out) }
        }
    }
}

#[cfg(any(test, kani))]
fn encode_base64_block_scalar(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
    let written = encode_base64_block_into_scalar(bytes, &mut out);
    out.truncate(written);
    out
}

fn encode_base64_block_into_scalar(bytes: &[u8], out: &mut [u8]) -> usize {
    let mut index = 0usize;
    let mut out_index = 0usize;
    while index + 3 <= bytes.len() {
        let a = bytes[index];
        let b = bytes[index + 1];
        let c = bytes[index + 2];
        out[out_index] = BASE64_ENCODE[(a >> 2) as usize];
        out[out_index + 1] = BASE64_ENCODE[(((a & 0x03) << 4) | (b >> 4)) as usize];
        out[out_index + 2] = BASE64_ENCODE[(((b & 0x0f) << 2) | (c >> 6)) as usize];
        out[out_index + 3] = BASE64_ENCODE[(c & 0x3f) as usize];
        index += 3;
        out_index += 4;
    }
    match bytes.len() - index {
        0 => {}
        1 => {
            let a = bytes[index];
            out[out_index] = BASE64_ENCODE[(a >> 2) as usize];
            out[out_index + 1] = BASE64_ENCODE[((a & 0x03) << 4) as usize];
            out[out_index + 2] = b'=';
            out[out_index + 3] = b'=';
        }
        2 => {
            let a = bytes[index];
            let b = bytes[index + 1];
            out[out_index] = BASE64_ENCODE[(a >> 2) as usize];
            out[out_index + 1] = BASE64_ENCODE[(((a & 0x03) << 4) | (b >> 4)) as usize];
            out[out_index + 2] = BASE64_ENCODE[((b & 0x0f) << 2) as usize];
            out[out_index + 3] = b'=';
        }
        _ => unreachable!(),
    }
    encoded_base64_len(bytes.len())
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn encode_base64_ascii_avx2(values: M256i) -> M256i {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        _mm256_add_epi8, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, _mm256_set1_epi8,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        _mm256_add_epi8, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, _mm256_set1_epi8,
    };
    let mut encoded = _mm256_add_epi8(values, _mm256_set1_epi8(65));

    let gt25 = _mm256_cmpgt_epi8(values, _mm256_set1_epi8(25));
    encoded = _mm256_add_epi8(encoded, _mm256_and_si256(gt25, _mm256_set1_epi8(6)));

    let gt51 = _mm256_cmpgt_epi8(values, _mm256_set1_epi8(51));
    encoded = _mm256_add_epi8(encoded, _mm256_and_si256(gt51, _mm256_set1_epi8(-75)));

    let eq62 = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(62));
    encoded = _mm256_add_epi8(encoded, _mm256_and_si256(eq62, _mm256_set1_epi8(-15)));

    let eq63 = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(63));
    encoded = _mm256_add_epi8(encoded, _mm256_and_si256(eq63, _mm256_set1_epi8(-12)));
    encoded
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn encode_base64_block_into_avx2_spmd(bytes: &[u8], out: &mut [u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{_mm256_loadu_si256, _mm256_storeu_si256};
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{_mm256_loadu_si256, _mm256_storeu_si256};

    let mut index = 0usize;
    let mut out_index = 0usize;
    while index + 24 <= bytes.len() {
        let chunk = &bytes[index..(index + 24)];
        let mut sextets = [0_u8; 32];
        for lane in 0..8 {
            let base = lane * 3;
            let a = chunk[base];
            let b = chunk[base + 1];
            let c = chunk[base + 2];
            let out_base = lane * 4;
            sextets[out_base] = a >> 2;
            sextets[out_base + 1] = ((a & 0x03) << 4) | (b >> 4);
            sextets[out_base + 2] = ((b & 0x0f) << 2) | (c >> 6);
            sextets[out_base + 3] = c & 0x3f;
        }
        let sextets = _mm256_loadu_si256(sextets.as_ptr() as *const _);
        let encoded = encode_base64_ascii_avx2(sextets);
        _mm256_storeu_si256(out[out_index..].as_mut_ptr() as *mut _, encoded);
        index += 24;
        out_index += 32;
    }

    if index < bytes.len() {
        out_index += encode_base64_block_into_scalar(&bytes[index..], &mut out[out_index..]);
    }
    out_index
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn encode_base64_block_into_avx2_shuffle(bytes: &[u8], out: &mut [u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        _mm256_and_si256, _mm256_loadu_si256, _mm256_mulhi_epu16, _mm256_mullo_epi16,
        _mm256_or_si256, _mm256_set1_epi32, _mm256_set_m128i, _mm256_shuffle_epi8,
        _mm256_storeu_si256, _mm_loadu_si128,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        _mm256_and_si256, _mm256_loadu_si256, _mm256_mulhi_epu16, _mm256_mullo_epi16,
        _mm256_or_si256, _mm256_set1_epi32, _mm256_set_m128i, _mm256_shuffle_epi8,
        _mm256_storeu_si256, _mm_loadu_si128,
    };

    const RESHUFFLE: [i8; 32] = [
        1, 0, 2, 1, 4, 3, 5, 4, 7, 6, 8, 7, 10, 9, 11, 10, 5, 4, 6, 5, 8, 7, 9, 8, 11, 10, 12, 11,
        14, 13, 15, 14,
    ];

    let reshuffle = _mm256_loadu_si256(RESHUFFLE.as_ptr() as *const _);
    let hi_mask = _mm256_set1_epi32(0x0fc0fc00_u32 as i32);
    let lo_mask = _mm256_set1_epi32(0x003f03f0_u32 as i32);
    let hi_mul = _mm256_set1_epi32(0x04000040_u32 as i32);
    let lo_mul = _mm256_set1_epi32(0x01000010_u32 as i32);

    let mut index = 0usize;
    let mut out_index = 0usize;
    while index + 24 <= bytes.len() {
        let chunk = bytes.as_ptr().add(index);
        let lo = _mm_loadu_si128(chunk as *const M128i);
        let hi = _mm_loadu_si128(chunk.add(8) as *const M128i);
        let packed = _mm256_set_m128i(hi, lo);
        let unpacked = _mm256_shuffle_epi8(packed, reshuffle);
        let hi_bits = _mm256_mulhi_epu16(_mm256_and_si256(unpacked, hi_mask), hi_mul);
        let lo_bits = _mm256_mullo_epi16(_mm256_and_si256(unpacked, lo_mask), lo_mul);
        let sextets = _mm256_or_si256(hi_bits, lo_bits);
        let encoded = encode_base64_ascii_avx2(sextets);
        _mm256_storeu_si256(out[out_index..].as_mut_ptr() as *mut _, encoded);
        index += 24;
        out_index += 32;
    }

    if index < bytes.len() {
        out_index += encode_base64_block_into_scalar(&bytes[index..], &mut out[out_index..]);
    }
    out_index
}

fn encode_base64_triplet(bytes: &[u8]) -> [u8; 4] {
    let a = bytes[0];
    let b = *bytes.get(1).unwrap_or(&0);
    let c = *bytes.get(2).unwrap_or(&0);
    let mut out = [b'='; 4];
    out[0] = BASE64_ENCODE[(a >> 2) as usize];
    out[1] = BASE64_ENCODE[(((a & 0x03) << 4) | (b >> 4)) as usize];
    if bytes.len() > 1 {
        out[2] = BASE64_ENCODE[(((b & 0x0f) << 2) | (c >> 6)) as usize];
    }
    if bytes.len() > 2 {
        out[3] = BASE64_ENCODE[(c & 0x3f) as usize];
    }
    out
}

fn decode_base64_quartet(quartet: [u8; 4]) -> Option<([u8; 3], usize)> {
    if quartet[0] == b'=' || quartet[1] == b'=' {
        return None;
    }

    let mut padding = 0u8;
    if quartet[2] == b'=' {
        if quartet[3] != b'=' {
            return None;
        }
        padding = 2;
    } else if quartet[3] == b'=' {
        padding = 1;
    }

    let decoded_len = base64_decoded_len_from_padding(padding)?;
    let x = base64_decode_value(quartet[0])?;
    let y = base64_decode_value(quartet[1])?;
    let z = if quartet[2] == b'=' {
        0
    } else {
        base64_decode_value(quartet[2])?
    };
    let w = if quartet[3] == b'=' {
        0
    } else {
        base64_decode_value(quartet[3])?
    };

    Some((
        [
            (x << 2) | (y >> 4),
            ((y & 0x0f) << 4) | (z >> 2),
            ((z & 0x03) << 6) | w,
        ],
        decoded_len,
    ))
}

fn write_base64_encoded_bytes<W: Write>(
    out: &mut W,
    bytes: &[u8],
    wrap_cols: usize,
    current_line_len: &mut usize,
) -> io::Result<()> {
    if wrap_cols == 0 {
        return out.write_all(bytes);
    }
    let mut wrapped = Vec::with_capacity(bytes.len() + (bytes.len() / wrap_cols.max(1)) + 2);
    append_wrapped_base64_bytes(&mut wrapped, bytes, wrap_cols, current_line_len);
    out.write_all(&wrapped)
}

fn write_base64_encoded_vec(
    out: &BufWriter,
    bytes: Vec<u8>,
    wrap_cols: usize,
    current_line_len: &mut usize,
) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    if wrap_cols == 0 {
        return out.write_vec(bytes);
    }
    let mut wrapped = Vec::with_capacity(bytes.len() + (bytes.len() / wrap_cols.max(1)) + 2);
    append_wrapped_base64_bytes(&mut wrapped, &bytes, wrap_cols, current_line_len);
    out.write_vec(wrapped)
}

fn append_wrapped_base64_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    wrap_cols: usize,
    current_line_len: &mut usize,
) {
    if wrap_cols == 0 {
        out.extend_from_slice(bytes);
        return;
    }

    let mut start = 0usize;
    while start < bytes.len() {
        let available = wrap_cols.saturating_sub(*current_line_len);
        let take = available.min(bytes.len() - start);
        out.extend_from_slice(&bytes[start..(start + take)]);
        *current_line_len += take;
        start += take;
        if *current_line_len == wrap_cols {
            out.push(b'\n');
            *current_line_len = 0;
        }
    }
}

fn encode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    wrap_cols: usize,
) -> io::Result<()> {
    let mut carry = [0_u8; 3];
    let mut carry_len = 0usize;
    let mut current_line_len = 0usize;
    let mut wrote_output = false;
    visit_ordered_input(input, io_mode, |block| {
        let mut index = 0usize;
        if carry_len != 0 {
            while carry_len < 3 && index < block.len() {
                carry[carry_len] = block[index];
                carry_len += 1;
                index += 1;
            }
            if carry_len == 3 {
                let encoded = encode_base64_triplet(&carry);
                write_base64_encoded_bytes(out, &encoded, wrap_cols, &mut current_line_len)?;
                wrote_output = true;
                carry_len = 0;
            }
        }

        while index + 3 <= block.len() {
            let encoded = encode_base64_triplet(&block[index..(index + 3)]);
            write_base64_encoded_bytes(out, &encoded, wrap_cols, &mut current_line_len)?;
            wrote_output = true;
            index += 3;
        }

        let remaining = block.len() - index;
        if remaining != 0 {
            carry[..remaining].copy_from_slice(&block[index..]);
            carry_len = remaining;
        }
        Ok(())
    })?;

    if carry_len != 0 {
        let encoded = encode_base64_triplet(&carry[..carry_len]);
        write_base64_encoded_bytes(out, &encoded, wrap_cols, &mut current_line_len)?;
        wrote_output = true;
    }

    if wrap_cols != 0 && wrote_output && current_line_len != 0 {
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn encode_base64_regular_file(
    out: &BufWriter,
    path: &str,
    _io_mode: IOMode,
    wrap_cols: usize,
) -> io::Result<()> {
    // Use ParallelStream to compute per-block encoded output into a temporary file,
    // then stream the encoded blocks in order to the provided BufWriter while
    // applying the requested wrapping.
    let config = load_config(None);
    let page_cache = config.get_params_for_path("compute", false, path);
    let _direct = config.get_params_for_path("compute", true, path);
    let page_cache_block_size = base64_parallel_encode_block_size(page_cache.block_size);
    // choose the read_block size for ParallelStream
    let read_block = page_cache_block_size;
    let write_block = encoded_base64_len(read_block as usize);

    // create a unique temporary file path
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tmp_path = std::env::temp_dir()
        .join(format!("fro-base64-{}-{}", std::process::id(), nanos))
        .to_string_lossy()
        .into_owned();

    // processor: encode one read block into base64 and return produced length
    let processor = |input: &[u8], out: &mut [u8]| -> io::Result<usize> {
        Ok(encode_base64_block_into(input, out))
    };

    // Run the parallel map to produce the encoded blocks into the temp file
    let report = ParallelStream::map_file_fixed_size(
        &config,
        path,
        &tmp_path,
        read_block,
        write_block,
        processor,
    )?;

    // Stream the encoded blocks from the produced file using the reported block_ranges to avoid scanning
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    let f = File::open(&tmp_path)?;
    let mut current_line_len = 0usize;
    let mut wrote_output = false;

    for br in report.block_ranges.iter() {
        if br.len == 0 {
            continue;
        }
        let mut buf = vec![0_u8; br.len as usize];
        f.read_at(&mut buf, br.offset)?;
        wrote_output = true;
        write_base64_encoded_vec(out, buf, wrap_cols, &mut current_line_len)?;
    }

    // clean up produced file
    let _ = std::fs::remove_file(&tmp_path);

    if wrap_cols != 0 && wrote_output && current_line_len != 0 {
        out.write_all(b"\n")?;
    }
    Ok(())
}

/// Variant that writes raw encoded blocks directly into an open destination File.
/// Caller must ensure the destination file is prepared (opened with desired flags).
pub fn encode_base64_regular_file_to_file(
    dest: &std::fs::File,
    path: &str,
    _io_mode: IOMode,
    _wrap_cols: usize,
) -> io::Result<()> {
    // This variant encodes input blocks into raw base64 bytes and writes them
    // directly into the provided destination file using ParallelStream.
    let config = load_config(None);
    let page_cache = config.get_params_for_path("compute", true, path);
    let page_cache_block_size = base64_parallel_encode_block_size(page_cache.block_size);
    let read_block = page_cache_block_size;
    let write_block = encoded_base64_len(read_block as usize);

    let processor = |input: &[u8], out: &mut [u8]| -> io::Result<usize> {
        Ok(encode_base64_block_into(input, out))
    };

    let _report = ParallelStream::map_file_fixed_size_to_fd(
        &config,
        path,
        dest,
        read_block,
        write_block,
        processor,
    )?;
    Ok(())
}

fn decode_base64_input<W: Write>(
    out: &mut W,
    input: &StreamInput,
    io_mode: IOMode,
    ignore_garbage: bool,
) -> io::Result<bool> {
    let mut quartet = [0_u8; 4];
    let mut quartet_len = 0usize;
    let mut invalid = false;
    visit_ordered_input(input, io_mode, |block| {
        for &byte in block {
            if invalid {
                break;
            }
            if base64_is_ignored_decode_byte(byte) {
                continue;
            }
            if byte == b'=' || base64_decode_value(byte).is_some() {
                quartet[quartet_len] = byte;
                quartet_len += 1;
                if quartet_len == 4 {
                    let Some((decoded, decoded_len)) = decode_base64_quartet(quartet) else {
                        invalid = true;
                        break;
                    };
                    out.write_all(&decoded[..decoded_len])?;
                    quartet_len = 0;
                }
                continue;
            }
            if ignore_garbage {
                continue;
            }
            invalid = true;
            break;
        }
        Ok(())
    })?;
    Ok(invalid || quartet_len != 0)
}

pub(super) fn run_base64(args: &[String]) -> io::Result<i32> {
    let options = match parse_base64_options(args)? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };
    //eprintln!("What is going on:");

    let mut out = stdout_buf_writer()?;
    let invalid = if options.decode {
        //eprintln!("Decoding file");
        decode_base64_input(
            &mut out,
            &options.input,
            options.io_mode,
            options.ignore_garbage,
        )?
    } else if let StreamInput::File(path) = &options.input {
        if is_regular_input_path(path)? {
            if set_stdout_direct()? {
                //eprintln!("Using file_to_file mapping");
                let mut stdout = unsafe { File::from_raw_fd(io::stdout().as_raw_fd()) };
                encode_base64_regular_file_to_file(
                    &mut stdout,
                    path,
                    options.io_mode,
                    options.wrap_cols,
                )?;
            } else {
                //eprintln!("Using file to pipe mapping");
                encode_base64_regular_file(&mut out, path, options.io_mode, options.wrap_cols)?;
            }
            false
        } else {
            //eprintln!("Input isn't a regular file: Using pipe to pipe mapping");
            encode_base64_input(&mut out, &options.input, options.io_mode, options.wrap_cols)?;
            false
        }
    } else {
        //eprintln!("Using pipe to pipe mapping");
        encode_base64_input(&mut out, &options.input, options.io_mode, options.wrap_cols)?;
        false
    };
    out.into_inner()?;
    if invalid {
        eprintln!("base64: invalid input");
        return Ok(1);
    }
    Ok(0)
}

pub(crate) fn bench_base64_encode(
    iterations: u64,
    requested_kernel: Base64EncodeKernel,
) -> io::Result<()> {
    if iterations == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iterations must be greater than zero",
        ));
    }

    let input = (0..BASE64_BENCH_INPUT_SIZE)
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    let mut output = vec![0_u8; BASE64_BENCH_OUTPUT_SIZE];
    debug_assert_eq!(encoded_base64_len(input.len()), BASE64_BENCH_OUTPUT_SIZE);

    let selected_kernel = encode_base64_kernel_for_block(input.len(), requested_kernel);

    for _ in 0..1024 {
        let written = encode_base64_block_into_with_kernel(&input, &mut output, selected_kernel);
        black_box(written);
    }

    let start = Instant::now();
    let mut sink = 0_u64;
    for _ in 0..iterations {
        let written = encode_base64_block_into_with_kernel(&input, &mut output, selected_kernel);
        sink ^= u64::from(output[0]);
        sink ^= u64::from(output[written - 1]);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let iterations_per_second = iterations as f64 / elapsed;
    let gb_per_second = (iterations as f64 * BASE64_BENCH_INPUT_SIZE as f64) / elapsed / 1e9;

    println!(
        "Base64 encode kernel [{}] {} iterations of {} -> {} bytes in {:.4} s, {:.0} it/s, {:.1} GB/s per core",
        selected_kernel.bench_name(),
        iterations,
        BASE64_BENCH_INPUT_SIZE,
        BASE64_BENCH_OUTPUT_SIZE,
        elapsed,
        iterations_per_second,
        gb_per_second
    );
    black_box(sink);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_base64_triplet_matches_expected_padding() {
        assert_eq!(encode_base64_triplet(b"f"), *b"Zg==");
        assert_eq!(encode_base64_triplet(b"fo"), *b"Zm8=");
        assert_eq!(encode_base64_triplet(b"foo"), *b"Zm9v");
    }

    #[test]
    fn base64_encode_ascii_scalar_glsl_matches_table() {
        for sextet in 0_u8..64 {
            assert_eq!(
                base64_encode_ascii_scalar_glsl(sextet),
                BASE64_ENCODE[sextet as usize]
            );
        }
    }

    #[test]
    fn decode_base64_quartet_matches_expected_lengths() {
        assert_eq!(decode_base64_quartet(*b"Zg=="), Some(([b'f', 0, 0], 1)));
        assert_eq!(decode_base64_quartet(*b"Zm8="), Some(([b'f', b'o', 0], 2)));
        assert_eq!(
            decode_base64_quartet(*b"Zm9v"),
            Some(([b'f', b'o', b'o'], 3))
        );
    }

    #[test]
    fn write_base64_encoded_bytes_wraps_at_requested_columns() {
        let mut out = Vec::new();
        let mut current_line_len = 0usize;
        write_base64_encoded_bytes(&mut out, b"YWJjZGVm", 5, &mut current_line_len).unwrap();
        assert_eq!(out, b"YWJjZ\nGVm");
        assert_eq!(current_line_len, 3);
    }

    #[test]
    fn append_wrapped_base64_bytes_batches_newlines_without_tiny_writes() {
        let mut out = Vec::new();
        let mut current_line_len = 2usize;
        append_wrapped_base64_bytes(&mut out, b"ABCDEFGH", 5, &mut current_line_len);
        assert_eq!(out, b"ABC\nDEFGH\n");
        assert_eq!(current_line_len, 0);
    }

    #[test]
    fn write_base64_encoded_vec_wraps_like_slice_path() {
        let mut out = Vec::new();
        let mut current_line_len = 2usize;
        append_wrapped_base64_bytes(&mut out, b"ABCDEFGH", 5, &mut current_line_len);
        assert_eq!(out, b"ABC\nDEFGH\n");
        assert_eq!(current_line_len, 0);
    }

    #[test]
    fn base64_parallel_encode_block_size_rounds_to_3page_multiple() {
        assert_eq!(
            base64_parallel_encode_block_size(4096),
            BASE64_ENCODE_INPUT_ALIGN
        );
        assert_eq!(
            base64_parallel_encode_block_size(BASE64_ENCODE_INPUT_ALIGN),
            BASE64_ENCODE_INPUT_ALIGN
        );
        assert_eq!(
            base64_parallel_encode_block_size((10 * 4096) + 17),
            3 * BASE64_ENCODE_INPUT_ALIGN
        );
    }

    #[test]
    fn encode_base64_block_matches_known_output() {
        assert_eq!(encode_base64_block(b""), b"");
        assert_eq!(encode_base64_block(b"foobar"), b"Zm9vYmFy");
        assert_eq!(encode_base64_block(b"fooba"), b"Zm9vYmE=");
    }

    #[test]
    fn encode_base64_block_into_matches_known_output() {
        let mut out = [0_u8; 8];
        let written = encode_base64_block_into(b"foobar", &mut out);
        assert_eq!(written, 8);
        assert_eq!(&out[..written], b"Zm9vYmFy");
    }

    #[test]
    fn encode_base64_block_scalar_matches_known_output() {
        assert_eq!(encode_base64_block_scalar(b""), b"");
        assert_eq!(encode_base64_block_scalar(b"foobar"), b"Zm9vYmFy");
        assert_eq!(encode_base64_block_scalar(b"fooba"), b"Zm9vYmE=");
    }

    #[test]
    fn base64_avx2_block_matches_scalar_block() {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if !std::arch::is_x86_feature_detected!("avx2") {
                return;
            }
            let bytes = (0..(24 * 7 + 5))
                .map(|i| ((i * 29 + 7) % 251) as u8)
                .collect::<Vec<_>>();
            let scalar = encode_base64_block_scalar(&bytes);
            let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
            let written = unsafe { encode_base64_block_into_avx2_spmd(&bytes, &mut out) };
            out.truncate(written);
            assert_eq!(out, scalar);

            let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
            let written = unsafe { encode_base64_block_into_avx2_shuffle(&bytes, &mut out) };
            out.truncate(written);
            assert_eq!(out, scalar);
        }
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    #[kani::proof]
    fn decoded_len_from_padding_only_accepts_zero_to_two() {
        let padding: u8 = kani::any();
        let result = base64_decoded_len_from_padding(padding);
        if padding <= 2 {
            assert!(matches!(result, Some(1..=3)));
        } else {
            assert!(result.is_none());
        }
    }

    #[kani::proof]
    fn parallel_encode_block_size_stays_aligned() {
        let block_size: u64 = kani::any();
        let result = base64_parallel_encode_block_size(block_size);
        assert!(result >= BASE64_ENCODE_INPUT_ALIGN);
        assert_eq!(result % BASE64_ENCODE_INPUT_ALIGN, 0);
    }

    #[kani::proof]
    fn base64_encode_ascii_scalar_glsl_matches_table() {
        let sextet: u8 = kani::any();
        kani::assume(sextet < 64);
        assert_eq!(
            base64_encode_ascii_scalar_glsl(sextet),
            BASE64_ENCODE[sextet as usize]
        );
    }
}
