use super::*;
use std::alloc::{alloc, handle_alloc_error, Layout};
use std::fs::File;
use std::hint::black_box;
use std::io::{self, Read, Write};
use std::os::unix::io::FromRawFd;
use std::time::Instant;

const BASE64_ENCODE: [u8; 64] =
    *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const BASE64_DEFAULT_WRAP: usize = 76;
const BASE64_ENCODE_INPUT_ALIGN: u64 = 3 * 4096;
const BASE64_ENCODE_FAST_READ_BLOCK_SIZE: u64 = 768 * 1024;
const BASE64_ENCODE_FAST_WRITE_BLOCK_SIZE: usize = 1024 * 1024;
const BASE64_ENCODE_FILE_PIPE_READ_BLOCK_SIZE: u64 = 1_572_864;
const BASE64_ENCODE_FILE_PIPE_WRITE_BLOCK_SIZE: usize = 2 * 1024 * 1024;
const BASE64_DECODE_FILE_PIPE_READ_BLOCK_SIZE: u64 = 2 * 1024 * 1024;
const BASE64_DECODE_FILE_PIPE_WRITE_BLOCK_SIZE: usize = 1_572_864;
const BASE64_DECODE_FAST_READ_BLOCK_SIZE: u64 = 1024 * 1024;
const BASE64_DECODE_FAST_WRITE_BLOCK_SIZE: usize = 768 * 1024;
const BASE64_STAGED_FILE_TO_FILE_LIMIT: u64 = 2 * 1024 * 1024;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Base64DecodeKernel {
    Auto,
    Scalar,
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    Avx2,
}

impl Base64DecodeKernel {
    fn bench_name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Scalar => "scalar",
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            Self::Avx2 => "avx2",
        }
    }
}

struct Base64ProcessPlan {
    read_block_size: u64,
    write_block_size: usize,
    file_pipe_read_block_size: u64,
    file_pipe_write_block_size: usize,
    pipe_file_read_block_size: u64,
    pipe_file_write_block_size: usize,
    pipe_input_read_block_size: u64,
    pipe_input_write_block_size: usize,
    input_chunk_multiple: usize,
    process_chunk: fn(&[u8], &mut [u8]) -> io::Result<usize>,
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
    report_gbps: bool,
    wrap_cols: usize,
    io_mode: IOMode,
    input: StreamInput,
}

mod bench;
#[cfg(kani)]
mod kani_proofs;
mod process;
mod process_layout;
#[cfg(test)]
mod tests;

pub(super) fn run_base64(args: &[String]) -> io::Result<i32> {
    process::run_base64(args)
}

pub(crate) fn bench_base64_wrapped_encode(iterations: u64, wrap_cols: usize) -> io::Result<()> {
    bench::bench_base64_wrapped_encode(iterations, wrap_cols)
}

pub(crate) fn bench_base64_wrapped_decode(iterations: u64, ignore_garbage: bool) -> io::Result<()> {
    bench::bench_base64_wrapped_decode(iterations, ignore_garbage)
}

pub(crate) fn bench_base64_decode_detect_fallback(
    iterations: u64,
    kernel: Base64DecodeKernel,
) -> io::Result<()> {
    bench::bench_base64_decode_detect_fallback(iterations, kernel)
}

fn print_base64_help() {
    println!("Usage: base64 [OPTION]... [FILE]");
    println!("Base64 encode or decode FILE, or standard input, to standard output.");
    println!();
    println!("  -d, --decode          decode data");
    println!("  -i, --ignore-garbage  when decoding, ignore non-alphabet characters");
    println!(
        "  -w, --wrap=COLS       wrap encoded lines after COLS characters (default 76, 0 disables)"
    );
    println!("      --auto            choose I/O mode automatically");
    println!("      --direct          force direct I/O where supported");
    println!("      --no-direct       force page-cache I/O");
    println!("      --help            display this help and exit");
    println!("      --version         output version information and exit");
}

fn parse_base64_options(args: &[String]) -> io::Result<Result<Base64Options, i32>> {
    let mut decode = false;
    let mut ignore_garbage = false;
    let mut report_gbps = false;
    let mut wrap_cols = BASE64_DEFAULT_WRAP;
    let mut io_mode = IOMode::Auto;
    let mut files = Vec::new();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--help" => {
                print_base64_help();
                return Ok(Err(0));
            }
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--report-gbps" => report_gbps = true,
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
            "--wrap" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("base64: option '--wrap' requires an argument");
                    eprintln!("Try 'base64 --help' for more information.");
                    return Ok(Err(1));
                }
                wrap_cols = parse_base64_wrap(&args[i]).map_err(|message| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("base64: {message}"))
                })?;
            }
            other if other.starts_with("-w") && other.len() > 2 => {
                wrap_cols = parse_base64_wrap(&other[2..]).map_err(|message| {
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
        report_gbps,
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

fn decode_base64_kernel_for_block(
    bytes_len: usize,
    requested: Base64DecodeKernel,
) -> Base64DecodeKernel {
    if bytes_len < 32 {
        return Base64DecodeKernel::Scalar;
    }
    match requested {
        Base64DecodeKernel::Auto => {
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            if std::arch::is_x86_feature_detected!("avx2") {
                return Base64DecodeKernel::Avx2;
            }
            Base64DecodeKernel::Scalar
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64DecodeKernel::Avx2 => {
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            if std::arch::is_x86_feature_detected!("avx2") {
                return requested;
            }
            Base64DecodeKernel::Scalar
        }
        Base64DecodeKernel::Scalar => Base64DecodeKernel::Scalar,
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

pub(crate) fn parse_base64_decode_kernel(value: &str) -> io::Result<Base64DecodeKernel> {
    match value {
        "auto" => Ok(Base64DecodeKernel::Auto),
        "scalar" => Ok(Base64DecodeKernel::Scalar),
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        "avx2" => Ok(Base64DecodeKernel::Avx2),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown base64 decode kernel variant '{value}'"),
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

#[allow(unused)]
pub fn encode_base64_block(bytes: &[u8]) -> Vec<u8> {
    let len = encoded_base64_len(bytes.len());
    let mut out = vec![0u8; len];
    let written = encode_base64_block_into(bytes, &mut out);
    out.truncate(written);
    out
}

#[allow(unused)]
pub fn encode_base64_block_aligned_small(bytes: &[u8]) -> Vec<u8> {
    let len = encoded_base64_len(bytes.len());
    let alignment = 4096;

    // 1. Create a layout with your specific alignment
    let layout =
        Layout::from_size_align(len, alignment).expect("Invalid layout: size or alignment issue");

    // 2. Allocate directly from the global allocator
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        handle_alloc_error(layout);
    }

    // 3. Convert the raw pointer into a Vec<u8>
    // SAFETY: Vec requires the pointer to have been allocated
    // with the same layout (size and alignment).
    let mut out_vec = unsafe { Vec::from_raw_parts(ptr, len, len) };

    // 4. Encode directly into the aligned Vec
    let written = encode_base64_block_into(bytes, &mut out_vec);

    // Optional: truncate if needed, though usually len is exact for base64
    out_vec.truncate(written);
    out_vec
}

#[allow(unused)]
pub fn encode_base64_block_aligned(bytes: &[u8]) -> Vec<u8> {
    let len = encoded_base64_len(bytes.len());
    let huge_page_size = 2 * 1024 * 1024;
    let capacity = (len + huge_page_size - 1) & !(huge_page_size - 1);
    let layout =
        Layout::from_size_align(capacity, huge_page_size).expect("Invalid layout for huge pages");

    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        handle_alloc_error(layout);
    }

    unsafe {
        let _ret = libc::madvise(ptr as *mut libc::c_void, capacity, libc::MADV_HUGEPAGE);
    }

    let mut out_vec = unsafe { Vec::from_raw_parts(ptr, capacity, capacity) };
    let written = encode_base64_block_into(bytes, &mut out_vec);
    unsafe {
        out_vec.set_len(written);
    }
    out_vec
}

fn encode_base64_block_into(bytes: &[u8], out: &mut [u8]) -> usize {
    encode_base64_block_into_with_kernel(bytes, out, Base64EncodeKernel::Auto)
}

fn encode_base64_block_processor(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    Ok(encode_base64_block_into(bytes, out))
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

fn decode_base64_block_into_scalar(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    let mut in_index = 0usize;
    let mut out_index = 0usize;
    while in_index < bytes.len() {
        if in_index + 4 > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid base64 input length",
            ));
        }
        let quartet = [
            bytes[in_index],
            bytes[in_index + 1],
            bytes[in_index + 2],
            bytes[in_index + 3],
        ];
        let Some((decoded, decoded_len)) = decode_base64_quartet(quartet) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid base64 quartet",
            ));
        };
        if out_index + decoded_len > out.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "decode output buffer too small",
            ));
        }
        out[out_index..out_index + decoded_len].copy_from_slice(&decoded[..decoded_len]);
        out_index += decoded_len;
        in_index += 4;
        if quartet[2] == b'=' || quartet[3] == b'=' {
            if in_index != bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "padding must terminate base64 stream",
                ));
            }
        }
    }
    Ok(out_index)
}

enum Base64DecodeFastOutcome {
    Decoded(usize),
    NeedsFallback,
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn decode_base64_block_into_avx2(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    match decode_base64_block_into_avx2_with_fused_fallback(bytes, out)? {
        Base64DecodeFastOutcome::Decoded(written) => Ok(written),
        Base64DecodeFastOutcome::NeedsFallback => decode_base64_block_into_scalar(bytes, out),
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn decode_base64_block_into_avx2_with_fused_fallback(
    bytes: &[u8],
    out: &mut [u8],
) -> io::Result<Base64DecodeFastOutcome> {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{
        _mm256_and_si256, _mm256_castsi256_si128, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8,
        _mm256_extract_epi32, _mm256_extracti128_si256, _mm256_loadu_si256, _mm256_madd_epi16,
        _mm256_maddubs_epi16, _mm256_movemask_epi8, _mm256_or_si256, _mm256_set1_epi32,
        _mm256_set1_epi8, _mm256_shuffle_epi8, _mm256_sub_epi8, _mm_storel_epi64,
    };
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        _mm256_and_si256, _mm256_castsi256_si128, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8,
        _mm256_extract_epi32, _mm256_extracti128_si256, _mm256_loadu_si256, _mm256_madd_epi16,
        _mm256_maddubs_epi16, _mm256_movemask_epi8, _mm256_or_si256, _mm256_set1_epi32,
        _mm256_set1_epi8, _mm256_shuffle_epi8, _mm256_sub_epi8, _mm_storel_epi64,
    };

    const PACK_SHUFFLE: [i8; 32] = [
        2, 1, 0, 6, 5, 4, 10, 9, 8, 14, 13, 12, -1, -1, -1, -1, 2, 1, 0, 6, 5, 4, 10, 9, 8, 14, 13,
        12, -1, -1, -1, -1,
    ];

    let mut in_index = 0usize;
    let mut out_index = 0usize;
    let input_len = bytes.len();
    let pack_shuffle = _mm256_loadu_si256(PACK_SHUFFLE.as_ptr() as *const _);
    let pack_pairs = _mm256_set1_epi32(0x0140_0140);
    let pack_quartets = _mm256_set1_epi32(0x0001_1000);

    while in_index + 32 <= input_len {
        let chunk = _mm256_loadu_si256(bytes.as_ptr().add(in_index) as *const _);

        let ge_upper_a = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'A' as i8) - 1));
        let le_upper_z = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'Z' as i8) + 1), chunk);
        let is_upper = _mm256_and_si256(ge_upper_a, le_upper_z);

        let ge_lower_a = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'a' as i8) - 1));
        let le_lower_z = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'z' as i8) + 1), chunk);
        let is_lower = _mm256_and_si256(ge_lower_a, le_lower_z);

        let ge_digit_0 = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8((b'0' as i8) - 1));
        let le_digit_9 = _mm256_cmpgt_epi8(_mm256_set1_epi8((b'9' as i8) + 1), chunk);
        let is_digit = _mm256_and_si256(ge_digit_0, le_digit_9);

        let is_plus = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'+' as i8));
        let is_slash = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'/' as i8));
        let is_pad = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'=' as i8));
        let is_lf = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\n' as i8));
        let is_cr = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\r' as i8));

        let upper_vals = _mm256_sub_epi8(chunk, _mm256_set1_epi8(b'A' as i8));
        let lower_vals = _mm256_sub_epi8(chunk, _mm256_set1_epi8(71));
        let digit_vals = _mm256_sub_epi8(chunk, _mm256_set1_epi8(-4));
        let plus_vals = _mm256_and_si256(is_plus, _mm256_set1_epi8(62));
        let slash_vals = _mm256_and_si256(is_slash, _mm256_set1_epi8(63));

        let mut sextets = _mm256_or_si256(
            _mm256_and_si256(is_upper, upper_vals),
            _mm256_and_si256(is_lower, lower_vals),
        );
        sextets = _mm256_or_si256(sextets, _mm256_and_si256(is_digit, digit_vals));
        sextets = _mm256_or_si256(sextets, plus_vals);
        sextets = _mm256_or_si256(sextets, slash_vals);

        let valid = _mm256_or_si256(
            _mm256_or_si256(is_upper, is_lower),
            _mm256_or_si256(is_digit, _mm256_or_si256(is_plus, is_slash)),
        );
        let valid_mask = _mm256_movemask_epi8(valid);
        if valid_mask != -1 {
            let acceptable = _mm256_or_si256(
                valid,
                _mm256_or_si256(is_pad, _mm256_or_si256(is_lf, is_cr)),
            );
            if _mm256_movemask_epi8(acceptable) == -1 {
                return Ok(Base64DecodeFastOutcome::NeedsFallback);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid base64 quartet",
            ));
        }

        let merged_pairs = _mm256_maddubs_epi16(sextets, pack_pairs);
        let merged_quartets = _mm256_madd_epi16(merged_pairs, pack_quartets);
        let packed = _mm256_shuffle_epi8(merged_quartets, pack_shuffle);
        let lower = _mm256_castsi256_si128(packed);
        let upper = _mm256_extracti128_si256(packed, 1);
        let out_ptr = out.as_mut_ptr().add(out_index);
        _mm_storel_epi64(out_ptr as *mut _, lower);
        *(out_ptr.add(8) as *mut u32) = _mm256_extract_epi32::<2>(packed) as u32;
        _mm_storel_epi64(out_ptr.add(12) as *mut _, upper);
        *(out_ptr.add(20) as *mut u32) = _mm256_extract_epi32::<6>(packed) as u32;
        out_index += 24;
        in_index += 32;
    }

    if in_index < input_len {
        let remainder = &bytes[in_index..];
        if remainder
            .iter()
            .any(|&byte| base64_is_ignored_decode_byte(byte) || byte == b'=')
        {
            return Ok(Base64DecodeFastOutcome::NeedsFallback);
        }
        out_index += decode_base64_block_into_scalar(remainder, &mut out[out_index..])?;
    }
    Ok(Base64DecodeFastOutcome::Decoded(out_index))
}

fn decode_base64_block_into_with_kernel(
    bytes: &[u8],
    out: &mut [u8],
    requested: Base64DecodeKernel,
) -> io::Result<usize> {
    match decode_base64_kernel_for_block(bytes.len(), requested) {
        Base64DecodeKernel::Scalar | Base64DecodeKernel::Auto => {
            decode_base64_block_into_scalar(bytes, out)
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64DecodeKernel::Avx2 => unsafe { decode_base64_block_into_avx2(bytes, out) },
    }
}

fn decode_base64_block_into_with_fused_fallback(
    bytes: &[u8],
    out: &mut [u8],
    requested: Base64DecodeKernel,
) -> io::Result<Base64DecodeFastOutcome> {
    match decode_base64_kernel_for_block(bytes.len(), requested) {
        Base64DecodeKernel::Scalar | Base64DecodeKernel::Auto => {
            if bytes
                .iter()
                .any(|&byte| base64_is_ignored_decode_byte(byte) || byte == b'=')
            {
                Ok(Base64DecodeFastOutcome::NeedsFallback)
            } else {
                Ok(Base64DecodeFastOutcome::Decoded(
                    decode_base64_block_into_scalar(bytes, out)?,
                ))
            }
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        Base64DecodeKernel::Avx2 => unsafe {
            decode_base64_block_into_avx2_with_fused_fallback(bytes, out)
        },
    }
}

fn decode_base64_block_processor_scalar(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    decode_base64_block_into_scalar(bytes, out)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn decode_base64_block_processor_avx2(bytes: &[u8], out: &mut [u8]) -> io::Result<usize> {
    if bytes.len() < 32 {
        return decode_base64_block_into_scalar(bytes, out);
    }
    // SAFETY: this processor is only selected after runtime AVX2 detection.
    match unsafe { decode_base64_block_into_avx2_with_fused_fallback(bytes, out) }? {
        Base64DecodeFastOutcome::Decoded(written) => Ok(written),
        Base64DecodeFastOutcome::NeedsFallback => decode_base64_block_into_scalar(bytes, out),
    }
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

pub(crate) fn bench_base64_decode(
    iterations: u64,
    requested_kernel: Base64DecodeKernel,
) -> io::Result<()> {
    if iterations == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iterations must be greater than zero",
        ));
    }

    let decoded = (0..BASE64_BENCH_INPUT_SIZE)
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    let encoded = encode_base64_block(&decoded);
    debug_assert_eq!(encoded.len(), BASE64_BENCH_OUTPUT_SIZE);
    let mut output = vec![0_u8; BASE64_BENCH_INPUT_SIZE];

    let selected_kernel = decode_base64_kernel_for_block(encoded.len(), requested_kernel);

    for _ in 0..1024 {
        let written = decode_base64_block_into_with_kernel(&encoded, &mut output, selected_kernel)?;
        black_box(written);
    }

    let start = Instant::now();
    let mut sink = 0_u64;
    for _ in 0..iterations {
        let written = decode_base64_block_into_with_kernel(&encoded, &mut output, selected_kernel)?;
        sink ^= u64::from(output[0]);
        sink ^= u64::from(output[written - 1]);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let iterations_per_second = iterations as f64 / elapsed;
    let gb_per_second = (iterations as f64 * decoded.len() as f64) / elapsed / 1e9;

    println!(
        "Base64 decode kernel [{}] {} iterations of {} -> {} bytes in {:.4} s, {:.0} it/s, {:.1} GB/s per core",
        selected_kernel.bench_name(),
        iterations,
        encoded.len(),
        decoded.len(),
        elapsed,
        iterations_per_second,
        gb_per_second
    );
    black_box(sink);
    Ok(())
}
