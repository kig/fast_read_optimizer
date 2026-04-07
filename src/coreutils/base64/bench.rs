use super::*;
use std::hint::black_box;
use std::time::Instant;

pub(crate) fn bench_base64_wrapped_encode(iterations: u64, wrap_cols: usize) -> io::Result<()> {
    if iterations == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iterations must be greater than zero",
        ));
    }
    let input = (0..BASE64_BENCH_INPUT_SIZE)
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    for _ in 0..1024 {
        let out = process::bytes::encode_base64_bytes_via_wrapped_path(&input, wrap_cols)?;
        black_box(out.len());
    }

    let start = Instant::now();
    let mut sink = 0_u64;
    for _ in 0..iterations {
        let out = process::bytes::encode_base64_bytes_via_wrapped_path(&input, wrap_cols)?;
        sink ^= u64::from(out[0]);
        sink ^= u64::from(out[out.len() - 1]);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let iterations_per_second = iterations as f64 / elapsed;
    let gb_per_second = (iterations as f64 * BASE64_BENCH_INPUT_SIZE as f64) / elapsed / 1e9;
    println!(
        "Base64 wrapped encode [{} cols] {} iterations of {} bytes in {:.4} s, {:.0} it/s, {:.1} GB/s per core",
        wrap_cols, iterations, BASE64_BENCH_INPUT_SIZE, elapsed, iterations_per_second, gb_per_second
    );
    black_box(sink);
    Ok(())
}

pub(crate) fn bench_base64_wrapped_decode(iterations: u64, ignore_garbage: bool) -> io::Result<()> {
    if iterations == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "iterations must be greater than zero",
        ));
    }
    let decoded = (0..BASE64_BENCH_INPUT_SIZE)
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    let encoded = process::bytes::encode_base64_bytes_via_wrapped_path(&decoded, 76)?;
    for _ in 0..1024 {
        let out = process::bytes::decode_base64_bytes_via_reorg_path(&encoded, ignore_garbage)?;
        black_box(out.len());
    }

    let start = Instant::now();
    let mut sink = 0_u64;
    for _ in 0..iterations {
        let out = process::bytes::decode_base64_bytes_via_reorg_path(&encoded, ignore_garbage)?;
        sink ^= u64::from(out[0]);
        sink ^= u64::from(out[out.len() - 1]);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let iterations_per_second = iterations as f64 / elapsed;
    let gb_per_second = (iterations as f64 * decoded.len() as f64) / elapsed / 1e9;
    println!(
        "Base64 wrapped decode [{}] {} iterations of {} bytes in {:.4} s, {:.0} it/s, {:.1} GB/s per core",
        if ignore_garbage { "ignore-garbage" } else { "whitespace-only" },
        iterations,
        encoded.len(),
        elapsed,
        iterations_per_second,
        gb_per_second
    );
    black_box(sink);
    Ok(())
}

pub(crate) fn bench_base64_decode_detect_fallback(
    iterations: u64,
    kernel: Base64DecodeKernel,
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
    let mut encoded = vec![0u8; BASE64_BENCH_OUTPUT_SIZE];
    let written = encode_base64_block_into(&decoded, &mut encoded);
    debug_assert_eq!(written, encoded.len());

    for _ in 0..1024 {
        let out = process::bytes::decode_base64_bytes_with_detect_fallback(&encoded, false, kernel)?;
        black_box(out.len());
    }

    let start = Instant::now();
    let mut sink = 0_u64;
    for _ in 0..iterations {
        let out = process::bytes::decode_base64_bytes_with_detect_fallback(&encoded, false, kernel)?;
        sink ^= u64::from(out[0]);
        sink ^= u64::from(out[out.len() - 1]);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let iterations_per_second = iterations as f64 / elapsed;
    let gb_per_second = (iterations as f64 * decoded.len() as f64) / elapsed / 1e9;
    println!(
        "Base64 decode detect+fallback [{}] {} iterations of {} bytes in {:.4} s, {:.0} it/s, {:.1} GB/s per core",
        kernel.bench_name(),
        iterations,
        encoded.len(),
        elapsed,
        iterations_per_second,
        gb_per_second
    );
    black_box(sink);
    Ok(())
}
