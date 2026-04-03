use super::*;
use std::io::Write;
use std::time::{Duration, Instant};

const PV_FALLBACK_BLOCK_SIZE: usize = 2 << 20;
const PV_REPORT_INTERVAL: Duration = Duration::from_millis(200);
const PV_REPORT_BYTES: u64 = 256 * 1024 * 1024;

fn render_rate(bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(1e-9);
    let rate = bytes as f64 / secs;
    if rate >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GiB/s", rate / (1024.0 * 1024.0 * 1024.0))
    } else if rate >= 1024.0 * 1024.0 {
        format!("{:.2} MiB/s", rate / (1024.0 * 1024.0))
    } else if rate >= 1024.0 {
        format!("{:.2} KiB/s", rate / 1024.0)
    } else {
        format!("{rate:.0} B/s")
    }
}

fn report_progress(
    bytes: u64,
    start: Instant,
    last_report: &mut Instant,
    last_reported_bytes: &mut u64,
) -> io::Result<()> {
    if bytes.saturating_sub(*last_reported_bytes) < PV_REPORT_BYTES {
        return Ok(());
    }
    let now = Instant::now();
    if now.duration_since(*last_report) < PV_REPORT_INTERVAL {
        return Ok(());
    }
    *last_report = now;
    *last_reported_bytes = bytes;
    let elapsed = now.duration_since(start);
    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "{} bytes\t{}\t{:.3}s",
        bytes,
        render_rate(bytes, elapsed),
        elapsed.as_secs_f64()
    )
}

fn finish_progress(bytes: u64, start: Instant) -> io::Result<()> {
    let elapsed = Instant::now().duration_since(start);
    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "{} bytes\t{}\t{:.3}s",
        bytes,
        render_rate(bytes, elapsed),
        elapsed.as_secs_f64()
    )
}

fn fallback_copy_with_progress(input: &StreamInput, io_mode: IOMode) -> io::Result<u64> {
    let out = stdout_buf_writer()?;
    let mut total = 0u64;
    let start = Instant::now();
    let mut last_report = start;
    let mut last_reported_bytes = 0u64;
    let mut emit_block = |block: &[u8]| -> io::Result<()> {
        out.write_all(block)?;
        total = total
            .checked_add(block.len() as u64)
            .ok_or_else(|| io::Error::other("pv byte count overflow"))?;
        report_progress(
            total,
            start,
            &mut last_report,
            &mut last_reported_bytes,
        )
    };
    match input {
        StreamInput::File(path) if is_regular_input_path(path)? => {
            visit_ordered_blocks(path, io_mode, &mut emit_block)?;
        }
        StreamInput::File(path) => {
            let mut reader = BufReader::new(std::fs::File::open(path)?);
            let mut buffer = vec![0_u8; PV_FALLBACK_BLOCK_SIZE];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                emit_block(&buffer[..read])?;
            }
        }
        StreamInput::Stdin { .. } => {
            let mut reader = stdin_buf_reader()?;
            let mut buffer = vec![0_u8; PV_FALLBACK_BLOCK_SIZE];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                emit_block(&buffer[..read])?;
            }
        }
    }
    out.into_inner()?;
    finish_progress(total, start)?;
    Ok(total)
}

pub(super) fn run_pv(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    for input in inputs {
        let start = Instant::now();
        let mut last_report = start;
        let mut last_reported_bytes = 0u64;
        if let Some(total) = try_fast_copy_to_stdout_counted(&input, io_mode, &mut |bytes| {
            report_progress(
                bytes,
                start,
                &mut last_report,
                &mut last_reported_bytes,
            )
        })? {
            finish_progress(total, start)?;
            continue;
        }
        let _ = fallback_copy_with_progress(&input, io_mode)?;
    }
    Ok(())
}
