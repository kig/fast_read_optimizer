use super::*;

fn count_newlines_up_to(
    path: &str,
    io_mode: IOMode,
    mode: &str,
    end_offset: u64,
) -> io::Result<u64> {
    let data = load_file_bytes(path, io_mode, mode)?;
    let bytes = data.data.as_slice();
    let end = usize::try_from(end_offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit in usize"))?
        .min(bytes.len());
    Ok(memchr_iter(b'\n', &bytes[..end]).count() as u64)
}

pub(super) fn run_cmp(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut io_mode = IOMode::Auto;
    let mut quiet = false;
    let mut files = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "-s" | "--quiet" | "--silent" => quiet = true,
            other => files.push(other.to_string()),
        }
    }
    let files = ensure_files(
        program,
        files,
        "[-s|--quiet|--silent] [--auto|--no-direct|--direct] <file1> <file2>",
    )?;
    if files.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cmp requires exactly two file operands",
        ));
    }

    let config = load_config(None);
    let diff_page_cache = config.get_params_for_path("diff", false, &files[0]);
    let diff_direct = config.get_params_for_path("diff", true, &files[0]);
    let mismatch = diff_files(
        &files[0],
        &files[1],
        diff_page_cache.num_threads,
        diff_page_cache.block_size,
        diff_page_cache.qd,
        diff_direct.num_threads,
        diff_direct.block_size,
        diff_direct.qd,
        internal_io_mode(io_mode),
        false,
        false,
    )?;
    if mismatch != 0 {
        if !quiet {
            let index = mismatch as usize - 1;
            let line = 1 + count_newlines_up_to(&files[0], io_mode, "read", mismatch - 1)?;
            println!(
                "{} {} differ: byte {}, line {}",
                files[0],
                files[1],
                index + 1,
                line
            );
        }
        return Ok(1);
    }

    let first_len = fs::metadata(&files[0])?.len();
    let second_len = fs::metadata(&files[1])?.len();
    let shared_len = first_len.min(second_len);
    if first_len != second_len {
        if !quiet {
            let eof_file = if first_len < second_len {
                &files[0]
            } else {
                &files[1]
            };
            let line = count_newlines_up_to(eof_file, io_mode, "read", shared_len)?;
            eprintln!(
                "cmp: EOF on {} after byte {}, line {}",
                eof_file, shared_len, line
            );
        }
        return Ok(1);
    }

    Ok(0)
}
