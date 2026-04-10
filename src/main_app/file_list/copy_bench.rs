use super::*;

pub(crate) fn bench_manifest_recursive_copy(
    manifest_path: &str,
    source_root: &str,
    target_root: &str,
    overlap_large_file: Option<&str>,
    verbose: bool,
) -> io::Result<u64> {
    let manifest = Path::new(manifest_path);
    let source_root = Path::new(source_root);
    let target_root = Path::new(target_root);
    if target_root.exists() {
        fs::remove_dir_all(target_root)?;
    }
    fs::create_dir_all(target_root)?;

    let entries = load_manifest_copy_entries(manifest, source_root)?;
    if entries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no regular files found in manifest {}", manifest.display()),
        ));
    }
    let total_bytes = entries.iter().map(|entry| entry.size).sum::<u64>();

    let overall_start = std::time::Instant::now();
    let (dirs_created, dir_elapsed) = create_manifest_target_dirs(&entries, target_root)?;

    let source_root_fd = open_dir_fd(source_root)?;
    let target_root_fd = open_dir_fd(target_root)?;
    let file_start = std::time::Instant::now();

    let overlap_handle = overlap_large_file.map(|large_src| {
        let large_src = large_src.to_string();
        let large_dst = target_root.join(".fro_manifest_overlap_large_copy.bin");
        std::thread::spawn(move || {
            let large_src_str = large_src;
            let large_dst_str = large_dst.display().to_string();
            let start = std::time::Instant::now();
            let status = Command::new(env::current_exe()?)
                .arg("copy")
                .arg("-n")
                .arg("1")
                .arg(&large_src_str)
                .arg(&large_dst_str)
                .status()?;
            if !status.success() {
                return Err(io::Error::other(format!(
                    "overlap fro copy failed with status {status}"
                )));
            }
            let copied = fs::metadata(&large_dst)?.len();
            Ok::<(u64, std::time::Duration), io::Error>((copied, start.elapsed()))
        })
    });

    for entry in &entries {
        copy_small_file_openat(
            entry,
            &source_root_fd,
            &target_root_fd,
            RelativeCopyMethod::CopyFileRange,
        )?;
    }
    let file_elapsed = file_start.elapsed();

    let (overlap_bytes, overlap_elapsed) = match overlap_handle {
        Some(handle) => {
            let (bytes, elapsed) = handle
                .join()
                .map_err(|_| io::Error::other("overlap large-file copy worker panicked"))??;
            (Some(bytes), Some(elapsed))
        }
        None => (None, None),
    };

    let result = ManifestCopyBenchmarkResult {
        entries: entries.len(),
        bytes: total_bytes,
        dirs_created,
        dir_phase_secs: dir_elapsed.as_secs_f64(),
        file_phase_secs: file_elapsed.as_secs_f64(),
        total_secs: overall_start.elapsed().as_secs_f64(),
        overlap_secs: overlap_elapsed.map(|d| d.as_secs_f64()),
        overlap_large_file_bytes: overlap_bytes,
    };

    fro::cio_println!(
        "manifest-recursive-copy {} bytes across {} files: dirs={} dir_phase={:.4}s file_phase={:.4}s total={:.4}s file_gbps={:.3}",
        result.bytes,
        result.entries,
        result.dirs_created,
        result.dir_phase_secs,
        result.file_phase_secs,
        result.total_secs,
        result.bytes as f64 / result.file_phase_secs.max(1e-9) / 1e9
    );
    if let (Some(bytes), Some(secs)) = (result.overlap_large_file_bytes, result.overlap_secs) {
        fro::cio_println!(
            "manifest-recursive-copy overlap-large-file {} bytes in {:.4}s {:.3} GB/s",
            bytes,
            secs,
            bytes as f64 / secs.max(1e-9) / 1e9
        );
    }
    if verbose {
        fro::cio_eprintln!(
            "manifest-recursive-copy details: source_root={} target_root={} manifest={}",
            source_root.display(),
            target_root.display(),
            manifest.display()
        );
    }
    Ok(result.bytes)
}
