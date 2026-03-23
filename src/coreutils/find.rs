use super::*;

const FIND_OUTPUT_CHUNK_BYTES: usize = 1 << 20;

pub(super) fn run_find(args: &[String]) -> io::Result<i32> {
    let roots = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        vec![".".to_string()]
    };
    let worker_count = parallel_find_worker_count();
    let config = load_config(None);
    let write_params = config.get_params("write", false);
    let output = Arc::new(BufWriter::stdout(
        write_params.qd,
        write_params.block_size,
        worker_count.saturating_mul(2),
    )?);
    let queue = Arc::new(WorkQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    let had_warnings = Arc::new(AtomicBool::new(false));

    for root in roots {
        let path = PathBuf::from(root);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("find", &path, &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        write_find_path(&output, &path)?;
        if metadata.file_type().is_dir() {
            queue.enqueue_one(path);
        }
    }

    run_parallel_work_queue(queue, stop, worker_count, {
        let output = output.clone();
        let had_warnings = had_warnings.clone();
        move |start_dir, queue, stop| {
            walk_find_subtree(start_dir, queue, &output, stop, &had_warnings)
        }
    })?;
    let output = Arc::into_inner(output)
        .ok_or_else(|| io::Error::other("find output writer still has active references"))?;
    output.into_inner()?;
    Ok(if had_warnings.load(Ordering::SeqCst) {
        1
    } else {
        0
    })
}

fn walk_find_subtree(
    start_dir: PathBuf,
    queue: &WorkQueue<PathBuf>,
    output: &BufWriter,
    stop: &AtomicBool,
    had_warnings: &AtomicBool,
) -> io::Result<()> {
    let mut stack = vec![start_dir];
    let mut chunk = Vec::with_capacity(FIND_OUTPUT_CHUNK_BYTES);
    while let Some(dir) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut child_dirs = Vec::new();
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("find", &dir, &err, "cannot read directory");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("find", &dir, &err, "cannot read directory");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let path = entry.path();
            append_find_path(&mut chunk, &path);
            if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                output.write_all(&std::mem::take(&mut chunk))?;
            }
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("find", &path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            if file_type.is_dir() {
                child_dirs.push(path);
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    output.write_all(&chunk)
}

fn parallel_find_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1)
}

fn write_find_path(output: &BufWriter, path: &Path) -> io::Result<()> {
    let mut chunk = Vec::with_capacity(path.as_os_str().as_bytes().len() + 1);
    append_find_path(&mut chunk, path);
    output.write_all(&chunk)
}

fn append_find_path(chunk: &mut Vec<u8>, path: &Path) {
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(b'\n');
}
