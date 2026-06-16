use super::*;

mod options;
use options::*;

#[derive(Clone)]
struct DuLine {
    path: PathBuf,
    usage: u64,
}

struct DuNode {
    path: PathBuf,
    total_usage: u64,
    exclusive_usage: u64,
    parent: Option<usize>,
    pending_children: usize,
    pending_file_stats: usize,
    scanned: bool,
    own_stat_done: bool,
    completed: bool,
    emit: bool,
}

struct DuSharedState {
    nodes: Mutex<Vec<DuNode>>,
    lines: Mutex<Vec<DuLine>>,
}

fn cstring_from_os_str(value: &std::ffi::OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))
}

fn stat_path(path: &Path, follow_symlinks: bool) -> io::Result<libc::stat> {
    let path = cstring_from_os_str(path.as_os_str())?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe {
        if follow_symlinks {
            libc::stat(path.as_ptr(), stat.as_mut_ptr())
        } else {
            libc::lstat(path.as_ptr(), stat.as_mut_ptr())
        }
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { stat.assume_init() })
}

fn fstatat_path(
    dirfd: RawFd,
    name: &std::ffi::OsStr,
    follow_symlinks: bool,
) -> io::Result<libc::stat> {
    let name = cstring_from_os_str(name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe {
        libc::fstatat(
            dirfd,
            name.as_ptr(),
            stat.as_mut_ptr(),
            if follow_symlinks {
                0
            } else {
                libc::AT_SYMLINK_NOFOLLOW
            },
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { stat.assume_init() })
}

fn stat_is_dir(stat: &libc::stat) -> bool {
    (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR
}

struct DirHandle(*mut libc::DIR);

struct DirEntryName {
    name: std::ffi::OsString,
}

impl DirHandle {
    fn open(path: &Path) -> io::Result<Self> {
        let path = cstring_from_os_str(path.as_os_str())?;
        let dir = unsafe { libc::opendir(path.as_ptr()) };
        if dir.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(dir))
    }

    fn fd(&self) -> RawFd {
        unsafe { libc::dirfd(self.0) }
    }

    fn next_entry(&mut self) -> io::Result<Option<DirEntryName>> {
        loop {
            #[cfg(target_os = "linux")]
            unsafe {
                *libc::__errno_location() = 0;
            }
            let entry = unsafe { libc::readdir(self.0) };
            if entry.is_null() {
                let err = io::Error::last_os_error();
                return match err.raw_os_error() {
                    Some(0) => Ok(None),
                    _ => Err(err),
                };
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            let bytes = name.to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            return Ok(Some(DirEntryName {
                name: std::ffi::OsString::from_vec(bytes.to_vec()),
            }));
        }
    }
}

impl Drop for DirHandle {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        unsafe {
            let _ = libc::closedir(self.0);
        }
    }
}

#[derive(Clone)]
struct DuTraversalTask {
    path: PathBuf,
    node_id: usize,
    depth: usize,
}

fn du_node_ready(
    scanned: bool,
    own_stat_done: bool,
    pending_children: usize,
    pending_file_stats: usize,
    completed: bool,
) -> bool {
    scanned && own_stat_done && pending_children == 0 && pending_file_stats == 0 && !completed
}

fn finish_du_node(node_id: usize, state: &DuSharedState, separate_dirs: bool) {
    let mut current = Some(node_id);
    let mut completed_lines = Vec::new();
    while let Some(id) = current {
        let mut next = None;
        {
            let mut nodes = state.nodes.lock().unwrap();
            if !du_node_ready(
                nodes[id].scanned,
                nodes[id].own_stat_done,
                nodes[id].pending_children,
                nodes[id].pending_file_stats,
                nodes[id].completed,
            ) {
                break;
            }
            let total_usage = nodes[id].total_usage;
            let display_usage =
                du_display_total_blocks(separate_dirs, nodes[id].exclusive_usage, total_usage);
            if nodes[id].emit {
                completed_lines.push(DuLine {
                    path: nodes[id].path.clone(),
                    usage: display_usage,
                });
            }
            nodes[id].completed = true;
            if let Some(parent_id) = nodes[id].parent {
                nodes[parent_id].total_usage += total_usage;
                nodes[parent_id].pending_children =
                    nodes[parent_id].pending_children.saturating_sub(1);
                if du_node_ready(
                    nodes[parent_id].scanned,
                    nodes[parent_id].own_stat_done,
                    nodes[parent_id].pending_children,
                    nodes[parent_id].pending_file_stats,
                    nodes[parent_id].completed,
                ) {
                    next = Some(parent_id);
                }
            }
        }
        current = next;
    }
    if !completed_lines.is_empty() {
        state.lines.lock().unwrap().extend(completed_lines);
    }
}

fn walk_du_subtree(
    start: DuTraversalTask,
    dir_queue: &WorkQueue<DuTraversalTask>,
    state: &DuSharedState,
    stop: &AtomicBool,
    had_warnings: &AtomicBool,
    summarize: bool,
    all: bool,
    separate_dirs: bool,
    max_depth: Option<usize>,
    usage_mode: DuUsageMode,
    dereference_mode: DuDereferenceMode,
) -> io::Result<()> {
    let mut stack = vec![start];
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let mut dir = match DirHandle::open(&task.path) {
            Ok(dir) => dir,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("du", &task.path, &err, "cannot read directory");
                had_warnings.store(true, Ordering::SeqCst);
                {
                    let mut nodes = state.nodes.lock().unwrap();
                    nodes[task.node_id].scanned = true;
                }
                finish_du_node(task.node_id, state, separate_dirs);
                continue;
            }
            Err(err) => return Err(err),
        };
        let dirfd = dir.fd();
        let mut child_dirs = Vec::new();
        let mut file_lines = Vec::new();
        let mut file_total_usage = 0u64;
        loop {
            let entry = match dir.next_entry() {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("du", &task.path, &err, "cannot read directory");
                    had_warnings.store(true, Ordering::SeqCst);
                    break;
                }
                Err(err) => return Err(err),
            };
            let child_path = task.path.join(&entry.name);
            let stat = match fstatat_path(dirfd, &entry.name, dereference_mode.follow_children()) {
                Ok(stat) => stat,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    write_warning_line("du", &child_path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("du", &child_path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let usage = match usage_mode {
                DuUsageMode::DiskBlocks => stat.st_blocks as u64,
                DuUsageMode::ApparentBytes => stat.st_size as u64,
            };
            if stat_is_dir(&stat) {
                let child_depth = task.depth + 1;
                let child_id = {
                    let mut nodes = state.nodes.lock().unwrap();
                    let child_id = nodes.len();
                    nodes.push(DuNode {
                        path: child_path.clone(),
                        total_usage: usage,
                        exclusive_usage: usage,
                        parent: Some(task.node_id),
                        pending_children: 0,
                        pending_file_stats: 0,
                        scanned: false,
                        own_stat_done: true,
                        completed: false,
                        emit: !summarize && du_depth_included(child_depth, max_depth),
                    });
                    child_id
                };
                child_dirs.push(DuTraversalTask {
                    path: child_path,
                    node_id: child_id,
                    depth: child_depth,
                });
            } else {
                file_total_usage += usage;
                if all && du_depth_included(task.depth + 1, max_depth) {
                    file_lines.push(DuLine {
                        path: child_path,
                        usage,
                    });
                }
            }
        }
        {
            let mut nodes = state.nodes.lock().unwrap();
            nodes[task.node_id].pending_children += child_dirs.len();
            nodes[task.node_id].total_usage += file_total_usage;
            nodes[task.node_id].exclusive_usage += file_total_usage;
            nodes[task.node_id].scanned = true;
        }
        if !file_lines.is_empty() {
            state.lines.lock().unwrap().extend(file_lines);
        }
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
        finish_du_node(task.node_id, state, separate_dirs);
    }
    Ok(())
}

const DU_MAX_WORKERS: usize = 8;
const DU_SERIAL_ROOT_ENTRY_THRESHOLD: usize = 64;

fn du_parallel_worker_count_for(available_parallelism: usize) -> usize {
    available_parallelism.max(1).min(DU_MAX_WORKERS)
}

fn parallel_du_worker_count() -> usize {
    du_parallel_worker_count_for(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    )
}

fn append_du_output(
    path: &Path,
    summarize: bool,
    all: bool,
    separate_dirs: bool,
    max_depth: Option<usize>,
    display_format: DuDisplayFormat,
    usage_mode: DuUsageMode,
    dereference_mode: DuDereferenceMode,
    line_terminator: DuLineTerminator,
    threshold: Option<DuThreshold>,
    output: &mut Vec<u8>,
    had_warnings: Arc<AtomicBool>,
) -> io::Result<u64> {
    let stat = stat_path(path, dereference_mode.follow_root())?;
    let root_usage = match usage_mode {
        DuUsageMode::DiskBlocks => stat.st_blocks as u64,
        DuUsageMode::ApparentBytes => stat.st_size as u64,
    };
    if !stat_is_dir(&stat) {
        if du_threshold_includes(root_usage, usage_mode, threshold) {
            append_du_line(
                output,
                root_usage,
                path,
                usage_mode,
                display_format,
                line_terminator,
            );
        }
        return Ok(root_usage);
    }

    let state = Arc::new(DuSharedState {
        nodes: Mutex::new(vec![DuNode {
            path: path.to_path_buf(),
            total_usage: root_usage,
            exclusive_usage: root_usage,
            parent: None,
            pending_children: 0,
            pending_file_stats: 0,
            scanned: false,
            own_stat_done: true,
            completed: false,
            emit: true,
        }]),
        lines: Mutex::new(Vec::new()),
    });
    let root_task = DuTraversalTask {
        path: path.to_path_buf(),
        node_id: 0,
        depth: 0,
    };
    if du_should_use_serial_walk(path)? {
        walk_du_subtree_serial(
            root_task,
            &state,
            &had_warnings,
            summarize,
            all,
            separate_dirs,
            max_depth,
            usage_mode,
            dereference_mode,
        )?;
    } else {
        let dir_queue = Arc::new(WorkQueue::default());
        let stop = Arc::new(AtomicBool::new(false));
        dir_queue.enqueue_one(root_task);
        run_parallel_work_queue(dir_queue, stop.clone(), parallel_du_worker_count(), {
            let state = state.clone();
            let had_warnings = had_warnings.clone();
            move |task, dir_queue, stop| {
                walk_du_subtree(
                    task,
                    dir_queue,
                    &state,
                    stop,
                    &had_warnings,
                    summarize,
                    all,
                    separate_dirs,
                    max_depth,
                    usage_mode,
                    dereference_mode,
                )
            }
        })?;
    }

    let state = Arc::into_inner(state)
        .ok_or_else(|| io::Error::other("du shared state still has active references"))?;
    let lines = state.lines.into_inner().unwrap();
    for line in lines {
        if du_threshold_includes(line.usage, usage_mode, threshold) {
            append_du_line(
                output,
                line.usage,
                &line.path,
                usage_mode,
                display_format,
                line_terminator,
            );
        }
    }
    let total_usage = state.nodes.into_inner().unwrap()[0].total_usage;
    Ok(total_usage)
}

fn du_should_use_serial_walk(root: &Path) -> io::Result<bool> {
    Ok(fs::read_dir(root)?
        .take(DU_SERIAL_ROOT_ENTRY_THRESHOLD + 1)
        .count()
        <= DU_SERIAL_ROOT_ENTRY_THRESHOLD)
}

fn walk_du_subtree_serial(
    start: DuTraversalTask,
    state: &DuSharedState,
    had_warnings: &AtomicBool,
    summarize: bool,
    all: bool,
    separate_dirs: bool,
    max_depth: Option<usize>,
    usage_mode: DuUsageMode,
    dereference_mode: DuDereferenceMode,
) -> io::Result<()> {
    let mut stack = vec![start];
    while let Some(task) = stack.pop() {
        let mut dir = match DirHandle::open(&task.path) {
            Ok(dir) => dir,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("du", &task.path, &err, "cannot read directory");
                had_warnings.store(true, Ordering::SeqCst);
                {
                    let mut nodes = state.nodes.lock().unwrap();
                    nodes[task.node_id].scanned = true;
                }
                finish_du_node(task.node_id, state, separate_dirs);
                continue;
            }
            Err(err) => return Err(err),
        };
        let dirfd = dir.fd();
        let mut child_dirs = Vec::new();
        let mut file_lines = Vec::new();
        let mut file_total_usage = 0u64;
        loop {
            let entry = match dir.next_entry() {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("du", &task.path, &err, "cannot read directory");
                    had_warnings.store(true, Ordering::SeqCst);
                    break;
                }
                Err(err) => return Err(err),
            };
            let child_path = task.path.join(&entry.name);
            let stat = match fstatat_path(dirfd, &entry.name, dereference_mode.follow_children()) {
                Ok(stat) => stat,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    write_warning_line("du", &child_path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("du", &child_path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let usage = match usage_mode {
                DuUsageMode::DiskBlocks => stat.st_blocks as u64,
                DuUsageMode::ApparentBytes => stat.st_size as u64,
            };
            if stat_is_dir(&stat) {
                let child_depth = task.depth + 1;
                let child_id = {
                    let mut nodes = state.nodes.lock().unwrap();
                    let child_id = nodes.len();
                    nodes.push(DuNode {
                        path: child_path.clone(),
                        total_usage: usage,
                        exclusive_usage: usage,
                        parent: Some(task.node_id),
                        pending_children: 0,
                        pending_file_stats: 0,
                        scanned: false,
                        own_stat_done: true,
                        completed: false,
                        emit: !summarize && du_depth_included(child_depth, max_depth),
                    });
                    child_id
                };
                child_dirs.push(DuTraversalTask {
                    path: child_path,
                    node_id: child_id,
                    depth: child_depth,
                });
            } else {
                file_total_usage += usage;
                if all && du_depth_included(task.depth + 1, max_depth) {
                    file_lines.push(DuLine {
                        path: child_path,
                        usage,
                    });
                }
            }
        }
        {
            let mut nodes = state.nodes.lock().unwrap();
            nodes[task.node_id].pending_children += child_dirs.len();
            nodes[task.node_id].total_usage += file_total_usage;
            nodes[task.node_id].exclusive_usage += file_total_usage;
            nodes[task.node_id].scanned = true;
        }
        if !file_lines.is_empty() {
            state.lines.lock().unwrap().extend(file_lines);
        }
        if let Some(local_dir) = child_dirs.pop() {
            stack.extend(child_dirs);
            stack.push(local_dir);
        }
        finish_du_node(task.node_id, state, separate_dirs);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs;

pub(super) fn run_du(args: &[String]) -> io::Result<i32> {
    let mut summarize = false;
    let mut all = false;
    let mut display_format = DuDisplayFormat::Kib;
    let mut usage_mode = DuUsageMode::DiskBlocks;
    let mut total = false;
    let mut separate_dirs = false;
    let mut dereference_mode = DuDereferenceMode::None;
    let mut line_terminator = DuLineTerminator::Newline;
    let mut max_depth = None;
    let mut threshold = None;
    let mut end_of_options = false;
    let mut paths = Vec::new();
    let mut index = 1usize;
    while index < args.len() {
        let arg = &args[index];
        if end_of_options {
            paths.push(arg.clone());
            index += 1;
            continue;
        }
        match arg.as_str() {
            "--" => end_of_options = true,
            "-s" | "--summarize" => summarize = true,
            "-a" | "--all" => all = true,
            "-b" | "--bytes" => {
                usage_mode = DuUsageMode::ApparentBytes;
                display_format = DuDisplayFormat::BlockSize(1);
            }
            "-h" | "--human-readable" => display_format = DuDisplayFormat::HumanReadableIec,
            "-k" => display_format = DuDisplayFormat::BlockSize(1024),
            "-m" => display_format = DuDisplayFormat::BlockSize(1024_u64.pow(2)),
            "--apparent-size" => usage_mode = DuUsageMode::ApparentBytes,
            "-c" | "--total" => total = true,
            "-S" | "--separate-dirs" => separate_dirs = true,
            "-D" | "-H" | "--dereference-args" => dereference_mode = DuDereferenceMode::Args,
            "-L" | "--dereference" => dereference_mode = DuDereferenceMode::All,
            "-P" | "--no-dereference" => dereference_mode = DuDereferenceMode::None,
            "-0" | "--null" => line_terminator = DuLineTerminator::Nul,
            "--si" => display_format = DuDisplayFormat::HumanReadableSi,
            "--max-depth" | "-d" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {arg}"),
                    ));
                };
                match parse_du_max_depth(value) {
                    Ok(depth) => max_depth = Some(depth),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        write_du_try_help();
                        return Ok(1);
                    }
                }
            }
            "--threshold" | "-t" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {arg}"),
                    ));
                };
                match parse_du_threshold(value) {
                    Ok(parsed) => threshold = Some(parsed),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        return Ok(1);
                    }
                }
            }
            "--block-size" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --block-size",
                    ));
                };
                match parse_du_block_size(value) {
                    Ok(size) => display_format = DuDisplayFormat::BlockSize(size),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        return Ok(1);
                    }
                }
            }
            "-B" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for -B",
                    ));
                };
                match parse_du_block_size(value) {
                    Ok(size) => display_format = DuDisplayFormat::BlockSize(size),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        return Ok(1);
                    }
                }
            }
            other if other.starts_with("--max-depth=") => {
                match parse_du_max_depth(other.trim_start_matches("--max-depth=")) {
                    Ok(depth) => max_depth = Some(depth),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        write_du_try_help();
                        return Ok(1);
                    }
                }
            }
            other if other.starts_with("--block-size=") => {
                match parse_du_block_size(other.trim_start_matches("--block-size=")) {
                    Ok(size) => display_format = DuDisplayFormat::BlockSize(size),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        return Ok(1);
                    }
                }
            }
            other if other.starts_with("--threshold=") => {
                match parse_du_threshold(other.trim_start_matches("--threshold=")) {
                    Ok(parsed) => threshold = Some(parsed),
                    Err(message) => {
                        write_du_stderr_line(&message);
                        return Ok(1);
                    }
                }
            }
            other if other.starts_with('-') && other != "-" && !other.starts_with("--") => {
                let bytes = other.as_bytes();
                let mut short_index = 1usize;
                while short_index < bytes.len() {
                    let flag = bytes[short_index];
                    if flag == b'd' {
                        let value = if short_index + 1 < bytes.len() {
                            std::str::from_utf8(&bytes[(short_index + 1)..]).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unsupported du flag: {other}"),
                                )
                            })?
                        } else {
                            index += 1;
                            args.get(index).map(String::as_str).ok_or_else(|| {
                                io::Error::new(io::ErrorKind::InvalidInput, "missing value for -d")
                            })?
                        };
                        match parse_du_max_depth(value) {
                            Ok(depth) => max_depth = Some(depth),
                            Err(message) => {
                                write_du_stderr_line(&message);
                                write_du_try_help();
                                return Ok(1);
                            }
                        }
                        break;
                    }
                    if flag == b'B' {
                        let value = if short_index + 1 < bytes.len() {
                            std::str::from_utf8(&bytes[(short_index + 1)..]).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unsupported du flag: {other}"),
                                )
                            })?
                        } else {
                            index += 1;
                            args.get(index).map(String::as_str).ok_or_else(|| {
                                io::Error::new(io::ErrorKind::InvalidInput, "missing value for -B")
                            })?
                        };
                        match parse_du_block_size(value) {
                            Ok(size) => display_format = DuDisplayFormat::BlockSize(size),
                            Err(message) => {
                                write_du_stderr_line(&message);
                                return Ok(1);
                            }
                        }
                        break;
                    }
                    if flag == b't' {
                        let value = if short_index + 1 < bytes.len() {
                            std::str::from_utf8(&bytes[(short_index + 1)..]).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unsupported du flag: {other}"),
                                )
                            })?
                        } else {
                            index += 1;
                            args.get(index).map(String::as_str).ok_or_else(|| {
                                io::Error::new(io::ErrorKind::InvalidInput, "missing value for -t")
                            })?
                        };
                        match parse_du_threshold(value) {
                            Ok(parsed) => threshold = Some(parsed),
                            Err(message) => {
                                write_du_stderr_line(&message);
                                return Ok(1);
                            }
                        }
                        break;
                    }
                    (
                        summarize,
                        all,
                        display_format,
                        usage_mode,
                        total,
                        separate_dirs,
                        dereference_mode,
                        line_terminator,
                    ) = du_apply_short_flag(
                        summarize,
                        all,
                        display_format,
                        usage_mode,
                        total,
                        separate_dirs,
                        dereference_mode,
                        line_terminator,
                        flag,
                    )?;
                    short_index += 1;
                }
            }
            other if other.starts_with('-') && other != "-" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported du flag: {other}"),
                ));
            }
            other => paths.push(other.to_string()),
        }
        index += 1;
    }

    if paths.is_empty() {
        paths.push(".".to_string());
    }
    if summarize && all {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "du does not support combining -a/--all with -s/--summarize",
        ));
    }
    if summarize {
        if let Some(depth) = max_depth {
            if depth == 0 {
                write_du_stderr_line("warning: summarizing is the same as using --max-depth=0");
            } else {
                write_du_stderr_line(&format!(
                    "warning: summarizing conflicts with --max-depth={depth}"
                ));
                write_du_try_help();
                return Ok(1);
            }
        }
    }

    let mut out = stdout_buf_writer()?;
    let had_warnings = Arc::new(AtomicBool::new(false));
    let mut grand_total_blocks = 0_u64;
    for path in paths {
        let mut chunk = Vec::new();
        let root_total_blocks = match append_du_output(
            Path::new(&path),
            summarize,
            all,
            separate_dirs,
            max_depth,
            display_format,
            usage_mode,
            dereference_mode,
            line_terminator,
            threshold,
            &mut chunk,
            had_warnings.clone(),
        ) {
            Ok(root_total_blocks) => root_total_blocks,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                write_warning_line("du", Path::new(&path), &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("du", Path::new(&path), &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        grand_total_blocks = grand_total_blocks.saturating_add(root_total_blocks);
        out.write_all(&chunk)?;
    }
    if total {
        let total_path = Path::new("total");
        let mut chunk = Vec::new();
        append_du_line(
            &mut chunk,
            grand_total_blocks,
            total_path,
            usage_mode,
            display_format,
            line_terminator,
        );
        out.write_all(&chunk)?;
    }
    out.into_inner()?;
    Ok(if had_warnings.load(Ordering::SeqCst) {
        1
    } else {
        0
    })
}
