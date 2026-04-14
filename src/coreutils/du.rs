use super::*;

fn disk_usage_kib(blocks: u64) -> u64 {
    blocks.div_ceil(2)
}

fn du_bytes_kib(bytes: u64) -> u64 {
    bytes.div_ceil(1024)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DuUsageMode {
    DiskBlocks,
    ApparentBytes,
}

fn du_usage_bytes(amount: u64, usage_mode: DuUsageMode) -> u64 {
    match usage_mode {
        DuUsageMode::DiskBlocks => amount.saturating_mul(512),
        DuUsageMode::ApparentBytes => amount,
    }
}

fn du_usage_display_units(amount: u64, usage_mode: DuUsageMode, block_size: u64) -> u64 {
    (du_usage_bytes(amount, usage_mode) as u128).div_ceil(block_size as u128) as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DuDisplayFormat {
    Kib,
    HumanReadableIec,
    HumanReadableSi,
    BlockSize(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DuLineTerminator {
    Newline,
    Nul,
}

impl DuLineTerminator {
    fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DuThreshold {
    Min(u64),
    Max(u64),
}

fn du_format_human_bytes(bytes: u64, unit_base: u64, units: &[&str]) -> String {
    if bytes < unit_base {
        return bytes.to_string();
    }
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= unit_base as f64 && unit + 1 < units.len() - 1 {
        value /= unit_base as f64;
        unit += 1;
    }
    if value >= 10.0 {
        format!("{}{}", value.ceil() as u64, units[unit])
    } else {
        let rounded_up = (value * 10.0).ceil() / 10.0;
        if rounded_up >= 10.0 {
            format!("{}{}", rounded_up as u64, units[unit])
        } else {
            format!("{rounded_up:.1}{}", units[unit])
        }
    }
}

fn du_format_usage(
    amount: u64,
    usage_mode: DuUsageMode,
    display_format: DuDisplayFormat,
) -> String {
    match display_format {
        DuDisplayFormat::Kib => match usage_mode {
            DuUsageMode::DiskBlocks => disk_usage_kib(amount).to_string(),
            DuUsageMode::ApparentBytes => du_bytes_kib(amount).to_string(),
        },
        DuDisplayFormat::HumanReadableIec => du_format_human_bytes(
            du_usage_bytes(amount, usage_mode),
            1024,
            &["", "K", "M", "G", "T", "P", "E", "Z", "Y"],
        ),
        DuDisplayFormat::HumanReadableSi => du_format_human_bytes(
            du_usage_bytes(amount, usage_mode),
            1000,
            &["", "k", "M", "G", "T", "P", "E", "Z", "Y"],
        ),
        DuDisplayFormat::BlockSize(block_size) => {
            du_usage_display_units(amount, usage_mode, block_size).to_string()
        }
    }
}

fn append_du_line(
    chunk: &mut Vec<u8>,
    amount: u64,
    path: &Path,
    usage_mode: DuUsageMode,
    display_format: DuDisplayFormat,
    line_terminator: DuLineTerminator,
) {
    chunk.extend_from_slice(du_format_usage(amount, usage_mode, display_format).as_bytes());
    chunk.push(b'\t');
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(line_terminator.byte());
}

fn du_depth_included(depth: usize, max_depth: Option<usize>) -> bool {
    max_depth.is_none_or(|limit| depth <= limit)
}

fn du_display_total_blocks(
    separate_dirs: bool,
    exclusive_blocks: u64,
    subtree_total_blocks: u64,
) -> u64 {
    if separate_dirs {
        exclusive_blocks
    } else {
        subtree_total_blocks
    }
}

fn parse_du_max_depth(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid maximum depth ‘{value}’"))
}

fn parse_du_block_size(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid --block-size argument '{value}'"));
    }
    let lower = trimmed.to_ascii_lowercase();
    let split = lower
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(lower.len());
    if split == 0 {
        return Err(format!("invalid --block-size argument '{value}'"));
    }
    let amount = lower[..split]
        .parse::<u64>()
        .map_err(|_| format!("invalid --block-size argument '{value}'"))?;
    let multiplier = match lower[split..].trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return Err(format!("invalid --block-size argument '{value}'")),
    };
    amount
        .checked_mul(multiplier)
        .filter(|size| *size > 0)
        .ok_or_else(|| format!("invalid --block-size argument '{value}'"))
}

fn parse_du_threshold(value: &str) -> Result<DuThreshold, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid --threshold argument '{value}'"));
    }
    let (negative, magnitude) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };
    let threshold = parse_du_block_size(magnitude)
        .map_err(|_| format!("invalid --threshold argument '{value}'"))?;
    Ok(if negative {
        DuThreshold::Max(threshold)
    } else {
        DuThreshold::Min(threshold)
    })
}

fn du_threshold_includes(
    amount: u64,
    usage_mode: DuUsageMode,
    threshold: Option<DuThreshold>,
) -> bool {
    let bytes = du_usage_bytes(amount, usage_mode);
    match threshold {
        Some(DuThreshold::Min(minimum)) => bytes >= minimum,
        Some(DuThreshold::Max(maximum)) => bytes <= maximum,
        None => true,
    }
}

fn write_du_stderr_line(message: &str) {
    let mut stderr = fro::command_io::stderr_buf_writer(4096).unwrap();
    let _ = writeln!(stderr, "du: {message}");
}

fn write_du_try_help() {
    let mut stderr = fro::command_io::stderr_buf_writer(4096).unwrap();
    let _ = writeln!(stderr, "Try 'du --help' for more information.");
}

fn du_apply_short_flag(
    summarize: bool,
    all: bool,
    display_format: DuDisplayFormat,
    usage_mode: DuUsageMode,
    total: bool,
    separate_dirs: bool,
    dereference_args: bool,
    line_terminator: DuLineTerminator,
    flag: u8,
) -> io::Result<(
    bool,
    bool,
    DuDisplayFormat,
    DuUsageMode,
    bool,
    bool,
    bool,
    DuLineTerminator,
)> {
    match flag {
        b's' => Ok((
            true,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'a' => Ok((
            summarize,
            true,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'h' => Ok((
            summarize,
            all,
            DuDisplayFormat::HumanReadableIec,
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'k' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1024),
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'm' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1024_u64.pow(2)),
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'c' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            true,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'S' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            true,
            dereference_args,
            line_terminator,
        )),
        b'b' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1),
            DuUsageMode::ApparentBytes,
            total,
            separate_dirs,
            dereference_args,
            line_terminator,
        )),
        b'D' | b'H' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            true,
            line_terminator,
        )),
        b'P' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            false,
            line_terminator,
        )),
        b'0' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_args,
            DuLineTerminator::Nul,
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported du flag: -{}", other as char),
        )),
    }
}

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

fn fstatat_no_follow(dirfd: RawFd, name: &std::ffi::OsStr) -> io::Result<libc::stat> {
    let name = cstring_from_os_str(name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe {
        libc::fstatat(
            dirfd,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
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
            let stat = match fstatat_no_follow(dirfd, &entry.name) {
                Ok(stat) => stat,
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
    dereference_args: bool,
    line_terminator: DuLineTerminator,
    threshold: Option<DuThreshold>,
    output: &mut Vec<u8>,
    had_warnings: Arc<AtomicBool>,
) -> io::Result<u64> {
    let stat = stat_path(path, dereference_args)?;
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
            let stat = match fstatat_no_follow(dirfd, &entry.name) {
                Ok(stat) => stat,
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
mod du_tests {
    use super::*;

    #[test]
    fn disk_usage_kib_rounds_512_byte_blocks_to_kib() {
        assert_eq!(disk_usage_kib(0), 0);
        assert_eq!(disk_usage_kib(1), 1);
        assert_eq!(disk_usage_kib(2), 1);
        assert_eq!(disk_usage_kib(3), 2);
    }

    #[test]
    fn stat_is_dir_detects_directory_mode() {
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        stat.st_mode = libc::S_IFDIR;
        assert!(stat_is_dir(&stat));
        stat.st_mode = libc::S_IFREG;
        assert!(!stat_is_dir(&stat));
    }

    #[test]
    fn cstring_from_os_str_rejects_nul() {
        let value = std::ffi::OsString::from_vec(b"bad\0name".to_vec());
        let err = cstring_from_os_str(&value).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn du_format_human_bytes_matches_gnu_style_rounding() {
        let units = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"];
        assert_eq!(du_format_human_bytes(1, 1024, &units), "1");
        assert_eq!(du_format_human_bytes(1023, 1024, &units), "1023");
        assert_eq!(du_format_human_bytes(1024, 1024, &units), "1.0K");
        assert_eq!(du_format_human_bytes(1025, 1024, &units), "1.1K");
        assert_eq!(du_format_human_bytes(1536, 1024, &units), "1.5K");
        assert_eq!(du_format_human_bytes(1024 * 1024, 1024, &units), "1.0M");
        assert_eq!(du_format_human_bytes(1024 * 1024 + 9, 1024, &units), "1.1M");
    }

    #[test]
    fn du_format_human_bytes_supports_si_units() {
        let units = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"];
        assert_eq!(du_format_human_bytes(999, 1000, &units), "999");
        assert_eq!(du_format_human_bytes(1000, 1000, &units), "1.0k");
        assert_eq!(du_format_human_bytes(4096, 1000, &units), "4.1k");
    }

    #[test]
    fn du_apply_short_flag_accepts_combined_supported_flags() {
        let h = du_apply_short_flag(
            false,
            false,
            DuDisplayFormat::Kib,
            DuUsageMode::DiskBlocks,
            false,
            false,
            false,
            DuLineTerminator::Newline,
            b'h',
        )
        .unwrap();
        let hc = du_apply_short_flag(h.0, h.1, h.2, h.3, h.4, h.5, h.6, h.7, b'c').unwrap();
        let hcs =
            du_apply_short_flag(hc.0, hc.1, hc.2, hc.3, hc.4, hc.5, hc.6, hc.7, b's').unwrap();
        assert_eq!(
            hcs,
            (
                true,
                false,
                DuDisplayFormat::HumanReadableIec,
                DuUsageMode::DiskBlocks,
                true,
                false,
                false,
                DuLineTerminator::Newline
            )
        );
    }

    #[test]
    fn du_display_total_blocks_respects_separate_dirs() {
        assert_eq!(du_display_total_blocks(false, 2, 9), 9);
        assert_eq!(du_display_total_blocks(true, 2, 9), 2);
    }

    #[test]
    fn du_depth_included_respects_optional_limit() {
        assert!(du_depth_included(0, None));
        assert!(du_depth_included(1, Some(1)));
        assert!(!du_depth_included(2, Some(1)));
    }

    #[test]
    fn parse_du_max_depth_accepts_non_negative_integers() {
        assert_eq!(parse_du_max_depth("0").unwrap(), 0);
        assert_eq!(parse_du_max_depth("17").unwrap(), 17);
        assert_eq!(
            parse_du_max_depth("bad").unwrap_err(),
            "invalid maximum depth ‘bad’"
        );
    }

    #[test]
    fn parse_du_block_size_accepts_positive_sizes() {
        assert_eq!(parse_du_block_size("1").unwrap(), 1);
        assert_eq!(parse_du_block_size("2K").unwrap(), 2048);
        assert_eq!(parse_du_block_size("3MiB").unwrap(), 3 * 1024 * 1024);
        assert_eq!(
            parse_du_block_size("0").unwrap_err(),
            "invalid --block-size argument '0'"
        );
        assert_eq!(
            parse_du_block_size("bad").unwrap_err(),
            "invalid --block-size argument 'bad'"
        );
    }

    #[test]
    fn parse_du_threshold_accepts_signed_sizes() {
        assert_eq!(parse_du_threshold("1").unwrap(), DuThreshold::Min(1));
        assert_eq!(parse_du_threshold("+2K").unwrap(), DuThreshold::Min(2048));
        assert_eq!(
            parse_du_threshold("-3MiB").unwrap(),
            DuThreshold::Max(3 * 1024 * 1024)
        );
        assert_eq!(
            parse_du_threshold("-0").unwrap_err(),
            "invalid --threshold argument '-0'"
        );
        assert_eq!(
            parse_du_threshold("bad").unwrap_err(),
            "invalid --threshold argument 'bad'"
        );
    }

    #[test]
    fn du_threshold_includes_uses_measured_usage_bytes() {
        assert!(du_threshold_includes(
            2,
            DuUsageMode::DiskBlocks,
            Some(DuThreshold::Min(1024))
        ));
        assert!(!du_threshold_includes(
            1,
            DuUsageMode::DiskBlocks,
            Some(DuThreshold::Min(1024))
        ));
        assert!(du_threshold_includes(
            2048,
            DuUsageMode::ApparentBytes,
            Some(DuThreshold::Max(2048))
        ));
        assert!(!du_threshold_includes(
            2049,
            DuUsageMode::ApparentBytes,
            Some(DuThreshold::Max(2048))
        ));
    }

    #[test]
    fn du_format_usage_supports_disk_and_apparent_sizes() {
        assert_eq!(
            du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::Kib),
            "4"
        );
        assert_eq!(
            du_format_usage(
                8,
                DuUsageMode::DiskBlocks,
                DuDisplayFormat::HumanReadableIec
            ),
            "4.0K"
        );
        assert_eq!(
            du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::HumanReadableSi),
            "4.1k"
        );
        assert_eq!(
            du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::BlockSize(512)),
            "8"
        );
        assert_eq!(
            du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::BlockSize(2048)),
            "2"
        );
        assert_eq!(
            du_format_usage(1536, DuUsageMode::ApparentBytes, DuDisplayFormat::Kib),
            "2"
        );
        assert_eq!(
            du_format_usage(
                1536,
                DuUsageMode::ApparentBytes,
                DuDisplayFormat::HumanReadableIec
            ),
            "1.5K"
        );
        assert_eq!(
            du_format_usage(
                1536,
                DuUsageMode::ApparentBytes,
                DuDisplayFormat::HumanReadableSi
            ),
            "1.6k"
        );
        assert_eq!(
            du_format_usage(
                1536,
                DuUsageMode::ApparentBytes,
                DuDisplayFormat::BlockSize(1)
            ),
            "1536"
        );
    }

    #[test]
    fn du_bytes_short_flag_enables_apparent_byte_output() {
        let b = du_apply_short_flag(
            false,
            false,
            DuDisplayFormat::Kib,
            DuUsageMode::DiskBlocks,
            false,
            false,
            false,
            DuLineTerminator::Newline,
            b'b',
        )
        .unwrap();
        assert_eq!(
            b,
            (
                false,
                false,
                DuDisplayFormat::BlockSize(1),
                DuUsageMode::ApparentBytes,
                false,
                false,
                false,
                DuLineTerminator::Newline
            )
        );
    }

    #[test]
    fn du_k_and_m_short_flags_override_only_display_units() {
        let k = du_apply_short_flag(
            false,
            false,
            DuDisplayFormat::HumanReadableIec,
            DuUsageMode::ApparentBytes,
            false,
            false,
            false,
            DuLineTerminator::Newline,
            b'k',
        )
        .unwrap();
        assert_eq!(k.2, DuDisplayFormat::BlockSize(1024));
        assert_eq!(k.3, DuUsageMode::ApparentBytes);

        let m = du_apply_short_flag(
            false,
            false,
            DuDisplayFormat::Kib,
            DuUsageMode::DiskBlocks,
            false,
            false,
            false,
            DuLineTerminator::Newline,
            b'm',
        )
        .unwrap();
        assert_eq!(m.2, DuDisplayFormat::BlockSize(1024_u64.pow(2)));
        assert_eq!(m.3, DuUsageMode::DiskBlocks);
    }

    #[test]
    fn permission_denied_components_accepts_permission_kind_and_errnos() {
        assert!(permission_denied_components(
            io::ErrorKind::PermissionDenied,
            None
        ));
        assert!(permission_denied_components(
            io::ErrorKind::Other,
            Some(libc::EACCES)
        ));
        assert!(permission_denied_components(
            io::ErrorKind::Other,
            Some(libc::EPERM)
        ));
        assert!(!permission_denied_components(
            io::ErrorKind::NotFound,
            Some(libc::ENOENT)
        ));
    }

    #[test]
    fn du_node_ready_requires_exact_completion_state() {
        assert!(du_node_ready(true, true, 0, 0, false));
        assert!(!du_node_ready(false, true, 0, 0, false));
        assert!(!du_node_ready(true, false, 0, 0, false));
        assert!(!du_node_ready(true, true, 1, 0, false));
        assert!(!du_node_ready(true, true, 0, 1, false));
        assert!(!du_node_ready(true, true, 0, 0, true));
    }

    #[test]
    fn du_parallel_worker_count_is_bounded_for_tiny_walks() {
        assert_eq!(du_parallel_worker_count_for(0), 1);
        assert_eq!(du_parallel_worker_count_for(1), 1);
        assert_eq!(du_parallel_worker_count_for(4), 4);
        assert_eq!(du_parallel_worker_count_for(32), DU_MAX_WORKERS);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::super::hash::{
        hash_check_should_print_result, hash_check_untagged_kind, HashCheckLineKind,
    };
    use super::{
        du_apply_short_flag, du_display_total_blocks, du_node_ready, permission_denied_components,
        DuDisplayFormat, DuLineTerminator, DuUsageMode,
    };
    use std::io;

    #[kani::proof]
    fn permission_denied_components_accepts_permission_cases() {
        let use_permission_kind: bool = kani::any();
        let errno_is_permission: bool = kani::any();
        let kind = if use_permission_kind {
            io::ErrorKind::PermissionDenied
        } else {
            io::ErrorKind::Other
        };
        let raw = if errno_is_permission {
            Some(if kani::any() {
                libc::EACCES
            } else {
                libc::EPERM
            })
        } else {
            None
        };
        assert_eq!(
            permission_denied_components(kind, raw),
            use_permission_kind || errno_is_permission
        );
    }

    #[kani::proof]
    fn permission_denied_components_rejects_non_permission_cases() {
        let kind = if kani::any() {
            io::ErrorKind::NotFound
        } else {
            io::ErrorKind::Other
        };
        let raw = if kani::any() {
            Some(libc::ENOENT)
        } else {
            None
        };
        assert!(!permission_denied_components(kind, raw));
    }

    #[kani::proof]
    fn du_node_ready_matches_completion_formula() {
        let scanned: bool = kani::any();
        let own_stat_done: bool = kani::any();
        let pending_children: usize = kani::any();
        let pending_file_stats: usize = kani::any();
        let completed: bool = kani::any();

        assert_eq!(
            du_node_ready(
                scanned,
                own_stat_done,
                pending_children,
                pending_file_stats,
                completed
            ),
            scanned
                && own_stat_done
                && pending_children == 0
                && pending_file_stats == 0
                && !completed
        );
    }

    #[kani::proof]
    fn du_short_flag_hcs_sets_expected_state() {
        let h = du_apply_short_flag(
            false,
            false,
            DuDisplayFormat::Kib,
            DuUsageMode::DiskBlocks,
            false,
            false,
            false,
            DuLineTerminator::Newline,
            b'h',
        )
        .unwrap();
        let hc = du_apply_short_flag(h.0, h.1, h.2, h.3, h.4, h.5, h.6, h.7, b'c').unwrap();
        let hcs =
            du_apply_short_flag(hc.0, hc.1, hc.2, hc.3, hc.4, hc.5, hc.6, hc.7, b's').unwrap();
        assert!(hcs.0);
        assert!(!hcs.1);
        assert_eq!(hcs.2, DuDisplayFormat::HumanReadableIec);
        assert_eq!(hcs.3, DuUsageMode::DiskBlocks);
        assert!(hcs.4);
        assert!(!hcs.5);
        assert!(!hcs.6);
    }

    #[kani::proof]
    fn du_display_total_blocks_matches_flag_formula() {
        let separate_dirs: bool = kani::any();
        let exclusive_blocks: u64 = kani::any();
        let subtree_total_blocks: u64 = kani::any();
        assert_eq!(
            du_display_total_blocks(separate_dirs, exclusive_blocks, subtree_total_blocks),
            if separate_dirs {
                exclusive_blocks
            } else {
                subtree_total_blocks
            }
        );
    }

    #[kani::proof]
    fn hash_check_untagged_kind_matches_separator_contract() {
        let separator: u8 = kani::any();
        let has_filename: bool = kani::any();
        let expected = if !has_filename {
            HashCheckLineKind::Invalid
        } else {
            match separator {
                b' ' => HashCheckLineKind::UntaggedText,
                b'*' => HashCheckLineKind::UntaggedBinary,
                _ => HashCheckLineKind::Invalid,
            }
        };
        assert_eq!(hash_check_untagged_kind(separator, has_filename), expected);
    }

    #[kani::proof]
    fn hash_check_print_policy_matches_flag_formula() {
        let success: bool = kani::any();
        let quiet: bool = kani::any();
        let status_only: bool = kani::any();
        assert_eq!(
            hash_check_should_print_result(success, quiet, status_only),
            !status_only && (!success || !quiet)
        );
    }
}

pub(super) fn run_du(args: &[String]) -> io::Result<i32> {
    let mut summarize = false;
    let mut all = false;
    let mut display_format = DuDisplayFormat::Kib;
    let mut usage_mode = DuUsageMode::DiskBlocks;
    let mut total = false;
    let mut separate_dirs = false;
    let mut dereference_args = false;
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
            "-D" | "-H" | "--dereference-args" => dereference_args = true,
            "-P" | "--no-dereference" => dereference_args = false,
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
                        dereference_args,
                        line_terminator,
                    ) = du_apply_short_flag(
                        summarize,
                        all,
                        display_format,
                        usage_mode,
                        total,
                        separate_dirs,
                        dereference_args,
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
            dereference_args,
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
