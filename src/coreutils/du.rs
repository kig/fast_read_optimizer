use super::*;

fn disk_usage_kib(blocks: u64) -> u64 {
    blocks.div_ceil(2)
}

fn du_format_kib(kib: u64, human_readable: bool) -> String {
    if !human_readable {
        return kib.to_string();
    }
    const UNITS: [&str; 8] = ["K", "M", "G", "T", "P", "E", "Z", "Y"];
    let mut value = kib as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if value >= 10.0 {
        format!("{}{}", value.ceil() as u64, UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

fn append_du_line(chunk: &mut Vec<u8>, kib: u64, path: &Path, human_readable: bool) {
    chunk.extend_from_slice(du_format_kib(kib, human_readable).as_bytes());
    chunk.push(b'\t');
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(b'\n');
}

fn du_depth_included(depth: usize, max_depth: Option<usize>) -> bool {
    max_depth.is_none_or(|limit| depth <= limit)
}

fn parse_du_max_depth(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid maximum depth ‘{value}’"))
}

fn write_du_stderr_line(message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "du: {message}");
}

fn write_du_try_help() {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "Try 'du --help' for more information.");
}

fn du_apply_short_flag(
    summarize: bool,
    all: bool,
    human_readable: bool,
    total: bool,
    flag: u8,
) -> io::Result<(bool, bool, bool, bool)> {
    match flag {
        b's' => Ok((true, all, human_readable, total)),
        b'a' => Ok((summarize, true, human_readable, total)),
        b'h' => Ok((summarize, all, true, total)),
        b'c' => Ok((summarize, all, human_readable, true)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported du flag: -{}", other as char),
        )),
    }
}

#[derive(Clone)]
struct DuLine {
    path: PathBuf,
    kib: u64,
}

struct DuNode {
    path: PathBuf,
    total_kib: u64,
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

fn lstat_no_follow(path: &Path) -> io::Result<libc::stat> {
    let path = cstring_from_os_str(path.as_os_str())?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::lstat(path.as_ptr(), stat.as_mut_ptr()) };
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

fn finish_du_node(node_id: usize, state: &DuSharedState) {
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
            let total_kib = nodes[id].total_kib;
            if nodes[id].emit {
                completed_lines.push(DuLine {
                    path: nodes[id].path.clone(),
                    kib: total_kib,
                });
            }
            nodes[id].completed = true;
            if let Some(parent_id) = nodes[id].parent {
                nodes[parent_id].total_kib += total_kib;
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
    max_depth: Option<usize>,
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
                finish_du_node(task.node_id, state);
                continue;
            }
            Err(err) => return Err(err),
        };
        let dirfd = dir.fd();
        let mut child_dirs = Vec::new();
        let mut file_lines = Vec::new();
        let mut file_total_kib = 0u64;
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
            let kib = disk_usage_kib(stat.st_blocks as u64);
            if stat_is_dir(&stat) {
                let child_depth = task.depth + 1;
                let child_id = {
                    let mut nodes = state.nodes.lock().unwrap();
                    let child_id = nodes.len();
                    nodes.push(DuNode {
                        path: child_path.clone(),
                        total_kib: kib,
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
                file_total_kib += kib;
                if all && du_depth_included(task.depth + 1, max_depth) {
                    file_lines.push(DuLine {
                        path: child_path,
                        kib,
                    });
                }
            }
        }
        {
            let mut nodes = state.nodes.lock().unwrap();
            nodes[task.node_id].pending_children += child_dirs.len();
            nodes[task.node_id].total_kib += file_total_kib;
            nodes[task.node_id].scanned = true;
        }
        if !file_lines.is_empty() {
            state.lines.lock().unwrap().extend(file_lines);
        }
        if let Some(local_dir) = child_dirs.pop() {
            dir_queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
        finish_du_node(task.node_id, state);
    }
    Ok(())
}

fn parallel_du_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(1)
}

fn append_du_output(
    path: &Path,
    summarize: bool,
    all: bool,
    max_depth: Option<usize>,
    human_readable: bool,
    output: &mut Vec<u8>,
    had_warnings: Arc<AtomicBool>,
) -> io::Result<u64> {
    let stat = lstat_no_follow(path)?;
    let root_kib = disk_usage_kib(stat.st_blocks as u64);
    if !stat_is_dir(&stat) {
        append_du_line(output, root_kib, path, human_readable);
        return Ok(root_kib);
    }

    let dir_queue = Arc::new(WorkQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(DuSharedState {
        nodes: Mutex::new(vec![DuNode {
            path: path.to_path_buf(),
            total_kib: root_kib,
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
    dir_queue.enqueue_one(DuTraversalTask {
        path: path.to_path_buf(),
        node_id: 0,
        depth: 0,
    });
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
                max_depth,
            )
        }
    })?;

    let state = Arc::into_inner(state)
        .ok_or_else(|| io::Error::other("du shared state still has active references"))?;
    let lines = state.lines.into_inner().unwrap();
    for line in lines {
        append_du_line(output, line.kib, &line.path, human_readable);
    }
    let total_kib = state.nodes.into_inner().unwrap()[0].total_kib;
    Ok(total_kib)
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
    fn du_format_kib_uses_expected_unit_suffixes() {
        assert_eq!(du_format_kib(7, false), "7");
        assert_eq!(du_format_kib(7, true), "7.0K");
        assert_eq!(du_format_kib(1024, true), "1.0M");
        assert_eq!(du_format_kib(1536, true), "1.5M");
    }

    #[test]
    fn du_apply_short_flag_accepts_combined_supported_flags() {
        let h = du_apply_short_flag(false, false, false, false, b'h').unwrap();
        let hc = du_apply_short_flag(h.0, h.1, h.2, h.3, b'c').unwrap();
        let hcs = du_apply_short_flag(hc.0, hc.1, hc.2, hc.3, b's').unwrap();
        assert_eq!(hcs, (true, false, true, true));
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
}

#[cfg(kani)]
mod kani_proofs {
    use super::super::hash::{
        hash_check_should_print_result, hash_check_untagged_kind, HashCheckLineKind,
    };
    use super::{du_apply_short_flag, du_node_ready, permission_denied_components};
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
        let h = du_apply_short_flag(false, false, false, false, b'h').unwrap();
        let hc = du_apply_short_flag(h.0, h.1, h.2, h.3, b'c').unwrap();
        let hcs = du_apply_short_flag(hc.0, hc.1, hc.2, hc.3, b's').unwrap();
        assert!(hcs.0);
        assert!(!hcs.1);
        assert!(hcs.2);
        assert!(hcs.3);
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
    let mut human_readable = false;
    let mut total = false;
    let mut max_depth = None;
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
            "-h" | "--human-readable" => human_readable = true,
            "-c" | "--total" => total = true,
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
                    (summarize, all, human_readable, total) =
                        du_apply_short_flag(summarize, all, human_readable, total, flag)?;
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

    let out = stdout_buf_writer()?;
    let had_warnings = Arc::new(AtomicBool::new(false));
    let mut grand_total_kib = 0_u64;
    for path in paths {
        let mut chunk = Vec::new();
        let root_total_kib = match append_du_output(
            Path::new(&path),
            summarize,
            all,
            max_depth,
            human_readable,
            &mut chunk,
            had_warnings.clone(),
        ) {
            Ok(root_total_kib) => root_total_kib,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("du", Path::new(&path), &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        grand_total_kib = grand_total_kib.saturating_add(root_total_kib);
        out.write_all(&chunk)?;
    }
    if total {
        let total_path = Path::new("total");
        let mut chunk = Vec::new();
        append_du_line(&mut chunk, grand_total_kib, total_path, human_readable);
        out.write_all(&chunk)?;
    }
    out.into_inner()?;
    Ok(if had_warnings.load(Ordering::SeqCst) {
        1
    } else {
        0
    })
}
