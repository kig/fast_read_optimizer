use super::*;

fn disk_usage_kib(blocks: u64) -> u64 {
    blocks.div_ceil(2)
}

fn append_du_line(chunk: &mut Vec<u8>, kib: u64, path: &Path) {
    chunk.extend_from_slice(kib.to_string().as_bytes());
    chunk.push(b'\t');
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(b'\n');
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
                        emit: !summarize,
                    });
                    child_id
                };
                child_dirs.push(DuTraversalTask {
                    path: child_path,
                    node_id: child_id,
                });
            } else {
                file_total_kib += kib;
                if all {
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
    output: &mut Vec<u8>,
    had_warnings: Arc<AtomicBool>,
) -> io::Result<()> {
    let stat = lstat_no_follow(path)?;
    let root_kib = disk_usage_kib(stat.st_blocks as u64);
    if !stat_is_dir(&stat) {
        append_du_line(output, root_kib, path);
        return Ok(());
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
    });
    run_parallel_work_queue(dir_queue, stop.clone(), parallel_du_worker_count(), {
        let state = state.clone();
        let had_warnings = had_warnings.clone();
        move |task, dir_queue, stop| {
            walk_du_subtree(task, dir_queue, &state, stop, &had_warnings, summarize, all)
        }
    })?;

    let state = Arc::into_inner(state)
        .ok_or_else(|| io::Error::other("du shared state still has active references"))?;
    let lines = state.lines.into_inner().unwrap();
    for line in lines {
        append_du_line(output, line.kib, &line.path);
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
    use super::{du_node_ready, permission_denied_components};
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
    let mut paths = Vec::new();
    for arg in &args[1..] {
        match arg.as_str() {
            "-s" | "--summarize" => summarize = true,
            "-a" | "--all" => all = true,
            other if other.starts_with('-') && other != "-" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported du flag: {other}"),
                ));
            }
            other => paths.push(other.to_string()),
        }
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

    let out = stdout_buf_writer()?;
    let had_warnings = Arc::new(AtomicBool::new(false));
    for path in paths {
        let mut chunk = Vec::new();
        match append_du_output(
            Path::new(&path),
            summarize,
            all,
            &mut chunk,
            had_warnings.clone(),
        ) {
            Ok(()) => {}
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("du", Path::new(&path), &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        }
        out.write_all(&chunk)?;
    }
    out.into_inner()?;
    Ok(if had_warnings.load(Ordering::SeqCst) {
        1
    } else {
        0
    })
}
