use super::*;
use std::sync::Mutex;

const FIND_OUTPUT_CHUNK_BYTES: usize = 1 << 20;
const FIND_STDOUT_BUFFER_BYTES: usize = 2 << 20;
const FIND_SERIAL_ROOT_ENTRY_THRESHOLD: usize = 64;

struct FindOutput {
    inner: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl FindOutput {
    fn stdout() -> Self {
        Self {
            inner: Mutex::new(fro::command_io::stdout_buf_writer(FIND_STDOUT_BUFFER_BYTES).unwrap()),
        }
    }

    fn write_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("find output writer lock poisoned"))?;
        inner.write_all(bytes)
    }

    fn flush(self) -> io::Result<()> {
        let mut inner = self
            .inner
            .into_inner()
            .map_err(|_| io::Error::other("find output writer lock poisoned"))?;
        inner.flush()
    }
}

#[derive(Clone, Copy)]
enum FindFileType {
    Block,
    Character,
    Directory,
    Fifo,
    File,
    Symlink,
    Socket,
}

impl FindFileType {
    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "b" => Ok(Self::Block),
            "c" => Ok(Self::Character),
            "d" => Ok(Self::Directory),
            "p" => Ok(Self::Fifo),
            "f" => Ok(Self::File),
            "l" => Ok(Self::Symlink),
            "s" => Ok(Self::Socket),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported find -type '{value}'"),
            )),
        }
    }

    fn matches(self, file_type: fs::FileType) -> bool {
        match self {
            Self::Block => file_type.is_block_device(),
            Self::Character => file_type.is_char_device(),
            Self::Directory => file_type.is_dir(),
            Self::Fifo => file_type.is_fifo(),
            Self::File => file_type.is_file(),
            Self::Symlink => file_type.is_symlink(),
            Self::Socket => file_type.is_socket(),
        }
    }
}

#[derive(Clone)]
struct FindGlobPattern {
    pattern: CString,
    fnmatch_flags: libc::c_int,
}

impl FindGlobPattern {
    fn parse(flag: &str, value: &str, fnmatch_flags: libc::c_int) -> io::Result<Self> {
        Ok(Self {
            pattern: CString::new(value.as_bytes()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("find {flag} pattern cannot contain NUL"),
                )
            })?,
            fnmatch_flags,
        })
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        let Ok(candidate) = CString::new(candidate) else {
            return false;
        };
        unsafe {
            libc::fnmatch(
                self.pattern.as_ptr(),
                candidate.as_ptr(),
                self.fnmatch_flags,
            ) == 0
        }
    }
}

#[derive(Clone)]
struct FindPlan {
    type_filter: Option<FindFileType>,
    name_pattern: Option<FindGlobPattern>,
    path_pattern: Option<FindGlobPattern>,
    max_depth: Option<usize>,
    output_delimiter: u8,
}

impl FindPlan {
    fn matches_root(&self, path: &Path, file_type: fs::FileType, depth: usize) -> bool {
        self.max_depth.is_none_or(|max_depth| depth <= max_depth)
            && self
                .type_filter
                .is_none_or(|expected| expected.matches(file_type))
            && self
                .name_pattern
                .as_ref()
                .is_none_or(|pattern| find_name_matches(pattern, path))
            && self
                .path_pattern
                .as_ref()
                .is_none_or(|pattern| find_path_matches(pattern, path))
    }

    fn matches_child(
        &self,
        dir: &Path,
        file_name: &std::ffi::OsStr,
        file_type: fs::FileType,
        depth: usize,
        path_bytes: &mut Vec<u8>,
    ) -> bool {
        self.max_depth.is_none_or(|max_depth| depth <= max_depth)
            && self
                .type_filter
                .is_none_or(|expected| expected.matches(file_type))
            && self
                .name_pattern
                .as_ref()
                .is_none_or(|pattern| pattern.matches(file_name.as_bytes()))
            && self.path_pattern.as_ref().is_none_or(|pattern| {
                path_bytes.clear();
                append_find_child_path_bytes(path_bytes, dir, file_name);
                pattern.matches(path_bytes)
            })
    }

    fn should_descend(&self, depth: usize) -> bool {
        self.max_depth.is_none_or(|max_depth| depth < max_depth)
    }
}

#[derive(Clone)]
struct FindTask {
    dir: PathBuf,
    depth: usize,
}

pub(super) fn run_find(args: &[String]) -> io::Result<i32> {
    if args
        .get(1)
        .is_some_and(|arg| arg == "-h" || arg == "--help")
    {
        print_find_help(args[0].as_str());
        return Ok(0);
    }
    let (roots, plan) = parse_find_args(args)?;
    let worker_count = parallel_find_worker_count();
    let output = Arc::new(FindOutput::stdout());
    let queue = Arc::new(WorkQueue::default());
    let stop = Arc::new(AtomicBool::new(false));
    let had_warnings = Arc::new(AtomicBool::new(false));
    let mut serial_task = None;
    let serial_candidate = roots.len() == 1;

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
        if plan.matches_root(&path, metadata.file_type(), 0) {
            write_find_path(&output, &path, plan.output_delimiter)?;
        }
        if metadata.file_type().is_dir() && plan.should_descend(0) {
            let task = FindTask {
                dir: path,
                depth: 0,
            };
            if serial_candidate && find_should_use_serial_walk(&task.dir)? {
                serial_task = Some(task);
            } else {
                queue.enqueue_one(task);
            }
        }
    }

    if let Some(task) = serial_task {
        walk_find_subtree_serial(task, output.as_ref(), &had_warnings, &plan)?;
    } else {
        run_parallel_work_queue(queue, stop, worker_count, {
            let output = output.clone();
            let had_warnings = had_warnings.clone();
            move |start_dir, queue, stop| {
                walk_find_subtree(start_dir, queue, &output, stop, &had_warnings, &plan)
            }
        })?;
    }
    let output = Arc::into_inner(output)
        .ok_or_else(|| io::Error::other("find output writer still has active references"))?;
    output.flush()?;
    Ok(if had_warnings.load(Ordering::SeqCst) {
        1
    } else {
        0
    })
}

fn find_should_use_serial_walk(root: &Path) -> io::Result<bool> {
    Ok(fs::read_dir(root)?
        .take(FIND_SERIAL_ROOT_ENTRY_THRESHOLD + 1)
        .count()
        <= FIND_SERIAL_ROOT_ENTRY_THRESHOLD)
}

fn walk_find_subtree(
    start_dir: FindTask,
    queue: &WorkQueue<FindTask>,
    output: &FindOutput,
    stop: &AtomicBool,
    had_warnings: &AtomicBool,
    plan: &FindPlan,
) -> io::Result<()> {
    let mut stack = vec![start_dir];
    let mut chunk = Vec::with_capacity(FIND_OUTPUT_CHUNK_BYTES);
    let mut path_bytes = Vec::new();
    while let Some(task) = stack.pop() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let dir = task.dir;
        let child_depth = task.depth + 1;
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
            let file_name = entry.file_name();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if is_permission_denied(&err) => {
                    let path = child_find_path(&dir, &file_name);
                    write_warning_line("find", &path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            if plan.matches_child(&dir, &file_name, file_type, child_depth, &mut path_bytes) {
                append_find_child_path(&mut chunk, &dir, &file_name, plan.output_delimiter);
                if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                    output.write_all(&chunk)?;
                    chunk.clear();
                }
            }
            if file_type.is_dir() && plan.should_descend(child_depth) {
                child_dirs.push(FindTask {
                    dir: child_find_path(&dir, &file_name),
                    depth: child_depth,
                });
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    output.write_all(&chunk)
}

fn walk_find_subtree_serial(
    start_dir: FindTask,
    output: &FindOutput,
    had_warnings: &AtomicBool,
    plan: &FindPlan,
) -> io::Result<()> {
    let mut stack = vec![start_dir];
    let mut chunk = Vec::with_capacity(FIND_OUTPUT_CHUNK_BYTES);
    let mut path_bytes = Vec::new();
    while let Some(task) = stack.pop() {
        let dir = task.dir;
        let child_depth = task.depth + 1;
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
            let file_name = entry.file_name();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if is_permission_denied(&err) => {
                    let path = child_find_path(&dir, &file_name);
                    write_warning_line("find", &path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            if plan.matches_child(&dir, &file_name, file_type, child_depth, &mut path_bytes) {
                append_find_child_path(&mut chunk, &dir, &file_name, plan.output_delimiter);
                if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                    output.write_all(&chunk)?;
                    chunk.clear();
                }
            }
            if file_type.is_dir() && plan.should_descend(child_depth) {
                child_dirs.push(FindTask {
                    dir: child_find_path(&dir, &file_name),
                    depth: child_depth,
                });
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            stack.extend(child_dirs);
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

fn parse_find_args(args: &[String]) -> io::Result<(Vec<String>, FindPlan)> {
    let mut roots = Vec::new();
    let mut index = 1;
    while let Some(arg) = args.get(index) {
        if is_find_expression_token(arg) {
            break;
        }
        roots.push(arg.clone());
        index += 1;
    }
    if roots.is_empty() {
        roots.push(".".to_string());
    }

    let mut type_filter = None;
    let mut name_pattern = None;
    let mut path_pattern = None;
    let mut max_depth = None;
    let mut output_delimiter = b'\n';
    let mut explicit_output_action = false;
    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-maxdepth" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -maxdepth",
                    )
                })?;
                max_depth = Some(parse_find_max_depth(value)?);
                index += 2;
            }
            "-type" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -type",
                    )
                })?;
                type_filter = Some(FindFileType::parse(value)?);
                index += 2;
            }
            "-name" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -name",
                    )
                })?;
                name_pattern = Some(FindGlobPattern::parse("-name", value, 0)?);
                index += 2;
            }
            "-iname" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -iname",
                    )
                })?;
                name_pattern = Some(FindGlobPattern::parse("-iname", value, libc::FNM_CASEFOLD)?);
                index += 2;
            }
            "-path" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -path",
                    )
                })?;
                path_pattern = Some(FindGlobPattern::parse("-path", value, 0)?);
                index += 2;
            }
            "-ipath" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -ipath",
                    )
                })?;
                path_pattern = Some(FindGlobPattern::parse("-ipath", value, libc::FNM_CASEFOLD)?);
                index += 2;
            }
            "-print" => {
                if explicit_output_action && output_delimiter != b'\n' {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "multiple explicit find output actions are not supported",
                    ));
                }
                explicit_output_action = true;
                output_delimiter = b'\n';
                index += 1;
            }
            "-print0" => {
                if explicit_output_action && output_delimiter != b'\0' {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "multiple explicit find output actions are not supported",
                    ));
                }
                explicit_output_action = true;
                output_delimiter = b'\0';
                index += 1;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported find expression: {other}"),
                ))
            }
        }
    }

    Ok((
        roots,
        FindPlan {
            type_filter,
            name_pattern,
            path_pattern,
            max_depth,
            output_delimiter,
        },
    ))
}

fn is_find_expression_token(arg: &str) -> bool {
    arg.starts_with('-') || matches!(arg, "!" | "(" | ")")
}

fn print_find_help(program: &str) {
    fro::cio_println!(
        "Usage: {program} [path ...] [-maxdepth N] [-type TYPE] [-name PATTERN|-iname PATTERN] [-path PATTERN|-ipath PATTERN] [-print|-print0]"
    );
    fro::cio_println!("Walk directory trees and print matching paths.");
    fro::cio_println!();
    fro::cio_println!("  -maxdepth N        descend at most N levels below each starting path");
    fro::cio_println!("  -type TYPE         filter by file type: b, c, d, p, f, l, or s");
    fro::cio_println!("  -name PATTERN      match the final path component using shell glob syntax");
    fro::cio_println!("  -iname PATTERN     like -name, but match ASCII case-insensitively");
    fro::cio_println!("  -path PATTERN      match the whole emitted path using shell glob syntax");
    fro::cio_println!("  -ipath PATTERN     like -path, but match ASCII case-insensitively");
    fro::cio_println!("  -print             print each matching path followed by a newline (default)");
    fro::cio_println!("  -print0            print each matching path followed by NUL");
    fro::cio_println!("  -h, --help         display this help and exit");
}

fn write_find_path(output: &FindOutput, path: &Path, output_delimiter: u8) -> io::Result<()> {
    let mut chunk = Vec::with_capacity(path.as_os_str().as_bytes().len() + 1);
    append_find_path(&mut chunk, path, output_delimiter);
    output.write_all(&chunk)
}

fn append_find_path(chunk: &mut Vec<u8>, path: &Path, output_delimiter: u8) {
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(output_delimiter);
}

fn append_find_child_path(
    chunk: &mut Vec<u8>,
    dir: &Path,
    file_name: &std::ffi::OsStr,
    output_delimiter: u8,
) {
    append_find_child_path_bytes(chunk, dir, file_name);
    chunk.push(output_delimiter);
}

fn append_find_child_path_bytes(chunk: &mut Vec<u8>, dir: &Path, file_name: &std::ffi::OsStr) {
    chunk.extend_from_slice(dir.as_os_str().as_bytes());
    if !dir.as_os_str().as_bytes().ends_with(b"/") {
        chunk.push(b'/');
    }
    chunk.extend_from_slice(file_name.as_bytes());
}

fn child_find_path(dir: &Path, file_name: &std::ffi::OsStr) -> PathBuf {
    let mut path = dir.to_path_buf();
    path.push(file_name);
    path
}

fn find_name_matches(pattern: &FindGlobPattern, path: &Path) -> bool {
    let name = path.file_name().unwrap_or(path.as_os_str());
    pattern.matches(name.as_bytes())
}

fn find_path_matches(pattern: &FindGlobPattern, path: &Path) -> bool {
    pattern.matches(path.as_os_str().as_bytes())
}

fn parse_find_max_depth(value: &str) -> io::Result<usize> {
    value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find -maxdepth '{value}'"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_find_max_depth_accepts_non_negative_integers() {
        assert_eq!(parse_find_max_depth("0").unwrap(), 0);
        assert_eq!(parse_find_max_depth("12").unwrap(), 12);
        assert_eq!(
            parse_find_max_depth("-1").unwrap_err().to_string(),
            "invalid find -maxdepth '-1'"
        );
        assert_eq!(
            parse_find_max_depth("abc").unwrap_err().to_string(),
            "invalid find -maxdepth 'abc'"
        );
    }

    #[test]
    fn find_plan_should_descend_stops_at_max_depth_boundary() {
        let plan = FindPlan {
            type_filter: None,
            name_pattern: None,
            path_pattern: None,
            max_depth: Some(1),
            output_delimiter: b'\n',
        };

        assert!(plan.should_descend(0));
        assert!(!plan.should_descend(1));
    }
}
