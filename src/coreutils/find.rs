use super::*;
use std::ffi::CString;
use std::os::unix::fs::MetadataExt;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const FIND_OUTPUT_CHUNK_BYTES: usize = 1 << 20;
const FIND_STDOUT_BUFFER_BYTES: usize = 2 << 20;
const FIND_SERIAL_ROOT_ENTRY_THRESHOLD: usize = 64;

struct FindOutput {
    inner: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl FindOutput {
    fn stdout() -> Self {
        Self {
            inner: Mutex::new(
                fro::command_io::stdout_buf_writer(FIND_STDOUT_BUFFER_BYTES).unwrap(),
            ),
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

#[derive(Clone, Copy)]
enum NumericComparison {
    Less(u64),
    Exactly(u64),
    Greater(u64),
}

impl NumericComparison {
    fn matches(self, actual: u64) -> bool {
        match self {
            Self::Less(expected) => actual < expected,
            Self::Exactly(expected) => actual == expected,
            Self::Greater(expected) => actual > expected,
        }
    }
}

#[derive(Clone, Copy)]
enum SizeUnit {
    Bytes,
    Words,
    Blocks,
    Kibibytes,
    Mebibytes,
    Gibibytes,
}

impl SizeUnit {
    fn compare_len(self, len: u64) -> u64 {
        match self {
            Self::Bytes => len,
            Self::Words => div_ceil_u64(len, 2),
            Self::Blocks => div_ceil_u64(len, 512),
            Self::Kibibytes => div_ceil_u64(len, 1024),
            Self::Mebibytes => div_ceil_u64(len, 1024 * 1024),
            Self::Gibibytes => div_ceil_u64(len, 1024 * 1024 * 1024),
        }
    }
}

#[derive(Clone, Copy)]
enum PermComparison {
    Exact,
    AllBits,
    AnyBit,
}

#[derive(Clone, Copy)]
enum TimeField {
    Modified,
    Accessed,
    Changed,
}

impl TimeField {
    fn read(self, metadata: &fs::Metadata) -> io::Result<SystemTime> {
        match self {
            Self::Modified => metadata.modified(),
            Self::Accessed => metadata.accessed(),
            Self::Changed => {
                let ctime = metadata.ctime();
                let secs = Duration::from_secs(ctime.unsigned_abs());
                Ok(if ctime >= 0 {
                    UNIX_EPOCH + secs
                } else {
                    UNIX_EPOCH - secs
                })
            }
        }
    }
}

#[derive(Clone)]
enum FindPredicate {
    Empty,
    Size(NumericComparison, SizeUnit),
    Links(NumericComparison),
    Inum(NumericComparison),
    Newer(SystemTime, TimeField),
    TimePeriods {
        cmp: NumericComparison,
        secs_per_period: u64,
        field: TimeField,
        now: SystemTime,
    },
    Perm(u32, PermComparison),
    Uid(NumericComparison),
    Gid(NumericComparison),
    Nouser,
    Nogroup,
    Readable,
    Writable,
    Executable,
}

impl FindPredicate {
    fn matches(&self, path: &Path, metadata: &fs::Metadata) -> io::Result<bool> {
        match self {
            Self::Empty => {
                let file_type = metadata.file_type();
                if file_type.is_file() {
                    Ok(metadata.len() == 0)
                } else if file_type.is_dir() {
                    let mut entries = fs::read_dir(path)?;
                    match entries.next() {
                        None => Ok(true),
                        Some(Ok(_)) => Ok(false),
                        Some(Err(err)) => Err(err),
                    }
                } else {
                    Ok(false)
                }
            }
            Self::Size(cmp, unit) => Ok(cmp.matches(unit.compare_len(metadata.len()))),
            Self::Links(cmp) => Ok(cmp.matches(metadata.nlink())),
            Self::Inum(cmp) => Ok(cmp.matches(metadata.ino())),
            Self::Newer(reference, field) => Ok(field.read(metadata)? > *reference),
            Self::TimePeriods {
                cmp,
                secs_per_period,
                field,
                now,
            } => {
                let age_secs = now
                    .duration_since(field.read(metadata)?)
                    .unwrap_or(Duration::ZERO)
                    .as_secs();
                Ok(cmp.matches(age_secs / secs_per_period))
            }
            Self::Perm(expected, comparison) => {
                let actual = metadata.mode() & 0o7777;
                Ok(match comparison {
                    PermComparison::Exact => actual == *expected,
                    PermComparison::AllBits => (actual & *expected) == *expected,
                    PermComparison::AnyBit => (actual & *expected) != 0,
                })
            }
            Self::Uid(cmp) => Ok(cmp.matches(metadata.uid().into())),
            Self::Gid(cmp) => Ok(cmp.matches(metadata.gid().into())),
            Self::Nouser => uid_is_unknown(metadata.uid()),
            Self::Nogroup => gid_is_unknown(metadata.gid()),
            Self::Readable => path_access(path, libc::R_OK),
            Self::Writable => path_access(path, libc::W_OK),
            Self::Executable => path_access(path, libc::X_OK),
        }
    }
}

#[derive(Clone)]
struct FindPlan {
    type_filter: Option<FindFileType>,
    name_pattern: Option<FindGlobPattern>,
    path_pattern: Option<FindGlobPattern>,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    extra_predicates: Vec<FindPredicate>,
    output_delimiter: u8,
}

impl FindPlan {
    fn matches_root(&self, path: &Path, file_type: fs::FileType, depth: usize) -> bool {
        self.min_depth.is_none_or(|min_depth| depth >= min_depth)
            && self.max_depth.is_none_or(|max_depth| depth <= max_depth)
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

    fn matches_child_basic(
        &self,
        dir: &Path,
        file_name: &std::ffi::OsStr,
        file_type: fs::FileType,
        depth: usize,
        path_bytes: &mut Vec<u8>,
    ) -> bool {
        self.min_depth.is_none_or(|min_depth| depth >= min_depth)
            && self.max_depth.is_none_or(|max_depth| depth <= max_depth)
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

    fn matches_extra(&self, path: &Path, metadata: &fs::Metadata) -> io::Result<bool> {
        for predicate in &self.extra_predicates {
            if !predicate.matches(path, metadata)? {
                return Ok(false);
            }
        }
        Ok(true)
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
    if args.get(1).is_some_and(|arg| arg == "--version") {
        print_coreutils_version("find");
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
            let extra_ok = if plan.extra_predicates.is_empty() {
                true
            } else {
                match plan.matches_extra(&path, &metadata) {
                    Ok(ok) => ok,
                    Err(err) if is_permission_denied(&err) => {
                        write_warning_line("find", &path, &err, "cannot access");
                        had_warnings.store(true, Ordering::SeqCst);
                        false
                    }
                    Err(err) => return Err(err),
                }
            };
            if extra_ok {
                write_find_path(&output, &path, plan.output_delimiter)?;
            }
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
            if plan.matches_child_basic(&dir, &file_name, file_type, child_depth, &mut path_bytes) {
                let emit = if plan.extra_predicates.is_empty() {
                    true
                } else {
                    let child_path = child_find_path(&dir, &file_name);
                    match fs::symlink_metadata(&child_path) {
                        Ok(meta) => plan.matches_extra(&child_path, &meta)?,
                        Err(err) if is_permission_denied(&err) => {
                            write_warning_line("find", &child_path, &err, "cannot access");
                            had_warnings.store(true, Ordering::SeqCst);
                            false
                        }
                        Err(_) => false,
                    }
                };
                if emit {
                    append_find_child_path(&mut chunk, &dir, &file_name, plan.output_delimiter);
                    if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                        output.write_all(&chunk)?;
                        chunk.clear();
                    }
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
            if plan.matches_child_basic(&dir, &file_name, file_type, child_depth, &mut path_bytes) {
                let emit = if plan.extra_predicates.is_empty() {
                    true
                } else {
                    let child_path = child_find_path(&dir, &file_name);
                    match fs::symlink_metadata(&child_path) {
                        Ok(meta) => plan.matches_extra(&child_path, &meta)?,
                        Err(err) if is_permission_denied(&err) => {
                            write_warning_line("find", &child_path, &err, "cannot access");
                            had_warnings.store(true, Ordering::SeqCst);
                            false
                        }
                        Err(_) => false,
                    }
                };
                if emit {
                    append_find_child_path(&mut chunk, &dir, &file_name, plan.output_delimiter);
                    if chunk.len() >= FIND_OUTPUT_CHUNK_BYTES {
                        output.write_all(&chunk)?;
                        chunk.clear();
                    }
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
    let now = SystemTime::now();
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
    let mut min_depth = None;
    let mut max_depth = None;
    let mut extra_predicates: Vec<FindPredicate> = Vec::new();
    let mut output_delimiter = b'\n';
    let mut explicit_output_action = false;
    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-mindepth" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -mindepth",
                    )
                })?;
                min_depth = Some(parse_find_depth("-mindepth", value)?);
                index += 2;
            }
            "-maxdepth" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -maxdepth",
                    )
                })?;
                max_depth = Some(parse_find_depth("-maxdepth", value)?);
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
            "-empty" => {
                extra_predicates.push(FindPredicate::Empty);
                index += 1;
            }
            "-size" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -size",
                    )
                })?;
                extra_predicates.push(parse_size_predicate("-size", value)?);
                index += 2;
            }
            "-links" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -links",
                    )
                })?;
                extra_predicates.push(FindPredicate::Links(parse_numeric_comparison(
                    "-links", value,
                )?));
                index += 2;
            }
            "-inum" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -inum",
                    )
                })?;
                extra_predicates.push(FindPredicate::Inum(parse_numeric_comparison(
                    "-inum", value,
                )?));
                index += 2;
            }
            "-newer" | "-anewer" | "-cnewer" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing argument to find {arg}"),
                    )
                })?;
                let metadata = fs::symlink_metadata(value)?;
                extra_predicates.push(FindPredicate::Newer(
                    metadata.modified()?,
                    match arg.as_str() {
                        "-newer" => TimeField::Modified,
                        "-anewer" => TimeField::Accessed,
                        "-cnewer" => TimeField::Changed,
                        _ => unreachable!(),
                    },
                ));
                index += 2;
            }
            "-mtime" | "-mmin" | "-atime" | "-amin" | "-ctime" | "-cmin" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing argument to find {arg}"),
                    )
                })?;
                let (secs_per_period, field) = match arg.as_str() {
                    "-mtime" => (86_400, TimeField::Modified),
                    "-mmin" => (60, TimeField::Modified),
                    "-atime" => (86_400, TimeField::Accessed),
                    "-amin" => (60, TimeField::Accessed),
                    "-ctime" => (86_400, TimeField::Changed),
                    "-cmin" => (60, TimeField::Changed),
                    _ => unreachable!(),
                };
                extra_predicates.push(FindPredicate::TimePeriods {
                    cmp: parse_numeric_comparison(arg, value)?,
                    secs_per_period,
                    field,
                    now,
                });
                index += 2;
            }
            "-perm" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -perm",
                    )
                })?;
                extra_predicates.push(parse_perm_predicate(value)?);
                index += 2;
            }
            "-uid" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -uid")
                })?;
                extra_predicates.push(FindPredicate::Uid(parse_numeric_comparison("-uid", value)?));
                index += 2;
            }
            "-user" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -user",
                    )
                })?;
                extra_predicates.push(FindPredicate::Uid(NumericComparison::Exactly(
                    resolve_user_to_uid(value)?,
                )));
                index += 2;
            }
            "-gid" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -gid")
                })?;
                extra_predicates.push(FindPredicate::Gid(parse_numeric_comparison("-gid", value)?));
                index += 2;
            }
            "-group" => {
                let value = args.get(index + 1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing argument to find -group",
                    )
                })?;
                extra_predicates.push(FindPredicate::Gid(NumericComparison::Exactly(
                    resolve_group_to_gid(value)?,
                )));
                index += 2;
            }
            "-nouser" => {
                extra_predicates.push(FindPredicate::Nouser);
                index += 1;
            }
            "-nogroup" => {
                extra_predicates.push(FindPredicate::Nogroup);
                index += 1;
            }
            "-readable" => {
                extra_predicates.push(FindPredicate::Readable);
                index += 1;
            }
            "-writable" => {
                extra_predicates.push(FindPredicate::Writable);
                index += 1;
            }
            "-executable" => {
                extra_predicates.push(FindPredicate::Executable);
                index += 1;
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
            min_depth,
            max_depth,
            extra_predicates,
            output_delimiter,
        },
    ))
}

fn is_find_expression_token(arg: &str) -> bool {
    arg.starts_with('-') || matches!(arg, "!" | "(" | ")")
}

fn print_find_help(program: &str) {
    fro::cio_println!(
        "Usage: {program} [path ...] [-mindepth N] [-maxdepth N] [-type TYPE] [-name PATTERN|-iname PATTERN] [-path PATTERN|-ipath PATTERN] [-empty] [-size N[cwbkMG]] [-links N] [-inum N] [-newer FILE|-anewer FILE|-cnewer FILE] [-mtime N|-mmin N|-atime N|-amin N|-ctime N|-cmin N] [-perm [/+-]MODE] [-user NAME|-uid N] [-group NAME|-gid N] [-nouser] [-nogroup] [-readable] [-writable] [-executable] [-print|-print0] [--help] [--version]"
    );
    fro::cio_println!("Walk directory trees and print matching paths.");
    fro::cio_println!();
    fro::cio_println!("  -mindepth N        emit only entries at depth N or deeper");
    fro::cio_println!("  -maxdepth N        descend at most N levels below each starting path");
    fro::cio_println!("  -type TYPE         filter by file type: b, c, d, p, f, l, or s");
    fro::cio_println!(
        "  -name PATTERN      match the final path component using shell glob syntax"
    );
    fro::cio_println!("  -iname PATTERN     like -name, but match ASCII case-insensitively");
    fro::cio_println!("  -path PATTERN      match the whole emitted path using shell glob syntax");
    fro::cio_println!("  -ipath PATTERN     like -path, but match ASCII case-insensitively");
    fro::cio_println!("  -empty             match empty regular files and empty directories");
    fro::cio_println!("  -size N[cwbkMG]    compare size using find-style numeric prefixes");
    fro::cio_println!("  -links N           compare hard-link count");
    fro::cio_println!("  -inum N            compare inode number");
    fro::cio_println!("  -newer FILE        match if mtime is newer than FILE's mtime");
    fro::cio_println!("  -anewer FILE       match if atime is newer than FILE's mtime");
    fro::cio_println!("  -cnewer FILE       match if ctime is newer than FILE's mtime");
    fro::cio_println!("  -mtime/-mmin N     compare modification age in days or minutes");
    fro::cio_println!("  -atime/-amin N     compare access age in days or minutes");
    fro::cio_println!("  -ctime/-cmin N     compare status-change age in days or minutes");
    fro::cio_println!("  -perm [/+-]MODE    compare permission bits using octal MODE");
    fro::cio_println!("  -user NAME, -uid N match owner by user name or numeric uid");
    fro::cio_println!("  -group NAME, -gid N match group by name or numeric gid");
    fro::cio_println!("  -nouser            match files whose uid has no passwd entry");
    fro::cio_println!("  -nogroup           match files whose gid has no group entry");
    fro::cio_println!("  -readable          match paths accessible for reading");
    fro::cio_println!("  -writable          match paths accessible for writing");
    fro::cio_println!("  -executable        match paths accessible for executing/searching");
    fro::cio_println!(
        "  -print             print each matching path followed by a newline (default)"
    );
    fro::cio_println!("  -print0            print each matching path followed by NUL");
    fro::cio_println!("  -h, --help         display this help and exit");
    fro::cio_println!("      --version      output version information and exit");
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

fn parse_find_depth(flag: &str, value: &str) -> io::Result<usize> {
    value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find {flag} '{value}'"),
        )
    })
}

fn parse_numeric_comparison(flag: &str, value: &str) -> io::Result<NumericComparison> {
    let (kind, digits) = match value.as_bytes().first().copied() {
        Some(b'+') => (1, &value[1..]),
        Some(b'-') => (2, &value[1..]),
        _ => (0, value),
    };
    let parsed = digits.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find {flag} '{value}'"),
        )
    })?;
    Ok(match kind {
        1 => NumericComparison::Greater(parsed),
        2 => NumericComparison::Less(parsed),
        _ => NumericComparison::Exactly(parsed),
    })
}

fn parse_size_predicate(flag: &str, value: &str) -> io::Result<FindPredicate> {
    let (number, unit) = match value.as_bytes().last().copied() {
        Some(b'c') => (&value[..value.len() - 1], SizeUnit::Bytes),
        Some(b'w') => (&value[..value.len() - 1], SizeUnit::Words),
        Some(b'b') => (&value[..value.len() - 1], SizeUnit::Blocks),
        Some(b'k') => (&value[..value.len() - 1], SizeUnit::Kibibytes),
        Some(b'M') => (&value[..value.len() - 1], SizeUnit::Mebibytes),
        Some(b'G') => (&value[..value.len() - 1], SizeUnit::Gibibytes),
        Some(last) if (last as char).is_ascii_digit() => (value, SizeUnit::Blocks),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid find {flag} '{value}'"),
            ))
        }
    };
    Ok(FindPredicate::Size(
        parse_numeric_comparison(flag, number)?,
        unit,
    ))
}

fn parse_perm_predicate(value: &str) -> io::Result<FindPredicate> {
    let (comparison, mode_str) = match value.as_bytes().first().copied() {
        Some(b'-') => (PermComparison::AllBits, &value[1..]),
        Some(b'/') | Some(b'+') => (PermComparison::AnyBit, &value[1..]),
        _ => (PermComparison::Exact, value),
    };
    let mode = u32::from_str_radix(mode_str, 8).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find -perm '{value}'"),
        )
    })?;
    Ok(FindPredicate::Perm(mode, comparison))
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
    value / divisor + u64::from(value % divisor != 0)
}

fn resolve_user_to_uid(name: &str) -> io::Result<u64> {
    if name.bytes().all(|byte| byte.is_ascii_digit()) {
        return name.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid find -user '{name}'"),
            )
        });
    }
    let name_c = CString::new(name.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find -user '{name}'"),
        )
    })?;
    let mut buf_len = passwd_group_buf_len();
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getpwnam_r(
                name_c.as_ptr(),
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        if result.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown user '{name}'"),
            ));
        }
        return Ok(unsafe { (*result).pw_uid.into() });
    }
}

fn resolve_group_to_gid(name: &str) -> io::Result<u64> {
    if name.bytes().all(|byte| byte.is_ascii_digit()) {
        return name.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid find -group '{name}'"),
            )
        });
    }
    let name_c = CString::new(name.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find -group '{name}'"),
        )
    })?;
    let mut buf_len = passwd_group_buf_len();
    loop {
        let mut group = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getgrnam_r(
                name_c.as_ptr(),
                group.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        if result.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unknown group '{name}'"),
            ));
        }
        return Ok(unsafe { (*result).gr_gid.into() });
    }
}

fn uid_is_unknown(uid: u32) -> io::Result<bool> {
    let mut buf_len = passwd_group_buf_len();
    loop {
        let mut pwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getpwuid_r(
                uid,
                pwd.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        return Ok(result.is_null());
    }
}

fn gid_is_unknown(gid: u32) -> io::Result<bool> {
    let mut buf_len = passwd_group_buf_len();
    loop {
        let mut group = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result = std::ptr::null_mut();
        let mut buf = vec![0u8; buf_len];
        let rc = unsafe {
            libc::getgrgid_r(
                gid,
                group.as_mut_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE {
            buf_len *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        return Ok(result.is_null());
    }
}

fn passwd_group_buf_len() -> usize {
    let size = unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) };
    if size <= 0 {
        1024
    } else {
        size as usize
    }
}

fn path_access(path: &Path, mode: libc::c_int) -> io::Result<bool> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;
    Ok(unsafe { libc::access(path.as_ptr(), mode) == 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_find_max_depth_accepts_non_negative_integers() {
        assert_eq!(parse_find_depth("-maxdepth", "0").unwrap(), 0);
        assert_eq!(parse_find_depth("-maxdepth", "12").unwrap(), 12);
        assert_eq!(
            parse_find_depth("-maxdepth", "-1").unwrap_err().to_string(),
            "invalid find -maxdepth '-1'"
        );
        assert_eq!(
            parse_find_depth("-maxdepth", "abc")
                .unwrap_err()
                .to_string(),
            "invalid find -maxdepth 'abc'"
        );
    }

    #[test]
    fn find_plan_should_descend_stops_at_max_depth_boundary() {
        let plan = FindPlan {
            type_filter: None,
            name_pattern: None,
            path_pattern: None,
            min_depth: None,
            max_depth: Some(1),
            extra_predicates: Vec::new(),
            output_delimiter: b'\n',
        };

        assert!(plan.should_descend(0));
        assert!(!plan.should_descend(1));
    }
}
