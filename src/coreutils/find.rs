use super::*;
use regex::bytes::{Regex, RegexBuilder};
use std::ffi::CString;
use std::io::BufRead;
use std::os::unix::fs::MetadataExt;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum FindFollowMode {
    Never,
    RootsOnly,
    Always,
}

impl FindFollowMode {
    fn follow_root(self) -> bool {
        matches!(self, Self::RootsOnly | Self::Always)
    }

    fn follow_children(self) -> bool {
        matches!(self, Self::Always)
    }
}

#[derive(Clone)]
struct FindEntryMetadata {
    effective: fs::Metadata,
    link: fs::Metadata,
    followed: bool,
}

#[derive(Clone, Copy)]
enum FindRegexType {
    Emacs,
    PosixExtended,
}

impl FindRegexType {
    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "findutils-default" | "emacs" => Ok(Self::Emacs),
            "posix-extended" | "egrep" | "posix-egrep" => Ok(Self::PosixExtended),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported find -regextype '{value}'"),
            )),
        }
    }
}

#[derive(Clone)]
struct FindRegexPattern {
    regex: Regex,
}

impl FindRegexPattern {
    fn parse(
        flag: &str,
        value: &str,
        regex_type: FindRegexType,
        case_insensitive: bool,
    ) -> io::Result<Self> {
        let pattern = match regex_type {
            FindRegexType::Emacs | FindRegexType::PosixExtended => value,
        };
        let regex = RegexBuilder::new(pattern)
            .case_insensitive(case_insensitive)
            .build()
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid find {flag} pattern '{value}': {err}"),
                )
            })?;
        Ok(Self { regex })
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        self.regex
            .find(candidate)
            .is_some_and(|matched| matched.start() == 0 && matched.end() == candidate.len())
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
    True,
    Type(FindFileType),
    Name(FindGlobPattern),
    Path(FindGlobPattern),
    Regex(FindRegexPattern),
    Empty,
    False,
    LinkName(FindGlobPattern),
    XType(FindFileType),
    Size(NumericComparison, SizeUnit),
    Links(NumericComparison),
    Inum(NumericComparison),
    Newer(SystemTime, TimeField),
    Used(NumericComparison),
    TimePeriods {
        cmp: NumericComparison,
        secs_per_period: u64,
        field: TimeField,
        now: SystemTime,
        use_daystart_boundary: bool,
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
    fn matches(
        &self,
        path: &Path,
        entry: &FindEntryMetadata,
        follow_mode: FindFollowMode,
    ) -> io::Result<bool> {
        match self {
            Self::True => Ok(true),
            Self::Type(expected) => Ok(expected.matches(entry.effective.file_type())),
            Self::Name(pattern) => Ok(find_name_matches(pattern, path)),
            Self::Path(pattern) => Ok(find_path_matches(pattern, path)),
            Self::Regex(pattern) => Ok(pattern.matches(path.as_os_str().as_bytes())),
            Self::Empty => {
                let file_type = entry.effective.file_type();
                if file_type.is_file() {
                    Ok(entry.effective.len() == 0)
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
            Self::False => Ok(false),
            Self::LinkName(pattern) => {
                if !entry.link.file_type().is_symlink() || entry.followed {
                    return Ok(false);
                }
                Ok(pattern.matches(fs::read_link(path)?.as_os_str().as_bytes()))
            }
            Self::XType(expected) => match_xtype(path, entry, follow_mode, *expected),
            Self::Size(cmp, unit) => Ok(cmp.matches(unit.compare_len(entry.effective.len()))),
            Self::Links(cmp) => Ok(cmp.matches(entry.effective.nlink())),
            Self::Inum(cmp) => Ok(cmp.matches(entry.effective.ino())),
            Self::Newer(reference, field) => Ok(field.read(&entry.effective)? > *reference),
            Self::Used(cmp) => {
                let accessed = entry.effective.accessed()?;
                let changed = TimeField::Changed.read(&entry.effective)?;
                let Ok(delta) = accessed.duration_since(changed) else {
                    return Ok(false);
                };
                let days = delta.as_secs() / 86_400;
                Ok(cmp.matches(days))
            }
            Self::TimePeriods {
                cmp,
                secs_per_period,
                field,
                now,
                use_daystart_boundary,
            } => {
                let actual = if *use_daystart_boundary {
                    let file_day = current_local_day_start(field.read(&entry.effective)?)?;
                    now.duration_since(file_day)
                        .unwrap_or(Duration::ZERO)
                        .as_secs()
                        / secs_per_period
                } else {
                    now.duration_since(field.read(&entry.effective)?)
                        .unwrap_or(Duration::ZERO)
                        .as_secs()
                        / secs_per_period
                };
                Ok(cmp.matches(actual))
            }
            Self::Perm(expected, comparison) => {
                let actual = entry.effective.mode() & 0o7777;
                Ok(match comparison {
                    PermComparison::Exact => actual == *expected,
                    PermComparison::AllBits => (actual & *expected) == *expected,
                    PermComparison::AnyBit => (actual & *expected) != 0,
                })
            }
            Self::Uid(cmp) => Ok(cmp.matches(entry.effective.uid().into())),
            Self::Gid(cmp) => Ok(cmp.matches(entry.effective.gid().into())),
            Self::Nouser => uid_is_unknown(entry.effective.uid()),
            Self::Nogroup => gid_is_unknown(entry.effective.gid()),
            Self::Readable => path_access(path, libc::R_OK),
            Self::Writable => path_access(path, libc::W_OK),
            Self::Executable => path_access(path, libc::X_OK),
        }
    }
}

#[derive(Clone)]
struct FindExecAction {
    argv: Vec<std::ffi::OsString>,
    prompt: bool,
    chdir_parent: bool,
}

impl FindExecAction {
    fn run(&self, path: &Path) -> io::Result<bool> {
        let Some(program) = self.argv.first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "find -exec requires a command",
            ));
        };
        if self.prompt && !prompt_find_exec(&self.argv, path)? {
            return Ok(false);
        }
        let replacement = if self.chdir_parent {
            let mut relative = std::ffi::OsString::from("./");
            relative.push(path.file_name().unwrap_or(path.as_os_str()));
            relative
        } else {
            path.as_os_str().to_os_string()
        };
        let mut command =
            std::process::Command::new(render_exec_arg_with(program, replacement.as_os_str()));
        command.args(
            self.argv
                .iter()
                .skip(1)
                .map(|arg| render_exec_arg_with(arg, replacement.as_os_str())),
        );
        if self.chdir_parent {
            command.current_dir(path.parent().unwrap_or_else(|| Path::new(".")));
        }
        command.stdin(std::process::Stdio::inherit());
        command.stdout(std::process::Stdio::inherit());
        command.stderr(std::process::Stdio::inherit());
        Ok(command.status()?.success())
    }
}

#[derive(Clone, Copy, Default)]
struct FindEvalOutcome {
    matched: bool,
    prune: bool,
    quit: bool,
}

impl FindEvalOutcome {
    fn matched(matched: bool) -> Self {
        Self {
            matched,
            ..Self::default()
        }
    }
}

#[derive(Clone)]
enum FindFormatDirective {
    Path,
    Basename,
    Parent,
    Size,
    FileType,
}

#[derive(Clone)]
enum FindFormatPart {
    Literal(Vec<u8>),
    Directive(FindFormatDirective),
}

#[derive(Clone)]
struct FindFormatTemplate {
    parts: Vec<FindFormatPart>,
}

impl FindFormatTemplate {
    fn parse(flag: &str, value: &str) -> io::Result<Self> {
        let bytes = value.as_bytes();
        let mut parts = Vec::new();
        let mut literal = Vec::new();
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'\\' => {
                    index += 1;
                    let Some(&escaped) = bytes.get(index) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid find {flag} format '{value}'"),
                        ));
                    };
                    literal.push(match escaped {
                        b'\\' => b'\\',
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'0' => b'\0',
                        other => other,
                    });
                    index += 1;
                }
                b'%' => {
                    index += 1;
                    let Some(&directive) = bytes.get(index) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid find {flag} format '{value}'"),
                        ));
                    };
                    if !literal.is_empty() {
                        parts.push(FindFormatPart::Literal(std::mem::take(&mut literal)));
                    }
                    match directive {
                        b'%' => literal.push(b'%'),
                        b'p' => parts.push(FindFormatPart::Directive(FindFormatDirective::Path)),
                        b'f' => parts.push(FindFormatPart::Directive(FindFormatDirective::Basename)),
                        b'h' => parts.push(FindFormatPart::Directive(FindFormatDirective::Parent)),
                        b's' => parts.push(FindFormatPart::Directive(FindFormatDirective::Size)),
                        b'y' => parts.push(FindFormatPart::Directive(FindFormatDirective::FileType)),
                        other => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "unsupported find {flag} format directive '%{}'",
                                    other as char
                                ),
                            ))
                        }
                    }
                    index += 1;
                }
                byte => {
                    literal.push(byte);
                    index += 1;
                }
            }
        }
        if !literal.is_empty() {
            parts.push(FindFormatPart::Literal(literal));
        }
        Ok(Self { parts })
    }

    fn render(&self, path: &Path, metadata: &fs::Metadata) -> Vec<u8> {
        let mut rendered = Vec::new();
        for part in &self.parts {
            match part {
                FindFormatPart::Literal(bytes) => rendered.extend_from_slice(bytes),
                FindFormatPart::Directive(FindFormatDirective::Path) => {
                    rendered.extend_from_slice(path.as_os_str().as_bytes());
                }
                FindFormatPart::Directive(FindFormatDirective::Basename) => {
                    rendered.extend_from_slice(
                        path.file_name()
                            .unwrap_or(path.as_os_str())
                            .as_bytes(),
                    );
                }
                FindFormatPart::Directive(FindFormatDirective::Parent) => {
                    let parent = path.parent().unwrap_or_else(|| Path::new("."));
                    rendered.extend_from_slice(parent.as_os_str().as_bytes());
                }
                FindFormatPart::Directive(FindFormatDirective::Size) => {
                    rendered.extend_from_slice(metadata.len().to_string().as_bytes());
                }
                FindFormatPart::Directive(FindFormatDirective::FileType) => {
                    rendered.push(find_file_type_letter(metadata.file_type()));
                }
            }
        }
        rendered
    }
}

#[derive(Clone)]
struct FindFileFormatAction {
    format: FindFormatTemplate,
    output: Arc<Mutex<std::io::BufWriter<std::fs::File>>>,
}

impl FindFileFormatAction {
    fn open(path: &str, format: FindFormatTemplate) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            format,
            output: Arc::new(Mutex::new(std::io::BufWriter::new(file))),
        })
    }

    fn write(&self, path: &Path, metadata: &fs::Metadata) -> io::Result<bool> {
        let rendered = self.format.render(path, metadata);
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("find fprintf writer lock poisoned"))?;
        output.write_all(&rendered)?;
        Ok(true)
    }
}

#[derive(Clone)]
struct FindFileLsAction {
    output: Arc<Mutex<std::io::BufWriter<std::fs::File>>>,
}

impl FindFileLsAction {
    fn open(path: &str) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            output: Arc::new(Mutex::new(std::io::BufWriter::new(file))),
        })
    }

    fn write(&self, path: &Path, metadata: &fs::Metadata) -> io::Result<bool> {
        let rendered = render_find_ls_line(path, metadata)?;
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("find fls writer lock poisoned"))?;
        output.write_all(&rendered)?;
        Ok(true)
    }
}

#[derive(Clone)]
enum FindAction {
    Print(u8),
    Exec(FindExecAction),
    PrintFormat(FindFormatTemplate),
    FileFormat(FindFileFormatAction),
    Ls,
    Fls(FindFileLsAction),
    Prune,
    Quit,
}

impl FindAction {
    fn evaluate(
        &self,
        path: &Path,
        metadata: &fs::Metadata,
        output: &FindOutput,
    ) -> io::Result<FindEvalOutcome> {
        match self {
            Self::Print(delimiter) => {
                write_find_path(output, path, *delimiter)?;
                Ok(FindEvalOutcome::matched(true))
            }
            Self::Exec(exec) => Ok(FindEvalOutcome::matched(exec.run(path)?)),
            Self::PrintFormat(format) => {
                output.write_all(&format.render(path, metadata))?;
                Ok(FindEvalOutcome::matched(true))
            }
            Self::FileFormat(file_action) => Ok(FindEvalOutcome::matched(file_action.write(
                path, metadata,
            )?)),
            Self::Ls => {
                output.write_all(&render_find_ls_line(path, metadata)?)?;
                Ok(FindEvalOutcome::matched(true))
            }
            Self::Fls(file_action) => Ok(FindEvalOutcome::matched(file_action.write(path, metadata)?)),
            Self::Prune => Ok(FindEvalOutcome {
                matched: true,
                prune: true,
                quit: false,
            }),
            Self::Quit => Ok(FindEvalOutcome {
                matched: true,
                prune: false,
                quit: true,
            }),
        }
    }
}

#[derive(Clone)]
enum FindExpression {
    Predicate(FindPredicate),
    Action(FindAction),
    Not(Box<FindExpression>),
    And(Box<FindExpression>, Box<FindExpression>),
    Or(Box<FindExpression>, Box<FindExpression>),
}

impl FindExpression {
    fn evaluate(
        &self,
        path: &Path,
        entry: &FindEntryMetadata,
        follow_mode: FindFollowMode,
        output: &FindOutput,
    ) -> io::Result<FindEvalOutcome> {
        match self {
            Self::Predicate(predicate) => Ok(FindEvalOutcome::matched(predicate.matches(
                path,
                entry,
                follow_mode,
            )?)),
            Self::Action(action) => action.evaluate(path, &entry.effective, output),
            Self::Not(expr) => {
                let mut outcome = expr.evaluate(path, entry, follow_mode, output)?;
                outcome.matched = !outcome.matched;
                Ok(outcome)
            }
            Self::And(left, right) => {
                let left_outcome = left.evaluate(path, entry, follow_mode, output)?;
                if left_outcome.quit {
                    return Ok(left_outcome);
                }
                if !left_outcome.matched {
                    return Ok(left_outcome);
                }
                let right_outcome = right.evaluate(path, entry, follow_mode, output)?;
                Ok(FindEvalOutcome {
                    matched: left_outcome.matched && right_outcome.matched,
                    prune: left_outcome.prune || right_outcome.prune,
                    quit: left_outcome.quit || right_outcome.quit,
                })
            }
            Self::Or(left, right) => {
                let left_outcome = left.evaluate(path, entry, follow_mode, output)?;
                if left_outcome.quit || left_outcome.matched {
                    return Ok(left_outcome);
                }
                let right_outcome = right.evaluate(path, entry, follow_mode, output)?;
                Ok(FindEvalOutcome {
                    matched: right_outcome.matched,
                    prune: left_outcome.prune || right_outcome.prune,
                    quit: left_outcome.quit || right_outcome.quit,
                })
            }
        }
    }
}

#[derive(Clone)]
struct FindPlan {
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    follow_mode: FindFollowMode,
    same_file_system: bool,
    depth_first: bool,
    has_action: bool,
    suppress_default_print: bool,
    expression: FindExpression,
}

impl FindPlan {
    fn should_descend(&self, depth: usize) -> bool {
        self.max_depth.is_none_or(|max_depth| depth < max_depth)
    }

    fn should_evaluate(&self, depth: usize) -> bool {
        self.min_depth.is_none_or(|min_depth| depth >= min_depth)
            && self.max_depth.is_none_or(|max_depth| depth <= max_depth)
    }

    fn evaluate_path(
        &self,
        path: &Path,
        entry: &FindEntryMetadata,
        depth: usize,
        output: &FindOutput,
    ) -> io::Result<FindEvalOutcome> {
        if !self.should_evaluate(depth) {
            return Ok(FindEvalOutcome::default());
        }
        let outcome = self
            .expression
            .evaluate(path, entry, self.follow_mode, output)?;
        if outcome.matched && !self.suppress_default_print {
            write_find_path(output, path, b'\n')?;
        }
        Ok(outcome)
    }
}

#[derive(Clone)]
struct FindTask {
    dir: PathBuf,
    depth: usize,
    root_device: u64,
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
    let output = Arc::new(FindOutput::stdout());
    let had_warnings = Arc::new(AtomicBool::new(false));
    let force_serial = plan.depth_first || plan.has_action || plan.follow_mode != FindFollowMode::Never;
    let worker_count = if force_serial { 0 } else { parallel_find_worker_count() };
    let queue = (!force_serial).then(|| Arc::new(WorkQueue::default()));
    let stop = (!force_serial).then(|| Arc::new(AtomicBool::new(false)));
    let serial_candidate = roots.len() == 1;
    let mut serial_tasks = Vec::new();

    for root in roots {
        let path = PathBuf::from(root);
        let entry = match load_find_entry_metadata(&path, plan.follow_mode.follow_root()) {
            Ok(entry) => entry,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("find", &path, &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        let root_type = entry.effective.file_type();
        let root_outcome = if !root_type.is_dir() || !plan.depth_first {
            Some(plan.evaluate_path(&path, &entry, 0, &output)?)
        } else {
            None
        };
        if root_outcome.is_some_and(|outcome| outcome.quit) {
            break;
        }
        if root_type.is_dir()
            && plan.should_descend(0)
            && !root_outcome.is_some_and(|outcome| outcome.prune)
        {
            let task = FindTask {
                dir: path.clone(),
                depth: 0,
                root_device: entry.effective.dev(),
            };
            if force_serial || (serial_candidate && find_should_use_serial_walk(&task.dir)?) {
                serial_tasks.push(task);
            } else {
                queue
                    .as_ref()
                    .expect("parallel queue missing")
                    .enqueue_one(task);
            }
        }
        if root_type.is_dir() && plan.depth_first {
            serial_tasks.push(FindTask {
                dir: path,
                depth: usize::MAX,
                root_device: entry.effective.dev(),
            });
        }
    }

    if force_serial {
        for task in serial_tasks {
            if task.depth == usize::MAX {
                let entry = load_find_entry_metadata(&task.dir, plan.follow_mode.follow_root())?;
                if plan.evaluate_path(&task.dir, &entry, 0, &output)?.quit {
                    break;
                }
            } else {
                if walk_find_subtree_serial(
                    task,
                    output.as_ref(),
                    &had_warnings,
                    &plan,
                    &[],
                )? {
                    break;
                }
            }
        }
    } else if !serial_tasks.is_empty() {
        for task in serial_tasks {
            if walk_find_subtree_serial(task, output.as_ref(), &had_warnings, &plan, &[])?
            {
                break;
            }
        }
    } else {
        run_parallel_work_queue(
            queue.expect("parallel queue missing"),
            stop.expect("parallel stop missing"),
            worker_count,
            {
            let output = output.clone();
            let had_warnings = had_warnings.clone();
            move |start_dir, queue, stop| {
                walk_find_subtree(
                    start_dir,
                    queue,
                    &output,
                    stop,
                    &had_warnings,
                    &plan,
                )
            }
        },
        )?;
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
            let child_path = child_find_path(&dir, &file_name);
            let entry = match load_find_entry_metadata(&child_path, plan.follow_mode.follow_children())
            {
                Ok(entry) => entry,
                Err(err) if is_permission_denied(&err) => {
                    write_warning_line("find", &child_path, &err, "cannot access");
                    had_warnings.store(true, Ordering::SeqCst);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let outcome = plan.evaluate_path(&child_path, &entry, child_depth, output)?;
            if outcome.quit {
                stop.store(true, Ordering::SeqCst);
                break;
            }
            if let Some(child_task) = find_child_task(
                plan,
                &child_path,
                child_depth,
                task.root_device,
                &entry,
                outcome.prune,
            )? {
                child_dirs.push(child_task);
            }
        }
        if let Some(local_dir) = child_dirs.pop() {
            queue.enqueue(child_dirs);
            stack.push(local_dir);
        }
    }
    Ok(())
}

fn walk_find_subtree_serial(
    start_dir: FindTask,
    output: &FindOutput,
    had_warnings: &AtomicBool,
    plan: &FindPlan,
    ancestor_dirs: &[(u64, u64)],
) -> io::Result<bool> {
    let dir = start_dir.dir;
    let child_depth = start_dir.depth + 1;
    let dir_entry = load_find_entry_metadata(&dir, plan.follow_mode.follow_root())?;
    let mut next_ancestors = ancestor_dirs.to_vec();
    let current_dir_key = (dir_entry.effective.dev(), dir_entry.effective.ino());
    next_ancestors.push(current_dir_key);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if is_permission_denied(&err) => {
            write_warning_line("find", &dir, &err, "cannot read directory");
            had_warnings.store(true, Ordering::SeqCst);
            return Ok(false);
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
        let child_path = child_find_path(&dir, &file_name);
        let entry = match load_find_entry_metadata(&child_path, plan.follow_mode.follow_children()) {
            Ok(entry) => entry,
            Err(err) if is_permission_denied(&err) => {
                write_warning_line("find", &child_path, &err, "cannot access");
                had_warnings.store(true, Ordering::SeqCst);
                continue;
            }
            Err(err) => return Err(err),
        };
        if entry.effective.file_type().is_dir() {
            if !plan.depth_first {
                let outcome = plan.evaluate_path(&child_path, &entry, child_depth, output)?;
                if outcome.quit {
                    return Ok(true);
                }
                if let Some(child_task) = find_child_task(
                    plan,
                    &child_path,
                    child_depth,
                    start_dir.root_device,
                    &entry,
                    outcome.prune,
                )? {
                    let child_key = (entry.effective.dev(), entry.effective.ino());
                    if next_ancestors.contains(&child_key) {
                        continue;
                    }
                    if walk_find_subtree_serial(
                        child_task,
                        output,
                        had_warnings,
                        plan,
                        &next_ancestors,
                    )?
                    {
                        return Ok(true);
                    }
                }
            } else {
                if let Some(child_task) = find_child_task(
                    plan,
                    &child_path,
                    child_depth,
                    start_dir.root_device,
                    &entry,
                    false,
                )? {
                    let child_key = (entry.effective.dev(), entry.effective.ino());
                    if next_ancestors.contains(&child_key) {
                        continue;
                    }
                    if walk_find_subtree_serial(
                        child_task,
                        output,
                        had_warnings,
                        plan,
                        &next_ancestors,
                    )?
                    {
                        return Ok(true);
                    }
                }
                if plan.evaluate_path(&child_path, &entry, child_depth, output)?.quit {
                    return Ok(true);
                }
            }
        } else {
            if plan.evaluate_path(&child_path, &entry, child_depth, output)?.quit {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn parallel_find_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1)
}

fn find_child_task(
    plan: &FindPlan,
    child_path: &Path,
    depth: usize,
    root_device: u64,
    entry: &FindEntryMetadata,
    pruned: bool,
) -> io::Result<Option<FindTask>> {
    if pruned || !entry.effective.file_type().is_dir() || !plan.should_descend(depth) {
        return Ok(None);
    }
    if plan.same_file_system {
        if entry.effective.dev() != root_device {
            return Ok(None);
        }
    }
    Ok(Some(FindTask {
        dir: child_path.to_path_buf(),
        depth,
        root_device,
    }))
}

fn parse_find_optimization_level(arg: &str) -> io::Result<bool> {
    if !arg.starts_with("-O") {
        return Ok(false);
    }
    let digits = &arg[2..];
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid find optimisation level '{arg}'"),
        ));
    }
    Ok(true)
}

fn current_local_day_start(now: SystemTime) -> io::Result<SystemTime> {
    let unix_secs = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "find daystart requires post-epoch time",
            )
        })?
        .as_secs();
    let raw_time = unix_secs.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "find daystart timestamp overflow",
        )
    })?;
    let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
    if unsafe { libc::localtime_r(&raw_time, &mut local) }.is_null() {
        return Err(io::Error::last_os_error());
    }
    local.tm_hour = 0;
    local.tm_min = 0;
    local.tm_sec = 0;
    let midnight = unsafe { libc::mktime(&mut local) };
    if midnight < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(UNIX_EPOCH + Duration::from_secs(midnight as u64))
}

fn match_xtype(
    path: &Path,
    entry: &FindEntryMetadata,
    follow_mode: FindFollowMode,
    expected: FindFileType,
) -> io::Result<bool> {
    let link_type = entry.link.file_type();
    if !link_type.is_symlink() {
        return Ok(expected.matches(entry.effective.file_type()));
    }
    if follow_mode == FindFollowMode::Always {
        return Ok(matches!(expected, FindFileType::Symlink));
    }
    match fs::metadata(path) {
        Ok(target_meta) => Ok(expected.matches(target_meta.file_type())),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            Ok(matches!(expected, FindFileType::Symlink))
        }
        Err(err) => Err(err),
    }
}

fn load_find_entry_metadata(path: &Path, follow: bool) -> io::Result<FindEntryMetadata> {
    let link = fs::symlink_metadata(path)?;
    if !follow {
        return Ok(FindEntryMetadata {
            effective: link.clone(),
            link,
            followed: false,
        });
    }
    let effective = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => link.clone(),
        Err(err) => return Err(err),
    };
    Ok(FindEntryMetadata {
        effective,
        link,
        followed: true,
    })
}

struct FindParseState {
    time_reference: SystemTime,
    daystart_active: bool,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    regex_type: FindRegexType,
    follow_mode: FindFollowMode,
    same_file_system: bool,
    depth_first: bool,
    has_action: bool,
    suppress_default_print: bool,
}

fn parse_find_expression(
    args: &[String],
    index: &mut usize,
    state: &mut FindParseState,
) -> io::Result<FindExpression> {
    if *index >= args.len() {
        return Ok(FindExpression::Predicate(FindPredicate::True));
    }
    parse_find_or(args, index, state)
}

fn parse_find_or(
    args: &[String],
    index: &mut usize,
    state: &mut FindParseState,
) -> io::Result<FindExpression> {
    let mut expr = parse_find_and(args, index, state)?;
    while let Some(token) = args.get(*index).map(String::as_str) {
        if !matches!(token, "-o" | "-or") {
            break;
        }
        *index += 1;
        let rhs = parse_find_and(args, index, state)?;
        expr = FindExpression::Or(Box::new(expr), Box::new(rhs));
    }
    Ok(expr)
}

fn parse_find_and(
    args: &[String],
    index: &mut usize,
    state: &mut FindParseState,
) -> io::Result<FindExpression> {
    let mut expr = parse_find_unary(args, index, state)?;
    while let Some(token) = args.get(*index).map(String::as_str) {
        if matches!(token, "-o" | "-or" | ")") {
            break;
        }
        if matches!(token, "-a" | "-and") {
            *index += 1;
        }
        let rhs = parse_find_unary(args, index, state)?;
        expr = FindExpression::And(Box::new(expr), Box::new(rhs));
    }
    Ok(expr)
}

fn parse_find_unary(
    args: &[String],
    index: &mut usize,
    state: &mut FindParseState,
) -> io::Result<FindExpression> {
    match args.get(*index).map(String::as_str) {
        Some("!") | Some("-not") => {
            *index += 1;
            Ok(FindExpression::Not(Box::new(parse_find_unary(
                args, index, state,
            )?)))
        }
        _ => parse_find_primary(args, index, state),
    }
}

fn parse_find_primary(
    args: &[String],
    index: &mut usize,
    state: &mut FindParseState,
) -> io::Result<FindExpression> {
    let Some(arg) = args.get(*index).map(String::as_str) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing find expression",
        ));
    };
    match arg {
        "(" | ")" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported find expression: {arg}"),
        )),
        "-mindepth" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -mindepth")
            })?;
            state.min_depth = Some(parse_find_depth("-mindepth", value)?);
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-maxdepth" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -maxdepth")
            })?;
            state.max_depth = Some(parse_find_depth("-maxdepth", value)?);
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-daystart" => {
            state.time_reference = current_local_day_start(state.time_reference)?;
            state.daystart_active = true;
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-depth" => {
            state.depth_first = true;
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-follow" => {
            state.follow_mode = FindFollowMode::Always;
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-mount" | "-xdev" => {
            state.same_file_system = true;
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-regextype" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -regextype")
            })?;
            state.regex_type = FindRegexType::parse(value)?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-type" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -type")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Type(
                FindFileType::parse(value)?,
            )))
        }
        "-xtype" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -xtype")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::XType(
                FindFileType::parse(value)?,
            )))
        }
        "-name" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -name")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Name(
                FindGlobPattern::parse("-name", value, 0)?,
            )))
        }
        "-iname" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -iname")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Name(
                FindGlobPattern::parse("-iname", value, libc::FNM_CASEFOLD)?,
            )))
        }
        "-path" | "-wholename" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("missing argument to find {arg}"))
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Path(
                FindGlobPattern::parse(arg, value, 0)?,
            )))
        }
        "-ipath" | "-iwholename" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("missing argument to find {arg}"))
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Path(
                FindGlobPattern::parse(arg, value, libc::FNM_CASEFOLD)?,
            )))
        }
        "-lname" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -lname")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::LinkName(
                FindGlobPattern::parse("-lname", value, 0)?,
            )))
        }
        "-ilname" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -ilname")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::LinkName(
                FindGlobPattern::parse("-ilname", value, libc::FNM_CASEFOLD)?,
            )))
        }
        "-regex" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -regex")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Regex(
                FindRegexPattern::parse("-regex", value, state.regex_type, false)?,
            )))
        }
        "-iregex" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -iregex")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Regex(
                FindRegexPattern::parse("-iregex", value, state.regex_type, true)?,
            )))
        }
        "-true" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-false" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::False))
        }
        "-noleaf" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::True))
        }
        "-empty" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Empty))
        }
        "-size" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -size")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(parse_size_predicate("-size", value)?))
        }
        "-links" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -links")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Links(
                parse_numeric_comparison("-links", value)?,
            )))
        }
        "-inum" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -inum")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Inum(
                parse_numeric_comparison("-inum", value)?,
            )))
        }
        "-newer" | "-anewer" | "-cnewer" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("missing argument to find {arg}"))
            })?;
            let metadata = fs::symlink_metadata(value)?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Newer(
                metadata.modified()?,
                match arg {
                    "-newer" => TimeField::Modified,
                    "-anewer" => TimeField::Accessed,
                    "-cnewer" => TimeField::Changed,
                    _ => unreachable!(),
                },
            )))
        }
        "-used" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -used")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Used(
                parse_numeric_comparison("-used", value)?,
            )))
        }
        "-mtime" | "-mmin" | "-atime" | "-amin" | "-ctime" | "-cmin" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("missing argument to find {arg}"))
            })?;
            let (secs_per_period, field) = match arg {
                "-mtime" => (86_400, TimeField::Modified),
                "-mmin" => (60, TimeField::Modified),
                "-atime" => (86_400, TimeField::Accessed),
                "-amin" => (60, TimeField::Accessed),
                "-ctime" => (86_400, TimeField::Changed),
                "-cmin" => (60, TimeField::Changed),
                _ => unreachable!(),
            };
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::TimePeriods {
                cmp: parse_numeric_comparison(arg, value)?,
                secs_per_period,
                field,
                now: state.time_reference,
                use_daystart_boundary: state.daystart_active && secs_per_period == 86_400,
            }))
        }
        "-perm" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -perm")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(parse_perm_predicate(value)?))
        }
        "-uid" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -uid")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Uid(
                parse_numeric_comparison("-uid", value)?,
            )))
        }
        "-user" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -user")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Uid(
                NumericComparison::Exactly(resolve_user_to_uid(value)?),
            )))
        }
        "-gid" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -gid")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Gid(
                parse_numeric_comparison("-gid", value)?,
            )))
        }
        "-group" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -group")
            })?;
            *index += 2;
            Ok(FindExpression::Predicate(FindPredicate::Gid(
                NumericComparison::Exactly(resolve_group_to_gid(value)?),
            )))
        }
        "-nouser" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Nouser))
        }
        "-nogroup" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Nogroup))
        }
        "-readable" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Readable))
        }
        "-writable" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Writable))
        }
        "-executable" => {
            *index += 1;
            Ok(FindExpression::Predicate(FindPredicate::Executable))
        }
        "-print" => {
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 1;
            Ok(FindExpression::Action(FindAction::Print(b'\n')))
        }
        "-print0" => {
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 1;
            Ok(FindExpression::Action(FindAction::Print(b'\0')))
        }
        "-printf" => {
            let value = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing argument to find -printf")
            })?;
            let format = FindFormatTemplate::parse("-printf", value)?;
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 2;
            Ok(FindExpression::Action(FindAction::PrintFormat(format)))
        }
        "-fprintf" => {
            let path = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing file argument to find -fprintf")
            })?;
            let value = args.get(*index + 2).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing format argument to find -fprintf")
            })?;
            let format = FindFormatTemplate::parse("-fprintf", value)?;
            let action = FindFileFormatAction::open(path, format)?;
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 3;
            Ok(FindExpression::Action(FindAction::FileFormat(action)))
        }
        "-exec" => {
            let action = parse_find_exec_action(args, index, false, false)?;
            state.has_action = true;
            state.suppress_default_print = true;
            Ok(FindExpression::Action(FindAction::Exec(action)))
        }
        "-ok" => {
            let action = parse_find_exec_action(args, index, true, false)?;
            state.has_action = true;
            state.suppress_default_print = true;
            Ok(FindExpression::Action(FindAction::Exec(action)))
        }
        "-okdir" => {
            let action = parse_find_exec_action(args, index, true, true)?;
            state.has_action = true;
            state.suppress_default_print = true;
            Ok(FindExpression::Action(FindAction::Exec(action)))
        }
        "-ls" => {
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 1;
            Ok(FindExpression::Action(FindAction::Ls))
        }
        "-fls" => {
            let path = args.get(*index + 1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing file argument to find -fls")
            })?;
            let action = FindFileLsAction::open(path)?;
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 2;
            Ok(FindExpression::Action(FindAction::Fls(action)))
        }
        "-prune" => {
            state.has_action = true;
            *index += 1;
            Ok(FindExpression::Action(FindAction::Prune))
        }
        "-quit" => {
            state.has_action = true;
            state.suppress_default_print = true;
            *index += 1;
            Ok(FindExpression::Action(FindAction::Quit))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported find expression: {other}"),
        )),
    }
}

fn parse_find_exec_action(
    args: &[String],
    index: &mut usize,
    prompt: bool,
    chdir_parent: bool,
) -> io::Result<FindExecAction> {
    let mut argv = Vec::new();
    *index += 1;
    while let Some(arg) = args.get(*index) {
        match arg.as_str() {
            ";" => {
                *index += 1;
                if argv.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "find -exec requires a command before ';'",
                    ));
                }
                return Ok(FindExecAction {
                    argv,
                    prompt,
                    chdir_parent,
                });
            }
            "+" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported find expression: -exec ... +",
                ))
            }
            _ => {
                argv.push(std::ffi::OsString::from(arg));
                *index += 1;
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "missing ';' terminator for find -exec",
    ))
}

fn parse_find_args(args: &[String]) -> io::Result<(Vec<String>, FindPlan)> {
    let mut roots = Vec::new();
    let mut index = 1;
    let mut follow_mode = FindFollowMode::Never;
    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "-P" => {
                follow_mode = FindFollowMode::Never;
                index += 1;
            }
            "-H" => {
                follow_mode = FindFollowMode::RootsOnly;
                index += 1;
            }
            "-L" => {
                follow_mode = FindFollowMode::Always;
                index += 1;
            }
            _ if parse_find_optimization_level(arg)? => index += 1,
            _ => break,
        }
    }
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

    let mut state = FindParseState {
        time_reference: SystemTime::now(),
        daystart_active: false,
        min_depth: None,
        max_depth: None,
        regex_type: FindRegexType::Emacs,
        follow_mode,
        same_file_system: false,
        depth_first: false,
        has_action: false,
        suppress_default_print: false,
    };
    let expression = parse_find_expression(args, &mut index, &mut state)?;
    if index != args.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unsupported find expression: {}",
                args.get(index).map(String::as_str).unwrap_or_default()
            ),
        ));
    }

    Ok((
        roots,
        FindPlan {
            min_depth: state.min_depth,
            max_depth: state.max_depth,
            follow_mode: state.follow_mode,
            same_file_system: state.same_file_system,
            depth_first: state.depth_first,
            has_action: state.has_action,
            suppress_default_print: state.suppress_default_print,
            expression,
        },
    ))
}

fn is_find_expression_token(arg: &str) -> bool {
    arg.starts_with('-') || matches!(arg, "!" | "(" | ")")
}

fn print_find_help(program: &str) {
    fro::cio_println!(
        "Usage: {program} [-P|-H|-L] [-Olevel] [path ...] [-mindepth N] [-maxdepth N] [-mount|-xdev] [-type TYPE] [-xtype TYPE] [-name PATTERN|-iname PATTERN] [-path PATTERN|-ipath PATTERN|-wholename PATTERN|-iwholename PATTERN] [-lname PATTERN|-ilname PATTERN] [-regex PATTERN|-iregex PATTERN] [-regextype TYPE] [-empty] [-size N[cwbkMG]] [-links N] [-inum N] [-newer FILE|-anewer FILE|-cnewer FILE] [-used N] [-daystart] [-mtime N|-mmin N|-atime N|-amin N|-ctime N|-cmin N] [-perm [/+-]MODE] [-user NAME|-uid N] [-group NAME|-gid N] [-nouser] [-nogroup] [-readable] [-writable] [-executable] [-true|-false] [-depth] [!|-not] [-a|-and] [-o|-or] [-noleaf] [-print|-print0|-printf FMT|-fprintf FILE FMT|-exec CMD ... ';'|-ok CMD ... ';'|-okdir CMD ... ';'|-ls|-fls FILE|-prune|-quit] [--help] [--version]"
    );
    fro::cio_println!("Walk directory trees and print matching paths.");
    fro::cio_println!();
    fro::cio_println!("  -P                 never follow symlinks (default)");
    fro::cio_println!("  -H                 follow command-line symlink roots only");
    fro::cio_println!("  -L                 follow command-line and discovered symlinks");
    fro::cio_println!("  -Olevel            GNU optimisation hint accepted before paths");
    fro::cio_println!("  -mindepth N        emit only entries at depth N or deeper");
    fro::cio_println!("  -maxdepth N        descend at most N levels below each starting path");
    fro::cio_println!("  -mount, -xdev      stay on the same device as each starting path");
    fro::cio_println!("  -type TYPE         filter by file type: b, c, d, p, f, l, or s");
    fro::cio_println!(
        "  -xtype TYPE        use symlink target type matching under -P/no-follow mode"
    );
    fro::cio_println!(
        "  -name PATTERN      match the final path component using shell glob syntax"
    );
    fro::cio_println!("  -iname PATTERN     like -name, but match ASCII case-insensitively");
    fro::cio_println!("  -path PATTERN      match the whole emitted path using shell glob syntax");
    fro::cio_println!("  -ipath PATTERN     like -path, but match ASCII case-insensitively");
    fro::cio_println!("  -wholename PATTERN alias for -path");
    fro::cio_println!("  -iwholename PATTERN alias for -ipath");
    fro::cio_println!("  -lname PATTERN     match symlink targets using shell glob syntax");
    fro::cio_println!("  -ilname PATTERN    like -lname, but match ASCII case-insensitively");
    fro::cio_println!("  -regex PATTERN     full-path regex match using the current regex syntax");
    fro::cio_println!("  -iregex PATTERN    like -regex, but match ASCII case-insensitively");
    fro::cio_println!("  -regextype TYPE    regex syntax: emacs/findutils-default or posix-extended/egrep");
    fro::cio_println!("  -empty             match empty regular files and empty directories");
    fro::cio_println!("  -size N[cwbkMG]    compare size using find-style numeric prefixes");
    fro::cio_println!("  -links N           compare hard-link count");
    fro::cio_println!("  -inum N            compare inode number");
    fro::cio_println!("  -newer FILE        match if mtime is newer than FILE's mtime");
    fro::cio_println!("  -anewer FILE       match if atime is newer than FILE's mtime");
    fro::cio_println!("  -cnewer FILE       match if ctime is newer than FILE's mtime");
    fro::cio_println!("  -used N            compare whole days between atime and ctime");
    fro::cio_println!("  -daystart          compare later time predicates from the start of today");
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
    fro::cio_println!("  -true              always match");
    fro::cio_println!("  -false             never match");
    fro::cio_println!("  -depth             visit directory entries before the directory itself");
    fro::cio_println!("  !, -not            negate the following predicate or action");
    fro::cio_println!("  -o, -or            logical OR between adjacent expressions");
    fro::cio_println!("  -noleaf            accept GNU noleaf and keep the current traversal plan");
    fro::cio_println!("  -a, -and           logical AND between adjacent predicates (default)");
    fro::cio_println!(
        "  -print             print each matching path followed by a newline (default)"
    );
    fro::cio_println!("  -print0            print each matching path followed by NUL");
    fro::cio_println!("  -printf FMT        write bounded formatted output to stdout");
    fro::cio_println!("  -fprintf FILE FMT  write bounded formatted output to FILE");
    fro::cio_println!("  -ls                emit a GNU-like long listing for each match");
    fro::cio_println!("  -fls FILE          write GNU-like long listings to FILE");
    fro::cio_println!("  -exec CMD ... ';'  run CMD once per matching path; '{{}}' expands to the path");
    fro::cio_println!("  -ok CMD ... ';'    like -exec, but prompt before each command");
    fro::cio_println!("  -okdir CMD ... ';' like -ok, but run in the match parent directory");
    fro::cio_println!("  -prune             skip descending into the current matched directory");
    fro::cio_println!("  -quit              stop the walk immediately");
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

fn child_find_path(dir: &Path, file_name: &std::ffi::OsStr) -> PathBuf {
    let mut path = dir.to_path_buf();
    path.push(file_name);
    path
}

fn render_exec_arg_with(
    template: &std::ffi::OsStr,
    replacement: &std::ffi::OsStr,
) -> std::ffi::OsString {
    let template_bytes = template.as_bytes();
    if !template_bytes.windows(2).any(|window| window == b"{}") {
        return template.to_os_string();
    }
    let mut rendered = Vec::with_capacity(template_bytes.len() + replacement.as_bytes().len());
    let mut remaining = template_bytes;
    while let Some(pos) = remaining.windows(2).position(|window| window == b"{}") {
        rendered.extend_from_slice(&remaining[..pos]);
        rendered.extend_from_slice(replacement.as_bytes());
        remaining = &remaining[pos + 2..];
    }
    rendered.extend_from_slice(remaining);
    std::os::unix::ffi::OsStringExt::from_vec(rendered)
}

fn prompt_find_exec(argv: &[std::ffi::OsString], path: &Path) -> io::Result<bool> {
    let program = argv
        .first()
        .map(std::ffi::OsString::as_os_str)
        .unwrap_or_else(|| std::ffi::OsStr::new(""));
    let mut prompt = Vec::new();
    prompt.extend_from_slice(b"< ");
    prompt.extend_from_slice(program.as_bytes());
    prompt.extend_from_slice(b" ... ");
    prompt.extend_from_slice(path.as_os_str().as_bytes());
    prompt.extend_from_slice(b" > ? ");
    {
        let mut stderr = std::io::stderr().lock();
        stderr.write_all(&prompt)?;
        stderr.flush()?;
    }
    let mut response = String::new();
    std::io::stdin().lock().read_line(&mut response)?;
    Ok(matches!(
        response.bytes().find(|byte| !byte.is_ascii_whitespace()),
        Some(b'y' | b'Y')
    ))
}

fn render_find_ls_line(path: &Path, metadata: &fs::Metadata) -> io::Result<Vec<u8>> {
    let inode = metadata.ino();
    let blocks = metadata.blocks() / 2;
    let mode = render_find_mode(metadata.file_type(), metadata.mode());
    let nlink = metadata.nlink();
    let owner = resolve_uid_name(metadata.uid())?;
    let group = resolve_gid_name(metadata.gid())?;
    let size = metadata.len();
    let time = format_find_ls_time(metadata.modified()?)?;
    let line = format!(
        "{inode:>9} {blocks:>6} {mode} {nlink:>3} {owner:<8} {group:<8} {size:>8} {time} {}\n",
        path.display()
    );
    Ok(line.into_bytes())
}

fn render_find_mode(file_type: fs::FileType, mode: u32) -> String {
    let mut rendered = String::with_capacity(11);
    rendered.push(find_ls_file_type_letter(file_type));
    let bits = [
        0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
    ];
    let chars = ['r', 'w', 'x', 'r', 'w', 'x', 'r', 'w', 'x'];
    for (bit, ch) in bits.into_iter().zip(chars) {
        rendered.push(if mode & bit != 0 { ch } else { '-' });
    }
    rendered
}

fn find_ls_file_type_letter(file_type: fs::FileType) -> char {
    if file_type.is_block_device() {
        'b'
    } else if file_type.is_char_device() {
        'c'
    } else if file_type.is_dir() {
        'd'
    } else if file_type.is_fifo() {
        'p'
    } else if file_type.is_file() {
        '-'
    } else if file_type.is_symlink() {
        'l'
    } else if file_type.is_socket() {
        's'
    } else {
        '?'
    }
}

fn format_find_ls_time(time: SystemTime) -> io::Result<String> {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "timestamp before unix epoch"))?
        .as_secs() as libc::time_t;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "system clock before unix epoch"))?
        .as_secs() as libc::time_t;
    let mut tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    let mut now_tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    if unsafe { libc::localtime_r(&secs, tm.as_mut_ptr()) }.is_null()
        || unsafe { libc::localtime_r(&now, now_tm.as_mut_ptr()) }.is_null()
    {
        return Err(io::Error::last_os_error());
    }
    let tm = unsafe { tm.assume_init() };
    let now_tm = unsafe { now_tm.assume_init() };
    let recent = (now - secs).unsigned_abs() <= 15_778_476;
    let fmt = if recent && tm.tm_year == now_tm.tm_year {
        "%b %e %H:%M"
    } else {
        "%b %e  %Y"
    };
    let fmt_c = CString::new(fmt).expect("valid ls time format");
    let mut buf = [0u8; 64];
    let written = unsafe {
        libc::strftime(
            buf.as_mut_ptr().cast(),
            buf.len(),
            fmt_c.as_ptr(),
            &tm,
        )
    };
    if written == 0 {
        return Err(io::Error::other("strftime failed for find -ls timestamp"));
    }
    Ok(String::from_utf8_lossy(&buf[..written]).into_owned())
}

fn find_file_type_letter(file_type: fs::FileType) -> u8 {
    if file_type.is_block_device() {
        b'b'
    } else if file_type.is_char_device() {
        b'c'
    } else if file_type.is_dir() {
        b'd'
    } else if file_type.is_fifo() {
        b'p'
    } else if file_type.is_file() {
        b'f'
    } else if file_type.is_symlink() {
        b'l'
    } else if file_type.is_socket() {
        b's'
    } else {
        b'?'
    }
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

fn resolve_uid_name(uid: u32) -> io::Result<String> {
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
        if result.is_null() {
            return Ok(uid.to_string());
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*result).pw_name) };
        return Ok(name.to_string_lossy().into_owned());
    }
}

fn resolve_gid_name(gid: u32) -> io::Result<String> {
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
        if result.is_null() {
            return Ok(gid.to_string());
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*result).gr_name) };
        return Ok(name.to_string_lossy().into_owned());
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
            min_depth: None,
            max_depth: Some(1),
            follow_mode: FindFollowMode::Never,
            same_file_system: false,
            depth_first: false,
            has_action: false,
            suppress_default_print: false,
            expression: FindExpression::Predicate(FindPredicate::True),
        };

        assert!(plan.should_descend(0));
        assert!(!plan.should_descend(1));
    }

    #[test]
    fn parse_find_args_accepts_explicit_default_and_and_tokens() {
        let args = vec![
            "find".to_string(),
            "-P".to_string(),
            "root".to_string(),
            "-type".to_string(),
            "f".to_string(),
            "-a".to_string(),
            "-name".to_string(),
            "*.rs".to_string(),
            "-and".to_string(),
            "-print".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(plan.has_action);
        assert!(matches!(plan.expression, FindExpression::And(_, _)));
    }

    #[test]
    fn parse_find_args_accepts_path_aliases_and_constant_predicates() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-wholename".to_string(),
            "*/src/*".to_string(),
            "-a".to_string(),
            "-false".to_string(),
            "-noleaf".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(plan.expression, FindExpression::And(_, _)));
    }

    #[test]
    fn parse_find_args_accepts_link_name_predicates() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-lname".to_string(),
            "target*".to_string(),
            "-ilname".to_string(),
            "TARGET*".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(
            plan.expression,
            FindExpression::And(_, _)
        ));
    }

    #[test]
    fn parse_find_args_accepts_xtype_predicate() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-xtype".to_string(),
            "d".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(
            plan.expression,
            FindExpression::Predicate(FindPredicate::XType(FindFileType::Directory))
        ));
    }

    #[test]
    fn parse_find_args_accepts_daystart_before_time_predicates() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-daystart".to_string(),
            "-mtime".to_string(),
            "0".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(
            plan.expression,
            FindExpression::And(_, _)
        ));
    }

    #[test]
    fn parse_find_args_accepts_optimization_level_before_roots() {
        let args = vec![
            "find".to_string(),
            "-O9".to_string(),
            "root".to_string(),
            "-type".to_string(),
            "f".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(
            plan.expression,
            FindExpression::Predicate(FindPredicate::Type(FindFileType::File))
        ));
    }

    #[test]
    fn parse_find_args_accepts_same_filesystem_aliases() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-mount".to_string(),
            "-xdev".to_string(),
            "-type".to_string(),
            "d".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(plan.same_file_system);
    }

    #[test]
    fn parse_find_args_accepts_symlink_modes_and_used() {
        let args = vec![
            "find".to_string(),
            "-H".to_string(),
            "root".to_string(),
            "-used".to_string(),
            "-1".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(plan.follow_mode, FindFollowMode::RootsOnly));
        assert!(matches!(
            plan.expression,
            FindExpression::Predicate(FindPredicate::Used(_))
        ));
    }

    #[test]
    fn parse_find_args_accepts_depth_not_or_and_exec_tokens() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-depth".to_string(),
            "-not".to_string(),
            "-name".to_string(),
            "*.tmp".to_string(),
            "-o".to_string(),
            "-exec".to_string(),
            "printf".to_string(),
            "%s\\n".to_string(),
            "{}".to_string(),
            ";".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(plan.depth_first);
        assert!(plan.has_action);
        assert!(matches!(plan.expression, FindExpression::Or(_, _)));
    }

    #[test]
    fn parse_find_args_accepts_ls_ok_and_prune_tokens() {
        let args = vec![
            "find".to_string(),
            "-L".to_string(),
            "root".to_string(),
            "-path".to_string(),
            "*/skip".to_string(),
            "-prune".to_string(),
            "-o".to_string(),
            "-ok".to_string(),
            "printf".to_string(),
            "%s\\n".to_string(),
            "{}".to_string(),
            ";".to_string(),
            "-ls".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(plan.follow_mode, FindFollowMode::Always));
        assert!(plan.has_action);
        assert!(plan.suppress_default_print);
        assert!(matches!(plan.expression, FindExpression::Or(_, _)));
    }

    #[test]
    fn parse_find_args_accepts_regex_predicates_and_regextype() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-regextype".to_string(),
            "posix-extended".to_string(),
            "-regex".to_string(),
            ".*/(src|tests)/.*".to_string(),
            "-iregex".to_string(),
            ".*RS".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(matches!(plan.expression, FindExpression::And(_, _)));
    }

    #[test]
    fn parse_find_args_rejects_unsupported_regextype() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-regextype".to_string(),
            "sed".to_string(),
            "-regex".to_string(),
            ".*".to_string(),
        ];

        let err = match parse_find_args(&args) {
            Ok(_) => panic!("expected unsupported regextype to fail"),
            Err(err) => err,
        };
        assert_eq!(err.to_string(), "unsupported find -regextype 'sed'");
    }

    #[test]
    fn parse_find_args_accepts_printf_and_fprintf_tokens() {
        let args = vec![
            "find".to_string(),
            "root".to_string(),
            "-printf".to_string(),
            "%p\\n".to_string(),
            "-fprintf".to_string(),
            "out.txt".to_string(),
            "%f\\n".to_string(),
        ];

        let (roots, plan) = parse_find_args(&args).unwrap();
        assert_eq!(roots, vec!["root".to_string()]);
        assert!(plan.has_action);
        assert!(matches!(plan.expression, FindExpression::And(_, _)));
    }
}
