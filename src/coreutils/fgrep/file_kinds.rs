use super::*;
use std::collections::HashSet;
use std::ffi::CString;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum FgrepDevicePolicy {
    #[default]
    Read,
    Skip,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum FgrepDirectoryPolicy {
    #[default]
    Read,
    Skip,
    Recurse,
    /// `-R` / `--dereference-recursive`: like `Recurse` but follows symlinks to
    /// both files and directories during tree traversal (with cycle detection).
    RecurseDereference,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepInputPathKind {
    Regular,
    Directory,
    Device,
    Other,
}

pub(super) fn parse_devices_value(value: &str) -> io::Result<FgrepDevicePolicy> {
    match value {
        "read" => Ok(FgrepDevicePolicy::Read),
        "skip" => Ok(FgrepDevicePolicy::Skip),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: unsupported --devices action '{value}'"),
        )),
    }
}

pub(super) fn parse_directories_value(value: &str) -> io::Result<FgrepDirectoryPolicy> {
    match value {
        "read" => Ok(FgrepDirectoryPolicy::Read),
        "skip" => Ok(FgrepDirectoryPolicy::Skip),
        "recurse" => Ok(FgrepDirectoryPolicy::Recurse),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: unsupported --directories action '{value}'"),
        )),
    }
}

pub(super) fn fgrep_input_path_kind(path: &str) -> io::Result<FgrepInputPathKind> {
    let file_type = fs::metadata(path)?.file_type();
    if file_type.is_file() {
        Ok(FgrepInputPathKind::Regular)
    } else if file_type.is_dir() {
        Ok(FgrepInputPathKind::Directory)
    } else if file_type.is_char_device()
        || file_type.is_block_device()
        || file_type.is_fifo()
        || file_type.is_socket()
    {
        Ok(FgrepInputPathKind::Device)
    } else {
        Ok(FgrepInputPathKind::Other)
    }
}

// ── glob filter ───────────────────────────────────────────────────────────

fn fnmatch_bytes(pattern: &CString, candidate: &[u8]) -> bool {
    let Ok(c) = CString::new(candidate) else {
        return false;
    };
    unsafe { libc::fnmatch(pattern.as_ptr(), c.as_ptr(), 0) == 0 }
}

/// A compiled glob pattern list for `--include`, `--exclude`, and
/// `--exclude-dir`.  Each pattern is matched against the file or directory
/// **basename** (not the full path), using POSIX `fnmatch(3)` with no flags.
#[derive(Default, Clone)]
pub(super) struct FgrepGlobFilter {
    /// `--include=GLOB` patterns.  If non-empty, a file must match at least
    /// one pattern to be searched.
    pub include: Vec<CString>,
    /// `--exclude=GLOB` patterns.  A file whose basename matches any pattern
    /// is skipped.
    pub exclude: Vec<CString>,
    /// `--exclude-dir=GLOB` patterns.  A directory whose basename matches any
    /// pattern is not entered during recursive traversal.
    pub exclude_dir: Vec<CString>,
}

impl FgrepGlobFilter {
    pub(super) fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty() && self.exclude_dir.is_empty()
    }

    /// Returns `true` if the file with the given basename should be searched.
    pub(super) fn file_allowed(&self, name: &[u8]) -> bool {
        if !self.include.is_empty() {
            let matched = self.include.iter().any(|p| fnmatch_bytes(p, name));
            if !matched {
                return false;
            }
        }
        for p in &self.exclude {
            if fnmatch_bytes(p, name) {
                return false;
            }
        }
        true
    }

    /// Returns `true` if the directory with the given basename should be
    /// entered during recursive traversal.
    pub(super) fn dir_allowed(&self, name: &[u8]) -> bool {
        for p in &self.exclude_dir {
            if fnmatch_bytes(p, name) {
                return false;
            }
        }
        true
    }

    /// Parse a single glob string into a `CString` pattern suitable for
    /// `fnmatch(3)`.  Returns an error if the string contains a NUL byte.
    pub(super) fn parse_glob(flag: &str, value: &str) -> io::Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("fgrep: {flag} pattern cannot contain NUL"),
            )
        })
    }

    /// Read a pattern file and push each non-empty line as an exclude glob.
    pub(super) fn load_from_file(&mut self, path: &str) -> io::Result<()> {
        let bytes = fs::read(path)
            .map_err(|e| io::Error::new(e.kind(), format!("fgrep: --exclude-from: {path}: {e}")))?;
        for line in super::parse_pattern_file_bytes(&bytes) {
            if !line.is_empty() {
                self.exclude.push(Self::parse_glob(
                    "--exclude-from",
                    &String::from_utf8_lossy(&line),
                )?);
            }
        }
        Ok(())
    }
}

// ── directory traversal ───────────────────────────────────────────────────

/// Recursively collect regular files under `dir` in sorted (depth-first,
/// lexicographic) order.  Symlinks to files are included; symlinks to
/// directories are not followed (matching `-r` / `--directories=recurse`
/// semantics).  Any I/O error encountered while listing a subdirectory is
/// forwarded to `on_error` and that subtree is skipped.
///
/// `filter` is applied to every file and directory basename encountered
/// during traversal.
pub(super) fn collect_dir_files_sorted(
    dir: &str,
    filter: &FgrepGlobFilter,
    on_error: &mut impl FnMut(&str, io::Error),
) -> Vec<String> {
    let mut result = Vec::new();
    collect_recurse(Path::new(dir), filter, &mut result, on_error);
    result
}

fn collect_recurse(
    dir: &Path,
    filter: &FgrepGlobFilter,
    result: &mut Vec<String>,
    on_error: &mut impl FnMut(&str, io::Error),
) {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            on_error(&dir.to_string_lossy(), e);
            return;
        }
    };
    let mut entries: Vec<_> = read_dir
        .filter_map(|e| match e {
            Ok(e) => Some(e),
            Err(err) => {
                on_error(&dir.to_string_lossy(), err);
                None
            }
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let sym_ft = match fs::symlink_metadata(&path) {
            Ok(m) => m.file_type(),
            Err(e) => {
                on_error(&path.to_string_lossy(), e);
                continue;
            }
        };
        if sym_ft.is_symlink() {
            // -r does not follow symlinks at all (GNU grep -r behavior).
            // Only -R (dereference-recursive) follows them.
        } else if sym_ft.is_dir() {
            let name = entry.file_name();
            if filter.dir_allowed(name.as_encoded_bytes()) {
                collect_recurse(&path, filter, result, on_error);
            }
        } else if sym_ft.is_file() {
            let name = entry.file_name();
            if filter.file_allowed(name.as_encoded_bytes()) {
                result.push(path.to_string_lossy().into_owned());
            }
        }
        // Block/char devices, FIFOs, sockets found during tree walk are
        // silently skipped; the caller's device_policy applies only to
        // explicit command-line arguments.
    }
}

/// Recursively collect regular files under `dir` in sorted (depth-first,
/// lexicographic) order, following symlinks to both files and directories
/// (`-R` / `--dereference-recursive` semantics).  Circular symlink chains are
/// detected via `(dev, ino)` pairs and silently skipped.  Any I/O error
/// encountered while listing a subdirectory is forwarded to `on_error` and
/// that subtree is skipped.
///
/// `filter` is applied to every file and directory basename encountered
/// during traversal.
pub(super) fn collect_dir_files_sorted_dereference(
    dir: &str,
    filter: &FgrepGlobFilter,
    on_error: &mut impl FnMut(&str, io::Error),
) -> Vec<String> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    if let Ok(m) = fs::metadata(dir) {
        visited.insert((m.dev(), m.ino()));
    }
    collect_recurse_dereference(Path::new(dir), filter, &mut result, on_error, &mut visited);
    result
}

fn collect_recurse_dereference(
    dir: &Path,
    filter: &FgrepGlobFilter,
    result: &mut Vec<String>,
    on_error: &mut impl FnMut(&str, io::Error),
    visited: &mut HashSet<(u64, u64)>,
) {
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) => {
            on_error(&dir.to_string_lossy(), e);
            return;
        }
    };
    let mut entries: Vec<_> = read_dir
        .filter_map(|e| match e {
            Ok(e) => Some(e),
            Err(err) => {
                on_error(&dir.to_string_lossy(), err);
                None
            }
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        // Use fs::metadata to follow symlinks (dereference).
        let meta = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                on_error(&path.to_string_lossy(), e);
                continue;
            }
        };
        if meta.file_type().is_dir() {
            let name = entry.file_name();
            if !filter.dir_allowed(name.as_encoded_bytes()) {
                continue;
            }
            let id = (meta.dev(), meta.ino());
            if visited.contains(&id) {
                // This directory is an ancestor of the current traversal path;
                // following it would create a cycle.  Warn and skip.
                on_error(
                    &path.to_string_lossy(),
                    io::Error::new(io::ErrorKind::Other, "warning: recursive directory loop"),
                );
                continue;
            }
            // Track only the current DFS ancestor path, not all ever-visited
            // directories.  Remove on exit so the same physical directory can
            // be reached via a separate symlink elsewhere in the tree.
            visited.insert(id);
            collect_recurse_dereference(&path, filter, result, on_error, visited);
            visited.remove(&id);
        } else if meta.file_type().is_file() {
            let name = entry.file_name();
            if filter.file_allowed(name.as_encoded_bytes()) {
                result.push(path.to_string_lossy().into_owned());
            }
        }
        // Devices, FIFOs, and sockets found during tree walk are silently
        // skipped; device_policy applies only to explicit CLI arguments.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_directories_value_accepts_recurse() {
        assert_eq!(
            parse_directories_value("recurse").unwrap(),
            FgrepDirectoryPolicy::Recurse
        );
    }

    #[test]
    fn parse_directories_value_accepts_read_skip() {
        assert_eq!(
            parse_directories_value("read").unwrap(),
            FgrepDirectoryPolicy::Read
        );
        assert_eq!(
            parse_directories_value("skip").unwrap(),
            FgrepDirectoryPolicy::Skip
        );
    }

    #[test]
    fn parse_directories_value_rejects_unknown() {
        assert!(parse_directories_value("other").is_err());
    }

    #[test]
    fn collect_dir_files_sorted_returns_sorted_files() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(format!(
            "collect-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("b.txt"), b"b").unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        std::fs::write(dir.join("sub").join("c.txt"), b"c").unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[test]
    fn collect_dir_files_sorted_does_not_follow_dir_symlinks() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(format!(
            "collect-symlink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sub = dir.join("real_sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("x.txt"), b"x").unwrap();
        std::os::unix::fs::symlink(&sub, dir.join("link_sub")).unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty());
        // link_sub (symlink to dir) must not be followed; only real_sub/x.txt
        assert_eq!(files.len(), 1);
        assert!(
            files[0].ends_with("x.txt"),
            "expected x.txt, got {:?}",
            files
        );
    }

    #[test]
    fn collect_dir_files_sorted_skips_all_symlinks() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(format!(
            "collect-filesym-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("real.txt"), b"r").unwrap();
        std::os::unix::fs::symlink(dir.join("real.txt"), dir.join("link.txt")).unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        // link.txt is a symlink; -r skips all symlinks, so only real.txt appears
        assert_eq!(names, vec!["real.txt"]);
    }

    // ── collect_dir_files_sorted_dereference tests ────────────────────────

    fn make_unique_dir(suffix: &str) -> std::path::PathBuf {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(format!(
            "{suffix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dir
    }

    #[test]
    fn collect_deref_follows_file_symlinks() {
        let dir = make_unique_dir("deref-filesym");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("real.txt"), b"r").unwrap();
        std::os::unix::fs::symlink(dir.join("real.txt"), dir.join("link.txt")).unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted_dereference(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        // -R follows symlinks to files, so both real.txt and link.txt appear.
        assert_eq!(names, vec!["link.txt", "real.txt"]);
    }

    #[test]
    fn collect_deref_follows_dir_symlinks() {
        let dir = make_unique_dir("deref-dirsym");
        let sub = dir.join("real_sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("x.txt"), b"x").unwrap();
        std::os::unix::fs::symlink(&sub, dir.join("link_sub")).unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted_dereference(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        // link_sub is a symlink-to-dir; -R follows it, so x.txt appears twice
        // (once under real_sub and once under link_sub).
        assert_eq!(names, vec!["x.txt", "x.txt"]);
    }

    #[test]
    fn collect_deref_detects_cycles() {
        let dir = make_unique_dir("deref-cycle");
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("a.txt"), b"a").unwrap();
        // Create a symlink inside sub that points back to the parent, forming a cycle.
        std::os::unix::fs::symlink(&dir, sub.join("parent_link")).unwrap();

        let mut errors = Vec::new();
        let files = collect_dir_files_sorted_dereference(
            &dir.to_string_lossy(),
            &FgrepGlobFilter::default(),
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert_eq!(
            errors.len(),
            1,
            "expected one cycle warning, got: {errors:?}"
        );
        assert!(
            errors[0].contains("warning: recursive directory loop"),
            "expected cycle warning, got: {:?}",
            errors[0]
        );
        // Only a.txt; the cycle symlink is emitted as a warning.
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["a.txt"]);
    }

    // ── FgrepGlobFilter tests ─────────────────────────────────────────────

    fn make_filter_dir(suffix: &str) -> std::path::PathBuf {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(format!(
            "filter-{suffix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dir
    }

    #[test]
    fn glob_filter_include_restricts_files() {
        let dir = make_filter_dir("include");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.rs"), b"rs").unwrap();
        std::fs::write(dir.join("bar.txt"), b"txt").unwrap();
        std::fs::write(dir.join("baz.rs"), b"rs2").unwrap();

        let filter = FgrepGlobFilter {
            include: vec![FgrepGlobFilter::parse_glob("--include", "*.rs").unwrap()],
            ..Default::default()
        };
        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(&dir.to_string_lossy(), &filter, &mut |p, e| {
            errors.push(format!("{p}: {e}"))
        });
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(
            names,
            vec!["bar.txt", "baz.rs", "foo.rs"]
                .iter()
                .filter(|n| n.ends_with(".rs"))
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert!(
            names.iter().all(|n| n.ends_with(".rs")),
            "expected only .rs files, got {names:?}"
        );
    }

    #[test]
    fn glob_filter_exclude_skips_files() {
        let dir = make_filter_dir("exclude");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.log"), b"log").unwrap();
        std::fs::write(dir.join("b.txt"), b"txt").unwrap();
        std::fs::write(dir.join("c.log"), b"log2").unwrap();

        let filter = FgrepGlobFilter {
            exclude: vec![FgrepGlobFilter::parse_glob("--exclude", "*.log").unwrap()],
            ..Default::default()
        };
        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(&dir.to_string_lossy(), &filter, &mut |p, e| {
            errors.push(format!("{p}: {e}"))
        });
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["b.txt"]);
    }

    #[test]
    fn glob_filter_exclude_dir_skips_directory() {
        let dir = make_filter_dir("exclude-dir");
        let skip = dir.join("skip_me");
        let keep = dir.join("keep_me");
        std::fs::create_dir_all(&skip).unwrap();
        std::fs::create_dir_all(&keep).unwrap();
        std::fs::write(skip.join("a.txt"), b"a").unwrap();
        std::fs::write(keep.join("b.txt"), b"b").unwrap();

        let filter = FgrepGlobFilter {
            exclude_dir: vec![FgrepGlobFilter::parse_glob("--exclude-dir", "skip_me").unwrap()],
            ..Default::default()
        };
        let mut errors = Vec::new();
        let files = collect_dir_files_sorted(&dir.to_string_lossy(), &filter, &mut |p, e| {
            errors.push(format!("{p}: {e}"))
        });
        assert!(errors.is_empty());
        let names: Vec<_> = files
            .iter()
            .map(|p| {
                std::path::Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["b.txt"]);
    }

    #[test]
    fn glob_filter_empty_allows_all() {
        let dir = make_filter_dir("empty");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("x.txt"), b"x").unwrap();

        let filter = FgrepGlobFilter::default();
        assert!(filter.file_allowed(b"x.txt"));
        assert!(filter.dir_allowed(b"anything"));
    }
}
