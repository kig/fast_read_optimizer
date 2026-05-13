use super::*;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepDevicePolicy {
    Read,
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepDirectoryPolicy {
    Read,
    Skip,
    Recurse,
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

/// Recursively collect regular files under `dir` in sorted (depth-first,
/// lexicographic) order.  Symlinks to files are included; symlinks to
/// directories are not followed (matching `-r` / `--directories=recurse`
/// semantics).  Any I/O error encountered while listing a subdirectory is
/// forwarded to `on_error` and that subtree is skipped.
pub(super) fn collect_dir_files_sorted(
    dir: &str,
    on_error: &mut impl FnMut(&str, io::Error),
) -> Vec<String> {
    let mut result = Vec::new();
    collect_recurse(Path::new(dir), &mut result, on_error);
    result
}

fn collect_recurse(
    dir: &Path,
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
            // Only -R (dereference-recursive) would follow them.
        } else if sym_ft.is_dir() {
            collect_recurse(&path, result, on_error);
        } else if sym_ft.is_file() {
            result.push(path.to_string_lossy().into_owned());
        }
        // Block/char devices, FIFOs, sockets found during tree walk are
        // silently skipped; the caller's device_policy applies only to
        // explicit command-line arguments.
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
            &mut |path, err| errors.push(format!("{path}: {err}")),
        );
        assert!(errors.is_empty());
        // link_sub (symlink to dir) must not be followed; only real_sub/x.txt
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("x.txt"), "expected x.txt, got {:?}", files);
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
}
