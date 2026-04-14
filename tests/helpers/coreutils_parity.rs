#![cfg(unix)]

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn is_executable_file(path: &Path) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn normalize_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn path_is_under_root(path: &Path, root: &Path) -> bool {
    normalize_path(path).starts_with(normalize_path(root))
}

fn resolve_program_in_path(
    program: &str,
    path: &OsStr,
    blocked_roots: &[PathBuf],
) -> Option<PathBuf> {
    env::split_paths(path)
        .filter(|entry| {
            !blocked_roots
                .iter()
                .any(|root| path_is_under_root(entry, root))
        })
        .find_map(|entry| {
            let candidate = entry.join(program);
            is_executable_file(&candidate).then_some(candidate)
        })
}

pub(crate) fn system_program_path(program: &str) -> PathBuf {
    let program_path = Path::new(program);
    if program_path.components().count() > 1 {
        return program_path.to_path_buf();
    }

    for dir in [
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
        "/usr/local/bin",
        "/usr/local/sbin",
    ] {
        let candidate = Path::new(dir).join(program);
        if is_executable_file(&candidate) {
            return candidate;
        }
    }

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let blocked_roots = vec![repo_root.clone(), repo_root.join("target")];
    let search_path = env::var_os("PATH").unwrap_or_default();
    resolve_program_in_path(program, &search_path, &blocked_roots).unwrap_or_else(|| {
        panic!(
            "failed to resolve system binary for {program} outside repo-managed paths; PATH={}",
            PathBuf::from(search_path).display()
        )
    })
}

#[allow(dead_code)]
pub(crate) struct CoreutilsParityFixture {
    pub root: PathBuf,
    pub text_file: PathBuf,
    pub text_symlink: PathBuf,
    pub binary_file: PathBuf,
    pub binary_symlink: PathBuf,
    pub empty_dir: PathBuf,
    pub tree_root: PathBuf,
    pub nested_dir: PathBuf,
    pub nested_text_file: PathBuf,
    pub nested_binary_file: PathBuf,
    pub directory_symlink: PathBuf,
    pub broken_symlink: PathBuf,
    pub text_bytes: Vec<u8>,
    pub binary_bytes: Vec<u8>,
}

impl CoreutilsParityFixture {
    pub(crate) fn new(prefix: &str) -> Self {
        let root = unique_temp_dir(prefix);
        let text_bytes = b"alpha\tone\n\nneedle beta\nomega\n".to_vec();
        let binary_bytes = (0..65557)
            .map(|i| ((i * 17) % 251) as u8)
            .collect::<Vec<_>>();

        let text_file = root.join("text.txt");
        let text_symlink = root.join("text-link.txt");
        let binary_file = root.join("binary.bin");
        let binary_symlink = root.join("binary-link.bin");
        let empty_dir = root.join("empty-dir");
        let tree_root = root.join("tree");
        let nested_dir = tree_root.join("nested/deeper");
        let nested_text_file = nested_dir.join("leaf.txt");
        let nested_binary_file = nested_dir.join("leaf.bin");
        let directory_symlink = root.join("tree-link");
        let broken_symlink = root.join("broken-link");

        fs::write(&text_file, &text_bytes).unwrap();
        fs::write(&binary_file, &binary_bytes).unwrap();
        fs::create_dir_all(&empty_dir).unwrap();
        fs::create_dir_all(&nested_dir).unwrap();
        fs::write(tree_root.join("root.txt"), b"root\n").unwrap();
        fs::write(&nested_text_file, b"nested needle\nbranch\n").unwrap();
        fs::write(&nested_binary_file, vec![0x33; 16384]).unwrap();

        symlink(&text_file, &text_symlink).unwrap();
        symlink(&binary_file, &binary_symlink).unwrap();
        symlink(&tree_root, &directory_symlink).unwrap();
        symlink(root.join("missing-target"), &broken_symlink).unwrap();

        Self {
            root,
            text_file,
            text_symlink,
            binary_file,
            binary_symlink,
            empty_dir,
            tree_root,
            nested_dir,
            nested_text_file,
            nested_binary_file,
            directory_symlink,
            broken_symlink,
            text_bytes,
            binary_bytes,
        }
    }

    pub(crate) fn text_path_inputs(&self) -> Vec<(&'static str, &Path)> {
        vec![
            ("regular", self.text_file.as_path()),
            ("symlink", self.text_symlink.as_path()),
            ("nested", self.nested_text_file.as_path()),
        ]
    }

    pub(crate) fn binary_path_inputs(&self) -> Vec<(&'static str, &Path)> {
        vec![
            ("regular", self.binary_file.as_path()),
            ("symlink", self.binary_symlink.as_path()),
            ("nested", self.nested_binary_file.as_path()),
        ]
    }

    #[allow(dead_code)]
    pub(crate) fn directory_inputs(&self) -> Vec<(&'static str, &Path)> {
        vec![
            ("empty-dir", self.empty_dir.as_path()),
            ("tree-root", self.tree_root.as_path()),
            ("nested-dir", self.nested_dir.as_path()),
            ("tree-symlink", self.directory_symlink.as_path()),
        ]
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StreamSurface {
    Stdin,
    Dash,
}

impl StreamSurface {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Stdin => "stdin",
            Self::Dash => "dash",
        }
    }

    pub(crate) fn args<'a>(self, base: &[&'a str]) -> Vec<&'a str> {
        let mut args = base.to_vec();
        if matches!(self, Self::Dash) {
            args.push("-");
        }
        args
    }
}

pub(crate) fn stream_surfaces() -> [StreamSurface; 2] {
    [StreamSurface::Stdin, StreamSurface::Dash]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn resolve_program_in_path_skips_blocked_entries() {
        let root = unique_temp_dir("coreutils-system-path-resolution");
        let blocked = root.join("blocked");
        let allowed = root.join("allowed");
        fs::create_dir_all(&blocked).unwrap();
        fs::create_dir_all(&allowed).unwrap();

        let blocked_tool = blocked.join("sha256sum");
        let allowed_tool = allowed.join("sha256sum");
        fs::write(&blocked_tool, b"#!/bin/sh\nexit 1\n").unwrap();
        fs::write(&allowed_tool, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&blocked_tool, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&allowed_tool, fs::Permissions::from_mode(0o755)).unwrap();

        let path = env::join_paths([blocked.as_path(), allowed.as_path()]).unwrap();
        let resolved =
            resolve_program_in_path("sha256sum", &path, std::slice::from_ref(&blocked)).unwrap();
        assert_eq!(resolved, allowed_tool);
    }
}
