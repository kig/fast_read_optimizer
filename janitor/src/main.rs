mod config;

use config::{
    FILE_SIZE_ROOTS, FORMAL_TOOL_BINARIES, MAX_FILE_LINES, MAX_FILE_LINES_ALLOWLIST,
    README_LINK_CHECK_FILES, SRC_TREE_MAX_DEPTH, SRC_TREE_SNAPSHOT_FILE,
};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

type CheckResult = Result<(), String>;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("janitor crate lives directly under the repo root")
        .to_path_buf()
}

fn usage(program: &str) {
    println!("Rust janitor checks for fast_read_optimizer");
    println!();
    println!("USAGE:");
    println!("  {program} all");
    println!("  {program} file-size");
    println!("  {program} links");
    println!("  {program} src-tree [--write]");
    println!("  {program} formal");
    println!("  {program} cargo-check");
}

fn count_lines(contents: &str) -> usize {
    if contents.is_empty() {
        0
    } else {
        contents.lines().count()
    }
}

fn collect_files(root: &Path, extension: &str, out: &mut Vec<PathBuf>) -> CheckResult {
    if !root.exists() {
        return Ok(());
    }
    let entries =
        fs::read_dir(root).map_err(|err| format!("read_dir {}: {err}", root.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("read_dir {}: {err}", root.display()))?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name == "target" || name == "__pycache__" {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, extension, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
            out.push(path);
        }
    }
    Ok(())
}

fn relative_path(path: &Path) -> String {
    let root = repo_root();
    path.strip_prefix(&root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn check_file_size() -> CheckResult {
    println!("[Janitor] Running file size check...");
    let root = repo_root();
    let allowlist: HashSet<&str> = MAX_FILE_LINES_ALLOWLIST.iter().copied().collect();
    let mut rust_files = Vec::new();
    for scan_root in FILE_SIZE_ROOTS {
        collect_files(&root.join(scan_root), "rs", &mut rust_files)?;
    }
    rust_files.sort();

    let mut violations = Vec::new();
    for path in rust_files {
        let rel = relative_path(&path);
        if allowlist.contains(rel.as_str()) {
            continue;
        }
        let contents =
            fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
        let line_count = count_lines(&contents);
        if line_count > MAX_FILE_LINES {
            violations.push((rel, line_count));
        }
    }

    if violations.is_empty() {
        println!("[Janitor] File size check passed.");
        return Ok(());
    }

    let mut message = format!(
        "found {} Rust files over {} lines:\n",
        violations.len(),
        MAX_FILE_LINES
    );
    for (path, lines) in violations {
        message.push_str(&format!("  {path} - {lines} lines\n"));
    }
    Err(message.trim_end().to_string())
}

fn normalize_markdown_anchor(heading: &str) -> String {
    let mut anchor = String::new();
    let mut last_was_dash = false;
    for ch in heading.trim().trim_start_matches('#').trim().chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            anchor.push(ch);
            last_was_dash = false;
        } else if ch.is_ascii_whitespace() || ch == '-' {
            if !anchor.is_empty() && !last_was_dash {
                anchor.push('-');
                last_was_dash = true;
            }
        }
    }
    while anchor.ends_with('-') {
        anchor.pop();
    }
    anchor
}

fn extract_markdown_links(contents: &str) -> Vec<String> {
    let bytes = contents.as_bytes();
    let mut links = Vec::new();
    let mut index = 0usize;

    while index + 3 < bytes.len() {
        if bytes[index] == b']' && bytes[index + 1] == b'(' {
            let start = index + 2;
            let mut end = start;
            while end < bytes.len() && bytes[end] != b')' {
                end += 1;
            }
            if end < bytes.len() {
                if let Ok(link) = std::str::from_utf8(&bytes[start..end]) {
                    links.push(link.to_string());
                }
                index = end;
            }
        }
        index += 1;
    }

    links
}

fn check_links() -> CheckResult {
    println!("[Janitor] Running markdown link check...");
    let root = repo_root();

    for rel in README_LINK_CHECK_FILES {
        let path = root.join(rel);
        let contents =
            fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
        let headings: HashSet<String> = contents
            .lines()
            .filter(|line| line.starts_with('#'))
            .map(normalize_markdown_anchor)
            .filter(|anchor| !anchor.is_empty())
            .collect();

        let mut failures = Vec::new();
        for target in extract_markdown_links(&contents) {
            if target.starts_with("http://")
                || target.starts_with("https://")
                || target.starts_with("mailto:")
            {
                continue;
            }

            if let Some(anchor) = target.strip_prefix('#') {
                if !headings.contains(anchor) {
                    failures.push(format!("{rel}: broken anchor #{anchor}"));
                }
                continue;
            }

            let mut parts = target.split('#');
            let path_part = parts.next().unwrap_or_default();
            if path_part.is_empty() {
                continue;
            }
            let resolved = root.join(path_part);
            if !resolved.exists() {
                failures.push(format!("{rel}: broken path {target}"));
            }
        }

        if !failures.is_empty() {
            return Err(failures.join("\n"));
        }
    }

    println!("[Janitor] Markdown link check passed.");
    Ok(())
}

fn build_src_tree(root: &Path, max_depth: usize) -> Result<String, String> {
    fn walk(
        dir: &Path,
        depth: usize,
        max_depth: usize,
        prefix: &str,
        lines: &mut Vec<String>,
    ) -> CheckResult {
        if depth > max_depth {
            return Ok(());
        }
        let mut entries = Vec::new();
        let read_dir =
            fs::read_dir(dir).map_err(|err| format!("read_dir {}: {err}", dir.display()))?;
        for entry in read_dir {
            let entry = entry.map_err(|err| format!("read_dir {}: {err}", dir.display()))?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name == "__pycache__" || name == "node_modules" {
                continue;
            }
            entries.push((name, path));
        }
        entries.sort_by(|a, b| {
            let a_dir = a.1.is_dir();
            let b_dir = b.1.is_dir();
            b_dir.cmp(&a_dir).then_with(|| a.0.cmp(&b.0))
        });

        for (name, path) in entries {
            if path.is_dir() {
                lines.push(format!("{prefix}{name}/"));
                let next_prefix = format!("{prefix}  ");
                walk(&path, depth + 1, max_depth, &next_prefix, lines)?;
            } else {
                lines.push(format!("{prefix}{name}"));
            }
        }
        Ok(())
    }

    let mut lines = vec!["src/".to_string()];
    walk(root, 1, max_depth, "  ", &mut lines)?;
    Ok(lines.join("\n") + "\n")
}

fn check_src_tree(write_snapshot: bool) -> CheckResult {
    println!("[Janitor] Running source tree snapshot check...");
    let root = repo_root();
    let src_root = root.join("src");
    let snapshot_path = root.join(SRC_TREE_SNAPSHOT_FILE);
    let tree = build_src_tree(&src_root, SRC_TREE_MAX_DEPTH)?;

    if write_snapshot {
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("create_dir_all {}: {err}", parent.display()))?;
        }
        fs::write(&snapshot_path, tree)
            .map_err(|err| format!("write {}: {err}", snapshot_path.display()))?;
        println!(
            "[Janitor] Wrote source tree snapshot to {}.",
            snapshot_path.display()
        );
        return Ok(());
    }

    if !snapshot_path.exists() {
        return Err(format!(
            "missing source tree snapshot {}; run `cargo run --manifest-path janitor/Cargo.toml -- src-tree --write`",
            snapshot_path.display()
        ));
    }

    let expected = fs::read_to_string(&snapshot_path)
        .map_err(|err| format!("read {}: {err}", snapshot_path.display()))?;
    if tree != expected {
        return Err(format!(
            "source tree snapshot drift detected; run `cargo run --manifest-path janitor/Cargo.toml -- src-tree --write`"
        ));
    }

    println!("[Janitor] Source tree snapshot matches.");
    Ok(())
}

fn find_in_path(binary: &str) -> bool {
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&paths).any(|dir| dir.join(binary).exists())
}

fn check_formal_toolchain() -> CheckResult {
    println!("[Janitor] Running formal toolchain probe...");
    let mut found = Vec::new();
    let mut missing = Vec::new();

    for (binary, description) in FORMAL_TOOL_BINARIES {
        if find_in_path(binary) {
            found.push(format!("  ✓ {binary} ({description})"));
        } else {
            missing.push(format!("  ⚠ {binary} missing ({description})"));
        }
    }

    if found.is_empty() {
        println!("[Janitor] No Rust formal tools detected; passing with warnings.");
    } else {
        println!("[Janitor] Detected Rust formal tools:");
        for line in &found {
            println!("{line}");
        }
    }

    if !missing.is_empty() {
        println!("[Janitor] Missing optional tools:");
        for line in &missing {
            println!("{line}");
        }
    }

    Ok(())
}

fn cargo_check() -> CheckResult {
    println!("[Janitor] Running cargo check...");
    let status = Command::new("cargo")
        .arg("check")
        .arg("--quiet")
        .current_dir(repo_root())
        .status()
        .map_err(|err| format!("failed to run cargo check: {err}"))?;
    if status.success() {
        println!("[Janitor] cargo check passed.");
        Ok(())
    } else {
        Err(format!("cargo check failed with status {status}"))
    }
}

fn run_all() -> CheckResult {
    check_file_size()?;
    check_links()?;
    check_src_tree(false)?;
    check_formal_toolchain()?;
    cargo_check()?;
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args[1] == "--help" || args[1] == "-h" {
        usage(&args[0]);
        return ExitCode::SUCCESS;
    }

    let result = match args[1].as_str() {
        "all" => run_all(),
        "file-size" => check_file_size(),
        "links" => check_links(),
        "src-tree" => check_src_tree(args.iter().any(|arg| arg == "--write")),
        "formal" => check_formal_toolchain(),
        "cargo-check" => cargo_check(),
        other => Err(format!("unknown janitor command: {other}")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("[Janitor] {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_markdown_links, normalize_markdown_anchor};

    #[test]
    fn normalize_markdown_anchor_matches_github_style_basics() {
        assert_eq!(
            normalize_markdown_anchor("### How `fro` utilities behave"),
            "how-fro-utilities-behave"
        );
        assert_eq!(
            normalize_markdown_anchor("## Rust crate API"),
            "rust-crate-api"
        );
    }

    #[test]
    fn extract_markdown_links_finds_inline_links() {
        let links = extract_markdown_links("[a](README.md) and [b](docs/API.md#intro)");
        assert_eq!(links, vec!["README.md", "docs/API.md#intro"]);
    }
}
