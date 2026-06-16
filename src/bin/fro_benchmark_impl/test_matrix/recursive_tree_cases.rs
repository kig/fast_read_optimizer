use super::*;

fn recursive_tree_fro_test(
    name: &'static str,
    args: Vec<String>,
    recursive_tree_str: &str,
    metric: MetricKind,
) -> TestCase {
    TestCase {
        name,
        program: "fro",
        args,
        target: 0.0,
        cache_state: CacheState::Hot,
        files_to_prep: vec![recursive_tree_str.to_string()],
        bytes_hint: BytesHint::RecursiveTree,
        metric,
        kind: CommandKind::Fro,
    }
}

fn recursive_tree_external_test(
    name: &'static str,
    program: &'static str,
    args: Vec<String>,
    recursive_tree_str: &str,
    metric: MetricKind,
) -> TestCase {
    TestCase {
        name,
        program,
        args,
        target: 0.0,
        cache_state: CacheState::Hot,
        files_to_prep: vec![recursive_tree_str.to_string()],
        bytes_hint: BytesHint::RecursiveTree,
        metric,
        kind: CommandKind::ExternalDiscardStdout,
    }
}

pub(super) fn recursive_tree_tests(
    recursive_tree_str: &str,
    recursive_tree_manifest: &str,
    recursive_copy_target: &str,
) -> Vec<TestCase> {
    vec![
        recursive_tree_fro_test(
            "copy (recursive, hot)",
            vec![
                "copy".into(),
                "--recursive".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::Gbps,
        ),
        recursive_tree_fro_test(
            "tree compare: copy --recursive (hot)",
            vec![
                "copy".into(),
                "--recursive".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::FilesPerSecond,
        ),
        recursive_tree_fro_test(
            "tree compare: copy --recursive --threaded-copy (hot)",
            vec![
                "copy".into(),
                "--recursive".into(),
                "--threaded-copy".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::FilesPerSecond,
        ),
        recursive_tree_fro_test(
            "tree compare: split-manifest-recursive-copy-bench (hot)",
            vec![
                "split-manifest-recursive-copy-bench".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::FilesPerSecond,
        ),
        recursive_tree_fro_test(
            "tree compare: manifest-recursive-copy-bench (hot)",
            vec![
                "manifest-recursive-copy-bench".into(),
                "-v".into(),
                "-n".into(),
                "1".into(),
                recursive_tree_manifest.to_string(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::FilesPerSecond,
        ),
        recursive_tree_external_test(
            "cp -r (recursive, hot)",
            "cp",
            vec![
                "-r".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::Gbps,
        ),
        recursive_tree_external_test(
            "tree compare: cp -r (hot)",
            "cp",
            vec![
                "-r".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::FilesPerSecond,
        ),
        recursive_tree_external_test(
            "rsync (recursive, hot)",
            "rsync",
            vec![
                "-a".into(),
                recursive_tree_str.to_string(),
                recursive_copy_target.to_string(),
            ],
            recursive_tree_str,
            MetricKind::Gbps,
        ),
    ]
}
