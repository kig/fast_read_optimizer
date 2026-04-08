use super::*;
use crate::common::{CopyStrategy, IOMode};
use crate::config::{AppConfig, LoadedConfig};
use crate::main_app::copy_plan::CopyRewriteMode;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_recursive_test_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from("target")
        .join("test-artifacts")
        .join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn test_recursive_copy_context(
    root: &Path,
    source_root: &Path,
    target_root: &Path,
) -> RecursiveCopyContext {
    let config = LoadedConfig::Legacy {
        path: root.join("fro-test-config.json"),
        config: AppConfig::default(),
    };
    let target_str = target_root.to_string_lossy();
    let params_page_cache = config.get_params_for_path("copy", false, target_str.as_ref());
    let params_direct = config.get_params_for_path("copy", true, target_str.as_ref());
    let params_copy_range = config.get_copy_range_params_for_path(target_str.as_ref());
    RecursiveCopyContext {
        config,
        source_root: source_root.to_path_buf(),
        target_root: target_root.to_path_buf(),
        optimizer_params: [
            params_page_cache.num_threads,
            params_page_cache.block_size,
            params_page_cache.qd as u64,
            params_direct.num_threads,
            params_direct.block_size,
            params_direct.qd as u64,
            params_copy_range.num_threads,
            params_copy_range.block_size,
            params_copy_range.qd as u64,
        ],
        requested_strategy: CopyStrategy::Auto,
        rewrite_mode: CopyRewriteMode::Auto,
        io_mode_read: IOMode::PageCache,
        io_mode_write: IOMode::PageCache,
        keep_target_size: false,
        use_lock: true,
        relative_copy_method: RelativeCopyMethod::CopyFileRange,
        verbose: false,
        cp_compat: false,
        cp_no_clobber: false,
        preserve_timestamps: false,
    }
}

#[test]
fn recursive_task_queue_enqueue_batch_preserves_fifo_order() {
    let queue = RecursiveTaskQueue::default();
    let stop = AtomicBool::new(false);

    queue.enqueue_batch([1_u8, 2, 3]).unwrap();
    queue.close();

    assert_eq!(queue.claim(&stop), Some(1));
    assert_eq!(queue.claim(&stop), Some(2));
    assert_eq!(queue.claim(&stop), Some(3));
    assert_eq!(queue.claim(&stop), None);
}

#[test]
fn walk_recursive_read_subtree_discovers_files_and_subdirs() {
    let root = unique_recursive_test_dir("recursive-read-walk");
    let nested = root.join("nested/deeper");
    let sibling = root.join("sibling");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&sibling).unwrap();
    fs::write(root.join("a.txt"), b"a").unwrap();
    fs::write(root.join("nested/b.txt"), b"bb").unwrap();
    fs::write(nested.join("c.txt"), b"ccc").unwrap();
    fs::write(sibling.join("d.txt"), b"dddd").unwrap();

    let dir_queue = RecursiveDirectoryQueue::default();
    let file_queue = RecursiveTaskQueue::default();
    let stop = AtomicBool::new(false);

    walk_recursive_read_subtree(
        RecursiveReadDirectoryTask {
            source_dir: root.clone(),
        },
        &dir_queue,
        &file_queue,
        &stop,
    )
    .unwrap();
    let mut queued_dirs = Vec::new();
    while let Some(task) = dir_queue.claim(&stop) {
        queued_dirs.push(task.source_dir.clone());
        walk_recursive_read_subtree(task, &dir_queue, &file_queue, &stop).unwrap();
        dir_queue.complete_claim();
    }
    file_queue.close();

    queued_dirs.sort();

    let mut queued_files = Vec::new();
    while let Some(task) = file_queue.claim(&stop) {
        queued_files.push(task.source_path);
    }
    queued_files.sort();

    assert_eq!(queued_dirs.len(), 1);
    assert!(
        queued_dirs[0] == root.join("nested") || queued_dirs[0] == root.join("sibling"),
        "queued sibling should come from the root fanout"
    );
    assert_eq!(
        queued_files,
        vec![
            root.join("a.txt"),
            root.join("nested/b.txt"),
            nested.join("c.txt"),
            sibling.join("d.txt"),
        ]
    );
    drop(file_queue);
    drop(dir_queue);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn recursive_read_bench_counts_empty_files_as_successful_reads() {
    let root = unique_recursive_test_dir("recursive-read-bench-empty-files");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();

    fs::write(root.join("empty.txt"), []).unwrap();
    fs::write(nested.join("payload.bin"), b"payload").unwrap();
    fs::write(nested.join("also-empty.txt"), []).unwrap();

    let config = LoadedConfig::Legacy {
        path: root.join("fro-test-config.json"),
        config: AppConfig::default(),
    };
    let page_cache = config.get_params_for_path("read", false, root.to_str().unwrap());
    let direct = config.get_params_for_path("read", true, root.to_str().unwrap());

    let bytes_read = bench::bench_recursive_read(
        &config,
        root.to_str().unwrap(),
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        common::IOMode::PageCache,
        false,
        Some(1),
    )
    .unwrap();

    assert_eq!(bytes_read, 7);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn split_manifest_recursive_copy_reports_manifest_phase_counts() {
    let root = unique_recursive_test_dir("split-manifest-recursive-copy");
    let source_root = root.join("source");
    let target_root = root.join("target");
    fs::create_dir_all(source_root.join("nested")).unwrap();
    fs::write(source_root.join("root.bin"), b"root").unwrap();
    fs::write(source_root.join("nested").join("child.bin"), b"child").unwrap();
    let expected_symlinks = if cfg!(unix) { 1 } else { 0 };
    #[cfg(unix)]
    std::os::unix::fs::symlink("nested/child.bin", source_root.join("child.link")).unwrap();

    let result = split_manifest::run_split_manifest_recursive_copy_with_result(
        test_recursive_copy_context(&root, &source_root, &target_root),
        false,
    )
    .unwrap();

    assert_eq!(result.bytes_copied, 9);
    assert_eq!(result.files_copied, 2);
    assert_eq!(result.small_file_tasks, 2);
    assert_eq!(result.large_file_tasks, 0);
    assert_eq!(result.dirs_created, 2);
    assert_eq!(result.symlinks_created, expected_symlinks);
    assert!(result.manifest_phase_secs >= 0.0);
    assert!(result.copy_phase_secs >= 0.0);
    assert!(result.total_secs >= result.manifest_phase_secs);
    assert!(result.total_secs >= result.copy_phase_secs);
    assert_eq!(fs::read(target_root.join("root.bin")).unwrap(), b"root");
    assert_eq!(
        fs::read(target_root.join("nested").join("child.bin")).unwrap(),
        b"child"
    );
    #[cfg(unix)]
    assert_eq!(
        fs::read_link(target_root.join("child.link")).unwrap(),
        PathBuf::from("nested/child.bin")
    );

    let _ = fs::remove_dir_all(root);
}
