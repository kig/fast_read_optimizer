use super::*;
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
