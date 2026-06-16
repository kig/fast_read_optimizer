use super::*;
use std::os::unix::fs::symlink;
use std::path::PathBuf;

fn clone_tree(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let file_type = entry.file_type().unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            clone_tree(&src_path, &dst_path);
        } else if file_type.is_symlink() {
            symlink(fs::read_link(&src_path).unwrap(), &dst_path).unwrap();
        } else {
            fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

fn snapshot_tree(root: &Path) -> Vec<String> {
    fn visit(root: &Path, rel: &Path, entries: &mut Vec<String>) {
        let path = if rel.as_os_str().is_empty() {
            root.to_path_buf()
        } else {
            root.join(rel)
        };
        let mut children = fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let child_rel = if rel.as_os_str().is_empty() {
                PathBuf::from(child.file_name())
            } else {
                rel.join(child.file_name())
            };
            let child_path = root.join(&child_rel);
            let metadata = fs::symlink_metadata(&child_path).unwrap();
            let file_type = metadata.file_type();
            let rel_text = child_rel.to_string_lossy();
            if file_type.is_dir() && !file_type.is_symlink() {
                entries.push(format!("d {rel_text}"));
                visit(root, &child_rel, entries);
            } else if file_type.is_symlink() {
                let target = fs::read_link(&child_path).unwrap();
                entries.push(format!("l {rel_text} -> {}", target.display()));
            } else {
                entries.push(format!("f {rel_text}"));
            }
        }
    }

    if !root.exists() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    visit(root, Path::new(""), &mut entries);
    entries
}

fn normalize_root_output(bytes: &[u8], root: &Path) -> String {
    String::from_utf8_lossy(bytes).replace(root.to_str().unwrap(), "<ROOT>")
}

fn assert_find_delete_same(setup: impl Fn(&Path), extra_args: &[&str], label: &str) {
    let tmp = unique_temp_dir("fro-fp-find-delete");
    let fixture = tmp.join("fixture");
    fs::create_dir_all(&fixture).unwrap();
    setup(&fixture);

    let fro_root = tmp.join("fro-root");
    let sys_root = tmp.join("sys-root");
    clone_tree(&fixture, &fro_root);
    clone_tree(&fixture, &sys_root);

    let fro_root_arg = fro_root.to_str().unwrap();
    let sys_root_arg = sys_root.to_str().unwrap();
    let mut fro_args = vec![fro_root_arg];
    fro_args.extend_from_slice(extra_args);
    let mut sys_args = vec![sys_root_arg];
    sys_args.extend_from_slice(extra_args);

    let fro = run_fro("find", &fro_args);
    let system = run_system("find", &sys_args);
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    assert_eq!(
        normalize_root_output(&fro.stdout, &fro_root),
        normalize_root_output(&system.stdout, &sys_root),
        "{label}: stdout mismatch"
    );
    assert_eq!(
        normalize_root_output(&fro.stderr, &fro_root),
        normalize_root_output(&system.stderr, &sys_root),
        "{label}: stderr mismatch"
    );
    assert_eq!(
        fro_root.exists(),
        sys_root.exists(),
        "{label}: root existence mismatch"
    );
    assert_eq!(
        snapshot_tree(&fro_root),
        snapshot_tree(&sys_root),
        "{label}: tree snapshot mismatch"
    );
}

#[test]
fn find_delete_removes_matching_files_only() {
    assert_find_delete_same(
        |root| {
            let sub = root.join("sub");
            fs::create_dir_all(&sub).unwrap();
            fs::write(root.join("alpha.txt"), b"alpha").unwrap();
            fs::write(root.join("keep.bin"), b"keep").unwrap();
            fs::write(sub.join("beta.txt"), b"beta").unwrap();
            fs::write(sub.join("keep.dat"), b"keep").unwrap();
        },
        &["-name", "*.txt", "-delete"],
        "find -name '*.txt' -delete",
    );
}

#[test]
fn find_delete_unlinks_symlinks_without_touching_targets() {
    assert_find_delete_same(
        |root| {
            let sub = root.join("sub");
            fs::create_dir_all(&sub).unwrap();
            fs::write(root.join("target.txt"), b"target").unwrap();
            fs::write(sub.join("nested.txt"), b"nested").unwrap();
            symlink(root.join("target.txt"), root.join("target-link")).unwrap();
            symlink(sub.join("nested.txt"), root.join("nested-link")).unwrap();
        },
        &["-type", "l", "-delete"],
        "find -type l -delete",
    );
}

#[test]
fn find_delete_can_remove_entire_root_in_depth_order() {
    assert_find_delete_same(
        |root| {
            let sub = root.join("sub");
            fs::create_dir_all(&sub).unwrap();
            fs::write(sub.join("file.txt"), b"file").unwrap();
            symlink(sub.join("file.txt"), root.join("file-link")).unwrap();
        },
        &["-delete", "-print"],
        "find -delete -print",
    );
}
