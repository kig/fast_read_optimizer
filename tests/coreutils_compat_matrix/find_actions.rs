use super::*;

fn assert_find_same_exact(root: &Path, extra_args: &[&str], label: &str) {
    let root_arg = root.to_str().unwrap();
    let mut fro_args = vec![root_arg];
    fro_args.extend_from_slice(extra_args);
    let mut sys_args = vec![root_arg];
    sys_args.extend_from_slice(extra_args);
    assert_same_result(
        run_fro("find", &fro_args),
        run_system("find", &sys_args),
        label,
    );
}

fn assert_find_fprintf_same(root: &Path, format: &str, label: &str) {
    let out_dir = root.parent().unwrap_or(root);
    let fro_out = out_dir.join("fro-output.txt");
    let sys_out = out_dir.join("sys-output.txt");
    let root_arg = root.to_str().unwrap();
    let fro_out_arg = fro_out.to_str().unwrap();
    let sys_out_arg = sys_out.to_str().unwrap();
    let fro = run_fro(
        "find",
        &[root_arg, "-type", "f", "-fprintf", fro_out_arg, format],
    );
    let system = run_system(
        "find",
        &[root_arg, "-type", "f", "-fprintf", sys_out_arg, format],
    );
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    assert_eq!(fro.stdout, system.stdout, "{label}: stdout mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
    assert_eq!(
        fs::read(&fro_out).unwrap(),
        fs::read(&sys_out).unwrap(),
        "{label}: file output mismatch"
    );
}

fn assert_find_fls_same(root: &Path, label: &str) {
    let out_dir = root.parent().unwrap_or(root);
    let fro_out = out_dir.join("fro-output.txt");
    let sys_out = out_dir.join("sys-output.txt");
    let root_arg = root.to_str().unwrap();
    let fro_out_arg = fro_out.to_str().unwrap();
    let sys_out_arg = sys_out.to_str().unwrap();
    let fro = run_fro("find", &[root_arg, "-type", "f", "-fls", fro_out_arg]);
    let system = run_system("find", &[root_arg, "-type", "f", "-fls", sys_out_arg]);
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    assert_eq!(fro.stdout, system.stdout, "{label}: stdout mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
    assert_eq!(
        fs::read(&fro_out).unwrap(),
        fs::read(&sys_out).unwrap(),
        "{label}: file output mismatch"
    );
}

fn assert_find_fprint_same(root: &Path, flag: &str, label: &str) {
    let out_dir = root.parent().unwrap_or(root);
    let fro_out = out_dir.join("fro-output.txt");
    let sys_out = out_dir.join("sys-output.txt");
    let root_arg = root.to_str().unwrap();
    let fro_out_arg = fro_out.to_str().unwrap();
    let sys_out_arg = sys_out.to_str().unwrap();
    let fro = run_fro("find", &[root_arg, "-type", "f", flag, fro_out_arg]);
    let system = run_system("find", &[root_arg, "-type", "f", flag, sys_out_arg]);
    assert_eq!(
        fro.status.code(),
        system.status.code(),
        "{label}: status mismatch"
    );
    assert_eq!(fro.stdout, system.stdout, "{label}: stdout mismatch");
    assert_eq!(fro.stderr, system.stderr, "{label}: stderr mismatch");
    assert_eq!(
        fs::read(&fro_out).unwrap(),
        fs::read(&sys_out).unwrap(),
        "{label}: file output mismatch"
    );
}

#[test]
fn find_exec_todo() {
    let tmp = unique_temp_dir("fro-fp-find-exec");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("sub/beta.txt"), b"beta").unwrap();
    assert_find_same_exact(
        &root,
        &["-type", "f", "-exec", "printf", "EXEC:%s\\n", "{}", ";"],
        "find -exec",
    );
}

#[test]
fn find_execdir_todo() {
    let tmp = unique_temp_dir("fro-fp-find-execdir");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/beta.txt"), b"beta").unwrap();
    assert_find_same_exact(
        &root,
        &["-type", "f", "-execdir", "printf", "EXEC:%s\\n", "{}", ";"],
        "find -execdir",
    );
}

#[test]
fn find_ok_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ok");
    let root = tmp.join("root");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_result(
        run_fro_with_stdin(
            "find",
            &[
                root_arg, "-type", "f", "-ok", "printf", "OK:%s\\n", "{}", ";",
            ],
            b"y\n",
        ),
        run_system_with_stdin(
            "find",
            &[
                root_arg, "-type", "f", "-ok", "printf", "OK:%s\\n", "{}", ";",
            ],
            b"y\n",
        ),
        "find -ok",
    );
}

#[test]
fn find_okdir_todo() {
    let tmp = unique_temp_dir("fro-fp-find-okdir");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("alpha.txt"), b"alpha").unwrap();
    let root_arg = root.to_str().unwrap();
    assert_same_result(
        run_fro_with_stdin(
            "find",
            &[
                root_arg, "-type", "f", "-okdir", "printf", "OK:%s\\n", "{}", ";",
            ],
            b"y\n",
        ),
        run_system_with_stdin(
            "find",
            &[
                root_arg, "-type", "f", "-okdir", "printf", "OK:%s\\n", "{}", ";",
            ],
            b"y\n",
        ),
        "find -okdir",
    );
}

#[test]
fn find_ls_todo() {
    let tmp = unique_temp_dir("fro-fp-find-ls");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_same_exact(&root, &["-type", "f", "-ls"], "find -ls");
}

#[test]
fn find_fls_todo() {
    let tmp = unique_temp_dir("fro-fp-find-fls");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_fls_same(&root, "find -fls");
}

#[test]
fn find_printf_todo() {
    let tmp = unique_temp_dir("fro-fp-find-printf");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_same_exact(
        &root,
        &["-type", "f", "-printf", "P:%p F:%f H:%h S:%s Y:%y %%\\n"],
        "find -printf",
    );
}

#[test]
fn find_fprintf_todo() {
    let tmp = unique_temp_dir("fro-fp-find-fprintf");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_fprintf_same(&root, "P:%p F:%f H:%h S:%s Y:%y %%\\n", "find -fprintf");
}

#[test]
fn find_fprint_todo() {
    let tmp = unique_temp_dir("fro-fp-find-fprint");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_fprint_same(&root, "-fprint", "find -fprint");
}

#[test]
fn find_fprint0_todo() {
    let tmp = unique_temp_dir("fro-fp-find-fprint0");
    let root = tmp.join("root");
    let sub = root.join("sub");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), b"abc").unwrap();
    assert_find_fprint_same(&root, "-fprint0", "find -fprint0");
}

#[test]
fn find_prune_todo() {
    let tmp = unique_temp_dir("fro-fp-find-prune");
    let root = tmp.join("root");
    let keep = root.join("keep");
    let skip = root.join("skip");
    fs::create_dir_all(&keep).unwrap();
    fs::create_dir_all(&skip).unwrap();
    fs::write(keep.join("keep.txt"), b"keep").unwrap();
    fs::write(skip.join("skip.txt"), b"skip").unwrap();
    assert_find_same_exact(
        &root,
        &[
            "-path", "*/skip", "-prune", "-o", "-name", "*.txt", "-print",
        ],
        "find -prune",
    );
}

#[test]
fn find_quit_todo() {
    let tmp = unique_temp_dir("fro-fp-find-quit");
    let root = tmp.join("root");
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(root.join("sub/beta.txt"), b"beta").unwrap();
    assert_find_same_exact(&root, &["-name", "*.txt", "-print", "-quit"], "find -quit");
}
