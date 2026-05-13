use super::*;

#[test]
fn find_print_flag_explicit_matches_system() {
    let tmp = unique_temp_dir("fro-fp-find-print");
    let root = tmp.join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(root.join("top.txt"), b"top").unwrap();
    fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

    let fro = run_fro("find", &[root.to_str().unwrap(), "-print"]);
    let system = run_system("find", &[root.to_str().unwrap(), "-print"]);
    assert_same_sorted_lines(fro, system, "find -print explicit");

    let fro = run_fro("find", &[root.to_str().unwrap(), "-type", "f", "-print"]);
    let system = run_system("find", &[root.to_str().unwrap(), "-type", "f", "-print"]);
    assert_same_sorted_lines(fro, system, "find -type f -print");
}
