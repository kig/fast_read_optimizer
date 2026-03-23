use super::*;

#[test]
fn cartesian_cat_and_tac_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-cat-matrix");
    let a = tmp.join("a.txt");
    let b = tmp.join("b.txt");
    fs::write(&a, b"alpha\nbeta\n").unwrap();
    fs::write(&b, b"uno\ndos\n").unwrap();

    for flags in io_flag_sets() {
        for files in [
            vec![a.to_str().unwrap()],
            vec![a.to_str().unwrap(), b.to_str().unwrap()],
        ] {
            let mut args = flags.clone();
            args.extend(files.iter().copied());
            assert_same_result(
                run_fro("cat", &args),
                run_system("cat", &files),
                &format!("cat {:?}", args),
            );
            assert_same_result(
                run_fro("tac", &args),
                run_system("tac", &files),
                &format!("tac {:?}", args),
            );
        }
    }
}
