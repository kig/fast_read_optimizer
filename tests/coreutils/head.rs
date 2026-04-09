use super::*;

#[test]
fn head_help_mentions_negative_counts_and_headers() {
    let output = run_fro("head", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("all but the last"));
    assert!(stdout.contains("--lines"));
    assert!(stdout.contains("--bytes"));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("--quiet"));
    assert!(stdout.contains("--verbose"));
}

#[test]
fn head_zero_terminated_matches_system_for_file_and_stdin() {
    let tmp = unique_temp_dir("fro-head-zero-terminated");
    let file = tmp.join("records.bin");
    let input = b"alpha\0beta\0gamma\0delta";
    std::fs::write(&file, input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["-z", "-n", "2", path],
        vec!["--zero-terminated", "-n", "-1", path],
        vec!["-z", "-n", "+2", path],
    ] {
        assert_same_result(
            run_fro("head", &args),
            run_system("head", &args),
            &format!("head zero-terminated file {:?}", args),
        );
    }

    for args in [
        vec!["-z", "-n", "2"],
        vec!["--zero-terminated", "-n", "-1"],
        vec!["-z", "-n", "+2", "-"],
    ] {
        assert_same_result(
            run_fro_with_stdin("head", &args, input),
            run_system_with_stdin("head", &args, input),
            &format!("head zero-terminated stdin {:?}", args),
        );
    }
}
