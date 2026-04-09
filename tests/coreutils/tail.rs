use super::*;

#[test]
fn tail_help_mentions_start_offsets_and_header_controls() {
    let output = run_fro("tail", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("+N start offsets"));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("-q"));
    assert!(stdout.contains("-v"));
}

#[test]
fn tail_byte_mode_matches_system_for_small_stdin_suffix() {
    let input = (0..(512 * 1024))
        .map(|idx| b'a' + (idx % 23) as u8)
        .collect::<Vec<_>>();

    for args in [
        vec!["-c", "64"],
        vec!["-c", "4096"],
        vec!["-c", "65536"],
        vec!["--no-direct", "-c", "64"],
        vec!["--no-direct", "-c", "4096"],
        vec!["--no-direct", "-c", "65536"],
    ] {
        let count = *args.last().unwrap();
        assert_same_result(
            run_fro_with_stdin("tail", &args, &input),
            run_system_with_stdin("tail", &["-c", count], &input),
            &format!("tail stdin bytes {:?}", args),
        );
    }
}

#[test]
fn tail_byte_mode_matches_system_for_large_stdin_suffix() {
    let input = (0..(3 * 1024 * 1024 + 131_072))
        .map(|idx| (idx % 251) as u8)
        .collect::<Vec<_>>();

    for args in [vec!["-c", "1572865"], vec!["--no-direct", "-c", "1572865"]] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, &input),
            run_system_with_stdin("tail", &["-c", "1572865"], &input),
            &format!("tail stdin large bytes {:?}", args),
        );
    }
}

#[test]
fn tail_zero_terminated_matches_system_for_file_and_stdin() {
    let tmp = unique_temp_dir("fro-tail-zero-terminated");
    let file = tmp.join("records.bin");
    let input = b"alpha\0beta\0gamma\0delta";
    std::fs::write(&file, input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["-z", "-n", "2", path],
        vec!["--zero-terminated", "-n", "+2", path],
        vec!["-z", "-n", "1", path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail zero-terminated file {:?}", args),
        );
    }

    for args in [
        vec!["-z", "-n", "2"],
        vec!["--zero-terminated", "-n", "+2"],
        vec!["-z", "-n", "1", "-"],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, input),
            run_system_with_stdin("tail", &args, input),
            &format!("tail zero-terminated stdin {:?}", args),
        );
    }
}
