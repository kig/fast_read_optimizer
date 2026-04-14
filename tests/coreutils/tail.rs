use super::*;

#[test]
fn tail_help_mentions_long_counts_help_and_version() {
    let output = run_fro("tail", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("+N start offsets"));
    assert!(stdout.contains("--lines"));
    assert!(stdout.contains("--lines="));
    assert!(stdout.contains("--bytes"));
    assert!(stdout.contains("--bytes="));
    assert!(stdout.contains("--zero-terminated"));
    assert!(stdout.contains("--quiet"));
    assert!(stdout.contains("--silent"));
    assert!(stdout.contains("--verbose"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro tail version string and exits."));
}

#[test]
fn tail_version_prints_version_string() {
    let output = run_fro("tail", &["--version"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tail (fro coreutils)"));
}

#[test]
fn tail_long_count_forms_match_system_for_file_and_stdin() {
    let tmp = unique_temp_dir("fro-tail-long-counts");
    let file = tmp.join("input.txt");
    let input = b"alpha\nbeta\ngamma\ndelta\n";
    std::fs::write(&file, input).unwrap();
    let path = file.to_str().unwrap();

    for args in [
        vec!["--lines", "2", path],
        vec!["--lines=+2", path],
        vec!["--bytes", "7", path],
        vec!["--bytes=+4", path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail long-count file {:?}", args),
        );
    }

    for args in [
        vec!["--lines", "2"],
        vec!["--lines=+2"],
        vec!["--bytes", "7"],
        vec!["--bytes=+4"],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, input),
            run_system_with_stdin("tail", &args, input),
            &format!("tail long-count stdin {:?}", args),
        );
    }
}

#[test]
fn tail_long_header_forms_match_system_for_multi_input_modes() {
    let tmp = unique_temp_dir("fro-tail-long-headers");
    let first = tmp.join("first.txt");
    let second = tmp.join("second.txt");
    std::fs::write(&first, b"first-a\nfirst-b\n").unwrap();
    std::fs::write(&second, b"second-a\nsecond-b\n").unwrap();
    let first_path = first.to_str().unwrap();
    let second_path = second.to_str().unwrap();

    for args in [
        vec!["--quiet", first_path, second_path],
        vec!["--silent", first_path, second_path],
        vec!["--verbose", first_path, second_path],
        vec!["--verbose", "--quiet", first_path, second_path],
        vec!["--quiet", "--verbose", first_path, second_path],
    ] {
        assert_same_result(
            run_fro("tail", &args),
            run_system("tail", &args),
            &format!("tail long headers file {:?}", args),
        );
    }

    let stdin = b"stdin-a\nstdin-b\n";
    for args in [
        vec!["--quiet", "-", second_path],
        vec!["--silent", "-", second_path],
        vec!["--verbose", "-", second_path],
    ] {
        assert_same_result(
            run_fro_with_stdin("tail", &args, stdin),
            run_system_with_stdin("tail", &args, stdin),
            &format!("tail long headers stdin {:?}", args),
        );
    }
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
