use super::*;

#[test]
fn tail_help_mentions_start_offsets_and_header_controls() {
    let output = run_fro("tail", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("+N start offsets"));
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
