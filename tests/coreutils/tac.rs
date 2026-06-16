use super::*;

#[test]
fn tac_help_mentions_separator_family_and_help_version() {
    let output = run_fro("tac", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Use -- to stop option parsing"));
    assert!(stdout.contains("-b/--before"));
    assert!(stdout.contains("-r/--regex"));
    assert!(stdout.contains("-s/--separator"));
    assert!(stdout.contains("--help shows this message and exits."));
    assert!(stdout.contains("--version prints the fro tac version string and exits."));
}

#[test]
fn tac_version_prints_version_string() {
    let output = run_fro("tac", &["--version"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("tac (fro coreutils)"));
}

#[test]
fn tac_separator_family_matches_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-tac-separator-family");
    let literal = tmp.join("literal.txt");
    std::fs::write(&literal, b"alpha::beta::gamma::").unwrap();
    let regex = tmp.join("regex.txt");
    std::fs::write(&regex, b"a12b345c\n").unwrap();

    for args in [
        vec!["-s", "::", literal.to_str().unwrap()],
        vec!["-b", "-s", "::", literal.to_str().unwrap()],
        vec!["-r", "-s", "[0-9][0-9]*", regex.to_str().unwrap()],
        vec!["-b", "-r", "-s", "[0-9][0-9]*", regex.to_str().unwrap()],
        vec!["--separator=", literal.to_str().unwrap()],
    ] {
        assert_same_result(
            run_fro("tac", &args),
            run_system("tac", &args),
            &format!("tac separator family {:?}", args),
        );
    }
}

#[test]
fn tac_separator_family_matches_system_output_on_stdin() {
    for (args, stdin) in [
        (vec!["-s", "::"], b"alpha::beta::gamma".as_slice()),
        (
            vec!["-b", "-r", "-s", "[0-9][0-9]*"],
            b"a12b345c".as_slice(),
        ),
    ] {
        assert_same_result(
            run_fro_with_stdin("tac", &args, stdin),
            run_system_with_stdin("tac", &args, stdin),
            &format!("tac separator family stdin {:?}", args),
        );
    }
}
