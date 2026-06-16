use super::helpers::StreamSurface;
use super::*;

#[test]
fn cartesian_cat_and_tac_match_system_output() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-cat-matrix");
    let a = fixture.text_file.clone();
    let b = fixture.nested_text_file.clone();

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

#[test]
fn cat_dash_can_mix_file_and_stdin_surfaces() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-cat-dash-mix");

    for surface in [StreamSurface::Dash] {
        let args = surface.args(&[fixture.text_file.to_str().unwrap()]);
        let stdin = b"stdin-line\n".as_slice();
        assert_same_result(
            run_fro_with_stdin("cat", &args, stdin),
            run_system_with_stdin("cat", &args, stdin),
            "cat mixed file and dash stdin",
        );
    }
}

#[test]
fn tac_separator_family_matches_system_output_across_surfaces() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-tac-separator-matrix");
    let literal = fixture.root.join("literal.txt");
    std::fs::write(&literal, b"left::middle::right::").unwrap();
    let regex = fixture.root.join("regex.txt");
    std::fs::write(&regex, b"a12b345c").unwrap();

    for args in [
        vec!["-s", "::", literal.to_str().unwrap()],
        vec!["-b", "--separator=::", literal.to_str().unwrap()],
        vec!["-r", "-s", "[0-9][0-9]*", regex.to_str().unwrap()],
        vec![
            "-b",
            "-r",
            "--separator",
            "[0-9][0-9]*",
            regex.to_str().unwrap(),
        ],
    ] {
        assert_same_result(
            run_fro("tac", &args),
            run_system("tac", &args),
            &format!("tac separator matrix {:?}", args),
        );
    }

    let stdin = b"left::middle::right";
    assert_same_result(
        run_fro_with_stdin("tac", &["-b", "-s", "::"], stdin),
        run_system_with_stdin("tac", &["-b", "-s", "::"], stdin),
        "tac separator matrix stdin",
    );
}
