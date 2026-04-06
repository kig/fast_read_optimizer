use super::*;

#[test]
fn cat_flag_matrix_reuses_shared_file_and_stream_surfaces() {
    let fixture = CoreutilsParityFixture::new("fro-coreutils-cat-flags");

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["-n"],
            vec!["-b"],
            vec!["-s"],
            vec!["-E"],
            vec!["-T"],
            vec!["-v"],
            vec!["-A"],
            vec!["-e"],
            vec!["-t"],
            vec!["-u", "-n"],
        ] {
            for (kind, path) in fixture.text_path_inputs() {
                let mut fro_args = io_flags.clone();
                fro_args.extend(compat_flags.iter().copied());
                fro_args.push(path.to_str().unwrap());
                let mut sys_args = compat_flags.clone();
                sys_args.push(path.to_str().unwrap());
                assert_same_result(
                    run_fro("cat", &fro_args),
                    run_system("cat", &sys_args),
                    &format!("cat {kind} {:?} {:?}", io_flags, compat_flags),
                );
            }
        }
    }

    for compat_flags in [
        vec!["-n"],
        vec!["-b"],
        vec!["-s"],
        vec!["-E"],
        vec!["-T"],
        vec!["-v"],
        vec!["-A"],
    ] {
        for surface in stream_surfaces() {
            let args = surface.args(&compat_flags);
            assert_same_result(
                run_fro_with_stdin("cat", &args, &fixture.text_bytes),
                run_system_with_stdin("cat", &args, &fixture.text_bytes),
                &format!("cat {} {:?}", surface.label(), args),
            );
        }
    }
}
