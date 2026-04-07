use super::*;
use std::fs::File;

#[derive(Clone, Copy, Debug)]
enum MatrixInput {
    ColdFile,
    HotFile,
    Pipe,
}

#[derive(Clone, Copy, Debug)]
enum MatrixOutput {
    ColdFile,
    HotFile,
    DevNull,
    Pipe,
}

impl MatrixInput {
    fn label(self) -> &'static str {
        match self {
            Self::ColdFile => "cold_file",
            Self::HotFile => "hot_file",
            Self::Pipe => "pipe",
        }
    }
}

impl MatrixOutput {
    fn label(self) -> &'static str {
        match self {
            Self::ColdFile => "cold_file",
            Self::HotFile => "hot_file",
            Self::DevNull => "dev_null",
            Self::Pipe => "pipe",
        }
    }
}

struct MatrixRun {
    output: Option<Vec<u8>>,
    stderr: Vec<u8>,
    status: std::process::ExitStatus,
}

fn spawn_base64_case(
    tmp: &std::path::Path,
    case_name: &str,
    args: &[&str],
    input_mode: MatrixInput,
    output_mode: MatrixOutput,
    input_bytes: &[u8],
) -> MatrixRun {
    let input_path = tmp.join(format!("{case_name}-input.bin"));
    let output_path = tmp.join(format!("{case_name}-output.bin"));

    let mut command = Command::new(env!("CARGO_BIN_EXE_fro"));
    command.arg("base64").args(args);

    let use_stdin = matches!(input_mode, MatrixInput::Pipe);
    if use_stdin {
        command.stdin(Stdio::piped());
    } else {
        fs::write(&input_path, input_bytes).unwrap();
        if matches!(input_mode, MatrixInput::HotFile) {
            let warmed = fs::read(&input_path).unwrap();
            assert_eq!(warmed.len(), input_bytes.len());
        }
        command.arg(&input_path);
    }

    match output_mode {
        MatrixOutput::Pipe => {
            command.stdout(Stdio::piped());
        }
        MatrixOutput::DevNull => {
            let dev_null = File::options().write(true).open("/dev/null").unwrap();
            command.stdout(Stdio::from(dev_null));
        }
        MatrixOutput::ColdFile | MatrixOutput::HotFile => {
            if matches!(output_mode, MatrixOutput::HotFile) {
                fs::write(&output_path, b"prewarm").unwrap();
                let warmed = fs::read(&output_path).unwrap();
                assert!(!warmed.is_empty());
            }
            let output_file = File::create(&output_path).unwrap();
            command.stdout(Stdio::from(output_file));
        }
    }
    command.stderr(Stdio::piped());

    let mut child = command.spawn().unwrap();
    let stdin_writer = if use_stdin {
        let mut stdin = child.stdin.take().expect("missing stdin");
        let input = input_bytes.to_vec();
        Some(std::thread::spawn(move || {
            stdin.write_all(&input).unwrap();
        }))
    } else {
        None
    };
    let output = child.wait_with_output().unwrap();
    if let Some(writer) = stdin_writer {
        writer.join().unwrap();
    }
    let captured = match output_mode {
        MatrixOutput::Pipe => Some(output.stdout),
        MatrixOutput::DevNull => None,
        MatrixOutput::ColdFile | MatrixOutput::HotFile => Some(fs::read(&output_path).unwrap()),
    };
    MatrixRun {
        output: captured,
        stderr: output.stderr,
        status: output.status,
    }
}

fn assert_matrix_success(run: &MatrixRun, label: &str) {
    assert!(
        run.status.success(),
        "{label} failed\nstderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.stderr.is_empty(),
        "{label} stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
}

#[test]
fn base64_encode_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-encode");
    let path = tmp.join("input.bin");
    let bytes = (0..211)
        .map(|i| ((i * 37 + 11) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec![],
            vec!["-w", "0"],
            vec!["--wrap", "0"],
            vec!["--wrap=0"],
            vec!["-w0"],
            vec!["-w", "12"],
            vec!["--wrap", "12"],
            vec!["--wrap=12"],
            vec!["-w12"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 encode {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [
            vec![],
            vec!["-w", "0"],
            vec!["--wrap", "0"],
            vec!["-w0"],
            vec!["--wrap=12"],
            vec!["--wrap", "12"],
            vec!["-w12"],
        ] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            assert_same_result(
                run_fro_with_stdin("base64", &fro_args, &bytes),
                run_system_with_stdin("base64", &compat_flags, &bytes),
                &format!("base64 stdin encode {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn base64_decode_flags_match_system_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-decode");
    let path = tmp.join("encoded.txt");
    let dirty_path = tmp.join("encoded-dirty.txt");
    let bytes = (0..197)
        .map(|i| ((i * 17 + 5) % 251) as u8)
        .collect::<Vec<_>>();
    let encoded = run_system_with_stdin("base64", &["-w", "16"], &bytes);
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    fs::write(&path, &encoded.stdout).unwrap();

    let dirty = b"!!"
        .iter()
        .copied()
        .chain(encoded.stdout.iter().copied())
        .chain(b"??\n".iter().copied())
        .collect::<Vec<_>>();
    fs::write(&dirty_path, &dirty).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [vec!["-d"], vec!["--decode"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 decode {:?} {:?}", io_flags, compat_flags),
            );
        }

        for compat_flags in [vec!["-d", "-i"], vec!["--decode", "--ignore-garbage"]] {
            let mut fro_args = io_flags.clone();
            fro_args.extend(compat_flags.iter().copied());
            fro_args.push(dirty_path.to_str().unwrap());
            let mut sys_args = compat_flags.clone();
            sys_args.push(dirty_path.to_str().unwrap());
            assert_same_result(
                run_fro("base64", &fro_args),
                run_system("base64", &sys_args),
                &format!("base64 ignore {:?} {:?}", io_flags, compat_flags),
            );
        }
    }
}

#[test]
fn base64_decode_large_wrapped_input_matches_original_bytes() {
    let tmp = unique_temp_dir("fro-coreutils-base64-large-decode");
    let raw_path = tmp.join("raw.bin");
    let encoded_path = tmp.join("wrapped.txt");
    let bytes = (0..((6 * 1024 * 1024) + 2048))
        .map(|i| ((i * 19 + 23) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&raw_path, &bytes).unwrap();

    let encoded = run_system("base64", &["-w", "17", raw_path.to_str().unwrap()]);
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    fs::write(&encoded_path, &encoded.stdout).unwrap();

    for io_flags in io_flag_sets() {
        let mut fro_args = io_flags.clone();
        fro_args.extend(["-d", encoded_path.to_str().unwrap()]);
        let fro = run_fro("base64", &fro_args);
        assert!(
            fro.status.success(),
            "base64 decode {:?} failed\nstdout:\n{}\nstderr:\n{}",
            io_flags,
            String::from_utf8_lossy(&fro.stdout),
            String::from_utf8_lossy(&fro.stderr),
        );
        assert_eq!(fro.stdout, bytes, "base64 decode {:?}", io_flags);
        assert!(
            fro.stderr.is_empty(),
            "unexpected stderr for {:?}: {}",
            io_flags,
            String::from_utf8_lossy(&fro.stderr)
        );
    }
}

#[test]
fn base64_rejects_extra_operands_like_system() {
    let tmp = unique_temp_dir("fro-coreutils-base64-extra");
    let a = tmp.join("a.bin");
    let b = tmp.join("b.bin");
    fs::write(&a, b"a").unwrap();
    fs::write(&b, b"b").unwrap();

    assert_same_result(
        run_fro("base64", &[a.to_str().unwrap(), b.to_str().unwrap()]),
        run_system("base64", &[a.to_str().unwrap(), b.to_str().unwrap()]),
        "base64 extra operand",
    );
}

#[test]
fn base64_wrap_argument_forms_match_system_errors_and_output() {
    let tmp = unique_temp_dir("fro-coreutils-base64-wrap-forms");
    let path = tmp.join("input.bin");
    let bytes = (0..137)
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &bytes).unwrap();

    for io_flags in io_flag_sets() {
        for compat_flags in [
            vec!["--wrap", "12", path.to_str().unwrap()],
            vec!["-w12", path.to_str().unwrap()],
            vec!["--wrap", "0", path.to_str().unwrap()],
            vec!["-w0", path.to_str().unwrap()],
        ] {
            assert_same_result(
                run_fro("base64", &[io_flags.clone(), compat_flags.clone()].concat()),
                run_system("base64", &compat_flags),
                &format!("base64 wrap form {:?} {:?}", io_flags, compat_flags),
            );
        }
    }

    assert_same_result(
        run_fro("base64", &["--wrap"]),
        run_system("base64", &["--wrap"]),
        "base64 missing --wrap argument",
    );
}

#[test]
fn base64_roundtrip_io_matrix() {
    let tmp = unique_temp_dir("fro-coreutils-base64-io-matrix");
    let cases = [
        (
            MatrixInput::ColdFile,
            MatrixOutput::ColdFile,
            MatrixInput::ColdFile,
            MatrixOutput::ColdFile,
        ),
        (
            MatrixInput::HotFile,
            MatrixOutput::HotFile,
            MatrixInput::HotFile,
            MatrixOutput::HotFile,
        ),
        (
            MatrixInput::ColdFile,
            MatrixOutput::Pipe,
            MatrixInput::Pipe,
            MatrixOutput::ColdFile,
        ),
        (
            MatrixInput::Pipe,
            MatrixOutput::ColdFile,
            MatrixInput::ColdFile,
            MatrixOutput::Pipe,
        ),
        (
            MatrixInput::Pipe,
            MatrixOutput::Pipe,
            MatrixInput::Pipe,
            MatrixOutput::Pipe,
        ),
        (
            MatrixInput::HotFile,
            MatrixOutput::Pipe,
            MatrixInput::Pipe,
            MatrixOutput::HotFile,
        ),
        (
            MatrixInput::Pipe,
            MatrixOutput::HotFile,
            MatrixInput::HotFile,
            MatrixOutput::Pipe,
        ),
    ];
    let dev_null_smoke_cases = [
        (MatrixInput::ColdFile, MatrixOutput::DevNull),
        (MatrixInput::HotFile, MatrixOutput::DevNull),
        (MatrixInput::Pipe, MatrixOutput::DevNull),
    ];

    for (size_label, size) in [("512kb", 512 * 1024usize), ("2500kb", 2500 * 1024usize)] {
        let original = (0..size)
            .map(|i| ((i * 31 + 9) % 251) as u8)
            .collect::<Vec<_>>();

        for (encode_source, encode_output) in dev_null_smoke_cases {
            let encode_dev_null = spawn_base64_case(
                &tmp,
                &format!(
                    "encode-{size_label}-{}-{}",
                    encode_source.label(),
                    encode_output.label()
                ),
                &["-w", "0"],
                encode_source,
                encode_output,
                &original,
            );
            assert_matrix_success(
                &encode_dev_null,
                &format!(
                    "encode {} -> {} {}",
                    encode_source.label(),
                    encode_output.label(),
                    size_label
                ),
            );
        }

        for (encode_source, encode_output, decode_source, decode_output) in cases {
            let encode_run = spawn_base64_case(
                &tmp,
                &format!(
                    "encode-{size_label}-{}-{}",
                    encode_source.label(),
                    encode_output.label()
                ),
                &["-w", "0"],
                encode_source,
                encode_output,
                &original,
            );
            assert_matrix_success(
                &encode_run,
                &format!(
                    "encode {} -> {} {}",
                    encode_source.label(),
                    encode_output.label(),
                    size_label
                ),
            );
            let encoded = encode_run.output.as_ref().unwrap();

            let decode_dev_null = spawn_base64_case(
                &tmp,
                &format!(
                    "decode-{size_label}-{}-{}-{}-dev-null",
                    encode_source.label(),
                    encode_output.label(),
                    decode_source.label()
                ),
                &["-d"],
                decode_source,
                MatrixOutput::DevNull,
                encoded,
            );
            assert_matrix_success(
                &decode_dev_null,
                &format!(
                    "decode {} -> /dev/null via {}->{} {}",
                    decode_source.label(),
                    encode_source.label(),
                    encode_output.label(),
                    size_label
                ),
            );

            let decode_run = spawn_base64_case(
                &tmp,
                &format!(
                    "decode-{size_label}-{}-{}-{}-{}",
                    encode_source.label(),
                    encode_output.label(),
                    decode_source.label(),
                    decode_output.label()
                ),
                &["-d"],
                decode_source,
                decode_output,
                encoded,
            );
            assert_matrix_success(
                &decode_run,
                &format!(
                    "roundtrip encode {}->{} decode {}->{} {}",
                    encode_source.label(),
                    encode_output.label(),
                    decode_source.label(),
                    decode_output.label(),
                    size_label
                ),
            );
            assert_eq!(
                decode_run.output.unwrap(),
                original,
                "roundtrip encode {}->{} decode {}->{} {}",
                encode_source.label(),
                encode_output.label(),
                decode_source.label(),
                decode_output.label(),
                size_label
            );
        }
    }
}
