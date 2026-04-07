use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    fs::create_dir_all(&base).unwrap();
    let dir = base.join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn dd_example_path() -> &'static Path {
    static DD_EXAMPLE: OnceLock<PathBuf> = OnceLock::new();
    DD_EXAMPLE.get_or_init(|| {
        let status = Command::new(env!("CARGO"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(["build", "--quiet", "--example", "dd"])
            .status()
            .expect("failed to build dd example");
        assert!(status.success(), "failed to build dd example");
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("debug")
            .join("examples")
            .join("dd")
    })
}

fn run_example(args: &[String]) -> Output {
    Command::new(dd_example_path())
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(args)
        .output()
        .expect("failed to run dd example")
}

fn run_fro_dd(args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("dd")
        .args(args)
        .output()
        .expect("failed to run fro dd")
}

fn run_system_dd(args: &[String]) -> Output {
    Command::new("dd")
        .args(args)
        .output()
        .expect("failed to run system dd")
}

struct DdCase {
    name: &'static str,
    initial_output: Option<&'static [u8]>,
    flags: &'static [&'static str],
}

fn build_args(input: &Path, output: &Path, flags: &[&str]) -> Vec<String> {
    let mut args = vec![
        format!("if={}", input.display()),
        format!("of={}", output.display()),
    ];
    args.extend(flags.iter().map(|flag| flag.to_string()));
    args
}

fn compare_outputs(
    case_name: &str,
    label: &str,
    actual: &Output,
    system: &Output,
    actual_output: &Path,
    system_output: &Path,
) {
    assert_eq!(
        actual.status.success(),
        system.status.success(),
        "case {} ({}): actual stderr=\n{}\nsystem stderr=\n{}",
        case_name,
        label,
        String::from_utf8_lossy(&actual.stderr),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(
        actual.stdout, system.stdout,
        "case {} ({}): stdout mismatch",
        case_name, label
    );
    assert_eq!(
        actual.stderr, system.stderr,
        "case {} ({}): stderr mismatch",
        case_name, label
    );
    assert_eq!(
        fs::read(actual_output).unwrap(),
        fs::read(system_output).unwrap(),
        "case {} ({}): output mismatch",
        case_name,
        label
    );
}

#[test]
fn dd_example_matches_system_dd_for_supported_flag_combinations() {
    let input_bytes = (0..97).map(|i| ((i * 13) % 251) as u8).collect::<Vec<_>>();
    let cases = [
        DdCase {
            name: "whole-file-copy",
            initial_output: None,
            flags: &["status=none"],
        },
        DdCase {
            name: "count-only",
            initial_output: None,
            flags: &["bs=7", "count=5", "status=none"],
        },
        DdCase {
            name: "skip-only",
            initial_output: None,
            flags: &["bs=9", "skip=3", "status=none"],
        },
        DdCase {
            name: "seek-only",
            initial_output: None,
            flags: &["bs=8", "seek=2", "status=none"],
        },
        DdCase {
            name: "skip-seek-count",
            initial_output: None,
            flags: &["bs=6", "count=4", "skip=2", "seek=1", "status=none"],
        },
        DdCase {
            name: "notrunc",
            initial_output: Some(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            flags: &[
                "bs=5",
                "count=3",
                "skip=1",
                "seek=2",
                "conv=notrunc",
                "status=none",
            ],
        },
        DdCase {
            name: "fsync",
            initial_output: None,
            flags: &["bs=4", "count=6", "conv=fsync", "status=none"],
        },
        DdCase {
            name: "notrunc-and-fsync",
            initial_output: Some(b"0123456789abcdefghijklmnopqrstuvwxyz"),
            flags: &[
                "bs=3",
                "count=7",
                "skip=2",
                "seek=4",
                "conv=notrunc,fsync",
                "status=none",
            ],
        },
        DdCase {
            name: "byte-count-flags",
            initial_output: Some(b"abcdefghijklmnopqrstuvwxyz0123456789"),
            flags: &[
                "bs=8",
                "skip=5",
                "seek=3",
                "count=13",
                "iflag=skip_bytes,count_bytes",
                "oflag=seek_bytes",
                "conv=notrunc",
                "status=none",
            ],
        },
    ];

    for case in cases {
        let tmp = unique_temp_dir(case.name);
        let input = tmp.join("input.bin");
        let example_output = tmp.join("example.bin");
        let fro_output = tmp.join("fro.bin");
        let system_output = tmp.join("system.bin");
        fs::write(&input, &input_bytes).unwrap();
        if let Some(initial) = case.initial_output {
            fs::write(&example_output, initial).unwrap();
            fs::write(&fro_output, initial).unwrap();
            fs::write(&system_output, initial).unwrap();
        }

        let example_args = build_args(&input, &example_output, case.flags);
        let fro_args = build_args(&input, &fro_output, case.flags);
        let system_args = build_args(&input, &system_output, case.flags);
        let example = run_example(&example_args);
        let fro = run_fro_dd(&fro_args);
        let system = run_system_dd(&system_args);

        compare_outputs(
            case.name,
            "example",
            &example,
            &system,
            &example_output,
            &system_output,
        );
        compare_outputs(
            case.name,
            "fro dd",
            &fro,
            &system,
            &fro_output,
            &system_output,
        );
    }
}

#[test]
fn dd_example_direct_flags_preserve_output_semantics() {
    let input_bytes = (0..(4096 * 3 + 1537))
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    let cases = [
        DdCase {
            name: "direct-read",
            initial_output: None,
            flags: &["bs=4096", "count=4", "iflag=direct", "status=none"],
        },
        DdCase {
            name: "direct-write",
            initial_output: None,
            flags: &[
                "bs=4096",
                "count=4",
                "seek=1",
                "oflag=direct",
                "status=none",
            ],
        },
        DdCase {
            name: "direct-read-write-notrunc",
            initial_output: Some(b"0123456789abcdefghijklmnopqrstuvwxyz"),
            flags: &[
                "bs=4096",
                "count=2",
                "skip=1",
                "seek=1",
                "iflag=direct",
                "oflag=direct",
                "conv=notrunc",
                "status=none",
            ],
        },
    ];

    for case in cases {
        let tmp = unique_temp_dir(case.name);
        let input = tmp.join("input.bin");
        let example_output = tmp.join("example.bin");
        let baseline_output = tmp.join("baseline.bin");
        let system_output = tmp.join("system.bin");
        fs::write(&input, &input_bytes).unwrap();
        if let Some(initial) = case.initial_output {
            fs::write(&example_output, initial).unwrap();
            fs::write(&baseline_output, initial).unwrap();
            fs::write(&system_output, initial).unwrap();
        }

        let example_args = build_args(&input, &example_output, case.flags);
        let baseline_flags = case
            .flags
            .iter()
            .copied()
            .filter(|flag| *flag != "iflag=direct" && *flag != "oflag=direct")
            .collect::<Vec<_>>();
        let baseline_args = build_args(&input, &baseline_output, &baseline_flags);
        let example = run_example(&example_args);
        let baseline = run_example(&baseline_args);
        let system_args = build_args(&input, &system_output, &baseline_flags);
        let system = run_system_dd(&system_args);

        compare_outputs(
            case.name,
            "baseline example",
            &baseline,
            &system,
            &baseline_output,
            &system_output,
        );
        assert_eq!(
            example.status.success(),
            baseline.status.success(),
            "case {}: direct flag changed exit status",
            case.name
        );
        assert_eq!(
            example.stdout, baseline.stdout,
            "case {}: direct flag changed stdout",
            case.name
        );
        assert_eq!(
            example.stderr, baseline.stderr,
            "case {}: direct flag changed stderr",
            case.name
        );
        assert_eq!(
            fs::read(&example_output).unwrap(),
            fs::read(&baseline_output).unwrap(),
            "case {}: direct flag changed output bytes",
            case.name
        );
    }
}

#[test]
fn dd_example_matches_system_dd_when_writing_to_dev_null() {
    let tmp = unique_temp_dir("dd-dev-null");
    let input = tmp.join("input.bin");
    let input_bytes = (0..(1024 * 1024 * 4 + 137))
        .map(|i| ((i * 19) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&input, input_bytes).unwrap();

    let args = vec![
        format!("if={}", input.display()),
        "of=/dev/null".to_string(),
        "bs=1M".to_string(),
        "skip=1".to_string(),
        "seek=1".to_string(),
        "count=2".to_string(),
        "status=none".to_string(),
    ];
    let example = run_example(&args);
    let fro = run_fro_dd(&args);
    let system = run_system_dd(&args);

    assert_eq!(
        example.status.success(),
        system.status.success(),
        "example stderr=\n{}\nsystem stderr=\n{}",
        String::from_utf8_lossy(&example.stderr),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(example.stdout, system.stdout);
    assert_eq!(example.stderr, system.stderr);
    assert_eq!(
        fro.status.success(),
        system.status.success(),
        "fro stderr=\n{}\nsystem stderr=\n{}",
        String::from_utf8_lossy(&fro.stderr),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(fro.stdout, system.stdout);
    assert_eq!(fro.stderr, system.stderr);
}

#[test]
fn dd_example_prints_dd_style_record_counts() {
    let tmp = unique_temp_dir("dd-record-counts");
    let input = tmp.join("input.bin");
    let output = tmp.join("output.bin");
    let input_bytes = (0..97).map(|i| ((i * 23) % 251) as u8).collect::<Vec<_>>();
    fs::write(&input, input_bytes).unwrap();

    let args = vec![
        format!("if={}", input.display()),
        format!("of={}", output.display()),
        "bs=10".to_string(),
    ];
    let output = run_example(&args);
    assert!(
        output.status.success(),
        "stderr=\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("9+1 records in"), "stderr=\n{stderr}");
    assert!(stderr.contains("9+1 records out"), "stderr=\n{stderr}");
    assert!(stderr.contains("97 bytes copied in"), "stderr=\n{stderr}");

    let fro_output = run_fro_dd(&args);
    assert!(
        fro_output.status.success(),
        "stderr=\n{}",
        String::from_utf8_lossy(&fro_output.stderr)
    );
    let fro_stderr = String::from_utf8_lossy(&fro_output.stderr);
    assert!(
        fro_stderr.contains("9+1 records in"),
        "stderr=\n{fro_stderr}"
    );
    assert!(
        fro_stderr.contains("9+1 records out"),
        "stderr=\n{fro_stderr}"
    );
    assert!(
        fro_stderr.contains("97 bytes copied in"),
        "stderr=\n{fro_stderr}"
    );
}

#[test]
fn dd_status_noxfer_matches_system_dd() {
    let tmp = unique_temp_dir("dd-status-noxfer");
    let input = tmp.join("input.bin");
    let input_bytes = (0..97).map(|i| ((i * 29) % 251) as u8).collect::<Vec<_>>();
    fs::write(&input, input_bytes).unwrap();

    for case in [
        (
            "full-copy",
            vec!["bs=10".to_string(), "status=noxfer".to_string()],
        ),
        (
            "count-zero",
            vec![
                "bs=4".to_string(),
                "count=0".to_string(),
                "status=noxfer".to_string(),
            ],
        ),
    ] {
        let case_dir = tmp.join(case.0);
        fs::create_dir_all(&case_dir).unwrap();
        let example_output = case_dir.join("example.bin");
        let fro_output = case_dir.join("fro.bin");
        let system_output = case_dir.join("system.bin");

        let mut example_args = vec![
            format!("if={}", input.display()),
            format!("of={}", example_output.display()),
        ];
        example_args.extend(case.1.iter().cloned());

        let mut fro_args = vec![
            format!("if={}", input.display()),
            format!("of={}", fro_output.display()),
        ];
        fro_args.extend(case.1.iter().cloned());

        let mut system_args = vec![
            format!("if={}", input.display()),
            format!("of={}", system_output.display()),
        ];
        system_args.extend(case.1.iter().cloned());

        let example = run_example(&example_args);
        let fro = run_fro_dd(&fro_args);
        let system = run_system_dd(&system_args);

        compare_outputs(
            case.0,
            "example",
            &example,
            &system,
            &example_output,
            &system_output,
        );
        compare_outputs(case.0, "fro dd", &fro, &system, &fro_output, &system_output);
        assert!(!String::from_utf8_lossy(&fro.stderr).contains("bytes copied"));
    }
}

#[test]
fn dd_count_zero_seek_matches_system_dd_and_preserves_notrunc() {
    let tmp = unique_temp_dir("dd-count-zero-seek");
    let input = tmp.join("input.bin");
    fs::write(&input, b"abcdef").unwrap();

    for case in [
        (
            "truncate-seek",
            None,
            vec!["bs=1", "count=0", "seek=5", "status=none"],
        ),
        (
            "preserve-notrunc",
            Some(b"abcdefghij".as_slice()),
            vec!["bs=1", "count=0", "seek=12", "conv=notrunc", "status=none"],
        ),
    ] {
        let case_dir = tmp.join(case.0);
        fs::create_dir_all(&case_dir).unwrap();
        let example_output = case_dir.join("example.bin");
        let fro_output = case_dir.join("fro.bin");
        let system_output = case_dir.join("system.bin");
        if let Some(initial) = case.1 {
            fs::write(&example_output, initial).unwrap();
            fs::write(&fro_output, initial).unwrap();
            fs::write(&system_output, initial).unwrap();
        }
        let example_args = build_args(&input, &example_output, &case.2);
        let fro_args = build_args(&input, &fro_output, &case.2);
        let system_args = build_args(&input, &system_output, &case.2);
        let example = run_example(&example_args);
        let fro = run_fro_dd(&fro_args);
        let system = run_system_dd(&system_args);

        compare_outputs(
            case.0,
            "example",
            &example,
            &system,
            &example_output,
            &system_output,
        );
        compare_outputs(case.0, "fro dd", &fro, &system, &fro_output, &system_output);
    }
}
