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
    example: &Output,
    system: &Output,
    example_output: &Path,
    system_output: &Path,
) {
    assert_eq!(
        example.status.success(),
        system.status.success(),
        "case {}: example stderr=\n{}\nsystem stderr=\n{}",
        case_name,
        String::from_utf8_lossy(&example.stderr),
        String::from_utf8_lossy(&system.stderr)
    );
    assert_eq!(
        example.stdout, system.stdout,
        "case {}: stdout mismatch",
        case_name
    );
    assert_eq!(
        example.stderr, system.stderr,
        "case {}: stderr mismatch",
        case_name
    );
    assert_eq!(
        fs::read(example_output).unwrap(),
        fs::read(system_output).unwrap(),
        "case {}: output mismatch",
        case_name
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
    ];

    for case in cases {
        let tmp = unique_temp_dir(case.name);
        let input = tmp.join("input.bin");
        let example_output = tmp.join("example.bin");
        let system_output = tmp.join("system.bin");
        fs::write(&input, &input_bytes).unwrap();
        if let Some(initial) = case.initial_output {
            fs::write(&example_output, initial).unwrap();
            fs::write(&system_output, initial).unwrap();
        }

        let example_args = build_args(&input, &example_output, case.flags);
        let system_args = build_args(&input, &system_output, case.flags);
        let example = run_example(&example_args);
        let system = run_system_dd(&system_args);

        compare_outputs(
            case.name,
            &example,
            &system,
            &example_output,
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
}
