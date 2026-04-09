#![cfg(unix)]

use std::os::unix::process::CommandExt;
use std::process::{Command, Output};

fn run_alias(alias: &str, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fro"));
    command.arg0(format!("bin/{alias}"));
    command.args(args);
    command
        .output()
        .unwrap_or_else(|err| panic!("failed to run argv0 alias {alias}: {err}"))
}

fn assert_success(output: &Output, alias: &str, mode: &str) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "{alias} {mode} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        output.stderr.is_empty(),
        "{alias} {mode} unexpectedly wrote stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn multicall_argv0_help_uses_alias_names() {
    let aliases = [
        "cat", "cksum", "cmp", "cp", "find", "head", "mv", "tail", "tar", "wc",
    ];
    for alias in aliases {
        let output = run_alias(alias, &["--help"]);
        assert_success(&output, alias, "--help");
        let stdout = String::from_utf8(output.stdout).expect("help output should be UTF-8");
        assert!(
            stdout.contains("USAGE:"),
            "{alias} help missing usage section:\n{stdout}"
        );
        assert!(
            stdout.contains(&format!("\n  {alias} ")),
            "{alias} help should use alias in usage:\n{stdout}"
        );
        assert!(
            !stdout.contains(&format!("bin/{alias}")),
            "{alias} help should not use argv0 path:\n{stdout}"
        );
    }

    let cp_help = String::from_utf8(run_alias("cp", &["--help"]).stdout).unwrap();
    assert!(
        cp_help.starts_with("cp - "),
        "cp help should use the cp alias name:\n{cp_help}"
    );
    assert!(
        !cp_help.contains("USAGE:\n  cp copy "),
        "cp help should not show the internal copy usage name:\n{cp_help}"
    );
    assert!(
        !cp_help.contains("\n    cp copy "),
        "cp help examples should not show the internal copy name:\n{cp_help}"
    );

    let find_help = String::from_utf8(run_alias("find", &["--help"]).stdout).unwrap();
    assert!(
        find_help.starts_with("find - "),
        "find help should use the find alias name:\n{find_help}"
    );
}

#[test]
fn multicall_argv0_version_uses_alias_names() {
    let aliases = [
        "cat", "cksum", "cmp", "cp", "find", "head", "mv", "tail", "tar", "wc",
    ];
    for alias in aliases {
        let output = run_alias(alias, &["--version"]);
        assert_success(&output, alias, "--version");
        let stdout = String::from_utf8(output.stdout).expect("version output should be UTF-8");
        assert!(
            stdout.starts_with(&format!("{alias} ")),
            "{alias} version should start with the alias name:\n{stdout}"
        );
        assert!(
            stdout.contains(env!("CARGO_PKG_VERSION")),
            "{alias} version should include the package version:\n{stdout}"
        );
        assert!(
            !stdout.contains(&format!("bin/{alias}")),
            "{alias} version should not use argv0 path:\n{stdout}"
        );
    }
}
