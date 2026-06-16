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

fn run_subcommand(command_name: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command_name)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run fro {command_name}: {err}"))
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
        "base64",
        "cat",
        "cksum",
        "cmp",
        "cp",
        "dd",
        "du",
        "find",
        "head",
        "md5sum",
        "mv",
        "shred",
        "sha256sum",
        "tail",
        "tar",
        "wc",
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
        "base64",
        "cat",
        "cksum",
        "cmp",
        "cp",
        "dd",
        "du",
        "find",
        "head",
        "md5sum",
        "mv",
        "shred",
        "sha256sum",
        "tail",
        "tar",
        "wc",
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

#[test]
fn cp_subcommand_help_and_version_use_bounded_cp_surface() {
    let help = run_subcommand("cp", &["--help"]);
    assert_success(&help, "cp", "subcommand --help");
    let help_stdout = String::from_utf8(help.stdout).expect("help output should be UTF-8");
    assert!(
        help_stdout.starts_with("cp - Bounded GNU cp-compatible alias over fro copy."),
        "cp subcommand help should use the bounded cp summary:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("fro cp [-a|-r|-R|--recursive]"),
        "cp subcommand help should show cp usage:\n{help_stdout}"
    );
    assert!(
        help_stdout
            .contains("Supported GNU cp flags in this slice: -a/--archive, -r/-R/--recursive"),
        "cp subcommand help should explicitly name supported long forms:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("--backup/-b")
            && help_stdout.contains("--copy-contents")
            && help_stdout.contains("--no-preserve=ATTR_LIST")
            && help_stdout.contains("--reflink=auto")
            && help_stdout.contains("--sparse=auto")
            && help_stdout.contains("--context[=CTX]/-Z"),
        "cp subcommand help should acknowledge the remaining bounded GNU cp tokens honestly:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("not implemented natively in this slice"),
        "cp subcommand help should describe unsupported GNU cp tokens as bounded omissions:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("--help shows this message and exits."),
        "cp subcommand help should mention --help:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("--version prints the fro cp version string and exits."),
        "cp subcommand help should mention --version:\n{help_stdout}"
    );
    assert!(
        !help_stdout.contains("When invoked via the cp multicall alias"),
        "cp subcommand help should be first-class, not copy-alias wording:\n{help_stdout}"
    );
    assert!(
        !help_stdout.contains("\n    fro copy "),
        "cp subcommand help should not show internal copy examples:\n{help_stdout}"
    );

    let version = run_subcommand("cp", &["--version"]);
    assert_success(&version, "cp", "subcommand --version");
    let version_stdout = String::from_utf8(version.stdout).expect("version output should be UTF-8");
    assert!(
        version_stdout.starts_with("cp "),
        "cp subcommand version should start with cp:\n{version_stdout}"
    );
    assert!(
        version_stdout.contains("(fro coreutils)"),
        "cp subcommand version should use fro's bounded version surface:\n{version_stdout}"
    );
    assert!(
        version_stdout.contains(env!("CARGO_PKG_VERSION")),
        "cp subcommand version should include the package version:\n{version_stdout}"
    );
}

#[test]
fn tar_subcommand_help_and_version_use_bounded_tar_surface() {
    let help = run_subcommand("tar", &["--help"]);
    assert_success(&help, "tar", "subcommand --help");
    let help_stdout = String::from_utf8(help.stdout).expect("help output should be UTF-8");
    assert!(
        help_stdout.starts_with("tar - Bounded GNU tar-compatible create/list/extract slice"),
        "tar subcommand help should use the bounded tar summary:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("Supported compatibility slice: create (-c/--create), whole-archive list (-t/--list), and whole-archive extract (-x/--extract) with -f/--file"),
        "tar subcommand help should explicitly name the bounded supported slice:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("optional extras in this slice are -v/--verbose, -C/--directory for extract, --auto-compress/-a, gzip (-z/--gzip/--gunzip/--ungzip), bzip2 (-j/--bzip2), xz (-J/--xz), zstd (--zstd), --help, and --version"),
        "tar subcommand help should distinguish supported extras from omitted GNU families:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("Common bounded short forms include -cf")
            && help_stdout.contains("-tvf")
            && help_stdout.contains("-xf"),
        "tar subcommand help should call out the bounded short-form clusters it actually documents:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("unsupported/omitted notes, not runtime promises")
            && help_stdout.contains("--absolute-names")
            && help_stdout.contains("--pax-option=keyword")
            && help_stdout.contains("--same-order")
            && help_stdout.contains("--rmt-command=")
            && help_stdout.contains("-f-"),
        "tar subcommand help should honestly surface omitted GNU tar token families:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("FIXME: Extract currently targets regular-file archives"),
        "tar subcommand help should preserve the bounded FIXME wording for extract:\n{help_stdout}"
    );
    assert!(
        help_stdout.contains("FIXME: Compressed create still uses a single tar stream"),
        "tar subcommand help should preserve the bounded FIXME wording for compressed paths:\n{help_stdout}"
    );

    let version = run_subcommand("tar", &["--version"]);
    assert_success(&version, "tar", "subcommand --version");
    let version_stdout = String::from_utf8(version.stdout).expect("version output should be UTF-8");
    assert!(
        version_stdout.starts_with("tar "),
        "tar subcommand version should start with tar:\n{version_stdout}"
    );
    assert!(
        version_stdout.contains("(fro coreutils)"),
        "tar subcommand version should use fro's bounded version surface:\n{version_stdout}"
    );
    assert!(
        version_stdout.contains(env!("CARGO_PKG_VERSION")),
        "tar subcommand version should include the package version:\n{version_stdout}"
    );
}

#[test]
fn cmp_argv0_short_version_uses_alias_name() {
    let output = run_alias("cmp", &["-v"]);
    assert_success(&output, "cmp", "-v");
    let stdout = String::from_utf8(output.stdout).expect("version output should be UTF-8");
    assert!(
        stdout.starts_with("cmp "),
        "cmp -v should start with the alias name:\n{stdout}"
    );
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "cmp -v should include the package version:\n{stdout}"
    );
    assert!(
        !stdout.contains("bin/cmp"),
        "cmp -v should not use argv0 path:\n{stdout}"
    );
}

#[test]
fn fgrep_argv0_short_version_uses_alias_name() {
    let output = run_alias("fgrep", &["-V"]);
    assert_success(&output, "fgrep", "-V");
    let stdout = String::from_utf8(output.stdout).expect("version output should be UTF-8");
    assert!(
        stdout.starts_with("fgrep "),
        "fgrep -V should start with the alias name:\n{stdout}"
    );
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "fgrep -V should include the package version:\n{stdout}"
    );
    assert!(
        !stdout.contains("bin/fgrep"),
        "fgrep -V should not use argv0 path:\n{stdout}"
    );
}
