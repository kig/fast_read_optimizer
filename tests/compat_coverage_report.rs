#![cfg(unix)]

use std::collections::BTreeSet;
use std::process::Command;

#[path = "../src/help_compat.rs"]
mod help_compat;

use help_compat::{
    parse_help_flag_tokens, tracked_help_tokens, CoverageRow, EXCLUDED_CUSTOM_MULTICALLS, ROWS,
};

fn render_report() -> String {
    let mut out = String::from(
        "# multicall compat coverage\n# tracked surface = canonical compat items from existing help/parser/tests\n# excluded custom multicalls: b3sum, decrypt, encrypt, pv\n",
    );
    for row in ROWS {
        out.push_str(&format!(
            "{:<9} {:>2}/{:<2} {:>3}%  remaining: {}  help: {}\n",
            row.name,
            row.covered.len(),
            row.total(),
            row.percent(),
            row.remaining_text(),
            help_superset_status(*row)
        ));
    }
    out
}

fn help_superset_status(row: CoverageRow) -> String {
    let missing = missing_help_tokens(row);
    if missing.is_empty() {
        "ok".to_string()
    } else {
        format!("missing: {}", missing.join(", "))
    }
}

fn missing_help_tokens(row: CoverageRow) -> Vec<String> {
    let fro_tokens = parse_help_flag_tokens(&fro_help_text(row.name));
    let system_tokens = parse_help_flag_tokens(&system_help_text(row.name));
    let tracked = tracked_help_tokens(row);
    system_tokens
        .intersection(&tracked)
        .filter(|token| !fro_tokens.contains(*token))
        .cloned()
        .collect()
}

fn fro_help_text(command: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_fro"))
        .arg(command)
        .arg("--help")
        .output()
        .unwrap_or_else(|err| panic!("failed to run fro {command} --help: {err}"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "fro {command} --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).expect("fro help should be UTF-8")
}

fn system_help_text(command: &str) -> String {
    let output = Command::new(command)
        .env("LC_ALL", "C")
        .arg("--help")
        .output()
        .unwrap_or_else(|err| panic!("failed to run {command} --help: {err}"));
    assert_eq!(
        output.status.code(),
        Some(0),
        "{command} --help failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).expect("system help should be UTF-8")
}

fn dispatched_compat_commands() -> BTreeSet<&'static str> {
    let source = include_str!("../src/coreutils/mod.rs");
    let section = source
        .split("pub fn is_coreutils_command")
        .nth(1)
        .expect("is_coreutils_command section")
        .split("pub fn rewrite_alias_args")
        .next()
        .expect("rewrite_alias_args section");

    let mut names = BTreeSet::new();
    let mut chars = section.chars();
    while let Some(ch) = chars.next() {
        if ch != '"' {
            continue;
        }
        let mut value = String::new();
        for ch in chars.by_ref() {
            if ch == '"' {
                break;
            }
            value.push(ch);
        }
        let leaked: &'static str = Box::leak(value.into_boxed_str());
        names.insert(leaked);
    }
    names.insert("cp");
    for excluded in EXCLUDED_CUSTOM_MULTICALLS {
        names.remove(excluded);
    }
    names
}

#[test]
fn compat_coverage_report_matches_snapshot() {
    let report = render_report();
    println!("{report}");

    let expected = "# multicall compat coverage
# tracked surface = canonical compat items from existing help/parser/tests
# excluded custom multicalls: b3sum, decrypt, encrypt, pv
base64     3/3  100%  remaining: none  help: ok
b2sum     11/11 100%  remaining: none  help: ok
cat       10/10 100%  remaining: none  help: ok
cksum      7/7  100%  remaining: none  help: ok
cmp        5/5  100%  remaining: none  help: ok
cp        10/10 100%  remaining: none  help: ok
dd        14/14 100%  remaining: none  help: ok
du         6/6  100%  remaining: none  help: ok
fgrep      9/9  100%  remaining: none  help: ok
find       9/9  100%  remaining: none  help: ok
head       4/4  100%  remaining: none  help: ok
md5sum    11/11 100%  remaining: none  help: ok
mv         3/3  100%  remaining: none  help: ok
rm         4/4  100%  remaining: none  help: ok
sha224sum 11/11 100%  remaining: none  help: ok
sha256sum 11/11 100%  remaining: none  help: ok
sha384sum 11/11 100%  remaining: none  help: ok
sha512sum 11/11 100%  remaining: none  help: ok
shred      6/6  100%  remaining: none  help: ok
sort       8/12  67%  remaining: -g/-h, -M, -V, -k  help: ok
tac        2/2  100%  remaining: none  help: ok
tail       4/4  100%  remaining: none  help: ok
tar        5/5  100%  remaining: none  help: ok
wc         6/6  100%  remaining: none  help: ok
";

    assert_eq!(report, expected);
}

#[test]
fn compat_coverage_rows_match_dispatch_minus_custom_multicalls() {
    let expected = dispatched_compat_commands();
    let actual = ROWS.iter().map(|row| row.name).collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn compat_help_token_parser_handles_alias_lists_and_dd_operands() {
    let parsed = parse_help_flag_tokens(
        "  -q, --quiet, --silent\n  --output=FILE\n  --check=diagnose-first\n  iflag=count_bytes\n  skip_bytes\n  status=none\n  FIXME: tracked GNU/coreutils flags not yet supported in this slice: -g/-h, -M, -V, -k\n",
    );
    let expected = BTreeSet::from([
        "--check".to_string(),
        "--check=diagnose-first".to_string(),
        "--output".to_string(),
        "--quiet".to_string(),
        "--silent".to_string(),
        "-M".to_string(),
        "-V".to_string(),
        "-g".to_string(),
        "-h".to_string(),
        "-k".to_string(),
        "-q".to_string(),
        "count_bytes".to_string(),
        "iflag=count_bytes".to_string(),
        "none".to_string(),
        "skip_bytes".to_string(),
        "status=none".to_string(),
    ]);
    assert_eq!(parsed, expected);
}

#[test]
fn compat_help_superset_matches_tracked_system_flags() {
    for row in ROWS {
        let missing = missing_help_tokens(*row);
        assert!(
            missing.is_empty(),
            "{} fro help is missing tracked system flags: {}",
            row.name,
            missing.join(", ")
        );
    }
}

#[test]
fn compat_help_fixmes_cover_remaining_or_incompatible_flags() {
    let cp_help = fro_help_text("cp");
    assert!(cp_help.contains("FIXME:"));
    assert!(cp_help.contains("ownership is not preserved"));

    let sort_help = fro_help_text("sort");
    assert!(sort_help.contains("FIXME:"));
    assert!(sort_help.contains("-g/-h, -M, -V, -k"));

    let tar_help = fro_help_text("tar");
    assert!(tar_help.contains("FIXME:"));
    assert!(tar_help.contains("whole-archive"));
}

#[test]
fn compat_coverage_explicit_gap_rows_match_source_help() {
    let help_source = include_str!("../src/main_app/help.rs");
    assert!(help_source.contains("FIXME: Unsupported GNU sort features currently return an error"));
    assert!(help_source.contains("month/version/human modes"));
    assert!(help_source
        .contains("FIXME: Extract currently targets uncompressed regular-file archives"));
    assert!(help_source.contains("FIXME: When invoked via the cp multicall alias"));

    let find_source = include_str!("../src/coreutils/find.rs");
    assert!(
        find_source.contains("-print             print each matching path followed by a newline (default)")
    );
}
