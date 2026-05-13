#![cfg(unix)]

use std::collections::BTreeSet;

#[path = "../src/help_compat.rs"]
mod help_compat;

use help_compat::{
    fro_help_text, help_token_coverage, parse_help_flag_surface_tokens, parse_help_flag_tokens,
    system_help_text, tracked_help_tokens, CoverageRow, EXCLUDED_CUSTOM_MULTICALLS, ROWS,
};

fn render_report() -> String {
    let mut out = String::from(
        "# multicall compat coverage\n# tracked surface = canonical compat items from existing help/parser/tests\n# excluded custom multicalls: b3sum, decrypt, encrypt, gzip, gunzip, pv, zcat\n",
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

fn render_actual_help_coverage_report() -> String {
    let mut out = String::from(
        "# actual tokenized help coverage\n# rows are tracked multicall commands; coverage = tokenized fro <cmd> --help vs system <cmd> --help on this host\n",
    );
    for row in ROWS {
        let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), row.name);
        out.push_str(&format!(
            "{:<9} {:>3}/{:<3} {:>3}%  remaining: {}\n",
            row.name,
            coverage.covered_count(),
            coverage.total_count(),
            coverage.percent(),
            coverage.remaining_text(),
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
    let fro_tokens = parse_help_flag_tokens(&fro_help_text(env!("CARGO_BIN_EXE_fro"), row.name));
    let system_tokens = parse_help_flag_tokens(&system_help_text(row.name));
    let tracked = tracked_help_tokens(row);
    system_tokens
        .intersection(&tracked)
        .filter(|token| !fro_tokens.contains(*token))
        .cloned()
        .collect()
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
# excluded custom multicalls: b3sum, decrypt, encrypt, gzip, gunzip, pv, zcat
base64     5/5  100%  remaining: none  help: ok
b2sum     12/12 100%  remaining: none  help: ok
cat       12/12 100%  remaining: none  help: ok
cksum      9/9  100%  remaining: none  help: ok
cmp        6/6  100%  remaining: none  help: ok
cp        16/16 100%  remaining: none  help: ok
dd        16/16 100%  remaining: none  help: ok
du        20/20 100%  remaining: none  help: ok
fgrep     42/52  81%  remaining: --directories=recurse, -E/--extended-regexp, -G/--basic-regexp, -P/--perl-regexp, -r/--recursive, -R/--dereference-recursive, --include=GLOB, --exclude=GLOB, --exclude-dir=GLOB, --exclude-from=FILE  help: ok
find      11/80  14%  remaining: -D, -H, -L, -N, -Olevel, -P, -a, -amin, -and, -anewer, -atime, -cmin, -cnewer, -context, -ctime, -daystart, -delete, -depth, -empty, -exec, -execdir, -executable, -false, -fls, -follow, -fprint, -fprint0, -fprintf, -fstype, -gid, -group, -ignore_readdir_race, -ilname, -inum, -iregex, -iwholename, -links, -lname, -ls, -mindepth, -mmin, -mount, -mtime, -newer, -nogroup, -noignore_readdir_race, -noleaf, -not, -nouser, -o, -ok, -okdir, -or, -perm, -printf, -prune, -quit, -readable, -regex, -regextype, -size, -true, -uid, -used, -user, -wholename, -writable, -xdev, -xtype  help: ok
head       7/7  100%  remaining: none  help: ok
md5sum    11/11 100%  remaining: none  help: ok
mv        12/12 100%  remaining: none  help: ok
rm        12/12 100%  remaining: none  help: ok
sha224sum 11/11 100%  remaining: none  help: ok
sha256sum 11/11 100%  remaining: none  help: ok
sha384sum 11/11 100%  remaining: none  help: ok
sha512sum 11/11 100%  remaining: none  help: ok
shred      9/9  100%  remaining: none  help: ok
sort      31/31 100%  remaining: none  help: ok
tac        7/7  100%  remaining: none  help: ok
tail      14/14 100%  remaining: none  help: ok
tar        5/5  100%  remaining: none  help: ok
wc         8/8  100%  remaining: none  help: ok
";

    assert_eq!(report, expected);
}

#[test]
fn actual_help_coverage_report_matches_snapshot() {
    let report = render_actual_help_coverage_report();
    println!("{report}");

    let expected = "# actual tokenized help coverage
# rows are tracked multicall commands; coverage = tokenized fro <cmd> --help vs system <cmd> --help on this host
base64      8/8   100%  remaining: none
b2sum      19/19  100%  remaining: none
cat        19/19  100%  remaining: none
cksum       2/2   100%  remaining: none
cmp        14/14  100%  remaining: none
cp         57/57  100%  remaining: none
dd          2/2   100%  remaining: none
du         44/44  100%  remaining: none
fgrep      87/87  100%  remaining: none
find       78/78  100%  remaining: none
head       15/15  100%  remaining: none
md5sum      6/6   100%  remaining: none
mv         23/23  100%  remaining: none
rm         17/17  100%  remaining: none
sha224sum  17/17  100%  remaining: none
sha256sum   6/6   100%  remaining: none
sha384sum  17/17  100%  remaining: none
sha512sum  17/17  100%  remaining: none
shred      17/17  100%  remaining: none
sort       55/55  100%  remaining: none
tac         8/8   100%  remaining: none
tail       24/24  100%  remaining: none
tar       230/230 100%  remaining: none
wc         13/13  100%  remaining: none
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
        "  -q, --quiet, --silent\n  --output=FILE\n  --check=diagnose-first\n  iflag=count_bytes\n  skip_bytes\n  status=none\n  FIXME: tracked GNU/coreutils flags not yet supported in this slice: -M, -V, -k\n",
    );
    let expected = BTreeSet::from([
        "--check".to_string(),
        "--check=diagnose-first".to_string(),
        "--output".to_string(),
        "--quiet".to_string(),
        "--silent".to_string(),
        "-M".to_string(),
        "-V".to_string(),
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
fn compat_help_surface_parser_skips_dash_prefixed_example_operands() {
    let parsed = parse_help_flag_surface_tokens(
        "  -f, --force\nTo remove a file whose name starts with a '-', for example '-foo',\nuse one of these commands:\n  rm -- -foo\n  rm ./-foo\nFIXME: bounded note still tracks unsupported flags like -Z and --context.\n",
    );
    let expected = BTreeSet::from([
        "--context".to_string(),
        "--force".to_string(),
        "-Z".to_string(),
        "-f".to_string(),
    ]);
    assert_eq!(parsed, expected);
    assert!(!parsed.contains("-foo"));
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
fn cmp_actual_help_surface_covers_help_and_version() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "cmp");
    for token in ["--help", "--version", "-v"] {
        assert!(
            coverage.system_tokens.contains(token),
            "system cmp --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro cmp --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing
            .iter()
            .any(|token| token == "--help" || token == "--version" || token == "-v"),
        "cmp help/version should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn bounded_actual_help_surface_covers_help_and_version() {
    for command in ["base64", "cat", "cksum", "cp", "dd"] {
        let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), command);
        assert!(
            coverage.system_tokens.contains("--help"),
            "system {command} --help should expose --help"
        );
        assert!(
            coverage.system_tokens.contains("--version"),
            "system {command} --help should expose --version"
        );
        assert!(
            coverage.fro_tokens.contains("--help"),
            "fro {command} --help should expose --help"
        );
        assert!(
            coverage.fro_tokens.contains("--version"),
            "fro {command} --help should expose --version"
        );
        let missing = coverage.missing_tokens();
        assert!(
            !missing
                .iter()
                .any(|token| token == "--help" || token == "--version"),
            "{command} help/version should not be missing from actual help coverage: {}",
            missing.join(", ")
        );
    }
}

#[test]
fn rm_actual_help_surface_covers_help_version_and_root_policy_flags() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "rm");
    for token in [
        "--help",
        "--version",
        "--one-file-system",
        "--preserve-root",
        "--no-preserve-root",
        "--interactive",
        "--recursive",
        "--verbose",
        "-I",
        "-d",
        "-f",
        "-i",
        "-r",
        "-R",
        "-v",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system rm --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro rm --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing.iter().any(|token| {
            matches!(
                token.as_str(),
                "--help"
                    | "--version"
                    | "--one-file-system"
                    | "--preserve-root"
                    | "--no-preserve-root"
                    | "--interactive"
                    | "--recursive"
                    | "--verbose"
                    | "-I"
                    | "-d"
                    | "-f"
                    | "-i"
                    | "-r"
                    | "-R"
                    | "-v"
            )
        }),
        "rm tracked help/root-policy tokens should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
    assert!(
        !coverage.system_tokens.contains("-foo"),
        "rm help coverage should ignore dash-prefixed example operands"
    );
}

#[test]
fn wc_actual_help_surface_covers_help_and_version() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "wc");
    assert!(
        coverage.system_tokens.contains("--help"),
        "system wc --help should expose --help"
    );
    assert!(
        coverage.system_tokens.contains("--version"),
        "system wc --help should expose --version"
    );
    assert!(
        coverage.fro_tokens.contains("--help"),
        "fro wc --help should expose --help"
    );
    assert!(
        coverage.fro_tokens.contains("--version"),
        "fro wc --help should expose --version"
    );
    let missing = coverage.missing_tokens();
    assert!(
        !missing
            .iter()
            .any(|token| token == "--help" || token == "--version"),
        "wc help/version should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn find_actual_help_surface_covers_help_and_version() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "find");
    for token in [
        "--help",
        "--version",
        "-maxdepth",
        "-print0",
        "-D",
        "-delete",
        "-exec",
        "-regex",
        "-xtype",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system find --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro find --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        missing.is_empty(),
        "find actual help coverage should be fully closed: {}",
        missing.join(", ")
    );
}

#[test]
fn du_actual_help_surface_covers_help_and_version() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "du");
    for token in [
        "--help",
        "--version",
        "--null",
        "--si",
        "--no-dereference",
        "--block-size",
        "--threshold",
        "-0",
        "-B",
        "-P",
        "-k",
        "-m",
        "-t",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system du --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro du --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing.iter().any(|token| {
            matches!(
                token.as_str(),
                "--help"
                    | "--version"
                    | "--null"
                    | "--si"
                    | "--no-dereference"
                    | "--block-size"
                    | "--threshold"
                    | "-0"
                    | "-B"
                    | "-P"
                    | "-k"
                    | "-m"
                    | "-t"
            )
        }),
        "du tracked help tokens should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn head_actual_help_surface_covers_bounded_help_and_version() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "head");
    for token in [
        "-n",
        "--lines",
        "--lines=",
        "-c",
        "--bytes",
        "--bytes=",
        "-z",
        "--zero-terminated",
        "-q",
        "--quiet",
        "--silent",
        "-v",
        "--verbose",
        "--help",
        "--version",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system head --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro head --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing.iter().any(|token| {
            matches!(
                token.as_str(),
                "-n" | "--lines"
                    | "--lines="
                    | "-c"
                    | "--bytes"
                    | "--bytes="
                    | "-z"
                    | "--zero-terminated"
                    | "-q"
                    | "--quiet"
                    | "--silent"
                    | "-v"
                    | "--verbose"
                    | "--help"
                    | "--version"
            )
        }),
        "head tracked help tokens should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn tac_actual_help_surface_covers_bounded_help_version_and_compat_tokens() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "tac");
    for token in [
        "-b",
        "--before",
        "-r",
        "--regex",
        "-s",
        "--separator",
        "--help",
        "--version",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system tac --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro tac --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing.iter().any(|token| {
            matches!(
                token.as_str(),
                "-b" | "--before"
                    | "-r"
                    | "--regex"
                    | "-s"
                    | "--separator"
                    | "--help"
                    | "--version"
            )
        }),
        "tac tracked help tokens should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn tail_actual_help_surface_covers_bounded_count_and_help_tokens() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "tail");
    for token in [
        "-n",
        "--lines",
        "--lines=",
        "-c",
        "--bytes",
        "--bytes=",
        "-z",
        "--zero-terminated",
        "-q",
        "--quiet",
        "--silent",
        "-v",
        "--verbose",
        "-f",
        "--follow",
        "--follow=name",
        "-F",
        "--retry",
        "-s",
        "--sleep-interval",
        "--pid",
        "--max-unchanged-stats",
        "--help",
        "--version",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system tail --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro tail --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    assert!(
        !missing.iter().any(|token| {
            matches!(
                token.as_str(),
                "-n" | "--lines"
                    | "--lines="
                    | "-c"
                    | "--bytes"
                    | "--bytes="
                    | "-z"
                    | "--zero-terminated"
                    | "-q"
                    | "--quiet"
                    | "--silent"
                    | "-v"
                    | "--verbose"
                    | "-f"
                    | "--follow"
                    | "--follow=name"
                    | "-F"
                    | "--retry"
                    | "-s"
                    | "--sleep-interval"
                    | "--pid"
                    | "--max-unchanged-stats"
                    | "--help"
                    | "--version"
            )
        }),
        "tail tracked help tokens should not be missing from actual help coverage: {}",
        missing.join(", ")
    );
}

#[test]
fn mv_actual_help_surface_covers_supported_overwrite_and_meta_flags() {
    let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), "mv");
    for token in [
        "-b",
        "--backup",
        "-Z",
        "--context",
        "-f",
        "--force",
        "-i",
        "--interactive",
        "-n",
        "--no-clobber",
        "-S",
        "--suffix",
        "--strip-trailing-slashes",
        "-u",
        "--update",
        "-v",
        "--verbose",
        "-t",
        "--target-directory",
        "-T",
        "--no-target-directory",
        "--help",
        "--version",
    ] {
        assert!(
            coverage.system_tokens.contains(token),
            "system mv --help should expose {token}"
        );
        assert!(
            coverage.fro_tokens.contains(token),
            "fro mv --help should expose {token}"
        );
    }
    let missing = coverage.missing_tokens();
    for token in [
        "--backup",
        "--context",
        "--suffix",
        "--strip-trailing-slashes",
        "--force",
        "--interactive",
        "--help",
        "--version",
        "-Z",
        "-i",
    ] {
        assert!(
            !missing.iter().any(|missing| missing == token),
            "mv help surface should not miss {token}: {}",
            missing.join(", ")
        );
    }
}

#[test]
fn digest_help_surface_covers_help_and_version() {
    for command in [
        "b2sum",
        "md5sum",
        "sha224sum",
        "sha256sum",
        "sha384sum",
        "sha512sum",
    ] {
        let coverage = help_token_coverage(env!("CARGO_BIN_EXE_fro"), command);
        assert!(
            coverage.system_tokens.contains("--help"),
            "system {command} --help should expose --help"
        );
        assert!(
            coverage.system_tokens.contains("--version"),
            "system {command} --help should expose --version"
        );
        assert!(
            coverage.fro_tokens.contains("--help"),
            "fro {command} --help should expose --help"
        );
        assert!(
            coverage.fro_tokens.contains("--version"),
            "fro {command} --help should expose --version"
        );
        let missing = coverage.missing_tokens();
        assert!(
            !missing
                .iter()
                .any(|token| token == "--help" || token == "--version"),
            "{command} help/version should not be missing from actual help coverage: {}",
            missing.join(", ")
        );
    }
}

#[test]
fn compat_help_fixmes_cover_remaining_or_incompatible_flags() {
    let cp_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "cp");
    assert!(cp_help.contains("FIXME:"));
    assert!(cp_help.contains("ownership is not preserved"));

    let sort_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "sort");
    assert!(sort_help.contains("FIXME:"));
    assert!(sort_help.contains("locale collation"));
    assert!(sort_help.contains("per-key modifiers"));

    let tar_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "tar");
    assert!(tar_help.contains("FIXME:"));
    assert!(tar_help.contains("whole-archive"));
}

#[test]
fn compat_coverage_explicit_gap_rows_match_source_help() {
    let help_source = concat!(
        include_str!("../src/main_app/help.rs"),
        include_str!("../src/main_app/help/commands.rs"),
        include_str!("../src/help_compat.rs")
    );
    assert!(help_source.contains("FIXME: Unsupported GNU sort features currently return an error"));
    assert!(help_source.contains("locale collation and per-key modifiers"));
    assert!(help_source.contains("--version prints the fro sort version string and exits."));
    assert!(help_source.contains("FIXME: Extract currently targets regular-file archives"));
    assert!(help_source.contains("unsupported/omitted notes, not runtime promises"));
    assert!(help_source.contains("--same-order"));
    assert!(help_source.contains("FIXME: When invoked via the cp multicall alias"));
    assert!(help_source.contains("-Z/--context remain unsupported in this bounded slice"));
    assert!(help_source.contains("bounded GNU find-compatible path-walking/predicate slice"));
    assert!(help_source.contains(
        "Tracked GNU find tokens implemented in this bounded in-process path-walking slice"
    ));
    assert!(help_source
        .contains("GNU find tokens intentionally omitted from this bounded in-process slice"));
    assert!(help_source.contains("\"-D\""));
    assert!(help_source.contains("\"-Olevel\""));
    assert!(help_source.contains("\"-xtype\""));
}
