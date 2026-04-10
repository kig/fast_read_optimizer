#![cfg(unix)]

use std::collections::BTreeSet;

#[path = "../src/help_compat.rs"]
mod help_compat;

use help_compat::{
    fro_help_text, help_token_coverage, parse_help_flag_tokens, system_help_text,
    tracked_help_tokens, CoverageRow, EXCLUDED_CUSTOM_MULTICALLS, ROWS,
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
head       5/5  100%  remaining: none  help: ok
md5sum    11/11 100%  remaining: none  help: ok
mv         5/5  100%  remaining: none  help: ok
rm         7/7  100%  remaining: none  help: ok
sha224sum 11/11 100%  remaining: none  help: ok
sha256sum 11/11 100%  remaining: none  help: ok
sha384sum 11/11 100%  remaining: none  help: ok
sha512sum 11/11 100%  remaining: none  help: ok
shred      6/6  100%  remaining: none  help: ok
sort      11/12  92%  remaining: -k  help: ok
tac        2/2  100%  remaining: none  help: ok
tail       5/5  100%  remaining: none  help: ok
tar        5/5  100%  remaining: none  help: ok
wc         6/6  100%  remaining: none  help: ok
";

    assert_eq!(report, expected);
}

#[test]
fn actual_help_coverage_report_matches_snapshot() {
    let report = render_actual_help_coverage_report();
    println!("{report}");

    let expected = "# actual tokenized help coverage
# rows are tracked multicall commands; coverage = tokenized fro <cmd> --help vs system <cmd> --help on this host
base64      6/8    75%  remaining: --help, --version
b2sum      15/19   79%  remaining: --help, --length, --version, -l
cat        17/19   89%  remaining: --help, --version
cksum       0/2     0%  remaining: --help, --version
cmp        11/14   79%  remaining: --help, --version, -v
cp         22/57   39%  remaining: --attributes-only, --backup, --context, --copy-contents, --dereference, --force, --help, --interactive, --link, --no-preserve, --one-file-system, --parents, --preserve=all, --preserve=links, --reflink=auto, --reflink=never, --remove-destination, --sparse, --sparse=always, --sparse=auto, --sparse=never, --strip-trailing-slashes, --suffix, --symbolic-link, --version, -H, -L, -S, -Z, -b, -d, -f, -i, -l, -x
dd          0/2     0%  remaining: --help, --version
du         12/44   27%  remaining: --apparent-size, --block-size, --bytes, --count-links, --dereference, --dereference-args, --exclude, --exclude-from, --files0-from, --help, --inodes, --no-dereference, --null, --one-file-system, --si, --threshold, --time, --time-style, --version, -0, -B, -D, -H, -L, -P, -X, -b, -k, -l, -m, -t, -x
fgrep      17/87   20%  remaining: --after-context, --basic-regexp, --before-context, --binary, --binary-files, --binary-files=text, --binary-files=without-match, --byte-offset, --color, --colour, --context, --dereference-recursive, --devices, --directories, --directories=recurse, --exclude, --exclude-dir, --exclude-from, --extended-regexp, --files-with-matches, --files-without-match, --group-separator, --help, --include, --initial-tab, --label, --line-buffered, --max-count, --no-filename, --no-group-separator, --no-messages, --null, --null-data, --only-matching, --perl-regexp, --quiet, --recursive, --silent, --text, --version, --with-filename, --word-regexp, -A, -B, -C, -D, -E, -G, -H, -I, -L, -N, -P, -R, -T, -U, -V, -Z, -a, -b, -d, -h, -l, -m, -o, -q, -r, -s, -w, -z
find        7/78    9%  remaining: --help, --version, -D, -H, -L, -N, -Olevel, -P, -a, -amin, -and, -anewer, -atime, -cmin, -cnewer, -context, -ctime, -daystart, -delete, -depth, -empty, -exec, -execdir, -executable, -false, -fls, -follow, -fprint, -fprint0, -fprintf, -fstype, -gid, -group, -ignore_readdir_race, -ilname, -inum, -iregex, -iwholename, -links, -lname, -ls, -mindepth, -mmin, -mount, -mtime, -newer, -nogroup, -noignore_readdir_race, -noleaf, -not, -nouser, -o, -ok, -okdir, -or, -perm, -printf, -prune, -quit, -readable, -regex, -regextype, -size, -true, -uid, -used, -user, -wholename, -writable, -xdev, -xtype
head       11/15   73%  remaining: --bytes=, --help, --lines=, --version
md5sum     15/17   88%  remaining: --help, --version
mv         11/23   48%  remaining: --backup, --context, --force, --help, --interactive, --strip-trailing-slashes, --suffix, --version, -S, -Z, -b, -i
rm         12/18   67%  remaining: --help, --no-preserve-root, --one-file-system, --preserve-root, --version, -foo
sha224sum  15/17   88%  remaining: --help, --version
sha256sum  15/17   88%  remaining: --help, --version
sha384sum  15/17   88%  remaining: --help, --version
sha512sum  15/17   88%  remaining: --help, --version
shred       9/17   53%  remaining: --exact, --help, --iterations, --random-source, --remove, --version, --zero, -x
sort       25/55   45%  remaining: --batch-size, --buffer-size, --check=diagnose-first, --check=quiet, --check=silent, --compress-program, --debug, --dictionary-order, --field-separator, --files0-from, --help, --ignore-case, --ignore-leading-blanks, --ignore-nonprinting, --key, --parallel, --random-sort, --random-source, --sort, --stable, --version, -C, -R, -S, -b, -d, -f, -i, -s, -t
tac         0/8     0%  remaining: --before, --help, --regex, --separator, --version, -b, -r, -s
tail        6/24   25%  remaining: --bytes, --bytes=, --follow, --follow=name, --help, --lines, --lines=, --max-unchanged-stats, --pid, --quiet, --retry, --silent, --sleep-interval, --verbose, --version, -F, -f, -s
tar        20/230   9%  remaining: --absolute-names, --acls, --add-file, --after-date, --after-date=DATE-OR-FILE, --anchored, --append, --atime-preserve, --auto-compress, --backup, --block-number, --blocking-factor, --bzip2, --catenate, --check-device, --check-links, --checkpoint, --checkpoint-action, --clamp-mtime, --compare, --compress, --concatenate, --confirmation, --delay-directory-restore, --delete, --dereference, --diff, --exclude, --exclude-backups, --exclude-caches, --exclude-caches-all, --exclude-caches-under, --exclude-from, --exclude-ignore, --exclude-ignore-recursive, --exclude-tag, --exclude-tag-all, --exclude-tag-under, --exclude-vcs, --exclude-vcs-ignores, --files-from, --force-local, --format, --format=gnu, --format=posix, --format=v7, --full-time, --get, --group, --group-map, --hard-dereference, --help, --hole-detection, --ignore-case, --ignore-command-error, --ignore-failed-read, --ignore-zeros, --incremental, --index-file, --info-script, --interactive, --keep-directory-symlink, --keep-newer-files, --keep-old-files, --label, --level, --listed-incremental, --lzip, --lzma, --lzop, --mode, --mtime, --mtime=DATE-OR-FILE, --multi-volume, --new-volume-script, --newer, --newer-mtime, --newer=DATE-OR-FILE, --no-acls, --no-anchored, --no-auto-compress, --no-check-device, --no-delay-directory-restore, --no-ignore-case, --no-ignore-command-error, --no-null, --no-overwrite-dir, --no-quote-chars, --no-recursion, --no-same-owner, --no-same-permissions, --no-seek, --no-selinux, --no-unquote, --no-verbatim-files-from, --no-wildcards, --no-wildcards-match-slash, --no-xattrs, --null, --numeric-owner, --occurrence, --old-archive, --one-file-system, --one-top-level, --overwrite, --overwrite-dir, --owner, --owner-map, --pax-option, --pax-option=keyword, --portability, --posix, --preserve-order, --preserve-permissions, --quote-chars, --quoting-style, --quoting-style=escape, --read-full-records, --record-size, --recursion, --recursive-unlink, --remove-files, --restrict, --rmt-command, --rmt-command=, --rsh-command, --rsh-command=, --same-order, --same-owner, --same-permissions, --seek, --selinux, --show-defaults, --show-omitted-dirs, --show-snapshot-field-ranges, --show-stored-names, --show-transformed-names, --skip-old-files, --sort, --sparse, --sparse-version, --starting-file, --starting-file=MEMBER-NAME, --strip-components, --suffix, --tape-length, --test-label, --to-command, --to-stdout, --totals, --touch, --transform, --uncompress, --unlink-first, --unquote, --update, --usage, --use-compress-program, --utc, --verbatim-files-from, --verify, --version, --volno-file, --warning, --wildcards, --wildcards-match-slash, --xattrs, --xattrs-exclude, --xattrs-include, --xform, --xz, -A, -B, -F, -G, -H, -I, -K, -L, -M, -N, -O, -P, -R, -S, -T, -U, -V, -W, -X, -Z, -a, -b, -d, -f-, -g, -h, -i, -j, -k, -l, -m, -n, -o, -p, -r, -s, -u, -w, -xf
wc         11/13   85%  remaining: --help, --version
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
    let cp_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "cp");
    assert!(cp_help.contains("FIXME:"));
    assert!(cp_help.contains("ownership is not preserved"));

    let sort_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "sort");
    assert!(sort_help.contains("FIXME:"));
    assert!(sort_help.contains("key selection (-k)"));

    let tar_help = fro_help_text(env!("CARGO_BIN_EXE_fro"), "tar");
    assert!(tar_help.contains("FIXME:"));
    assert!(tar_help.contains("whole-archive"));
}

#[test]
fn compat_coverage_explicit_gap_rows_match_source_help() {
    let help_source = include_str!("../src/main_app/help.rs");
    assert!(help_source.contains("FIXME: Unsupported GNU sort features currently return an error"));
    assert!(help_source.contains("key selection (-k) and locale collation"));
    assert!(help_source.contains("FIXME: Extract currently targets regular-file archives"));
    assert!(help_source.contains("FIXME: When invoked via the cp multicall alias"));

    let find_source = include_str!("../src/coreutils/find.rs");
    assert!(find_source
        .contains("-print             print each matching path followed by a newline (default)"));
}
