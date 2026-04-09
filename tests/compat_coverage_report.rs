#![cfg(unix)]

use std::collections::BTreeSet;

#[derive(Clone, Copy)]
struct CoverageRow {
    name: &'static str,
    covered: &'static [&'static str],
    remaining: &'static [&'static str],
}

impl CoverageRow {
    fn total(self) -> usize {
        self.covered.len() + self.remaining.len()
    }

    fn percent(self) -> usize {
        let total = self.total();
        if total == 0 {
            100
        } else {
            (self.covered.len() * 100 + total / 2) / total
        }
    }

    fn remaining_text(self) -> String {
        if self.remaining.is_empty() {
            "none".to_string()
        } else {
            self.remaining.join(", ")
        }
    }
}

const DIGEST_COVERED: &[&str] = &[
    "(default)",
    "-b/--binary",
    "-t/--text",
    "--tag",
    "-z/--zero",
    "-c/--check",
    "--quiet",
    "--status",
    "-w/--warn",
    "--strict",
    "--ignore-missing",
];

const ROWS: &[CoverageRow] = &[
    CoverageRow {
        name: "base64",
        covered: &["-d/--decode", "-i/--ignore-garbage", "-w/--wrap"],
        remaining: &[],
    },
    CoverageRow {
        name: "b2sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "cat",
        covered: &[
            "-n/--number",
            "-b/--number-nonblank",
            "-s/--squeeze-blank",
            "-E/--show-ends",
            "-T/--show-tabs",
            "-v/--show-nonprinting",
            "-A/--show-all",
            "-e",
            "-t",
            "-u",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "cksum",
        covered: &["(default)"],
        remaining: &[],
    },
    CoverageRow {
        name: "cmp",
        covered: &[
            "-s/--quiet/--silent",
            "-l/--verbose",
            "-b/--print-bytes",
            "-n/--bytes",
            "-i/--ignore-initial",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "cp",
        covered: &[
            "-a/--archive",
            "-r/-R",
            "-n/--no-clobber",
            "-u/--update",
            "-T/--no-target-directory",
            "-t/--target-directory",
            "-p/--preserve",
            "-P/--no-dereference",
            "-v/--verbose",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "dd",
        covered: &[
            "bs=",
            "count=",
            "skip=",
            "seek=",
            "iflag=count_bytes",
            "iflag=skip_bytes",
            "oflag=seek_bytes",
            "conv=notrunc",
            "conv=fsync",
            "iflag=direct",
            "oflag=direct",
            "status=none",
            "status=noxfer",
        ],
        remaining: &["status=progress"],
    },
    CoverageRow {
        name: "du",
        covered: &[
            "-s/--summarize",
            "-a/--all",
            "-h/--human-readable",
            "-c/--total",
            "-S/--separate-dirs",
            "-d/--max-depth",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "fgrep",
        covered: &[
            "-F/--fixed-strings",
            "-n/--line-number",
            "-x/--line-regexp",
            "-i/--ignore-case",
            "--no-ignore-case",
            "-c/--count",
            "-v/--invert-match",
            "-e/--regexp",
            "-f/--file",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "find",
        covered: &[
            "(default)",
            "-maxdepth",
            "-type",
            "-name",
            "-iname",
            "-path",
            "-ipath",
            "-print",
            "-print0",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "head",
        covered: &[
            "-n/--lines",
            "-c/--bytes",
            "-q/--quiet/--silent",
            "-v/--verbose",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "md5sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "mv",
        covered: &[
            "-v/--verbose",
            "-t/--target-directory",
            "-T/--no-target-directory",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "rm",
        covered: &[
            "-d/--dir",
            "-f/--force",
            "-r/-R/--recursive",
            "-v/--verbose",
        ],
        remaining: &[],
    },
    CoverageRow {
        name: "sha224sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "sha256sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "sha384sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "sha512sum",
        covered: DIGEST_COVERED,
        remaining: &[],
    },
    CoverageRow {
        name: "shred",
        covered: &["-n", "-s/--size", "-z", "-u", "-f/--force", "-v/--verbose"],
        remaining: &[],
    },
    CoverageRow {
        name: "sort",
        covered: &[
            "(default bytewise ascending)",
            "-r/--reverse",
            "-u/--unique",
        ],
        remaining: &["-n/-g/-h", "-M", "-V", "-k", "-m/-c", "-z", "-o/-T"],
    },
    CoverageRow {
        name: "tac",
        covered: &["(default)", "--"],
        remaining: &[],
    },
    CoverageRow {
        name: "tail",
        covered: &["-n", "-c", "-q", "-v"],
        remaining: &[],
    },
    CoverageRow {
        name: "tar",
        covered: &["-c/--create", "-f/--file", "-t/--list"],
        remaining: &["-v/--verbose", "-x/--extract"],
    },
    CoverageRow {
        name: "wc",
        covered: &[
            "-l/--lines",
            "-w/--words",
            "-m/--chars",
            "-c/--bytes",
            "-L/--max-line-length",
            "--files0-from",
        ],
        remaining: &[],
    },
];

const EXCLUDED_CUSTOM_MULTICALLS: &[&str] = &["b3sum", "decrypt", "encrypt", "pv"];

fn render_report() -> String {
    let mut out = String::from(
        "# multicall compat coverage\n# tracked surface = canonical compat items from existing help/parser/tests\n# excluded custom multicalls: b3sum, decrypt, encrypt, pv\n",
    );
    for row in ROWS {
        out.push_str(&format!(
            "{:<9} {:>2}/{:<2} {:>3}%  remaining: {}\n",
            row.name,
            row.covered.len(),
            row.total(),
            row.percent(),
            row.remaining_text()
        ));
    }
    out
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
base64     3/3  100%  remaining: none
b2sum     11/11 100%  remaining: none
cat       10/10 100%  remaining: none
cksum      1/1  100%  remaining: none
cmp        5/5  100%  remaining: none
cp         9/9  100%  remaining: none
dd        13/14  93%  remaining: status=progress
du         6/6  100%  remaining: none
fgrep      9/9  100%  remaining: none
find       9/9  100%  remaining: none
head       4/4  100%  remaining: none
md5sum    11/11 100%  remaining: none
mv         3/3  100%  remaining: none
rm         4/4  100%  remaining: none
sha224sum 11/11 100%  remaining: none
sha256sum 11/11 100%  remaining: none
sha384sum 11/11 100%  remaining: none
sha512sum 11/11 100%  remaining: none
shred      6/6  100%  remaining: none
sort       3/10  30%  remaining: -n/-g/-h, -M, -V, -k, -m/-c, -z, -o/-T
tac        2/2  100%  remaining: none
tail       4/4  100%  remaining: none
tar        3/5   60%  remaining: -v/--verbose, -x/--extract
wc         6/6  100%  remaining: none
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
fn compat_coverage_explicit_gap_rows_match_source_help() {
    let sort_source = include_str!("../src/coreutils/sort.rs");
    assert!(sort_source.contains("Unsupported GNU sort features currently return an error"));
    assert!(sort_source.contains("numeric/month/version modes, keys, merge/check modes,"));
    assert!(sort_source
        .contains("zero-terminated records, output/temp-file controls, and locale collation."));

    let tar_source = include_str!("../src/coreutils/tar.rs");
    assert!(tar_source
        .contains("supports only create (-c/--create) and whole-archive list (-t/--list) modes"));

    let find_source = include_str!("../src/coreutils/find.rs");
    assert!(find_source
        .contains("-print             print each matching path followed by a newline (default)"));
}
