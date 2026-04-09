#![allow(dead_code)]

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoverageRow {
    pub name: &'static str,
    pub covered: &'static [&'static str],
    pub remaining: &'static [&'static str],
}

impl CoverageRow {
    pub fn total(self) -> usize {
        self.covered.len() + self.remaining.len()
    }

    pub fn percent(self) -> usize {
        let total = self.total();
        if total == 0 {
            100
        } else {
            (self.covered.len() * 100 + total / 2) / total
        }
    }

    pub fn remaining_text(self) -> String {
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

pub const ROWS: &[CoverageRow] = &[
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
        covered: &[
            "(default)",
            "-c/--check",
            "--quiet",
            "--status",
            "-w/--warn",
            "--strict",
            "--ignore-missing",
        ],
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
            "--preserve=timestamps",
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
            "status=progress",
        ],
        remaining: &[],
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
            "-i",
            "-I",
            "--interactive[=WHEN]",
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
            "-m/-c",
            "-n/--numeric-sort",
            "-r/--reverse",
            "-u/--unique",
            "-z/--zero-terminated",
            "-o/--output",
            "-T/--temporary-directory",
        ],
        remaining: &["-g/-h", "-M", "-V", "-k"],
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
        covered: &[
            "-c/--create",
            "-f/--file",
            "-t/--list",
            "-v/--verbose",
            "-x/--extract",
        ],
        remaining: &[],
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

pub const EXCLUDED_CUSTOM_MULTICALLS: &[&str] = &["b3sum", "decrypt", "encrypt", "pv"];

pub fn row_for(name: &str) -> Option<&'static CoverageRow> {
    let canonical = match name {
        "copy" | "copy-via-memory" => "cp",
        other => other,
    };
    ROWS.iter().find(|row| row.name == canonical)
}

pub fn help_section_lines(name: &str) -> Option<Vec<String>> {
    let row = row_for(name)?;
    let mut lines = vec![format!(
        "Tracked GNU/coreutils flags for this slice: {}",
        row.covered.join(", ")
    )];
    if !row.remaining.is_empty() {
        lines.push(format!(
            "FIXME: tracked GNU/coreutils flags not yet supported in this slice: {}",
            row.remaining.join(", ")
        ));
    }
    Some(lines)
}

pub fn tracked_help_tokens(row: CoverageRow) -> BTreeSet<String> {
    row.covered
        .iter()
        .chain(row.remaining.iter())
        .flat_map(|entry| parse_help_flag_tokens(entry))
        .collect()
}

pub fn parse_help_flag_tokens(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for line in text.lines() {
        if let Some(token) = leading_bare_value_token(line) {
            tokens.insert(token.to_string());
        }
        let chars = line.char_indices().collect::<Vec<_>>();
        let mut index = 0usize;
        while index < chars.len() {
            let (byte_index, ch) = chars[index];
            if index > 0 && !is_token_boundary(chars[index - 1].1) {
                index += 1;
                continue;
            }
            if ch == '-' || ch.is_ascii_alphabetic() {
                let mut end = byte_index + ch.len_utf8();
                let mut next = index + 1;
                while next < chars.len() && is_token_char(chars[next].1) {
                    end = chars[next].0 + chars[next].1.len_utf8();
                    next += 1;
                }
                if let Some(token) = normalize_help_token(&line[byte_index..end]) {
                    tokens.extend(token);
                }
                index = next;
                continue;
            }
            index += 1;
        }
    }
    tokens
}

fn leading_bare_value_token(line: &str) -> Option<&str> {
    if !line.starts_with([' ', '\t']) {
        return None;
    }
    let trimmed = line.trim_start();
    let token = trimmed.split_whitespace().next()?;
    if token.starts_with('-')
        || token.contains('=')
        || !token
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
    {
        return None;
    }
    Some(token)
}

fn is_token_boundary(ch: char) -> bool {
    !is_token_char(ch)
}

fn is_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=')
}

fn normalize_help_token(token: &str) -> Option<Vec<String>> {
    if token == "-" || token == "--" {
        return None;
    }
    if token.starts_with('-') {
        return normalize_dash_token(token);
    }
    if token.contains('=') {
        return normalize_bare_assignment(token);
    }
    None
}

fn normalize_dash_token(token: &str) -> Option<Vec<String>> {
    if let Some(eq_pos) = token.find('=') {
        let base = &token[..eq_pos];
        let rhs = &token[(eq_pos + 1)..];
        let mut tokens = vec![base.to_string()];
        if !looks_like_metavar(rhs) {
            tokens.push(token.to_string());
        }
        return Some(tokens);
    }
    if token.starts_with('-') && token.len() > 2 && !token.starts_with("--") {
        let suffix = &token[2..];
        if looks_like_metavar(suffix) {
            return Some(vec![token[..2].to_string()]);
        }
    }
    Some(vec![token.to_string()])
}

fn normalize_bare_assignment(token: &str) -> Option<Vec<String>> {
    let eq_pos = token.find('=')?;
    let left = &token[..eq_pos];
    let rhs = &token[(eq_pos + 1)..];
    if rhs.is_empty() || looks_like_metavar(rhs) {
        return Some(vec![format!("{left}=")]);
    }
    Some(vec![token.to_string(), rhs.to_string()])
}

fn looks_like_metavar(rhs: &str) -> bool {
    !rhs.is_empty()
        && rhs.chars().all(|ch| {
            ch.is_ascii_uppercase()
                || ch.is_ascii_digit()
                || matches!(ch, '_' | '[' | ']' | '<' | '>')
        })
}
