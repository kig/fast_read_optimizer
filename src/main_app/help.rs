mod commands;
mod hash;

use super::*;
use crate::help_compat::help_section_lines;
use std::borrow::Cow;
#[derive(Clone, Copy)]
pub(super) struct CommandHelp {
    name: &'static str,
    usage: &'static str,
    summary: &'static str,
    notes: &'static [&'static str],
    examples: &'static [(&'static str, &'static str)],
}
pub(super) fn is_help_flag(arg: &str) -> bool {
    arg == "--help" || arg == "-h"
}
pub(super) fn is_version_flag(arg: &str) -> bool {
    arg == "--version"
}
pub(super) fn print_version(program: &str) {
    fro::cio_println!("{program} {FRO_VERSION}");
}
pub(super) fn command_help(name: &str) -> Option<CommandHelp> {
    if let Some(help) = hash::command_help(name) {
        return Some(help);
    }
    commands::command_help(name)
}

pub(super) fn print_command_help(program: &str, help: CommandHelp) {
    fro::cio_println!("{} - {}", help.name, help.summary);
    fro::cio_println!();
    fro::cio_println!("USAGE:");
    fro::cio_println!("  {} {}", program, help.usage);
    if !help.notes.is_empty() {
        fro::cio_println!();
        fro::cio_println!("NOTES:");
        for note in help.notes {
            fro::cio_println!("  - {}", note);
        }
    }
    if let Some(lines) = help_section_lines(help.name) {
        fro::cio_println!();
        fro::cio_println!("COMPAT:");
        for line in lines {
            fro::cio_println!("  - {}", line);
        }
    }
    if !help.examples.is_empty() {
        fro::cio_println!();
        fro::cio_println!("EXAMPLES:");
        for (description, command) in help.examples {
            fro::cio_println!("  {}", description);
            fro::cio_println!("    {} {}", program, command);
        }
    }
}

fn rewrite_help_command<'a>(text: &'a str, from: &str, to: &str) -> Cow<'a, str> {
    if text == from {
        return Cow::Owned(to.to_string());
    }
    if let Some(rest) = text.strip_prefix(from).filter(|rest| rest.starts_with(' ')) {
        return Cow::Owned(format!("{to}{rest}"));
    }
    Cow::Borrowed(text)
}

pub(crate) fn print_direct_command_help(command_name: &str, help_name: &str) -> bool {
    let Some(help) = command_help(help_name) else {
        return false;
    };
    let display_name = rewrite_help_command(help.name, help.name, command_name);
    fro::cio_println!("{display_name} - {}", help.summary);
    fro::cio_println!();
    fro::cio_println!("USAGE:");
    fro::cio_println!(
        "  {}",
        rewrite_help_command(help.usage, help.name, command_name)
    );
    if !help.notes.is_empty() {
        fro::cio_println!();
        fro::cio_println!("NOTES:");
        for note in help.notes {
            fro::cio_println!("  - {}", note);
        }
    }
    if let Some(lines) = help_section_lines(help_name) {
        fro::cio_println!();
        fro::cio_println!("COMPAT:");
        for line in lines {
            fro::cio_println!("  - {}", line);
        }
    }
    if !help.examples.is_empty() {
        fro::cio_println!();
        fro::cio_println!("EXAMPLES:");
        for (description, command) in help.examples {
            fro::cio_println!("  {}", description);
            fro::cio_println!(
                "    {}",
                rewrite_help_command(command, help.name, command_name)
            );
        }
    }
    true
}

pub(super) fn print_general_help(program: &str) {
    fro::cio_println!("fast_read_optimizer (fro)");
    fro::cio_println!(
        "High-throughput Linux file IO utilities with companion benchmark and optimizer tooling."
    );
    fro::cio_println!();
    fro::cio_println!("USAGE:");
    fro::cio_println!("  {} <command> [options]", program);
    fro::cio_println!("  {} <command> --help", program);
    fro::cio_println!();
    fro::cio_println!("Utilities:");
    for (name, summary) in [
        ("cat", "print files using the fro read path"),
        ("base64", "encode or decode base64 data"),
        (
            "gzip",
            "experimental mgzip plus optional rapidgzip gzip slice",
        ),
        (
            "encrypt",
            "encrypt data via the in-process OpenSSL library path",
        ),
        (
            "decrypt",
            "decrypt data via the in-process OpenSSL library path",
        ),
        ("cmp", "compare two files using the fro diff engine"),
        ("dd", "copy byte ranges with dd-style operands"),
        ("fgrep", "literal line-oriented grep compatibility wrapper"),
        ("find", "walk directory trees and print every path"),
        ("du", "report disk usage from filesystem block counts"),
        ("grep", "search for a literal byte substring while reading"),
        (
            "config",
            "print config JSON or explain config selection for a path",
        ),
        ("head", "print the first lines or bytes of each input"),
        (
            "sort",
            "sort newline-delimited records in ascending byte order",
        ),
        ("tac", "print files in reverse line order"),
        ("tail", "print the last lines or bytes of each input"),
        (
            "wc",
            "count lines, words, characters, bytes, and max line length",
        ),
        ("cksum", "POSIX cksum compatibility wrapper"),
        ("b3sum", "print BLAKE3 digests"),
        ("b2sum", "print BLAKE2b-512 digests"),
        ("md5sum", "print MD5 digests"),
        ("sha224sum", "print SHA-224 digests"),
        ("sha256sum", "print SHA-256 digests"),
        ("sha384sum", "print SHA-384 digests"),
        ("sha512sum", "print SHA-512 digests"),
        ("shred", "overwrite files with patterns"),
        (
            "write",
            "rewrite or create a file through the tuned write path",
        ),
        (
            "copy",
            "copy one file to another with tuned read/write settings",
        ),
        ("diff", "compare two files and report the first mismatch"),
        (
            "recursive-read-bench",
            "read every byte of every file in a tree",
        ),
        (
            "file-list-read-bench",
            "read every file named in a manifest",
        ),
        (
            "file-list-read-uring-bench",
            "sweep many-small-file io_uring manifest reads",
        ),
        (
            "file-list-read-open-read-close-sweep",
            "compare manifest open-read-close reader variants",
        ),
        (
            "manifest-recursive-copy-bench",
            "benchmark manifest-driven recursive copy phases",
        ),
        (
            "split-manifest-recursive-copy-bench",
            "benchmark split manifest-build recursive copy",
        ),
        (
            "bench-recursive-small-file-threads",
            "sweep recursive small-file worker counts",
        ),
        ("hash", "write 1 MiB block-hash sidecars"),
        ("verify", "scrub a file against its block-hash sidecars"),
        (
            "recover",
            "repair corrupted blocks from one or more replicas",
        ),
    ] {
        fro::cio_println!("  {:<16} {}", name, summary);
    }
    fro::cio_println!();
    fro::cio_println!("Benchmarks:");
    fro::cio_println!("  read               measure striped file read throughput");
    fro::cio_println!("  dual-read-bench    benchmark the read pressure of diff");
    fro::cio_println!("  recursive-read-bench benchmark aggregate read throughput of a tree");
    fro::cio_println!(
        "  file-list-read-bench benchmark aggregate read throughput from a file manifest"
    );
    fro::cio_println!(
        "  file-list-read-uring-bench sweep io_uring aggregate throughput from a file manifest"
    );
    fro::cio_println!("  file-list-read-open-read-close-sweep compare manifest reader variants across file-count prefixes");
    fro::cio_println!(
        "  manifest-recursive-copy-bench benchmark manifest-driven recursive copy phase timing"
    );
    fro::cio_println!("  split-manifest-recursive-copy-bench benchmark split manifest-build recursive copy timing");
    fro::cio_println!("  bench-recursive-small-file-threads sweep recursive small-file worker counts and save hot/cold per mount");
    fro::cio_println!("  bench-read-sweep  sweep read variants across file sizes");
    fro::cio_println!("  fro-optimize       tune configs for one or more commands / mounts");
    fro::cio_println!("  fro-benchmark      run the regression benchmark suite");
    fro::cio_println!("  bench-diff         in-memory diff microbenchmark");
    fro::cio_println!("  bench-memcpy       in-memory memcpy microbenchmark");
    fro::cio_println!(
        "  bench-tar-archive  benchmark tar assembly into RAM / RAM+write / mmap file"
    );
    fro::cio_println!("  bench-base64-encode base64 encode kernel microbenchmark");
    fro::cio_println!("  bench-base64-decode base64 decode kernel microbenchmark");
    fro::cio_println!("  bench-base64-wrapped-encode wrapped base64 encode path microbenchmark");
    fro::cio_println!("  bench-base64-wrapped-decode wrapped base64 decode path microbenchmark");
    fro::cio_println!("  bench-mmap-write   mmap write microbenchmark");
    fro::cio_println!("  bench-write        plain write microbenchmark");
    fro::cio_println!();
    fro::cio_println!("Common flags:");
    fro::cio_println!("  --auto | --no-direct | --direct");
    fro::cio_println!("  --auto-write | --no-direct-write | --direct-write");
    fro::cio_println!(
        "  --no-fallback     disable external fallback for unsupported multicall flags"
    );
    fro::cio_println!(
        "  -n <iterations>    use -n 1 for one measured run with current tuned params"
    );
    fro::cio_println!(
        "  -s, --save         save tuned params when forcing --direct or --no-direct"
    );
    fro::cio_println!("  -c, --config PATH  override config path");
    fro::cio_println!("  -v, --verbose      print more about the current run");
    fro::cio_println!();
    fro::cio_println!("Coreutils compatibility names:");
    fro::cio_println!(
        "  cp cmp dd fgrep find du rm mv sort tar cat base64 encrypt decrypt head tac tail wc cksum b3sum b2sum md5sum sha224sum sha256sum sha384sum sha512sum shred"
    );
    fro::cio_println!("  (use as `fro <name> ...` or invoke via argv[0] multicall)");
    fro::cio_println!();
    fro::cio_println!("Related tools:");
    fro::cio_println!("  ./target/release/fro-optimize --help");
    fro::cio_println!("  ./target/release/fro-benchmark --help");
    fro::cio_println!();
    fro::cio_println!(
        "Config resolution (when -c is not provided): $FRO_CONFIG, then ~/.fro/fro.json, then /etc/fro.json"
    );
    fro::cio_println!(
        "Fallback logging: set $FRO_LOG_FALLBACKS=1 to log each external delegation on stderr"
    );
}
