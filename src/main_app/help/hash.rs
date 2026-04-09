use super::CommandHelp;

const HASH_REPORT_GBPS_NOTES: &[&str] =
    &["--report-gbps writes effective hashed-input throughput to stderr."];

fn digest_command_help(
    name: &'static str,
    usage: &'static str,
    summary: &'static str,
    examples: &'static [(&'static str, &'static str)],
) -> CommandHelp {
    CommandHelp {
        name,
        usage,
        summary,
        notes: HASH_REPORT_GBPS_NOTES,
        examples,
    }
}

pub(super) fn command_help(name: &str) -> Option<CommandHelp> {
    match name {
        "cksum" => Some(CommandHelp {
            name: "cksum",
            usage: "cksum [--auto|--no-direct|--direct] [--report-gbps] [--check] [--quiet|--status|-w|--warn|--strict|--ignore-missing] [--] <file> [file ...]",
            summary: "POSIX cksum compatibility wrapper on top of fro file reads.",
            notes: &[
                "Uses a table-driven POSIX CRC32 path while reusing the existing ordered input reader.",
                "-c/--check verifies fro-style manifest lines in the emitted '<crc> <bytes> <path>' format.",
                "--quiet, --status, --warn, --strict, and --ignore-missing share the same verification behavior as the digest-family check path.",
                "--report-gbps writes effective hashed-input throughput to stderr.",
            ],
            examples: &[
                ("Print POSIX CRC32 and size", "cksum archive.tar"),
                ("Verify a fro-style manifest", "cksum --check manifest.cksum"),
            ],
        }),
        "b3sum" => Some(digest_command_help(
            "b3sum",
            "b3sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print BLAKE3 digests for one or more files.",
            &[("Hash one file with BLAKE3", "b3sum bigfile.dat")],
        )),
        "b2sum" => Some(digest_command_help(
            "b2sum",
            "b2sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print BLAKE2b-512 digests for one or more files.",
            &[("Hash one file with BLAKE2b-512", "b2sum bigfile.dat")],
        )),
        "md5sum" => Some(digest_command_help(
            "md5sum",
            "md5sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print MD5 digests for one or more files.",
            &[("Hash one file with MD5", "md5sum bigfile.dat")],
        )),
        "sha224sum" => Some(digest_command_help(
            "sha224sum",
            "sha224sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print SHA-224 digests for one or more files.",
            &[("Hash one file with SHA-224", "sha224sum bigfile.dat")],
        )),
        "sha256sum" => Some(digest_command_help(
            "sha256sum",
            "sha256sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print SHA-256 digests for one or more files.",
            &[("Hash one file with SHA-256", "sha256sum bigfile.dat")],
        )),
        "sha384sum" => Some(digest_command_help(
            "sha384sum",
            "sha384sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print SHA-384 digests for one or more files.",
            &[("Hash one file with SHA-384", "sha384sum bigfile.dat")],
        )),
        "sha512sum" => Some(digest_command_help(
            "sha512sum",
            "sha512sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            "Print SHA-512 digests for one or more files.",
            &[("Hash one file with SHA-512", "sha512sum bigfile.dat")],
        )),
        _ => None,
    }
}
