use super::*;

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
    println!("{program} {FRO_VERSION}");
}

pub(super) fn command_help(name: &str) -> Option<CommandHelp> {
    match name {
        "read" => Some(CommandHelp {
            name: "read",
            usage: "read [--auto-lift] [--to-memory] [--paged-shared-buffer|--mmap|--mmap-read-pages|--multiple-target-buffers] [--threads N] [--qd N] [--blocksize SIZE] [--disable-hugepages] [--measure-unmap-time] [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Striped multi-threaded file read for measuring raw throughput on one file.",
            notes: &[
                "Use -n 1 for one measured run with the current tuned parameters.",
                "Use -s together with --direct or --no-direct to save the best result back to config.",
                "With --direct, fro prints one stderr warning if any requested direct read falls back to page cache because O_DIRECT open failed or a tail/range was unaligned.",
                "--auto-lift starts cold files on the direct path while a background thread warms the page cache for later iterations in the same process.",
                "--to-memory defaults to an auto backend: mmap when the first page looks cached, otherwise the direct/shared-buffer loader.",
                "--paged-shared-buffer forces the old shared destination-buffer loader for read --to-memory.",
                "--mmap maps the file instead of reading into a destination buffer; --mmap-read-pages also walks the mapped bytes in userspace.",
                "--multiple-target-buffers gives each reader thread its own destination buffer with no consolidation step.",
                "--to-memory uses hugepage advice automatically for files >= 64 MiB; smaller files default to non-hugepages. --disable-hugepages turns advice off entirely for the mapped or destination buffer backing.",
                "--measure-unmap-time keeps mmap teardown inside the timed region for --mmap and --mmap-read-pages.",
                "--threads, --qd, and --blocksize override the read-side tuned params so you can do one-off perf sweeps without editing fro.json.",
            ],
            examples: &[
                (
                    "Measure direct-IO read throughput once",
                    "read --direct -n 1 /mnt/fast/bigfile.dat",
                ),
                (
                    "Let read --to-memory auto-pick mmap vs direct based on cache state",
                    "read --to-memory -n 1 /mnt/fast/bigfile.dat",
                ),
                (
                    "Start cold reads on direct IO, then flip later iterations onto page cache",
                    "read --auto-lift -n 100 /mnt/fast/bigfile.dat",
                ),
                (
                    "Map a hot file and read all mapped bytes",
                    "read --to-memory --mmap-read-pages --no-direct -n 1 /mnt/fast/bigfile.dat",
                ),
            ],
        }),
        "grep" => Some(CommandHelp {
            name: "grep",
            usage: "grep [--auto-lift] [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <pattern> <filename>",
            summary: "Read plus literal byte-substring search over one file.",
            notes: &[
                "This is a literal substring search, not a regex engine.",
                "Matches are printed as offset:pattern.",
                "With --direct, fro prints one stderr warning if any requested direct scan falls back to page cache because O_DIRECT open failed or a tail request was unaligned.",
                "--auto-lift starts cold files on the direct path while a background thread warms the page cache for later iterations in the same process.",
            ],
            examples: &[
                (
                    "Scan a file in page cache for a literal marker string",
                    "grep --no-direct -n 1 needle /mnt/fast/bigfile.dat",
                ),
                (
                    "Start cold and improve over repeated scans of the same file",
                    "grep --auto-lift -n 100 needle /mnt/fast/bigfile.dat",
                ),
            ],
        }),
        "config" => Some(CommandHelp {
            name: "config",
            usage: "config <print|explain> [--for <path>] [-c config.json]",
            summary: "Print raw config or explain the effective config chosen for one path.",
            notes: &[
                "config print emits the currently loaded config file as JSON.",
                "config explain shows defaults, matching mount info, extracted device signature details, any matched device-db profile, any mount override, and the effective config after applying device-db params and then mount overrides.",
                "The --for path is resolved the same way runtime path-based config selection resolves mount and device context for I/O decisions.",
                "fro-optimize --for <path> uses the same mount selection rule when deciding which mount_overrides entry to write.",
            ],
            examples: &[
                ("Print the active config file", "config print"),
                (
                    "Explain config selection for a file on a specific mount",
                    "config explain --for /mnt/nvme/data.bin",
                ),
                (
                    "Explain config selection using an explicit config file",
                    "config explain -c fro.json --for ./target/test.bin",
                ),
            ],
        }),
        "cat" => Some(CommandHelp {
            name: "cat",
            usage: "cat [--auto|--no-direct|--direct] [--report-gbps] <file> [file ...]",
            summary: "Print one or more files using fro's fast read path.",
            notes: &[
                "Useful as a compatibility wrapper over the same IO-mode flags as fro reads.",
                "--report-gbps writes effective input throughput to stderr after the payload finishes.",
            ],
            examples: &[("Print two files", "cat a.txt b.txt")],
        }),
        "rm" => Some(CommandHelp {
            name: "rm",
            usage: "rm [-f] [-d] [-r|-R|--recursive] [-v] <file> [file ...]",
            summary: "Remove files or directory trees, with recursive delete using fro's tree-walk plumbing.",
            notes: &[
                "-d/--dir removes empty directories without switching to the recursive tree-walk path.",
                "-f/--force ignores missing operands and missing files, matching the common cleanup flow.",
                "Recursive removal uses the existing tree-delete helper; plain file removal stays on the simple unlink path.",
            ],
            examples: &[
                ("Ignore cache-cleanup misses", "rm -f build/output.bin"),
                ("Remove a directory tree verbosely", "rm -rv scratch-tree"),
            ],
        }),
        "mv" => Some(CommandHelp {
            name: "mv",
            usage: "mv [-f] [-v] [-T] [-t DIRECTORY] <source>... <target>",
            summary: "Rename files or move them into a directory, with cross-filesystem fallback via fro copy helpers.",
            notes: &[
                "Same-filesystem moves use rename(2) when possible.",
                "Cross-filesystem file and directory moves fall back to the fro copy/remove path.",
                "-T/--no-target-directory treats the destination as a path, matching GNU mv.",
            ],
            examples: &[
                ("Rename one file", "mv old.bin new.bin"),
                ("Move multiple files into a directory", "mv a.bin b.bin archive/"),
                ("Move files into a specific directory", "mv -t archive/ a.bin b.bin"),
            ],
        }),
        "base64" => Some(CommandHelp {
            name: "base64",
            usage: "base64 [-d|--decode] [-i|--ignore-garbage] [-w cols|--wrap=cols] [--auto|--no-direct|--direct] [--report-gbps] [file]",
            summary: "Encode or decode one file or stdin using RFC 4648 base64.",
            notes: &[
                "Without a file operand, or when the file is -, base64 reads standard input.",
                "Encoding wraps at 76 columns by default; use -w 0 to disable wrapping.",
                "--ignore-garbage only affects decode mode.",
                "--report-gbps writes effective input throughput to stderr after processing.",
            ],
            examples: &[
                ("Encode stdin without wrapping", "base64 -w 0 < input.bin"),
                ("Decode one file", "base64 -d payload.b64"),
            ],
        }),
        "encrypt" => Some(CommandHelp {
            name: "encrypt",
            usage: "encrypt --passphrase-file PATH [--cipher NAME] [-o file] [input]",
            summary: "Encrypt data into OpenSSL-compatible aes-256-ctr output via the OpenSSL library.",
            notes: &[
                "Regular-file inputs are processed in parallel 512 KiB blocks.",
                "Only aes-256-ctr is supported, derived with PBKDF2-HMAC-SHA256 and the standard `Salted__` header.",
                "No trailing b3sum is appended because extra bytes would break `openssl enc` compatibility.",
                "aes-256-ctr is unauthenticated, so wrong-passphrase decrypts may return garbage rather than a hard error.",
                "Use -o/--output to write to a file; otherwise ciphertext is written to stdout.",
            ],
            examples: &[
                (
                    "Encrypt one file into a sibling output file",
                    "encrypt --passphrase-file secret.txt -o payload.enc payload.bin",
                ),
                (
                    "Encrypt stdin to stdout with the supported cipher",
                    "encrypt --passphrase-file secret.txt --cipher aes-256-ctr < payload.bin > payload.enc",
                ),
            ],
        }),
        "decrypt" => Some(CommandHelp {
            name: "decrypt",
            usage: "decrypt --passphrase-file PATH [--cipher NAME] [-o file] [input]",
            summary: "Decrypt OpenSSL-compatible aes-256-ctr ciphertext in-process.",
            notes: &[
                "Regular-file inputs are processed in parallel 512 KiB blocks.",
                "Only aes-256-ctr with the OpenSSL `Salted__` header and PBKDF2-HMAC-SHA256 derivation is supported.",
                "Ciphertext must match the bytes produced by `fro encrypt` or `openssl enc -aes-256-ctr -pbkdf2 -salt`.",
            ],
            examples: &[
                (
                    "Decrypt a file to stdout",
                    "decrypt --passphrase-file secret.txt payload.enc > payload.bin",
                ),
                (
                    "Decrypt into a named output file",
                    "decrypt --passphrase-file secret.txt -o payload.bin payload.enc",
                ),
            ],
        }),
        "cmp" => Some(CommandHelp {
            name: "cmp",
            usage: "cmp [--auto|--no-direct|--direct] [--] <file1> <file2>",
            summary: "Compare two files using fro's diff engine and GNU cmp-style reporting.",
            notes: &["Exits nonzero on mismatch or size difference."],
            examples: &[("Compare two files", "cmp a.bin b.bin")],
        }),
        "fgrep" => Some(CommandHelp {
            name: "fgrep",
            usage: "fgrep [-n] [-i] [-x] [-v] [-e PATTERN | -f FILE]... [--no-ignore-case] [--auto|--no-direct|--direct] [--report-gbps] [pattern] <file> [file ...]",
            summary: "Literal line-oriented grep on top of fro's fast substring scanner.",
            notes: &[
                "Matches GNU grep -F visible behavior for the covered compatibility matrix.",
                "-i/--ignore-case folds ASCII case for literal matching; --no-ignore-case turns it back off.",
                "-v/--invert-match selects non-matching lines while keeping the same literal matcher.",
                "-e/--regexp and -f/--file can be repeated; if neither is provided, the first positional argument is the pattern.",
                "--report-gbps writes effective scanned-input throughput to stderr.",
            ],
            examples: &[
                ("Print matching lines with numbers", "fgrep -n needle notes.txt"),
                ("Match any listed pattern file entry", "fgrep -f patterns.txt notes.txt"),
                ("Match literally without ASCII case sensitivity", "fgrep -i needle notes.txt"),
                ("Match whole lines literally", "fgrep -x needle notes.txt"),
                ("Print lines that do not contain the literal", "fgrep -v needle notes.txt"),
            ],
        }),
        "find" => Some(CommandHelp {
            name: "find",
            usage: "find [path ...] [-maxdepth N] [-type TYPE] [-name PATTERN] [-path PATTERN] [-print|-print0]",
            summary: "Walk one or more directory trees and print matching paths.",
            notes: &[
                "This correctness slice does not guarantee output ordering.",
                "-maxdepth limits descent below each starting path while still printing matching roots.",
                "-type supports the common GNU/POSIX letters b, c, d, p, f, l, and s.",
                "-name matches only the final path component using shell glob syntax.",
                "-path matches the whole emitted path using shell glob syntax.",
                "-print is the default action; -print0 emits NUL-delimited paths for xargs -0 style pipelines.",
            ],
            examples: &[
                ("Walk the current tree", "find ."),
                ("Stay at the top level", "find src -maxdepth 1"),
                ("Walk two roots", "find src tests"),
                ("List only regular files", "find . -type f"),
                ("Match Rust sources by basename", "find src -name '*.rs'"),
                ("Match a subtree by emitted path", "find . -path '*/target/*'"),
                ("Emit NUL-delimited directory paths", "find src -type d -print0"),
            ],
        }),
        "du" => Some(CommandHelp {
            name: "du",
            usage: "du [-s] [-a] [-d depth|--max-depth=depth] [--] [path ...]",
            summary: "Report disk usage from filesystem block counts for files and directories.",
            notes: &[
                "Without -s, directory arguments print descendant directory totals plus the root total.",
                "-a includes non-directory entries in the output.",
                "-d/--max-depth limits which descendant depths are printed while preserving full subtree totals.",
                "-- stops option parsing so paths beginning with '-' are treated as operands.",
            ],
            examples: &[
                ("Summarize one tree", "du -s ."),
                ("Print all entries in src", "du -a src"),
                ("Show only top-level directory totals", "du --max-depth=1 ."),
            ],
        }),
        "head" => Some(CommandHelp {
            name: "head",
            usage: "head [-n lines|-c bytes] [--lines=lines|--bytes=bytes] [-q|-v] [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print the first lines or bytes of each input.",
            notes: &[
                "Supports classic head counts including obsolete -NUM and -NUM[bkm][cqv] forms, plus -NUM \"all but last\" forms for -n/-c.",
                "GNU-style --lines/--bytes long forms are accepted, including =VALUE syntax and negative counts.",
                "Use -q/--quiet/--silent to suppress headers and -v/--verbose to always print them; the last one wins.",
            ],
            examples: &[
                ("Print the first ten lines", "head notes.txt"),
                ("Print all but the last 4 KiB", "head -c -4KiB disk.log"),
            ],
        }),
        "tac" => Some(CommandHelp {
            name: "tac",
            usage: "tac [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print files with line order reversed within each file.",
            notes: &[],
            examples: &[("Reverse one file by line", "tac notes.txt")],
        }),
        "tail" => Some(CommandHelp {
            name: "tail",
            usage: "tail [-n lines|-c bytes] [-q|-v] [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Print the last lines or bytes of each input.",
            notes: &[
                "Supports classic tail counts including +N start offsets for -n/-c.",
                "Use -q to suppress headers and -v to always print them; the last one wins.",
                "Byte-mode on non-seekable inputs keeps only a bounded trailing window before final output.",
            ],
            examples: &[
                ("Print the last ten lines", "tail notes.txt"),
                ("Print the last 4 KiB", "tail -c 4KiB disk.log"),
            ],
        }),
        "wc" => Some(CommandHelp {
            name: "wc",
            usage: "wc [-l] [-w] [-m] [-c] [-L] [--lines] [--words] [--chars] [--bytes] [--max-line-length] [--files0-from=F] [--auto|--no-direct|--direct] [--report-gbps] <file> [file ...]",
            summary: "Count lines, words, characters, bytes, and max line length using fro block visitors.",
            notes: &[
                "Without -l/-w/-m/-c/-L, prints lines, words, and bytes.",
                "--lines/--words/--chars/--bytes match the GNU wc long count flags.",
                "--files0-from=F reads NUL-delimited input names from F (or stdin when F is -).",
                "--report-gbps writes effective processed-input throughput to stderr.",
            ],
            examples: &[
                ("Count lines and words", "wc -l -w notes.txt"),
                ("Count UTF-8 characters", "wc -m notes.txt"),
                ("Print the maximum display width", "wc -L notes.txt"),
                ("Read NUL-delimited paths from a list file", "wc --files0-from=list0"),
            ],
        }),
        "dd" => Some(CommandHelp {
            name: "dd",
            usage: "dd if=<input> of=<output> [bs=<size>] [count=<blocks>] [skip=<blocks>] [seek=<blocks>] [iflag=direct] [oflag=direct] [conv=notrunc,fsync] [status=none|progress]",
            summary: "Copy byte ranges with dd-style operands on top of fro I/O primitives.",
            notes: &[
                "Whole-file copies without offset/count flags use the tuned copy path directly.",
                "Compatibility currently focuses on the covered operands from tests/dd_example_compat.rs.",
            ],
            examples: &[("Copy five 4 KiB blocks with no summary", "dd if=src.bin of=dst.bin bs=4K count=5 status=none")],
        }),
        "cksum" => Some(CommandHelp {
            name: "cksum",
            usage: "cksum [--auto|--no-direct|--direct] [--report-gbps] <file> [file ...]",
            summary: "POSIX cksum compatibility wrapper on top of fro file reads.",
            notes: &[
                "Uses a table-driven POSIX CRC32 path while reusing the existing ordered input reader.",
                "--report-gbps writes effective hashed-input throughput to stderr.",
            ],
            examples: &[("Print POSIX CRC32 and size", "cksum archive.tar")],
        }),
        "b3sum" => Some(CommandHelp {
            name: "b3sum",
            usage: "b3sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print BLAKE3 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with BLAKE3", "b3sum bigfile.dat")],
        }),
        "b2sum" => Some(CommandHelp {
            name: "b2sum",
            usage: "b2sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print BLAKE2b-512 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with BLAKE2b-512", "b2sum bigfile.dat")],
        }),
        "md5sum" => Some(CommandHelp {
            name: "md5sum",
            usage: "md5sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print MD5 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with MD5", "md5sum bigfile.dat")],
        }),
        "sha224sum" => Some(CommandHelp {
            name: "sha224sum",
            usage: "sha224sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print SHA-224 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with SHA-224", "sha224sum bigfile.dat")],
        }),
        "sha256sum" => Some(CommandHelp {
            name: "sha256sum",
            usage: "sha256sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print SHA-256 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with SHA-256", "sha256sum bigfile.dat")],
        }),
        "sha384sum" => Some(CommandHelp {
            name: "sha384sum",
            usage: "sha384sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print SHA-384 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with SHA-384", "sha384sum bigfile.dat")],
        }),
        "sha512sum" => Some(CommandHelp {
            name: "sha512sum",
            usage: "sha512sum [--auto|--no-direct|--direct] [--report-gbps] [--] <file> [file ...]",
            summary: "Print SHA-512 digests for one or more files.",
            notes: &["--report-gbps writes effective hashed-input throughput to stderr."],
            examples: &[("Hash one file with SHA-512", "sha512sum bigfile.dat")],
        }),
        "tar" => Some(CommandHelp {
            name: "tar",
            usage: "tar -cf <archive.tar> [-v] <source>",
            summary: "Create an uncompressed ustar archive from a file or directory tree.",
            notes: &["Only create mode is currently implemented; extraction stays delegated to system tar."],
            examples: &[("Archive one directory", "tar -cf tree.tar mytree")],
        }),
        "shred" => Some(CommandHelp {
            name: "shred",
            usage: "shred [-n passes] [-s size] [-z] [-u] [-f] [-v] [--auto|--no-direct|--direct] <file> [file ...]",
            summary: "Overwrite files with random or zero patterns, optionally removing them.",
            notes: &["This compatibility surface covers the basic GNU-compatible size, force, and verbose flags."],
            examples: &[
                ("Zero a file once and keep it", "shred -n 0 -z scratch.bin"),
                ("Overwrite only the first 4 KiB verbosely", "shred -n 0 -z -v -s 4KiB scratch.bin"),
            ],
        }),
        "write" => Some(CommandHelp {
            name: "write",
            usage: "write [--create <size>] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <filename>",
            summary: "Write a file using the tuned pipeline, optionally creating and sizing it first.",
            notes: &[
                "Without --create, the existing file size is used.",
                "--direct/--no-direct control the read-side planner mode; --direct-write/--no-direct-write control the write side.",
                "With --direct-write, fro prints one stderr warning if any requested direct write falls back to page cache because O_DIRECT open failed or a tail block was unaligned.",
            ],
            examples: &[
                (
                    "Create a fresh 64 MiB file and fill it through the write path",
                    "write --create 64MiB --no-direct -n 1 out.bin",
                ),
                (
                    "Rewrite an existing file with direct writes",
                    "write --direct-write -n 1 out.bin",
                ),
            ],
        }),
        "copy" | "copy-via-memory" => Some(CommandHelp {
            name: "copy",
            usage: "copy [--recursive|-r|-R] [--via-memory] [--keep-target-size] [--diff|--full] [--copy-file-range|--copy-file-range-single|--threaded-copy|--reflink] [--no-lock] [--verify|--verify-diff] [--hash] [--xxh3|--sha256] [--hash-base path] [-q|--quiet] [--auto|--no-direct|--direct] [--auto-write|--no-direct-write|--direct-write] [-v] [-n iterations] [-s] [-c config.json] <source> <target>",
            summary: "Copy one file, or recursively copy one directory tree, using the tuned read/write pipeline.",
            notes: &[
                "--direct/--no-direct/--auto control source reads.",
                "--direct-write/--no-direct-write/--auto-write control destination writes.",
                "With forced direct read/write flags, fro prints one stderr warning if any source or destination leg falls back to page cache because O_DIRECT open failed or a request tail was unaligned.",
                "--recursive (or -r/-R) enables directory-tree copies; the destination behaves like cp -r, so an existing destination directory receives the source basename as a child.",
                "--copy-file-range uses the tunable multi-call copy_file_range(2) strategy with its own optimizer params.",
                "--copy-file-range-single forces the one-call copy_file_range(2) baseline for benchmarking.",
                "--threaded-copy forces the existing tuned striped io_uring copy path.",
                "--reflink requests a CoW clone/reflink when the filesystem supports it; this is fast but does not promise physically independent storage blocks.",
                "--diff forces chunked diff-and-overwrite copy when supported; --full disables diffing and always rewrites the full file.",
                "Without either flag, plain copy uses copy auto mode: when source and target share a reflink-capable filesystem and the target storage topology is positively identified as redundant, auto prefers reflink; when the target topology is positively identified as non-redundant (for example RAID0, ZFS stripe, or a degraded mirror), auto forces a real full copy; otherwise it falls back to the existing cache-aware threaded heuristic.",
                "--keep-target-size preserves an already-sized destination instead of re-truncating/re-preallocating it; this is mainly useful for best-case benchmarking.",
                "Copy takes an advisory shared lock on the source and an advisory exclusive lock on the destination by default; use --no-lock to skip that cooperative locking.",
                "--via-memory loads the whole source file into RAM first, then writes that buffer to the destination.",
                "--verify hashes the source, copies into a temporary sibling, fsyncs and verifies that file, then renames it into place without leaving sidecars by default.",
                "--hash with --verify leaves durable sidecars at the destination and at the source if the source did not already have sidecars, and syncs their parent directories too.",
                "--verify-diff fsyncs the destination and then runs a diff pass instead of block-hash verification.",
                "For non-verified copy modes, fro also checks whether the source file's size/mtime/ctime changed during the operation and fails if it did.",
                "When using --via-memory, tune read and write separately instead of saving copy params.",
                "Verification success is reported to stderr unless --quiet is used.",
                "When invoked via the cp multicall alias, the wrapper also understands GNU cp's -n/--no-clobber, -t/--target-directory, -u/--update, -v/--verbose, -T/--no-target-directory, -P/--no-dereference, and -p/--preserve (mode+timestamps for recursive and regular copies; ownership is not preserved) compatibility flags.",
            ],
            examples: &[
                (
                    "Copy in.bin to out.bin",
                    "copy in.bin out.bin",
                ),
                (
                    "Recursively copy a tree into an existing destination directory",
                    "copy --recursive srcdir outdir",
                ),
                (
                    "Read through page cache but force direct writes to the destination",
                    "copy --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Benchmark the kernel copy_file_range syscall path directly",
                    "copy --copy-file-range-single --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Optimize the chunked copy_file_range path separately",
                    "copy --copy-file-range --no-direct -n 32 -s in.bin out.bin",
                ),
                (
                    "Force the existing striped io_uring copy path for comparison",
                    "copy --threaded-copy --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Request a CoW reflink/soft copy when the filesystem supports it",
                    "copy --reflink --no-direct -n 1 in.bin out.bin",
                ),
                (
                    "Load the whole source into RAM, then flush it with direct writes",
                    "copy --via-memory --no-direct --direct-write in.bin out.bin",
                ),
                (
                    "Skip advisory locking when cooperating lock semantics would get in the way",
                    "copy --no-lock --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file through a verified temp target swap without leaving sidecars",
                    "copy --verify --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file, verify it, and leave source/destination sidecars",
                    "copy --verify --hash --sha256 --no-direct in.bin out.bin",
                ),
                (
                    "Copy a file and run a diff pass after fsync",
                    "copy --verify-diff --no-direct in.bin out.bin",
                ),
            ],
        }),
        "diff" => Some(CommandHelp {
            name: "diff",
            usage: "diff [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Compare two files and report the first mismatch.",
            notes: &["Exits nonzero on mismatch."],
            examples: &[(
                "Check whether two large files are byte-identical",
                "diff --direct a.bin b.bin",
            )],
        }),
        "dual-read-bench" => Some(CommandHelp {
            name: "dual-read-bench",
            usage: "dual-read-bench [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] <file1> <file2>",
            summary: "Read two files like diff, but treat it as a throughput benchmark rather than a mismatch-reporting tool.",
            notes: &["Useful when you want the read pressure of diff without stopping to explain a mismatch."],
            examples: &[(
                "Benchmark reading two files from page cache",
                "dual-read-bench --no-direct -n 1 a.bin b.bin",
            )],
        }),
        "recursive-read-bench" => Some(CommandHelp {
            name: "recursive-read-bench",
            usage: "recursive-read-bench [--auto|--no-direct|--direct] [-v] <directory>",
            summary: "Read every byte of every regular file in a directory tree and report aggregate throughput.",
            notes: &[
                "This is intended as a read-side roofline for recursive copy/cp measurements.",
                "Symlinks and non-regular files are skipped.",
            ],
            examples: &[(
                "Benchmark the page-cache read roofline of a source tree",
                "recursive-read-bench --no-direct /data/tree",
            )],
        }),
        "file-list-read-bench" => Some(CommandHelp {
            name: "file-list-read-bench",
            usage: "file-list-read-bench [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Read every file named in a newline-delimited manifest and report aggregate throughput without tree-walk cost.",
            notes: &[
                "Use this to separate traversal cost from pure many-small-file read throughput.",
                "Each non-empty line in the manifest is treated as one path.",
            ],
            examples: &[(
                "Benchmark reading files listed by a prior find pass",
                "find /data/tree -type f > files.txt && file-list-read-bench --no-direct files.txt",
            )],
        }),
        "file-list-read-uring-bench" => Some(CommandHelp {
            name: "file-list-read-uring-bench",
            usage: "file-list-read-uring-bench [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Sweep many-small-file io_uring reads from a manifest using 32 worker threads and varying in-flight file counts.",
            notes: &[
                "Each worker issues separate whole-file reads via its own io_uring.",
                "Sweeps in-flight files per thread over 32, 64, 128, 256, and 512.",
            ],
            examples: &[(
                "Benchmark a many-open-files io_uring approach against a prior find manifest",
                "find /data/tree -type f > files.txt && file-list-read-uring-bench --auto files.txt",
            )],
        }),
        "file-list-read-open-read-close-sweep" => Some(CommandHelp {
            name: "file-list-read-open-read-close-sweep",
            usage: "file-list-read-open-read-close-sweep [--auto|--no-direct|--direct] [-v] <manifest>",
            summary: "Compare open-read-close manifest readers across blocking and io_uring variants over file-count prefixes.",
            notes: &[
                "Runs single-thread, multi-thread, single-thread io_uring, and multi-thread io_uring variants.",
                "Useful for finding the file-count inflection points between reader designs.",
            ],
            examples: &[(
                "Sweep manifest reader variants over small-file prefixes",
                "find /data/tree -type f > files.txt && file-list-read-open-read-close-sweep --auto files.txt",
            )],
        }),
        "manifest-recursive-copy-bench" => Some(CommandHelp {
            name: "manifest-recursive-copy-bench",
            usage: "manifest-recursive-copy-bench [--overlap-large-file PATH] [-v] <manifest> <source_root> <target_root>",
            summary: "Benchmark manifest-driven recursive copy with separate directory-build and file-copy phase timing.",
            notes: &[
                "The manifest must list regular files under <source_root>.",
                "Keep source and target on the same mount when benchmarking the openat+copy_file_range small-file phase.",
                "--overlap-large-file runs one additional large-file copy in parallel with the file-copy phase.",
            ],
            examples: &[(
                "Time recursive copy phases and overlap one large file",
                "manifest-recursive-copy-bench --overlap-large-file /data/ilmari_cache/fro-test/coreutils-1g.bin files.txt /data/tree /data/out",
            )],
        }),
        "split-manifest-recursive-copy-bench" => Some(CommandHelp {
            name: "split-manifest-recursive-copy-bench",
            usage: "split-manifest-recursive-copy-bench [-v] <source_dir> <target_dir>",
            summary: "Benchmark a recursive copy design that first builds the full manifest, then dispatches copy work.",
            notes: &[
                "Uses the same recursive copy lanes as copy --recursive, but delays file-copy dispatch until the full manifest is built.",
                "This exists to compare walk-as-you-go scheduling against a split manifest-build -> dispatch design.",
            ],
            examples: &[(
                "Benchmark split-manifest recursive copy on one tree",
                "split-manifest-recursive-copy-bench /data/tree /data/out",
            )],
        }),
        "bench-recursive-small-file-threads" => Some(CommandHelp {
            name: "bench-recursive-small-file-threads",
            usage: "bench-recursive-small-file-threads [--auto|--no-direct|--direct] [--hot|--cold] [-v] [-s] <directory>",
            summary: "Sweep recursive small-file worker counts for the current cache state and optionally save the per-mount winner.",
            notes: &[
                "The current cache state is inferred from the directory path's first-page residency.",
                "When used with --save, only the hot or cold slot for the current mount is updated.",
            ],
            examples: &[(
                "Tune recursive small-file worker count for the current mount/cache state",
                "bench-recursive-small-file-threads --auto -s /data/tree",
            )],
        }),
        "bench-read-sweep" => Some(CommandHelp {
            name: "bench-read-sweep",
            usage: "bench-read-sweep",
            summary: "Sweep single-file read performance across size buckets for simple ST, ST io_uring, and current MT readers.",
            notes: &[
                "Uses temporary files sized 4 KiB through 256 MiB.",
                "Reports effective GB/s and, when built with the read-phase-timing feature, MT phase timestamps.",
            ],
            examples: &[(
                "Run the reader crossover sweep",
                "bench-read-sweep",
            )],
        }),
        "hash" => Some(CommandHelp {
            name: "hash",
            usage: "hash [--auto|--no-direct|--direct] [--xxh3|--sha256] [--hash-only] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Hash a file in parallel 1 MiB blocks, hash the hashes, and write three JSON sidecar replicas.",
            notes: &[
                "Default sidecar base is <file>.fro-hash.",
                "Use --xxh3 or --sha256 to choose the sidecar digest algorithm. (NB: this is not sha256sum-compatible.)",
                "Use --hash-only to only print the filename and hash of hashes.",
                "Default -n for hash is 1.",
            ],
            examples: &[(
                "Create block-hash sidecars for one large file",
                "hash --no-direct bigfile.dat",
            ),
            (
                "Get a SHA256 hash of block hashes for easy file comparisons",
                "hash --sha256 --hash-only bigfile.dat",
            )],
        }),
        "verify" => Some(CommandHelp {
            name: "verify",
            usage: "verify [--auto|--no-direct|--direct] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <filename>",
            summary: "Re-hash a file, compare it to its sidecars, and report bad blocks.",
            notes: &[
                "Read-only command.",
                "verify follows the hash type stored in the sidecar manifest.",
                "On a clean file with intact sidecars, verify hashes the file once and stops.",
            ],
            examples: &[(
                "Scrub one file against its block-hash sidecars",
                "verify --no-direct bigfile.dat",
            )],
        }),
        "recover" => Some(CommandHelp {
            name: "recover",
            usage: "recover [--auto|--no-direct|--direct] [--fast] [--in-place-all] [-v] [-n iterations] [-s] [-c config.json] [--hash-base path] <target> <copy1> [copy2 ...]",
            summary: "Repair corrupted 1 MiB blocks using one or more full-file replicas.",
            notes: &[
                "Default recover rewrites only the first file; later files are read-only sources.",
                "--fast behaves like verify on the first file unless corruption forces a full multi-file scan.",
                "--in-place-all attempts to repair every input file and refresh broken sidecars.",
                "recover follows each file's stored sidecar hash type.",
            ],
            examples: &[
                (
                    "Repair a target file from one clean copy",
                    "recover --no-direct target.bin backup.bin",
                ),
                (
                    "Use verify-like fast scrub behavior and only fall back to full recovery if needed",
                    "recover --fast --no-direct target.bin backup.bin",
                ),
            ],
        }),
        "bench-diff" => Some(CommandHelp {
            name: "bench-diff",
            usage: "bench-diff",
            summary: "In-memory diff microbenchmark used by the benchmark harness.",
            notes: &["This is mainly for development and regression tracking."],
            examples: &[("Run the in-memory diff microbenchmark", "bench-diff")],
        }),
        "bench-memcpy" => Some(CommandHelp {
            name: "bench-memcpy",
            usage: "bench-memcpy [--size <bytes>] [--threads <count>]",
            summary: "In-memory memcpy microbenchmark for establishing the RAM copy ceiling.",
            notes: &[
                "Defaults to --size 4GiB and --threads 32.",
                "Reports effective bandwidth as source read plus destination write bytes.",
            ],
            examples: &[(
                "Benchmark a 4 GiB to 4 GiB memcpy with 32 threads",
                "bench-memcpy --size 4GiB --threads 32",
            )],
        }),
        "bench-tar-archive" => Some(CommandHelp {
            name: "bench-tar-archive",
            usage: "bench-tar-archive <ram|ram-write|mmap-file> <source-dir> [target-file]",
            summary: "Benchmark tar archive assembly into RAM, RAM+write, or an mmap-backed target file.",
            notes: &[
                "ram: precompute tar offsets, write headers, and copy payload blocks into a preallocated RAM archive buffer.",
                "ram-write: build the tar archive in RAM, then flush it with the existing parallel file-write helpers.",
                "mmap-file: map the output file and write the archive directly into the mapping before msync.",
            ],
            examples: &[
                (
                    "Benchmark tar archive construction into RAM only",
                    "bench-tar-archive ram /data/ilmari_cache/fro-test/tar-mixedbench",
                ),
                (
                    "Benchmark tar archive construction in RAM plus parallel file write",
                    "bench-tar-archive ram-write /data/ilmari_cache/fro-test/tar-mixedbench /data/ilmari_cache/fro-test/tar-mixedbench.tar",
                ),
                (
                    "Benchmark writing the archive directly into an mmap-backed target file",
                    "bench-tar-archive mmap-file /data/ilmari_cache/fro-test/tar-mixedbench /data/ilmari_cache/fro-test/tar-mixedbench.tar",
                ),
            ],
        }),
        "bench-base64-encode" => Some(CommandHelp {
            name: "bench-base64-encode",
            usage: "bench-base64-encode [-n iterations] [--variant auto|scalar|spmd|shuffle]",
            summary: "Hot-loop the in-memory 12 KiB -> 16 KiB base64 encode kernel on one core.",
            notes: &[
                "Uses a fixed 12 KiB source buffer and fixed 16 KiB destination buffer.",
                "Reports iterations per second and effective input GB/s per core.",
                "Use --variant to compare scalar, AVX2 SPMD, and AVX2 shuffle-unpack kernels.",
            ],
            examples: &[(
                "Run one million kernel iterations",
                "bench-base64-encode -n 1000000",
            )],
        }),
        "bench-base64-decode" => Some(CommandHelp {
            name: "bench-base64-decode",
            usage: "bench-base64-decode [-n iterations] [--variant auto|scalar|avx2]",
            summary: "Hot-loop the in-memory 16 KiB -> 12 KiB base64 decode kernel on one core.",
            notes: &[
                "Uses a fixed 16 KiB encoded buffer generated from a fixed 12 KiB source buffer.",
                "Reports iterations per second and effective decoded GB/s per core.",
                "Use --variant to compare scalar and AVX2 decode kernels.",
            ],
            examples: &[(
                "Run one million decode kernel iterations",
                "bench-base64-decode -n 1000000",
            )],
        }),
        "bench-base64-decode-detect-fallback" => Some(CommandHelp {
            name: "bench-base64-decode-detect-fallback",
            usage: "bench-base64-decode-detect-fallback [-n iterations] [--variant auto|scalar|avx2]",
            summary: "Hot-loop decode with a pre-scan that falls back to the wrapped/dirty path when needed.",
            notes: &[
                "Uses clean unwrapped base64 generated from the fixed 12 KiB source buffer.",
                "Measures the cost of checking for garbage/newlines before choosing the fast decode kernel.",
            ],
            examples: &[(
                "Run one million detect+fallback decode iterations",
                "bench-base64-decode-detect-fallback -n 1000000 --variant avx2",
            )],
        }),
        "bench-base64-wrapped-encode" => Some(CommandHelp {
            name: "bench-base64-wrapped-encode",
            usage: "bench-base64-wrapped-encode [-n iterations] [--wrap COLS]",
            summary: "Hot-loop the wrapped base64 encode path on one core.",
            notes: &[
                "Uses the wrapped slow-path implementation over a fixed 12 KiB source buffer.",
                "Reports iterations per second and effective input GB/s per core.",
            ],
            examples: &[(
                "Run one million wrapped encode iterations at 76 columns",
                "bench-base64-wrapped-encode -n 1000000 --wrap 76",
            )],
        }),
        "bench-base64-wrapped-decode" => Some(CommandHelp {
            name: "bench-base64-wrapped-decode",
            usage: "bench-base64-wrapped-decode [-n iterations] [--ignore-garbage]",
            summary: "Hot-loop the wrapped/dirty base64 decode reorganization path on one core.",
            notes: &[
                "Uses wrapped base64 generated from the fixed 12 KiB source buffer.",
                "Reports iterations per second and effective decoded GB/s per core.",
            ],
            examples: &[(
                "Run one million wrapped decode iterations",
                "bench-base64-wrapped-decode -n 1000000",
            )],
        }),
        "bench-mmap-write" => Some(CommandHelp {
            name: "bench-mmap-write",
            usage: "bench-mmap-write <filename>",
            summary: "Memory-mapped write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the mmap write microbenchmark against an existing file",
                "bench-mmap-write out.bin",
            )],
        }),
        "bench-write" => Some(CommandHelp {
            name: "bench-write",
            usage: "bench-write <filename>",
            summary: "Plain write microbenchmark used by the benchmark harness.",
            notes: &[],
            examples: &[(
                "Run the plain write microbenchmark against an existing file",
                "bench-write out.bin",
            )],
        }),
        _ => None,
    }
}

pub(super) fn print_command_help(program: &str, help: CommandHelp) {
    println!("{} - {}", help.name, help.summary);
    println!();
    println!("USAGE:");
    println!("  {} {}", program, help.usage);
    if !help.notes.is_empty() {
        println!();
        println!("NOTES:");
        for note in help.notes {
            println!("  - {}", note);
        }
    }
    if !help.examples.is_empty() {
        println!();
        println!("EXAMPLES:");
        for (description, command) in help.examples {
            println!("  {}", description);
            println!("    {} {}", program, command);
        }
    }
}

pub(super) fn print_general_help(program: &str) {
    println!("fast_read_optimizer (fro)");
    println!(
        "High-throughput Linux file IO utilities with companion benchmark and optimizer tooling."
    );
    println!();
    println!("USAGE:");
    println!("  {} <command> [options]", program);
    println!("  {} <command> --help", program);
    println!();
    println!("Utilities:");
    for (name, summary) in [
        ("cat", "print files using the fro read path"),
        ("base64", "encode or decode base64 data"),
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
        println!("  {:<16} {}", name, summary);
    }
    println!();
    println!("Benchmarks:");
    println!("  read               measure striped file read throughput");
    println!("  dual-read-bench    benchmark the read pressure of diff");
    println!("  recursive-read-bench benchmark aggregate read throughput of a tree");
    println!("  file-list-read-bench benchmark aggregate read throughput from a file manifest");
    println!(
        "  file-list-read-uring-bench sweep io_uring aggregate throughput from a file manifest"
    );
    println!("  file-list-read-open-read-close-sweep compare manifest reader variants across file-count prefixes");
    println!(
        "  manifest-recursive-copy-bench benchmark manifest-driven recursive copy phase timing"
    );
    println!("  split-manifest-recursive-copy-bench benchmark split manifest-build recursive copy timing");
    println!("  bench-recursive-small-file-threads sweep recursive small-file worker counts and save hot/cold per mount");
    println!("  bench-read-sweep  sweep read variants across file sizes");
    println!("  fro-optimize       tune configs for one or more commands / mounts");
    println!("  fro-benchmark      run the regression benchmark suite");
    println!("  bench-diff         in-memory diff microbenchmark");
    println!("  bench-memcpy       in-memory memcpy microbenchmark");
    println!("  bench-tar-archive  benchmark tar assembly into RAM / RAM+write / mmap file");
    println!("  bench-base64-encode base64 encode kernel microbenchmark");
    println!("  bench-base64-decode base64 decode kernel microbenchmark");
    println!("  bench-base64-wrapped-encode wrapped base64 encode path microbenchmark");
    println!("  bench-base64-wrapped-decode wrapped base64 decode path microbenchmark");
    println!("  bench-mmap-write   mmap write microbenchmark");
    println!("  bench-write        plain write microbenchmark");
    println!();
    println!("Common flags:");
    println!("  --auto | --no-direct | --direct");
    println!("  --auto-write | --no-direct-write | --direct-write");
    println!("  -n <iterations>    use -n 1 for one measured run with current tuned params");
    println!("  -s, --save         save tuned params when forcing --direct or --no-direct");
    println!("  -c, --config PATH  override config path");
    println!("  -v, --verbose      print more about the current run");
    println!();
    println!("Coreutils compatibility names:");
    println!(
        "  cp cmp dd fgrep find du rm mv tar cat base64 encrypt decrypt head tac tail wc cksum b3sum b2sum md5sum sha224sum sha256sum sha384sum sha512sum shred"
    );
    println!("  (use as `fro <name> ...` or invoke via argv[0] multicall)");
    println!();
    println!("Related tools:");
    println!("  ./target/release/fro-optimize --help");
    println!("  ./target/release/fro-benchmark --help");
    println!();
    println!(
        "Config resolution (when -c is not provided): $FRO_CONFIG, then ~/.fro/fro.json, then /etc/fro.json"
    );
}
