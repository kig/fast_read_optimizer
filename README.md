# fast_read_optimizer (`fro`)

`fro` is a Linux CLI and Rust crate for very fast large-file IO. On one observed release-mode run against a page-cached 5 GB file, it measured about **46.4 GB/s** for `hash`, **49.8 GB/s** for `verify`, **52.0 GB/s** for `recover --fast`, and **39.4 GB/s** for full two-file `recover`. On a 4x PCIe4 NVMe array, the performance was still **>20 GB/s**, making it a practical scrub / repair tool for disk images, archives, model checkpoints, and other large mostly-static blobs.

It combines tuned striped IO, literal search, copy / diff utilities, a benchmark harness, an optimizer, and block-hash sidecars for verification and repair. If you work with big files and care whether direct IO, page cache, block size, queue depth, or replica scrubbing actually help on your machine, `fro` is built for that job.

`fro` is especially useful for fast NVMe arrays and cached workloads:

- hashing large files for integrity checking using per-block xxHash
- verifying and repairing corruption when you have one or more full-file replicas
- fast literal search over huge files
- checking if two files are equal
- copying large files and creating large non-sparse files
- tuning read / write / copy / diff parameters for a specific machine or mount
- running repeatable performance regressions

It is **not** a recursive file copier, a regex engine, or a parity / erasure-coding system.

## Utilities

Build the release binaries first:

```bash
cargo build --release
cargo build --release --examples
# Optionally install
cargo install --path .
```

Example runs:

```bash
fro write -v --create 4GB some_huge_file 
# write 4294967296 bytes in 1.1062 s, 3.9 GB/s

fro hash -v --no-direct -n 10 some_huge_file
# Cold cache: hash 4294967296 bytes in 1.0280 s, 4.2 GB/s
# Hot cache: hash 4294967296 bytes in 0.0777 s, 55.3 GB/s
# Using direct IO: hash 4294967296 bytes in 0.2148 s, 20.0 GB/s
# For comparison (these are with hot page cache):
# time xxhsum some_huge_file
# real    0.796s
# time b3sum some_huge_file
# real    0.374s (cold cache is 1.5s)

# There's a b3sum example that's using fro for IO and the blake3 crate for hashing
time target/release/examples/b3sum some_huge_file
# real    0.117s (cold cache is 0.253s)

fro verify -v some_huge_file
# verify 4294967296 bytes in 0.0849 s, 50.6 GB/s

fro grep -v my_needle_string some_huge_file
# grep 4294967296 bytes in 0.0755 s, 56.9 GB/s
# For comparison:
# time grep -FUbcoa my_needle_string some_huge_file
# real    1.223s
# time rg -FUbcoa my_needle_string some_huge_file
# real    0.874s

fro copy -v some_huge_file copy_of_the_file
# copy 4294967296 bytes in 0.9693 s, 4.4 GB/s
# For comparison:
# time cp some_huge_file copy_of_the_file
# real    7.341s

fro copy --verify --sha256 --no-direct some_huge_file verified_copy_of_the_file
# stderr: copy verify: success; verified_blocks=4096, repaired_blocks=0, used_recovery=false, hash_type=Sha256, sidecars_written=false

fro diff -v some_huge_file copy_of_the_file
# Using direct IO: diff 8589934592 bytes in 0.4167 s, 20.6 GB/s
# Hot cache: diff 8589934592 bytes in 0.1468 s, 58.5 GB/s

# Mess up the files
dd if=/dev/urandom of=copy_of_the_file bs=512k count=3 seek=4 conv=notrunc
dd if=/dev/urandom of=some_huge_file bs=512k count=5 seek=15 conv=notrunc

# And resilver them using the hash sidecars from the first command
fro recover --in-place-all some_huge_file copy_of_the_file
# recover: repaired_blocks=5, repaired_files=2, failed_blocks=0
# recover 8589934592 bytes in 0.4837 s, 17.8 GB/s

fro verify -v some_huge_file
# verify: loaded 3/3 hash replicas, ok_blocks=4096, bad_blocks=0
# verify 4294967296 bytes in 0.0877 s, 49.0 GB/s

fro diff -v some_huge_file copy_of_the_file
# diff 8589934592 bytes in 0.5486 s, 15.7 GB/s

# If you want to use SHA-256 for the block hash instead of xxh3
fro hash -v --sha256 myfile
# hash 4376176641 bytes in 0.1394 s, 31.4 GB/s

# You can also get a one-line hash-of-hashes
fro hash --sha256 --hash-only myfile
# 14733bac870c294e483edafcaa26701554d2a238d542b6e5d9bd1e4abbf12705  myfile
```

Then inspect the tools:

```bash
fro --help
fro-optimize --help
fro-benchmark --help
```

For local guardrails, enable the shared pre-commit hook once per clone:

```bash
git config core.hooksPath .githooks
```

The hook runs:

```bash
cargo test --quiet
cargo run --quiet --manifest-path janitor/Cargo.toml -- all
```

For verification status and the current proof outline, see [`VERIFICATION.md`](VERIFICATION.md). For performance work, see `docs/profiling.md` for the repo's measurement workflow, including why `--test-size 4GB` matters on fast NVMe arrays and why hot-path edits should always be re-benchmarked before re-tuning config.

## Examples

See `examples/` for example programs that use the fro library, including a BLAKE3 `b3sum`, `dd`, and direct-IO `sha256sum`.

## Rust crate API

The crate now exposes a small opinionated high-level API for the common cases, loading the default `fro.json` automatically and using tuned parameters by default:

```rust
let bytes = fro::read_file("checkpoint.bin")?;
fro::write_file("copy.bin", &bytes)?;
fro::write_file_range("copy.part", &bytes, 4096, 1 << 20)?;
fro::copy_file("checkpoint.bin", "checkpoint.backup")?;
fro::copy_file_via_memory("checkpoint.bin", "checkpoint.ram-copy")?;
fro::copy_file_verified("checkpoint.bin", "checkpoint.verified-copy")?;

let reader = fro::open("checkpoint.bin")?;
reader.foreach_block(|block_index, block| {
    println!("block {} has {} bytes", block_index, block.len());
    Ok(())
})?;
```

For stream-style writes, use `fro::create(...)` for sequential output or `fro::offset_writer(...)` for parallel offset writes. `offset_writer()` prepares a fixed-size output and zero-fills any unwritten gaps by default; use `offset_writer_with_options(..., truncate = false)` when you need to preserve existing bytes outside the written ranges. See `examples/dd.rs`, `examples/sha256sum.rs`, and `examples/b3sum.rs` for end-to-end usage.

### How `fro` utilities behave

Every `fro <command>` run goes through the optimizer.

- `-n <iterations>` controls how many optimization iterations are run
- default is `1000` for `read`
- default is `1` for all other commands

When you want one real run using the current tuned parameters, use `-n 1`.

Config is loaded from:

1. `-c <path>` / `--config <path>`
2. `$FRO_CONFIG`
3. `~/.fro/fro.json`
4. `/etc/fro.json`

Save best-found parameters with `-s` / `--save`, but only while forcing `--direct` or `--no-direct`.

IO mode overview:

- `--direct`: force `O_DIRECT`
- `--no-direct`: force page-cache IO
- `--auto`: choose based on whether the file appears cached

`write` and `copy` also support write-side overrides:

- `--direct-write`
- `--no-direct-write`
- `--auto-write`

Important direct-IO rule:

- direct IO is only used for 4 KiB-aligned offsets and full aligned blocks
- otherwise `fro` falls back to the non-`O_DIRECT` file descriptor

### `fro grep`

Use `grep` when you want to search a large file while still exercising the tuned read path.

```bash
fro grep needle /mnt/fast/bigfile.dat
```

This scans a cached file for the literal byte substring `needle`. It is a literal substring search, not a regex engine, and matches are printed as `offset:pattern`.

### `fro write`

Use `write` to stress the write path or to create deterministic test files for later copy / diff / verify work.

```bash
fro write --create 64MiB out.bin
```

This creates `out.bin`, sizes it to 64 MiB, and fills it through the write pipeline in one step.

```bash
truncate -s 4GiB out.bin
fro write --no-direct-write out.bin
```

This rewrites an existing 4 GiB file while forcing non-direct writes (direct IO is the default for writes.)

### `fro copy`

Use `copy` when you want a high-throughput single-file copy path with separate control over read-side and write-side mode selection.

```bash
fro copy in.bin out.bin
```

Copy defaults to direct IO reads and writes, as that seemed to perform the best in testing.


```bash
fro copy --no-direct --direct-write in.bin out.bin
```

This reads `in.bin` through the page cache but forces direct writes to `out.bin`, which is useful when experimenting with mixed cache / direct behavior.

```bash
fro copy --verify --sha256 --no-direct in.bin out.bin
```

This hashes `in.bin`, copies it to `out.bin`, `fsync`s the destination file, and then verifies `out.bin` against the **source-derived** manifest without leaving sidecars by default. If the first verification pass finds bad blocks, `fro` repairs `out.bin` from `in.bin` using the source-derived manifest as the gold standard, `fsync`s again, and verifies one final time before returning success.

If you also want to leave sidecars behind:

```bash
fro copy --verify --hash --sha256 --no-direct in.bin out.bin
```

That writes durable sidecars for `out.bin` and, if `in.bin` did not already have them, also leaves durable sidecars at the source.

If you want a simpler postcondition check:

```bash
fro copy --verify-diff --no-direct in.bin out.bin
```

That copies, `fsync`s the destination file, checks that the source metadata stayed stable, and then runs a diff pass.

Current copy-verification limits:

- it syncs the destination file and sidecar files, but not the parent directory
- it verifies that the destination matches the source as observed during this run
- copy now takes advisory locks by default (shared on the source, exclusive on the destination), but those locks only constrain cooperating processes
- non-verified copy modes also fail if the source file's size, `mtime`, or `ctime` changes during the operation

### `fro diff`

Use `diff` to compare two large files and stop on the first mismatch.

```bash
fro diff --direct a.bin b.bin
```

This does one direct-IO comparison pass and exits nonzero if the files differ.

### `fro hash`

Use `hash` to generate block-hash sidecars for a file.

```bash
fro hash bigfile.dat
```

This hashes `bigfile.dat` and writes three JSON sidecar replicas at `bigfile.dat.fro-hash.[0-2].json`.

The sidecars contain a hash for each block in the file (default 1MiB), and a hash of the hashes for integrity-checking the sidecar.
There are three copies to protect against inode corruption and data corruption. Even if you have two corrupted files and two corrupted sidecars,
you may be able to recover your data, as long as there's a good-block quorum. (If speed is no issue, use wirehair or some other FEC algorithm.)

### `fro verify`

Use `verify` to scrub a file against its sidecars without modifying the file.

```bash
fro verify bigfile.dat
```

This re-hashes the file, compares it to the stored manifests, and reports any bad blocks. On the clean path with intact sidecars, `verify` hashes the file once and stops.

### `fro recover`

Use `recover` when you have one or more full-file replicas and want to repair corrupted 1 MiB blocks.

```bash
fro recover target.bin backup.bin
```

This hashes the target and the backup, elects the winning block hash, and repairs only the **first** file.

```bash
fro recover --fast target.bin backup.bin
```

This first behaves like `verify` on `target.bin`. If the target is already clean and its sidecars are intact, the command stops there; if not, it falls back to full multi-file recovery.

```bash
fro recover --in-place-all copy1.bin copy2.bin copy3.bin
```

This attempts to repair every input file in place and refreshes missing or corrupt sidecar replicas for files that end clean.

Current recover semantics:

- default `recover` rewrites only the first file
- later files are treated as read-only candidate sources
- all files must have the same size
- `--fast` and `--in-place-all` cannot be combined

Good fits for block-hash workflows:

- disk / VM images
- model checkpoints
- archives / ISOs / tarballs
- replicated backup blobs

### Recovery decision table

The table below is generated from an executed test so it documents observed behavior rather than guessed prose:

```bash
cargo test document_block_recovery_decision_table -- --nocapture
```

| Scenario | Elected hash | Repair source | Basis | Failure | Status message |
| --- | --- | --- | --- | --- | --- |
| target block matches an intact manifest | 111 | 0 | IntactHash | - | recovered block based on intact hash |
| intact manifests agree, but no available file copy matches them | 111 | - | - | IntactHashWithoutMatchingBlock | failed to recover corrupt block [intact hash found but no matching file block] |
| intact manifests disagree with each other | - | - | - | ConflictingIntactHashes | failed to recover corrupt block [conflicting intact hashes] |
| two file copies agree and no intact manifest overrides them | 444 | 0 | FileAndFileAgreement | - | recovered block based on file+file agreement |
| one file copy agrees with a manifest witness | 555 | 0 | FileAndManifestAgreement | - | recovered block based on file+manifest agreement |
| two manifest witnesses agree, but no file copy matches them | 111 | - | - | ManifestOnlyAgreement | failed to recover corrupt block [manifest+manifest hashes agree] |
| all witnesses are missing | - | - | - | NoBlockHashFound | failed to recover corrupt block [no block hash found either] |

## Benchmarks

### Observed release-mode snapshot

These numbers are here to show what the tool can do in practice, not to promise a universal result:

| Command | Observed throughput from page cache |
| --- | --- |
| `fro hash -v /mnt/fast/5GB_file.dat` | `46.4 GB/s` |
| `fro verify -v /mnt/fast/5GB_file.dat` | `49.8 GB/s` |
| `fro recover --fast -v /mnt/fast/5GB_file.dat /mnt/fast/5GB_file_copy.dat` | `52.0 GB/s` |
| `fro recover -v /mnt/fast/5GB_file.dat /mnt/fast/5GB_file_copy.dat` | `39.4 GB/s` |

Those relationships are expected:

- `verify` hashes one file
- `recover --fast` can be nearly verify-cost on a clean target with intact sidecars
- full `recover` hashes every candidate file because it is preparing for election and repair

### Measurement commands

`read` and `dual-read-bench` are measurement-oriented tools rather than day-to-day data-management utilities.

```bash
fro read --direct -n 1 /mnt/fast/bigfile.dat
```

This runs one direct-IO read measurement against `bigfile.dat` using the current tuned parameters instead of a hill-climb search.

```bash
fro dual-read-bench --no-direct a.bin b.bin
```

This applies the read pressure of `diff` to both files but treats the run as a benchmark rather than a mismatch-reporting workflow.

### `fro-optimize`

Use `fro-optimize` to discover good parameters for one or more commands and save them into config.

```bash
fro-optimize --test-dir /mnt/fast read grep diff write copy
```

This tunes the main utility commands for the filesystem mounted at `/mnt/fast`.

```bash
fro-optimize --test-dir /mnt/fast --test-size 64MiB --iters 5 read
```

This does a smaller, quicker optimization pass that is useful in tests or during development.

Useful options:

- `--all`
- `--all-dir <path>` (repeatable)
- `--plan`
- `--test-dir`
- `--test-size`
- `--max-drive-writes`
- `--iters <n>`
- `--list-devices`
- `--list-devices-all`

Wear note:

- optimizer runs do real writes
- rough rule of thumb for a full optimizer run:
  - `bytes_written ~= 83 x test_size`

So:

- `1 GiB` test size -> about `83 GiB` written
- `4 GiB` test size -> about `332 GiB` written

To compare that to SSD endurance:

```text
drive_writes = bytes_written / SSD_capacity
```

### `fro-benchmark`

Use `fro-benchmark` to run the regression suite after code changes or when comparing systems.

```bash
fro-benchmark --test-dir /mnt/fast
```

This builds release, creates temporary files, runs the benchmark set, and reports PASS / REGRESSION / FAILED against the stored targets.

```bash
fro-benchmark --skip-build --no-fail --iters 5 --test-size 64MiB 'read (forced page cache, hot)'
```

This runs a smaller targeted benchmark without rebuilding and without failing the process on regression.

Useful options:

- `--plan`
- `--skip-build`
- `--no-fail`
- `--iters <n>`
- `--test-dir`
- `--test-size`
- `--max-drive-writes`

Wear note:

- a full benchmark run does real writes too
- rough rule of thumb:
  - `bytes_written ~= 13 x test_size + 2 GiB`

### Microbenchmarks

`fro` also exposes a few harness-facing microbenchmarks:

- `fro bench-diff`
- `fro bench-mmap-write <filename>`
- `fro bench-write <filename>`

These are mainly for development and regression tracking rather than normal end-user workflows.

## Library

This repository also exposes a small public Rust API through the `fro` crate:

- `fro::config`
- `fro::block_hash`
- `fro::reader`
- `fro::stream`
- `fro::writer`
- `fro::IOMode`

For the Rust-facing API notes and examples, see [`docs/API.md`](docs/API.md).

There are now example programs under `examples/` showing how to build on the crate:

- `examples/model_loader.rs`
- `examples/hw_sha256.rs`
- `examples/parallel_zstd.rs`

`examples/parallel_zstd.rs` now includes both compression and decompression paths and demonstrates the small streaming helpers intended for building high-throughput block pipelines with minimal glue code.
The high-level Rust API now centers on `fro::stream::ParallelFile` and `fro::stream::ParallelWriter`, so blockwise compressors and decompressors can read like the obvious pseudocode: iterate blocks in parallel, then `write_at_index(...)` or `write_at_offset(...)`.

## Development notes

For performance investigation and profiling workflow, see [`GEMINI.md`](GEMINI.md) and the scripts in [`perf/`](perf/).

## License

MIT
