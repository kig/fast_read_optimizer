# History

## 2026-04-09

- `sort` now uses a StringZilla-backed bytewise argsort fast path for in-memory ordering, and large or streamed inputs spill sorted runs plus perform an n-way merge so newline-delimited sorting can complete out-of-core instead of assuming every input fits in RAM.
- `tar` now supports a bounded GNU-style extract slice for uncompressed whole-archive extraction: `tar -xf` / `tar --extract --file` can unpack regular files, directories, and symlinks into the current directory or `-C` destination, reuse the existing archive reader plus threaded copy helper for file payloads, and reject unsafe or unsupported member paths/types explicitly.
- `sort` gained a bounded numeric-ordering slice on top of the same newline-delimited backend: `-n` / `--numeric-sort` now compare leading C-locale-style numeric prefixes, compose with `-r`, `-u`, and `-o`, and keep GNU-like `-n -u` first-line retention for numerically equal records instead of broadening into general-key or locale work.
- `cksum` gained a bounded fro-style `--check` slice without bloating `src/coreutils/hash.rs`: shared checksum-check policy/reporting now lives in `src/coreutils/hash/check.rs`, `cksum`-specific parsing/execution lives in `src/coreutils/hash/cksum.rs`, and the verifier reuses the regular-file CRC/hash fast path while supporting `--quiet`, `--status`, `--warn`, `--strict`, and `--ignore-missing`.
- `sort` gained a bounded output-file slice: `-o FILE`, `--output=FILE`, and attached `-oFILE` now write the sorted result to a file after all input has been read, so stdout is suppressed and in-place rewrites reuse the same bytewise ordering backend instead of requiring temp-file machinery.
- `tar -tvf` now prints GNU-style verbose whole-archive listings for uncompressed archives on top of the same reader used by plain `tar -tf`, raising the tracked tar compatibility slice to create/list/verbose-list while still leaving extraction unsupported.
- `dd status=progress` now reports periodic line-based byte-progress snapshots without replacing the tuned copy engines: the progress path threads an optional completed-bytes counter through the existing copy strategies and still prints the normal final summary.
- `tar` gained a bounded compatibility slice for whole-archive listing: `tar -tf` / `tar --list --file` now streams member names from uncompressed archives while create mode stays on the existing archive writer path, with focused parity coverage against GNU tar on both fro-generated and system-generated archives.
- `sort` gained a bounded common-flags slice on top of the existing bytewise newline-delimited path: `-r` / `--reverse`, `-u` / `--unique`, and clustered `-ru` / `-ur` now compose with the same ordering backend instead of erroring, with focused parity coverage against system `sort`.
- Added a bounded `read` fast-path verification slice to complement the existing `cat` backend checks: plain small regular-file `read --no-direct` / `read --direct` cases now prove they stay on the simple single-thread path, while large reads still prove they pick up tuned threaded params once they cross the strategy cutoff.

## 2026-04-08

- Recursive dir-queue wakeups now track sleeping workers and wake only the number needed for newly queued sibling subtrees, while still broadcasting when the last active worker drains; focused queue tests cover both fanout wakeup and clean shutdown.
- Added a test-backed multicall/coreutils compatibility coverage report that prints, for each implemented GNU-targeting command, the percentage of compatibly implemented flag surface plus a compact one-line list of remaining gaps, so planning can target the biggest real compatibility holes without re-deriving the matrix by hand.
- `wc` now honors `--` to stop option parsing, so dashed filenames reuse the same optimized metadata, mapped-block, and fd-parallel counting backends instead of being rejected as flags, with focused helper-selection and compatibility coverage.
- `cp` now supports GNU-style `-a` / `--archive` as a bounded recursive-copy compatibility slice, mapping it onto the existing preserve + no-dereference behavior while keeping the threaded recursive-copy backend on real-copy executions and adding focused parity coverage for recursive timestamps and symlink metadata.
- The shared `find`/`du` work queue now tracks actual waiters so batched subtree enqueue only wakes the number of sleeping workers that can claim new tasks, and worker completion only broadcasts when the last active worker drains the queue; focused queue tests plus a synthetic ignored perf surface make the dirwalk scheduler's wakeup behavior easier to validate.
- `head -n` small streamed-stdin fast paths now defer stdin/stdout pipe growth until after the first full block if the newline cutoff was not already satisfied, removing `F_SETPIPE_SZ` setup work from one-read completions while reusing a shared newline-prefix helper across the raw and buffered line writers.
- `cksum` now uses a fixed-function cached `CRC-32/CKSUM` combine operator instead of calling the generic `crc-fast` matrix-building combine path for every mapped block merge, keeping the file-side CRC math aligned with the same polynomial while removing repeated GF(2) setup work.
- `base64` decode reorg now caches its chosen decode kernel per stream and keeps the AVX2 sanitize/compact path active for `--ignore-garbage` inputs until padding, with focused coverage for both large dirty-input success and valid-bytes-after-padding rejection.
- `split-manifest-recursive-copy-bench` now reports manifest-build vs copy-phase timing plus dir/symlink/small/large task counts, and focused regression coverage pins both the benchmark registration and the helper's reported phase/task counters.
- `fro-benchmark` now accepts `-c` / `--config` and forwards that config path to fixture setup plus benchmarked `fro` subprocesses, so benchmark runs can use the same tuned config file as `fro-optimize`.
- `du` gained a bounded common slice: `-S` / `--separate-dirs` now excludes child-directory totals from parent directory totals while keeping the existing parallel dirwalk and descendant emission behavior intact, with focused parity coverage plus a small rollup-helper proof.
- `src/stream/transform.rs` now exposes mapper-style transform dispatch helpers that preserve automatic regular-file vs stream pairing while still telling callers whether the output side is a regular file or a stream.
- `encrypt` / `decrypt` now use that shared mapper helper instead of open-coding their four-way pairing match, and focused transform + encrypt tests cover the intended dispatch surface.
- `cat` path verification now pins backend selection: plain / `-u` / `--no-direct` regular-file cases stay on the fast copy path, representative formatting flags switch to ordered transform, and plain `--direct` stays on the buffered-copy path.

## 2026-04-07

### Archived from TODO: review-driven hardening and maintainability wave

- Integrated a review-driven fix wave on top of `coreutils-multicall`, then validated it with the full test suite plus janitor in a clean integration worktree before advancing the branch.
- `diff` now hardens resolved IO params so zero `num_threads` and `qd` clamp to safe minimums while zero `block_size` still errors, preventing false "equal" results from malformed config, with focused `diff_cli` regression coverage.
- `find` now honors `--` as the end of options, so dashed roots and child paths are parsed as operands instead of unsupported expressions, with parity coverage in `tests/find_cli.rs`.
- `find` now supports GNU-style `-iname` / `-ipath` case-insensitive glob predicates as emission-time filters, preserving the existing parallel traversal scheduler while broadening common replacement coverage.
- `encrypt` / `decrypt` now zeroize passphrases, PBKDF2 material, derived key/IV state, and per-chunk CTR IV buffers where feasible, and the help/tests make the unauthenticated AES-CTR caveat more explicit.
- `sort` now has a bounded first multicall/coreutils slice for locale-independent bytewise ascending sorting of newline-delimited records, with explicit help for unsupported GNU modes and focused CLI/multicall coverage.
- `mv` same-filesystem rename health now has focused inode-preservation regression coverage for file overwrite and directory-parent moves, so the rename-only fast path is pinned separately from cross-filesystem copy+remove behavior.
- `wc` now has helper-selection assertions for its main execution paths:
  - byte-only regular-file mode stays on the metadata fast path
  - default / line / word counting on regular files stays on the mapped-block backend
  - char-count and max-line-length modes stay on the fd-parallel path
- Public Rust path APIs now document and enforce the current UTF-8-only contract consistently via a shared `io_util::utf8_path(...)` helper, with Unix non-UTF-8 rejection tests covering the bounded public surface.
- Janitor-oriented maintainability cleanup landed again:
  - `src/coreutils/hash/cksum.rs` now holds the `cksum`-specific CRC/combine logic extracted from `src/coreutils/hash.rs`
  - `src/main_app/cli/execute/config_command.rs` now holds config-command dispatch extracted from `src/main_app/cli/execute.rs`
  - the generated source-tree snapshot was refreshed and janitor file-size checks are green on the integrated branch

### Archived from TODO: focused throughput/parity slices and config layering follow-up

### Archived from TODO: config/optimizer cleanup after shipped work

- `TODO.md` now drops the obsolete pre-April active-items block so the file has a single current backlog instead of duplicated planning sections.
- Shipped config/optimizer items are now marked complete in the active backlog: composite `md`/`dm` signatures, device-db precedence layering, mount-override persistence for `fro-optimize --for`, deterministic test sizing, and direct-I/O fallback observability.
- The active backlog stays focused on remaining validation, high-use parity, and tuning work instead of re-listing already-landed config foundations.

- `cat` stdin/plain-copy handling now avoids creating the buffered stdout writer thread when every input can stay on the fast kernel-copy path, so redirected stdin stays near pathname-speed on the plain byte-copy case without changing mixed stdin/file semantics.
- `wc` byte-counting advanced in a path-preserving way:
  - `wc --bytes` now reaches the existing regular-file metadata-length fast path just like `wc -c`
  - the metadata shortcut is explicitly blocked for combinations like `--bytes -L` where streaming inspection is still required
  - focused parity and multicall coverage now exercises sparse-file `--bytes` behavior and stdin `--bytes`
- Device-db profile matching now layers using the same sparse patch shape as mount overrides:
  - resolved precedence is `defaults -> matched device-db profile -> explicit mount override`
  - matched profiles can now affect non-`read`/`grep` settings such as `copy_auto_mode` and recursive small-file thread counts
  - focused config-selection tests cover sparse profile application and override precedence
- Wrapped `base64` output no longer emits tiny per-line writes in the stateful wrapped path:
  - wrapped chunks and trailing newlines are batched before flushing to stdout/pipes
  - exact output behavior is preserved, including final newline handling
  - validation also fixed a latent wrapped-stateful slice-length bug and added a batching-oriented regression test
- `fro-optimize --for <path>` now persists tuned params into the selected mount override entry instead of flattening them into global defaults, with end-to-end coverage that the resolved `mount_overrides.by_mountpoint` entry receives the saved `read` params while defaults stay unchanged.
- The compatibility/API matrix gained a compact read-surface slice over regular files, directories, symlinks, and permission-gated paths, and `ParallelFile::open` now rejects non-regular files up front with `InvalidInput` instead of allowing surprising successes deeper in the stack.
- Benchmark/optimizer auto-sizing now uses a shared deterministic sizing policy that subtracts fixed benchmark-write overhead from the wear budget before deriving `test_size`, while still preserving explicit `--test-size` overrides.
- Recursive `cp` advanced with a focused preserve-metadata slice: `cp -p`, `-rp`, and `--preserve` now keep mode + timestamps for regular files, symlinks, and directories while still using the existing optimized recursive-copy backend and scheduling lanes.
- Config/device selection gained stable composite stack match keys for `dm`/`md` storage graphs (`kind=*`, `component.*`, `stack=*`, and `leaf.*`) so profile matching can target layered storage reliably.
- Config regression coverage now includes compact contract tests for precedence and path-specific save/explain behavior, plus host-independent mountinfo/device-signature tests for longest mount matching, octal escape decoding, missing sources, and minimal signatures for non-block mounts.
- `fro-optimize` now supports a narrow `--global --for <path>` flow: it tunes the selected mount as usual, then promotes that mount's saved override into config defaults and removes the override entry, while rejecting unsupported `--global` combinations cleanly.
- `head -n` gained a tiny streamed-stdin fast path for small single-input cutoffs (`-n <= 64`, no headers), bypassing the async stdout writer when setup overhead would dominate the job.
- `find` gained a compact high-value parity slice: common `-type {b,c,d,p,f,l,s}` filtering plus explicit `-print` / `-print0`, implemented as emission-time filtering so the existing parallel traversal stays intact.
- Shared digest tools (`md5sum` and family) now accept GNU-style tagged manifests during `--check`, while still rejecting `--check --tag` with GNU-matching behavior and parity coverage.
- Recursive `rm` now batches wide directory fanout into the shared directory queue instead of enqueueing one child at a time, reducing futex/condvar churn on large trees while preserving delete semantics and byte accounting.
- The hot-cache `fro cat file | fro wc` pipeline improved materially by raising the shared coreutils pipe target size to 2 MiB and reusing that helper in `wc`, cutting syscall churn and moving the 1 GiB hot path from roughly `0.48–0.54s` down to `0.33–0.35s`.
- Streamed `wc` default counting now uses a large buffered reader after best-effort pipe growth instead of the old `vmsplice`-driven pipe fast path, keeping the same counting logic while reducing syscall-pattern overhead on large stdin streams and adding large-stdin parity coverage for both default `wc` and `wc -c`.
- `base64` transport/orchestration now reuses shared transform runner dispatch helpers in `src/stream/transform.rs`, so future transform-style tools can build on the same file/stream pairing path while base64 keeps its format-specific fast paths and wrapped-output behavior.
- Forced direct-I/O modes now surface when they really fall back to the page cache:
  - one scoped stderr warning is emitted when `O_DIRECT` open is unsupported
  - one scoped stderr warning is emitted when forced direct requests hit unaligned tail/range fallbacks
  - read-path CLI coverage and tracker unit tests now pin this behavior without breaking `cp` compatibility stderr parity
- `cp` path verification now proves that real-copy executions for path-preserving flags (`-v`, `-p`, `-n`, `-u`, `-T`, plus recursive `-p -v`) stay on the threaded copy backend, using test-only backend tracing rather than changing production copy selection.
- `fro-benchmark` gained a focused `tree compare:` slice for recursive/tree-walk work:
  - read-side compares `recursive-read-bench`, `file-list-read-bench`, and `fd`
  - copy-side compares `fro copy --recursive`, split/prebuilt-manifest recursive-copy variants, and `cp -r`
  - the slice reports `files/s` while keeping elapsed-time summaries, and the docs now point tree-work profiling toward this narrower benchmark family
- `cksum` now uses the `crc-fast` SIMD CRC-32/CKSUM core instead of the old in-tree slicing-table implementation, while preserving the existing parallel file map/reduce shape and POSIX length-suffix finalize semantics through chunk-level `checksum_combine`.

### Archived from TODO: completed slices pruned from stale backlog

- The stale duplicated active-backlog section was removed from `TODO.md` so the newer priority-based backlog is the only active planning surface again.
- `cp -t` / `--target-directory` is now treated as shipped compatibility work rather than an active checkbox.
- `fgrep -v` / `--invert-match` is now treated as shipped compatibility work rather than an active checkbox.
- The uncompressed dirtree-to-file tar path is now treated as shipped foundation work rather than an active checkbox.

## 2026-04-06

### Archived from TODO: low-priority parity sprawl and utility long tail

- Reworked `TODO.md` so the active backlog now reflects project goals and observed command usage instead of carrying a giant per-flag parity ledger.
- Elevated shared fast-I/O work plus the highest-value utility families called out by `cmd_counts_nz.txt`: `cat`, `rm`, `find`, `cp`, `wc`, `mv`, `head`, `tail`, `dd`, `md5sum`, and `du`.
- Kept `base64` in view only as architecturally useful transform-style I/O work, not as a top-line command-priority item.
- De-emphasized or parked lower-return backlog items that were cluttering the active plan:
  - exhaustive flag-by-flag compatibility tracking for every implemented utility
  - long-tail digest CLI parity beyond common `md5sum` / shared checksum flows
  - `parallel zstd`, compressed `tar`, HDD-specific streaming tweaks, and `rdma-pipe` integration
- Preserved the rationale that parity work should follow the fast path: active TODO items now explicitly say to prefer high-use, path-preserving compatibility slices over low-frequency corners.

### Archived from TODO: coreutils parity, config CLI, and follow-up utility work

- Added a substantial coreutils parity wave and moved the completed slices out of the active backlog:
  - `tail` landed as a multicall/subcommand using the same range/offset helpers as `head`, including default behavior, `-n`, `-c`, size suffixes, `+N` semantics, and `-q` / `-v` header controls.
  - `fgrep` gained `-x` / `--line-regexp`, then `-i` / `--ignore-case` and `--no-ignore-case`.
  - `wc` gained `-m` / `--chars`, `-L` / `--max-line-length`, and `--files0-from`.
  - `shred` gained `-s` / `--size`, `-v` / `--verbose`, and `-f` / `--force`.
  - `cp` compatibility gained `-n` / `--no-clobber`, `-u` / `--update`, `-v` / `--verbose`, and `-T` / `--no-target-directory`.
- Added shared repo-local parity fixtures in `tests/helpers/coreutils_parity.rs` for regular files, symlinks, nested trees, and stdin/`-`, and wired new flag slices into the growing parity suite.
- Documented the new flag slices more explicitly in repo docs so future work distinguishes path-preserving flags from flags that intentionally force slower transform/buffered execution, and so parity tests are paired with performance-path verification.
- Added a first user-visible config/mount introspection slice:
  - `fro config print`
  - `fro config explain --for <path>`
- Improved `dd` small/medium transfer routing by reusing lighter existing copy primitives instead of always forcing the threaded path.
- Implemented a first practical `fro encrypt` / `fro decrypt` slice by delegating to the system `openssl enc` CLI with `--passphrase-file`, `--cipher`, and `-o/--output`, but this was later clarified by the user as the wrong long-term architecture.
- The intended follow-up encryption design is now recorded as:
  - OpenSSL **library** integration
  - blockwise encryption/decryption in 512 KiB chunks
  - existing `ParallelStream` mapper-style processing
  - `num_cpus` worker parallelism
- Writer-path follow-up work completed in the same period:
  - small direct-write heuristics for generated writes / RAM-buffer flushes
  - cross-filesystem recursive `mv` pending-state race fix plus regression coverage

### Archived from TODO: mount/device signatures, flag-path docs, benchmarks, and in-process crypto

- `config explain --for <path>` now includes richer device signature extraction:
  - canonical `/dev/...` source resolution
  - `/dev/disk/by-id` aliases
  - sysfs vendor/model/rotational metadata
  - composite `dm` / `md` stack details via recursive `slaves`
  - flattened device `match_keys` for future profile matching
- Documentation and benchmarking follow-ups landed for the expanding flag-parity work:
  - docs now classify path-preserving vs path-changing flags and require perf-path verification alongside parity work
  - benchmark notes now include flagged multicall cases
  - a practical benchmark slice compares selected `fro` flag paths against GNU coreutils and uutils where locally available
- More parity slices landed:
  - `fgrep` gained `-e` / `--regexp` and `-f` / `--file`
- A first in-process OpenSSL-library crypto path landed:
  - regular-file encrypt/decrypt uses 512 KiB framed blocks with parallel file readers/writers and CPU-parallel workers
  - stdin/stdout remains sequential
  - this first version currently uses a `fro`-specific framed format and CBC-family ciphers, which the user has since redirected toward OpenSSL-compatible `aes-256-ctr`
  - follow-up work should therefore pivot from the framed CBC container toward OpenSSL-compatible CTR output plus better automatic stream/file pairing helpers

## 2026-03-16

### Completed in PR #4

- Added a breaking sidecar schema with explicit `hash_type`.
- Added `sha256` alongside `xxh3` for block hashes and manifest hash-of-hashes.
- Hardened recovery writes with `sync_all()`.
- Tightened manifest integrity validation and made recovery voting more conservative.
- Propagated more io_uring failures as `std::io::Result` errors instead of panics.
- Fixed literal grep matches that cross block boundaries.
- Preserved malformed config files and warned on mountinfo/direct-I/O fallback cases.
- Added end-to-end CLI coverage for SHA-256 sidecars and grep boundary matching.

Reference:

- PR: https://github.com/kig/fast_read_optimizer/pull/4
- Initial shipped commit: `797c6a8`

## 2026-04-03

### Archived from TODO: completed shipped work through `a7c297d`

- `TODO.md` was cleaned up so it tracks open work instead of mixing backlog with release notes and already-completed checkboxes.
- The following shipped items were moved out of the active TODO backlog because they are already done in the repo:
  - benchmark/optimizer temp-file creation now only creates files needed by the selected modes
  - checked arithmetic and checked `u64` to `usize` conversions landed for block/offset math, with explicit overflow-boundary coverage
  - short `io_uring` read handling now treats partial CQE results as retry-or-error instead of silently accepting truncated logical blocks
  - sparse offset write semantics were decided, documented, and tested
  - verified write/copy mode now stages hash/copy/fsync/recover-or-repair/fsync/optional-verify with a documented contract
  - dirwalk subtree processing by multi-tree parallel DFS landed as the current best-known `io_uring` scheduling fallback
- Additional shipped coreutils/library progress archived from the old TODO "recent progress" section:
  - `du` is a real multicall/subcommand and no longer uses the naive recursive metadata walk
  - `find` keeps the coarse subtree-stealing traversal; `du` uses the wider split stat-worker scheduler
  - `cp -r` / `copy --recursive`, `cat`, `wc`, `fgrep`, checksum multicalls, `head`, `pv`, and read-to-memory flows are wired into the main tool surface
  - `benchmark_page_cache_lift()` exists as a public API hook for direct-read-plus-page-cache-warm benchmarking
  - `read` and `grep` expose `--auto-lift`
  - base64 regained a working GNU-style `--help` path, large wrapped decode regression coverage, and submodule splits that satisfy janitor line-count limits
  - `io_uring` `GETDENTS` remains blocked in this environment because the available headers/crates do not expose `IORING_OP_GETDENTS`

## Archived planning and previous TODO backlog

The following planning notes and backlog were moved out of `TODO.md` so that `TODO.md`
can stay focused on the current active work.

# TODO: Global + per-mount configuration, device DB, and adaptive optimization

## Summary / objective

Make `fro` “just work” on most systems without manual tuning by:

1. Supporting **multiple config layers** (system-wide + per-user + optional local/project), including `-c <path>` to point at an explicit config.
2. Storing **optimized parameters per mount point** (because RAID/FS/mount options can matter as much as the raw device).
3. Supporting a **device/filesystem database** (`fro-device-db.json`) so most users can skip optimization.
4. Making optimization safer by **auto-sizing test files** to avoid excessive SSD wear and to keep runs neither too short nor too long.

---

## Current state (anchor points in this repo)

- `fro` currently loads exactly one config file: `AppConfig::load("fro.json")` in `src/main.rs`.
- `AppConfig` is a single struct with per-tool configs, and each tool has `{direct, page_cache}` params: `num_threads`, `block_size`, `qd`.
- `fro-optimize` and `fro-benchmark` currently create **4 GiB** temp files and run fixed sets of scenarios.
  - Planned: adaptive sizing (v1: wear/space based; v2: probe-based time targeting).

This plan expands config and selection logic without breaking today’s workflow.

---

## Proposed CLI additions

### For `fro`

- `-c <path>`, `--config <path>`
  - Explicitly select a config bundle file.
  - When provided, **do not** look at other default locations (except optional system DB paths referenced inside the bundle).

Optional (recommended) introspection commands:

- `fro config print [--json] [--for <file>]`
  - Print the resolved config and what entry was selected for the given file/mount.
- `fro config explain --for <file>`
  - Print the decision chain: mount → device signature → device-db match → override → final params.

Optional device DB from global server:

- `fro config update`
  - Downloads the device db for the detected devices (run as root, uses nvme list, lspci, lsblk, mdadm, zfs, etc.)
- `fro config default [--json] [--for <file>]`
  - Print the device db config for the given file(s)/mount(s).

### For `fro-optimize`

- `-g/--global`
  - Write to /etc/fro.d/disk-id.json instead of user dir. Run as root.
- `-a/--all`
  - Optimize all accessible mountpoints (find a user-writable dir in each). Write results to ~/.fro/fro.d/disk-id.json.
- `-c/--config <path>`
  - Use this config bundle as the base and write results back into it (or into the user layer it points at).
- `--for <path>` (or reuse `--test-dir`)
  - Optimize specifically for the filesystem/mount containing this path.

### For `fro-benchmark`

- `-c/--config <path>`
  - Benchmark using the resolved config selection logic.

---

## Default config locations (no `-c`)

### Read-side config bundle lookup order

1. `$FRO_CONFIG` (if set)
2. `~/.fro/fro.json` & `~/.fro/fro.d/disk-id.json` (per-user defaults)
3. `/etc/fro.json` & `/etc/fro.d/disk-id.json` (system-wide defaults)

### Device DB lookup order

1. `/etc/fro.d/disk-id.json` (system-provided disk-id mapping + per-disk overrides)
2. `/etc/fro.d/fro-device-db.json` (system-provided device profiles)
3. `~/.config/fro/fro-device-db.json` (user cache of downloaded profiles)

Backward compatibility note: No auto-loading ./fro.json files, use -c fro.json to load local configs.

---

## Config model (new): layering + selectors

### Key idea

Instead of a single `AppConfig`, introduce a **ConfigBundle** with:

- base defaults (same shape as `AppConfig` today)
- per-mount overrides
- optional device-db references

Selection happens at runtime based on the file path and current IO mode.

### Proposed JSON: `~/.fro.json` (bundle)

```jsonc
{
  "version": 1,
  "defaults": {
    "read": { "direct": {"num_threads":16,"block_size":3145728,"qd":2}, "page_cache": {"num_threads":31,"block_size":131072,"qd":1} },
    "grep": { "direct": {"num_threads":16,"block_size":3145728,"qd":2}, "page_cache": {"num_threads":31,"block_size":131072,"qd":1} },
    "write": { "direct": {"num_threads":4,"block_size":262144,"qd":3}, "page_cache": {"num_threads":4,"block_size":1048576,"qd":2} },
    "copy":  { "direct": {"num_threads":4,"block_size":262144,"qd":3}, "page_cache": {"num_threads":4,"block_size":1048576,"qd":2} },
    "diff":  { "direct": {"num_threads":16,"block_size":3145728,"qd":2}, "page_cache": {"num_threads":4,"block_size":131072,"qd":1} },
    "dual_read_bench": { "direct": {"num_threads":16,"block_size":3145728,"qd":2}, "page_cache": {"num_threads":4,"block_size":131072,"qd":1} }
  },

  "mount_overrides": {
    "by_mountpoint": {
      "/mnt/nvme": {
        "read": { "direct": {"num_threads":18,"block_size":2621440,"qd":1} }
      },
      "/data": {
        "grep": { "page_cache": {"num_threads":32,"block_size":131072,"qd":2} }
      }
    }
  },

  "device_db": {
    "paths": [
      "/etc/fro.d/disk-id.json",
      "/etc/fro.d/fro-device-db.json",
      "~/.config/fro/fro-device-db.json"
    ],
    "allow_online_update": false
  }
}
```

Notes:

- Overrides should be sparse: only specify the fields you want to change.
- The resolved config for a run is: defaults → device-db match (optional) → mount override (if present).

---

## Mount/device detection (Linux)

### Resolve the mount point for a path

- Canonicalize the path.
- Parse `/proc/self/mountinfo` and find the *best (longest)* matching mountpoint prefix.
- Extract:
  - mountpoint
  - filesystem type (e.g., `ext4`, `xfs`)
  - mount options
  - the backing device major:minor

### Resolve the backing block device(s)

- Use major:minor to locate `/sys/dev/block/<major>:<minor>` and walk to the underlying block device.
- Handle common stacks:
  - `md` RAID: `/sys/block/md*/slaves/*`
  - `dm-crypt`/LVM: `/sys/block/dm-*/slaves/*`
  - partitions: map `nvme0n1p2` → `nvme0n1`

### Create a stable “device signature”

Goal: build a string that is stable across reboots and portable across machines.

- For single devices: use `/dev/disk/by-id/*` if available.
- For NVMe: include model + serial + firmware + PCI ID + current / max lanes & total GT/s when available from sysfs.
- For RAID/LVM/DM: create a composite signature:
  - RAID level + chunk size (if readable)
  - list of member devices with the above info

Example signature strings:

- `nvme:Samsung_PM1733:SERIAL:FW=...`
- `mdraid0:chunk=512K:[nvme:...][nvme:...][nvme:...][nvme:...]`

Also include filesystem type as part of the match key (because FS can shift optimal block size):

- `(<device_signature>, fstype, mount_opts_subset)`

Mount options: pick a small “meaningful subset” for matching (e.g., `nodiscard`, `noatime`, `barrier`, `data=`) rather than the full raw string.

---

## Device DB: `fro-device-db.json`

### Purpose

A curated database of “good enough defaults” so that most users can skip local optimization.

### Proposed format

```jsonc
{
  "version": 1,
  "profiles": [
    {
      "id": "pm1733-raid0-ext4",
      "match": {
        "device": { "kind": "mdraid", "level": 0, "member_model": "Samsung PM1733" },
        "fstype": "ext4"
      },
      "params": {
        "read": { "direct": {"num_threads":18,"block_size":2621440,"qd":1}, "page_cache": {"num_threads":32,"block_size":131072,"qd":1} },
        "grep": { "direct": {"num_threads":9,"block_size":524288,"qd":6}, "page_cache": {"num_threads":31,"block_size":131072,"qd":2} }
      },
      "metadata": {
        "source": "community",
        "measured_on": "linux",
        "notes": "md RAID0, ext4"
      }
    }
  ]
}
```

Matching rules:

- Support partial matching (e.g., model match without firmware).
- Prefer the “most specific” profile.
- Never execute code from the DB; it’s just JSON data.

Override precedence:

1. mount override in user config
2. device-db matched profile
3. global defaults

---

## Per-mount optimized settings

### What gets keyed by mount

Store per-mount overrides under the mountpoint path (string).

- Pros: simple and debuggable.
- Cons: mountpoint string may change.

Optional improvement:

- Key by a stable mount identifier (UUID/LABEL) when available, but still store the mountpoint string for readability.

### Selection

When running `fro <cmd> ... <file>`:

1. Determine mountpoint for `<file>`.
2. Load config bundle.
3. Determine device signature.
4. Find device-db profile match.
5. Apply mount override.
6. Use final params.

---

## Adaptive test file sizing (optimize/benchmark)

### Goals

- Avoid excessive wear (especially for write-heavy `fro-optimize`).
- Keep each measurement in a reasonable time window.
- Ensure the file is large enough to be representative (avoid “tiny file” artifacts).

### Inputs

- device capacity and free space (via `statvfs`)
- a user-configurable wear budget:
  - max total bytes written per run
  - max drive writes fraction (e.g., 0.02 DW)
- a time budget:
  - target seconds per measurement iteration (e.g., 1–3s)
  - target total runtime
- system RAM size (for page-cache “hot” tests)

### Sizing algorithm

#### v1 (simple, low-risk): wear + free-space based

- Inputs:
  - filesystem size + free space (via `statvfs` on `--test-dir`)
  - estimated number of full-file writes performed by the selected run
    - `fro-benchmark` (full suite): ~13× `test_size` writes, plus the small internal micro-benches
    - `fro-optimize` (full suite): 83× `test_size` writes (setup + optimize loops)
  - file count (1–3 files, depending on which tests are selected)

- Choose `test_size` as the minimum of:
  - `--max-test-size` (default: 4GiB for continuity)
  - a wear cap: `fs_total_bytes * max_drive_writes / num_full_writes`
    - `--max-drive-writes` is a fraction of total capacity written per run (default should be conservative)
  - a free-space cap: keep total allocated temp files well below available space

- Also reduce unnecessary writes:
  - only create the temp files actually required by the selected tests (e.g. `read`-only runs shouldn’t pre-create copy targets).

This version is deterministic and avoids the “probe” phase entirely.

#### v2 (more accurate): probe-based time targeting

1. **Probe phase** (low wear):
   - Create a small file (e.g., 256MiB) on the target mount.
   - Run one `read --direct -n 1` and one `write --direct-write -n 1` to estimate baseline GB/s.

2. **Select size**:
   - Choose `size_for_time = throughput_gbps * target_seconds`.
   - Clamp to `[min_size, max_size]`.
     - `max_size` additionally capped by wear + free space.

3. If still over wear budget, reduce size and/or iterations.

### User controls

Add flags (for both `fro-benchmark` and `fro-optimize`):

- `--test-size <bytes|MiB|GiB>` (force size, disables auto sizing)
- `--max-drive-writes <fraction>`
  - Maximum total bytes written per run as a fraction of filesystem capacity (DW-style budget).
- `--min-test-size <...>`, `--max-test-size <...>`

Optional (v2):

- `--target-seconds <s>` (controls probe-based sizing)

All defaults should be conservative.

---

## Online database workflow (optional)

### Goal

Let users download a device DB so optimization is usually unnecessary.

### Proposed commands

- `fro db update`
  - downloads latest `fro-device-db.json` into `~/.config/fro/`
- `fro db print-match --for <file>`
  - shows what profile would apply
- `fro-optimize --publish` (opt-in)
  - exports a sanitized result blob (no hostnames, no paths)

### Data to publish

- device signature (redacted/hashed serials by default)
- filesystem type and a minimal mount-options subset
- measured results summary
- resulting params for each tool and mode

### Trust and safety

- The DB is only used as defaults; it should not override explicit user config.
- Consider signing releases of the DB (or pin via known repo / tag).

---

## Implementation steps (work breakdown)

### Phase 1: Config bundle + `-c`

- [ ] Introduce `ConfigBundle` and JSON schema versioning.
- [ ] Implement config search paths and `-c/--config` override.
- [ ] Keep legacy `AppConfig` load/save behavior as an internal component.
- [ ] Update `fro`, `fro-optimize`, `fro-benchmark` to accept `-c` and pass it through.

### Phase 2: Mount/device detection

- [ ] Implement mount resolution via `/proc/self/mountinfo`.
- [ ] Implement device signature extraction via sysfs + `/dev/disk/by-id`.
- [ ] Implement composite signatures for `md`/`dm` stacks.

### Phase 3: Selection logic

- [ ] Apply device-db profile matches (optional).
- [ ] Apply mount overrides.
- [ ] Add `fro config explain --for <file>`.

### Phase 4: Optimizer writes per-mount

- [ ] Teach `fro-optimize` to write results into `mount_overrides` for the target mount.
- [ ] Add `--for <path>` (or define `--test-dir` as the target mount selector).

### Phase 5: Adaptive test sizing + wear budget

- [ ] v1: Add deterministic wear/space-based auto sizing logic.
- [ ] v1: Only create temp files needed by the selected tests to reduce writes.
- [ ] v1: Add user flags for size and wear budgets.
- [ ] Update README wear math to reference the adaptive sizing behavior.
- [ ] v2: Add optional probe phase and time-targeting.

### Phase 6: Online DB (optional)

- [ ] Define published schema and matching rules.
- [ ] Create update mechanism (download + cache).
- [ ] Add publish/export tool (opt-in) and document privacy.

### Phase 7: Validation

- [ ] Unit tests for mount parsing and device signature extraction.
- [ ] Golden-file tests for config selection precedence.
- [ ] Manual test matrix: single NVMe, md RAID0, dm-crypt, tmpfs, network FS (should not crash; should fall back).

### Additional

- [ ] Separate presets for cold / hot page cache.
- [ ] Estimate max performance for the mounts and compare to achieved.
- [ ] Store max performance achieved for mounts.
- [ ] Use max performance to decide which IO path to take (zfs may have slow hot page cache perf compared to direct IO due to ARC.)

---

## Open questions

- [x] Should `./fro.json` ever be auto-loaded, or only via explicit `-c`?
    - Only via explicit `-c`.
- [x] Should mount overrides key by mountpoint string, filesystem UUID/LABEL, or both?
    - Filesystem UUID primarily. If only mountpoint string is defined, use that.
- [x] How conservative should the default drive-write budget be?
    - 0.05 DPWD (ok to do at least 20 optimize runs per day on 1 DPWD drive.)
- [x] How should `fro` behave on filesystems where direct I/O is unsupported or unreliable? 
    - Use non-direct I/O. Flag to user if --direct specified.

## Compat work

  - [x] `cat`
    - [x] `-A`, `--show-all`
      - [x] equality test
      - [x] implementation
    - [x] `-b`, `--number-nonblank`
      - [x] equality test
      - [x] implementation
    - [x] `-e`
      - [x] equality test
      - [x] implementation
    - [x] `-E`, `--show-ends`
      - [x] equality test
      - [x] implementation
    - [x] `-n`, `--number`
      - [x] equality test
      - [x] implementation
    - [x] `-s`, `--squeeze-blank`
      - [x] equality test
      - [x] implementation
    - [x] `-t`
      - [x] equality test
      - [x] implementation
    - [x] `-T`, `--show-tabs`
      - [x] equality test
      - [x] implementation
    - [x] `-u`
      - [x] equality test
      - [x] implementation
    - [x] `-v`, `--show-nonprinting`
      - [x] equality test
      - [x] implementation
    - [x] `-b`, `--print-bytes`
      - [x] equality test
      - [x] implementation
    - [x] `-i`, `--ignore-initial=SKIP`
      - [x] equality test
      - [x] implementation
    - [x] `-i`, `--ignore-initial=SKIP1:SKIP2`
      - [x] equality test
      - [x] implementation
    - [x] `-l`, `--verbose`
      - [x] equality test
      - [x] implementation
    - [x] `-n`, `--bytes=LIMIT`
      - [x] equality test
      - [x] implementation
    - [x] `-s`, `--quiet`, `--silent`
      - [x] equality test
      - [x] implementation
    - [x] `-F`, `--fixed-strings`
      - [x] equality test
      - [x] implementation
    - [x] `-n`, `--line-number`
      - [x] equality test
      - [x] implementation
    - [x] `-b`, `--binary`
      - [x] equality test
      - [x] implementation
    - [x] `-c`, `--check`
      - [x] equality test
      - [x] implementation
    - [x] `--tag`
      - [x] equality test
      - [x] implementation
    - [x] `-t`, `--text`
      - [x] equality test
      - [x] implementation
    - [x] `-z`, `--zero`
      - [x] equality test
      - [x] implementation
    - [x] `--ignore-missing`
      - [x] equality test
      - [x] implementation
    - [x] `--quiet`
      - [x] equality test
      - [x] implementation
    - [x] `--status`
      - [x] equality test
      - [x] implementation
    - [x] `--strict`
      - [x] equality test
      - [x] implementation
    - [x] `-w`, `--warn`
      - [x] equality test
      - [x] implementation
- [x] cksum, b2sum, md5sum, sha*sum
  - [x] cksum
  - [x] sha224sum / sha256sum / sha384sum / sha512sum
  - [x] b3sum
  - [x] b2sum
  - [x] md5sum
- [x] shred (this is basically write)
- [x] wc
- [x] head
  - [x] `-c` fast path for regular files and regular stdin
  - [x] suffixed counts like `1KiB`, `1MiB`, `1GiB`
- [x] cat / tac
- [x] pv that's a hugepages splice + print to stderr
- [x] find, as part of dirwalk work
- [x] Read sequentially
- [x] Write sequentially
