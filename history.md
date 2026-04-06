# History

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
