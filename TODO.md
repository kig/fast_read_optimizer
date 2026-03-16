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

## Open questions / decisions needed

- Should `./fro.json` in the current directory ever be auto-loaded, or only via `-c`?
- Should mount overrides key by mountpoint string, filesystem UUID/LABEL, or both?
- How conservative should the default wear budget be (bytes written per optimize run)?
- For page-cache “hot” tests, should we cap test size relative to RAM (to avoid false “hot”)?
- How do we want to handle devices where direct IO is unsupported or unreliable?

