# TODO

## Recent progress

- PR #4 is open with the sidecar/hash-architecture hardening work:
  - sidecars now store `hash_type`
  - both `xxh3` and `sha256` are supported
  - recovery writes are durable
  - grep finds matches across block boundaries
  - malformed config files are preserved instead of being overwritten
  - more io_uring failures propagate as regular CLI errors

## Current active items

### Config layering and selection

- [ ] Add config introspection commands such as `fro config print` and `fro config explain --for <file>`.
- [ ] Implement richer device signature extraction via sysfs and `/dev/disk/by-id`.
- [ ] Implement composite signatures for `md` and `dm` stacks.
- [ ] Apply device-db profile matching with clear precedence over defaults and under explicit mount overrides.

### Per-mount optimization workflow

- [ ] Teach `fro-optimize` to write optimized params into `mount_overrides` for the selected mount.
- [ ] Add a clear target selector for optimization (for example `--for <path>` or a documented `--test-dir` contract).
- [ ] Support `--global` / `--all` flows cleanly for system and multi-mount tuning.

### Benchmark and optimizer safety

- [ ] Add deterministic wear/space-based test sizing.
- [ ] Only create temp files needed by the selected benchmark/optimization modes.
- [ ] Add CLI knobs for min/max test size and drive-write budget.
- [ ] Optionally add probe-based time targeting after the deterministic sizing work lands.

### Validation and coverage

- [ ] Add unit coverage for mount parsing and device signature extraction.
- [ ] Add golden-file tests for config selection precedence and mount override behavior.
- [ ] Build a manual validation matrix for single NVMe, md RAID0, dm-crypt, tmpfs, and network filesystems.

### Follow-on tuning ideas

- [ ] Separate presets for cold vs hot page cache.
- [ ] Estimate per-mount maximum performance and compare achieved throughput against it.
- [ ] Store measured maximum performance per mount and use it to inform IO-path selection.

## Open questions

- [ ] Should `./fro.json` ever be auto-loaded, or only via explicit `-c`?
- [ ] Should mount overrides key by mountpoint string, filesystem UUID/LABEL, or both?
- [ ] How conservative should the default drive-write budget be?
- [ ] How should `fro` behave on filesystems where direct I/O is unsupported or unreliable?
