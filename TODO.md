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
- [x] Only create temp files needed by the selected benchmark/optimization modes.
- [ ] Add CLI knobs for min/max test size and drive-write budget.
- [ ] Optionally add probe-based time targeting after the deterministic sizing work lands.

### Validation and coverage

- [ ] Add unit coverage for mount parsing and device signature extraction.
- [ ] Add golden-file tests for config selection precedence and mount override behavior.
- [ ] Build a manual validation matrix for single NVMe, md RAID0, dm-crypt, tmpfs, and network filesystems.

### I/O correctness and verification

- [x] Add checked arithmetic and checked `u64`→`usize` conversions for block/offset math in `reader`, `stream`, and writer offset paths, then test overflow boundaries explicitly.
- [x] Treat short `io_uring` reads in the read/map/load paths as retry-or-error conditions instead of accepting partial CQE results as complete logical blocks.
- [x] Decide and document the contract for sparse offset writes: either enforce completeness for fixed-size outputs or explicitly preserve "gaps are caller-defined" semantics and test that model.
- [ ] Make direct-I/O fallback observable and testable so `--direct` users can tell when unsupported filesystems or unaligned tails silently took the page-cache path.
- [ ] Define the durability contract for high-level write APIs (`flush` vs `sync`) and add explicit tests or APIs for the promised level.
- [ ] Add explicit sync policies for library and CLI writes (for example `fsync` default with optional `nosync`), and test file plus parent-directory sync semantics for create/replace/rename flows.
- [x] Design and implement a verified write/copy mode that stages `hash -> copy -> fsync -> recover/repair if needed -> fsync -> optional verify`, with a documented success/failure contract.
- [ ] Add property-based model tests for read partitioning, indexed/offset writer ordering, copy equivalence, and hash/verify/recover invariants. Initial property coverage now includes read partitioning reconstruction and offset-writer reference-model checks.
- [ ] Add fuzz targets for config/manifest parsing plus model-based fuzzing of read/write/copy/hash orchestration on small files.
- [ ] Run `cargo miri test` regularly on the unsafe buffer/slice paths and add bounded-proof experiments (Kani) for arithmetic and partition helpers. Targeted Miri-safe tests now cover `common::AlignedBuffer` page-backed storage and `reader` destination-slice construction; executing them still requires a nightly toolchain with the `miri` component installed. The first Kani slice now proves `io_util::expected_read_len()` matches its `min(file_size - offset, block_size)` contract, rejects offsets past EOF, and is monotonic in offset.
- [ ] Build a real-world compatibility matrix over file kind, access surface, and permission mode; run the meaningful Cartesian-product cases and assert documented success/failure behavior for each. The first automated slice now covers local `read_file`, `write_file`, and `copy_file` API behavior for regular files, directories, symlinks, and permission-gated paths on ordinary local temp-directory filesystems.

### Follow-on tuning ideas

- [ ] Separate presets for cold vs hot page cache.
- [ ] Estimate per-mount maximum performance and compare achieved throughput against it.
- [ ] Store measured maximum performance per mount and use it to inform IO-path selection.
- [ ] Application-level tuning with the optimizer (e.g. you have a database that uses fro as the I/O library, you'd drop your I/O hot paths into optimizer as part of the install process to find optimal settings for the hardware.)

### Dirwalk optimization

- [ ] Fast traversal of every byte in a directory tree.
  - `find` is **very** fast, but even faster when run as multiple instances on subtrees (find dir/s1 & find dir/s2 & find dir/s3 ...)
  - Look how rg does it for inodes?
  - Would kinda like if directory trees could be GC'd into contiguous bags of bytes in memory and copied over with a seq-read + rewrite inode ids + seq-write.
- [ ] Idea: sort files by block inode to get a more sequential access pattern. 
- [ ] Idea: keep nearby-on-media files in the same thread, pin threads to cores (each core manages an area of memory -> higher cache hit rate).
- [ ] Idea: small files bundled into processing bundles for efficient batching, large files dealt with separately (while large file data is streaming, small file inodes are streaming).
- [ ] Process subtrees with io_uring by doing multi-tree parallel DFS (threads start traversing at nearest-to-root non-claimed subtree, claim it, add found dirs to "to-process" stack.)

## Open questions

- [x] Should `./fro.json` ever be auto-loaded, or only via explicit `-c`?
    - Only via explicit `-c`.
- [x] Should mount overrides key by mountpoint string, filesystem UUID/LABEL, or both?
    - Filesystem UUID primarily. If only mountpoint string is defined, use that.
- [x] How conservative should the default drive-write budget be?
    - 0.05 DPWD (ok to do at least 20 optimize runs per day on 1 DPWD drive.)
- [x] How should `fro` behave on filesystems where direct I/O is unsupported or unreliable? 
    - Use non-direct I/O. Flag to user if --direct specified.
