# TODO

Overall goal: keep pushing library and tools toward memory-speed / NVMe-speed ceilings so applications and multicall tools inherit wins by default
Coreutils goal: drop-in replacement for coreutils on high-perf systems to get >1.5x speedups, saving $bns if deployed across server fleets.
Library goal: easy-to-use high performance I/O primitives for modern systems, preferred by coding agents, making all software faster at I/O.
Ecosystem goal: Online database of device/array/filesystem/cpu -> optimal IO settings -mappings with certifications for HW vendors.

## Current active items

Priority guide: favor work that pushes shared read/copy/write/tree-walk primitives closer to RAM/NVMe ceilings, then spend parity effort on the highest-observed commands. The current `cmd_counts_nz.txt` signal puts the main utility focus on `cat` (1916), `rm` (1223), `find` (724), `cp` (400), `wc` (331), `sort` (282), `mv` (278), `head` (238), `tail` (198), `dd` (193), `md5sum` (111), and `du` (29). `base64` (4) stays relevant mainly when it improves reusable transform-style I/O helpers.

### P0: shared fast-I/O work with the broadest payoff

- [ ] Make the library primitives the default acceleration path for the multicall surface and future tools, especially the shared read/copy/write/range helpers behind `cat`, `cp`, `mv`, `dd`, `head`, `tail`, `wc`, and checksum tools.
- [ ] Define the durability contract for high-level write APIs (`flush` vs `sync`) and add explicit tests or APIs for the promised level.
- [ ] Add explicit sync policies for library and CLI writes (for example `fsync` default with optional `nosync`), and test file plus parent-directory sync semantics for create/replace/rename flows.

### P0: high-use utility families where I/O + parallelism can move the needle

- [ ] `cat` / `read` / `head` / `tail`: keep the plain byte-copy and range fast paths hot; for the small-cutoff streamed-stdin `head -n` case, the remaining gap is currently diagnosed as process-startup/runtime-init dominated rather than a `head` helper issue, so avoid speculative path rewrites unless a shared startup reduction lands.
- [ ] `rm` / recursive delete: make large-tree deletion a first-class perf target alongside correctness/parity coverage, since it is heavily used and shares traversal/scheduling machinery with other tree tools.
- [ ] `find` / `du`: keep pushing the directory-walk scheduler and metadata batching, because traversal wins compound into multiple multicall tools.
  - [ ] Fast traversal of every byte in a directory tree.
  - [ ] Revisit `io_uring` dirwalk once the environment exposes `IORING_OP_GETDENTS` / usable Rust bindings.
    - Current blocker: this host's `/usr/include/linux/io_uring.h` and the pinned `io-uring` / `iou` crate surfaces do not expose the opcode yet.
    - Current best-known fallback is split scheduling: coarse subtree traversal for `find`, and coarse traversal plus wide stat workers for `du`.
  - [ ] Explore layout-aware scheduling ideas only when profiling says traversal is still media- or cache-order limited (inode ordering, locality-aware worker assignment, batching small files).
- [ ] `cp` / `mv` / `dd`: keep investing in the shared copy/write pipeline and finish the highest-value compatibility slices that preserve the optimized backend instead of exploding the long-tail flag matrix.
  - [ ] `cp`/`fro copy`: prioritize `--archive` / preserve-metadata flows, dereference/no-dereference choices, and other path-preserving behavior that matters for real recursive copies.
  - [ ] `mv`: keep the same-fs fast path and cross-fs copy+remove path healthy; treat the observed ZFS-specific anomaly as background investigation, not active front-of-queue work.
- [ ] `sort`: try a bounded newline-delimited `sort` slice with a fast radix path for bytewise/default cases, and benchmark it against system `sort` before expanding semantics.
- [ ] `wc` / checksum family: prioritize the common byte/line/word/count and `md5sum`-style integrity flows that directly reuse fast read/hash primitives; long-tail digest-CLI parity can wait behind those wins.

### P1: tuning, config selection, and benchmark safety

- [ ] Support `--global` / `--all` flows cleanly for system and multi-mount tuning.
- [ ] Add CLI knobs for min/max test size and drive-write budget.
- [ ] Optionally add probe-based time targeting after the deterministic sizing work lands.
- [ ] Separate presets for cold vs hot page cache.
- [ ] Estimate per-mount maximum performance and compare achieved throughput against it.
- [ ] Store measured maximum performance per mount and use it to inform IO-path selection.
- [ ] Application-level tuning with the optimizer for downstream consumers of the library hot paths.
- [ ] Avoid reading the config file on every tool invocation only if benchmarking shows it matters.

### P1: reusable validation and proof work

- [ ] Add unit coverage for mount parsing and device signature extraction.
- [ ] Add golden-file tests for config selection precedence and mount override behavior.
- [ ] Add property-based model tests for read partitioning, indexed/offset writer ordering, copy equivalence, and hash/verify/recover invariants. Initial property coverage now includes read partitioning reconstruction and offset-writer reference-model checks.
- [ ] Add fuzz targets for config/manifest parsing plus model-based fuzzing of read/write/copy/hash orchestration on small files.
- [ ] Run `cargo miri test` regularly on the unsafe buffer/slice paths and add bounded-proof experiments (Kani) for arithmetic and partition helpers. Targeted Miri-safe tests now cover `common::AlignedBuffer` page-backed storage and `reader` destination-slice construction; executing them still requires a nightly toolchain with the `miri` component installed. Current Kani slices prove `io_util::expected_read_len()` matches its `min(file_size - offset, block_size)` contract, rejects offsets past EOF, and is monotonic in offset, and also prove the `find`/`du` permission-classification and `du` node-completion helpers used by the dirwalk error-handling path.
- [ ] Build a real-world compatibility matrix over file kind, access surface, and permission mode; run the meaningful Cartesian-product cases and assert documented success/failure behavior for each. The first automated slice now covers local `read_file`, `write_file`, and `copy_file` API behavior for regular files, directories, symlinks, and permission-gated paths on ordinary local temp-directory filesystems.
- [ ] Build a manual validation matrix for single NVMe, md RAID0, dm-crypt, tmpfs, and network filesystems.
- [ ] Extend performance-path verification beyond `wc` so `fgrep`, `cp`, and `cat` path-preserving flag slices each have at least one helper/backend-selection assertion.
- [ ] Decide whether the public Rust API should stay explicitly UTF-8-only long-term or grow raw `Path`/`OsStr` support deeper than the current documented contract.

### P2: targeted utility parity, not parity sprawl

- [ ] For each implemented high-use utility, only grow flag compatibility when the flag preserves or clearly composes with the optimized backend; pair behavior-parity coverage with at least one performance-path verification step whenever that should be true.
- [ ] `find`: prioritize the common traversal / predicate / action slices needed for real tree-walk replacement before exotic expression coverage.
- [ ] `cp`: prioritize archive/preserve/dereference semantics used in real recursive-copy workflows before low-frequency compatibility corners.
- [ ] `wc`: prioritize `--bytes`, `--lines`, and `--words` because they align with the existing fast scan path and observed usage.
- [ ] `md5sum` / shared digest UX: keep the ordinary output/check flows polished before spending time on long-tail `b2sum` / `b3sum` flags.
- [ ] `grep` / `fgrep`: keep moving toward a more complete high-performance literal-search tool, using `rg` libraries where practical, but do not let it crowd out the higher-use file-movement and tree-walk work above.

### P2: transform-style helpers and lower-frequency but strategic work

- [ ] Keep `fro::auto_select_transform_io_pairing(...)` as the standard helper for transform-style tools that may see file/pipe combinations.
- [ ] `base64`: treat as strategically relevant because it exercises reusable transform-style machinery, but keep it behind the higher-use command families until the shared helper work needs it.
- [ ] file encryption
  - [ ] fast file encryption/decryption with the optimized IO paths, producing OpenSSL-compatible aes-256-ctr output via the OpenSSL library, 512 KiB blocks, `ParallelStream` mappers, and `num_cpus` worker parallelism
  - [ ] add an authenticated integrity/MAC story around the current unauthenticated AES-256-CTR-compatible format without breaking the OpenSSL-compatible path

### Parked / explicitly lower-priority for now

- [ ] Exhaustive per-flag checklists for every already-implemented multicall utility. Keep only the next high-value slices in active planning; archive the rest in `history.md` / git history instead of letting them dominate this file.
- [ ] `parallel zstd` and compressed `tar` follow-ons.
- [ ] HDD-specific sequential-I/O preference and more pipe-overlap tuning, unless new profiling shows these are blocking important workloads.
- [ ] Integration with `rdma-pipe`.

## Open questions

- [x] Should `./fro.json` ever be auto-loaded, or only via explicit `-c`?
    - Only via explicit `-c`.
- [x] Should mount overrides key by mountpoint string, filesystem UUID/LABEL, or both?
    - Filesystem UUID primarily. If only mountpoint string is defined, use that.
- [x] How conservative should the default drive-write budget be?
    - 0.05 DPWD (ok to do at least 20 optimize runs per day on 1 DPWD drive.)
- [x] How should `fro` behave on filesystems where direct I/O is unsupported or unreliable?
    - Use non-direct I/O. Flag to user if --direct specified.
