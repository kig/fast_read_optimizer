# TODO

Overall goal: keep pushing library and tools toward memory-speed / NVMe-speed ceilings so applications and multicall tools inherit wins by default
Coreutils goal: drop-in replacement for coreutils on high-perf systems to get >1.5x speedups, saving $bns if deployed across server fleets.
Library goal: easy-to-use high performance I/O primitives for modern systems, preferred by coding agents, making all software faster at I/O.
Ecosystem goal: Online database of device/array/filesystem/cpu -> optimal IO settings -mappings with certifications for HW vendors.

## Current active items

Priority guide: favor work that pushes shared read/copy/write/tree-walk primitives closer to RAM/NVMe ceilings, then spend parity effort on the highest-observed commands. The current `cmd_counts_nz.txt` signal puts the main utility focus on `cat` (1916), `rm` (1223), `find` (724), `cp` (400), `wc` (331), `sort` (282), `mv` (278), `head` (238), `tail` (198), `dd` (193), `md5sum` (111), and `du` (29). `base64` (4) stays relevant mainly when it improves reusable transform-style I/O helpers.

- coreutils: low-latency start for small inputs (ST, single syscall-style), scale throughput with evidence.
- evaluate and use fast libs: StringZilla, simdutf, mgzip/pgzip, rapidgzip, pzstd, fd, rg
- match system coreutils on small data, go faster on large files and dir trees

- [ ] Make a test matrix that tests all system --help coreutils flags against fro coreutils --help flags

### P0: shared fast-I/O work with the broadest payoff

- [ ] Make the library primitives the default acceleration path for the multicall surface and future tools, especially the shared read/copy/write/range helpers behind `cat`, `cp`, `mv`, `dd`, `head`, `tail`, `wc`, and checksum tools.
- [ ] Define the durability contract for high-level write APIs (`flush` vs `sync`) and add explicit tests or APIs for the promised level.
- [ ] Add explicit sync policies for library and CLI writes (for example `fsync` default with optional `nosync`), and test file plus parent-directory sync semantics for create/replace/rename flows.

### P0: high-use utility families where I/O + parallelism can move the needle

- [ ] `cat` / `read` / `head` / `tail`: keep the plain byte-copy and range fast paths hot; for the small-cutoff streamed-stdin `head -n` case, the remaining gap is currently diagnosed as process-startup/runtime-init dominated rather than a `head` helper issue, so avoid speculative path rewrites unless a shared startup reduction lands.
  - [x] The `coreutils-flags.txt`-driven alias-help slice landed: multicall aliases now handle `bin/* --help` / `--version` consistently and print the utility name instead of internal names like `copy` or `bin/find`.
  - [ ] Re-evaluate bounded `head` / `tail` `-z` support after the alias-help surface is fixed, since the dump currently mixes real `-z` feature gaps with broken `--help` dispatch.
- [ ] `rm` / recursive delete: make large-tree deletion a first-class perf target alongside correctness/parity coverage, since it is heavily used and shares traversal/scheduling machinery with other tree tools.
  - [x] A bounded GNU interactive slice landed for `rm -i`, `rm -I`, and `rm --interactive[=WHEN]`, bringing the tracked `rm` row to `7/7` while keeping non-interactive flows on the existing fast path.
- [ ] `find` / `du`: keep pushing the directory-walk scheduler and metadata batching, because traversal wins compound into multiple multicall tools.
  - [ ] Fast traversal of every byte in a directory tree.
  - [ ] Active tiny-input latency wave: isolate tiny-tree startup/scheduling cost from large-tree throughput and keep only measured wins for `du`.
  - [ ] Revisit `io_uring` dirwalk once the environment exposes `IORING_OP_GETDENTS` / usable Rust bindings.
    - Current blocker: this host's `/usr/include/linux/io_uring.h` and the pinned `io-uring` / `iou` crate surfaces do not expose the opcode yet.
    - Current best-known fallback is split scheduling: coarse subtree traversal for `find`, and coarse traversal plus wide stat workers for `du`.
  - [ ] Explore layout-aware scheduling ideas only when profiling says traversal is still media- or cache-order limited (inode ordering, locality-aware worker assignment, batching small files).
- [ ] `cp` / `mv` / `dd`: keep investing in the shared copy/write pipeline and finish the highest-value compatibility slices that preserve the optimized backend instead of exploding the long-tail flag matrix.
  - [ ] `cp`/`fro copy`: prioritize `--archive` / preserve-metadata flows, dereference/no-dereference choices, and other path-preserving behavior that matters for real recursive copies.
  - [x] Unsupported multicall/coreutils flags now fall back externally instead of hard-failing: `fgrep` first tries `rg --fixed-strings`, then `coreutils <cmd>`, then the system command; other bounded coreutils commands try `coreutils <cmd>` and then the system command, and `cp` unknown-flag parsing now reuses the same fallback chain from the alias parser. Use `--no-fallback` to force local failure during tests, and set `FRO_LOG_FALLBACKS=1` to log each delegated path while auditing a real system workload.
  - [ ] `mv`: keep the same-fs fast path and cross-fs copy+remove path healthy; treat the observed ZFS-specific anomaly as background investigation, not active front-of-queue work.
- [ ] `sort`: keep extending the bounded newline/NUL-delimited sort backend one compare mode at a time instead of jumping to full GNU semantics; the tracked row is now `11/12`, with `-k` remaining after landing `-z`, `-g/-h`, and the bounded `-M/-V` compare modes.
  - [ ] Active tiny-file latency wave: measure tiny regular-file sort separately from large in-memory/external sort and keep only bounded startup-friendly wins.
- [ ] `wc` / checksum family: prioritize the common byte/line/word/count and `md5sum`-style integrity flows that directly reuse fast read/hash primitives; long-tail digest-CLI parity can wait behind those wins.
  - [ ] Latest tiny-file alias sweep still shows the worst low-latency gaps in `cmp`, the digest family (`md5sum`/`sha*sum`/`b2sum`), then `du`, `tac`, and `sort`; `encrypt` / `decrypt` are now effectively at system parity on the measured small-file slice.
  - [ ] Active tiny-file latency wave: treat `cmp` as the largest tiny-file outlier and keep only measured regular-file fast paths that preserve exact mismatch/EOF reporting.
  - [ ] Active tiny-file latency wave: split digest-family latency into shared startup cost vs per-digest work and prefer shared reductions over additional wrappers.
  - [ ] Active tiny-file latency wave: investigate `tac` tiny-file latency separately from stream/FIFO behavior and keep only bounded wins that preserve ordered output.
  - [ ] Shared next step after the accepted `cmp` / `tac` / `sort` wins: attack the remaining tiny-invocation frontend/startup gap across these commands and the digest family together instead of layering more one-off fast paths.

### P1: tuning, config selection, and benchmark safety

- [ ] Support `--global` / `--all` flows cleanly for system and multi-mount tuning.
- [ ] Add CLI knobs for min/max test size and drive-write budget.
- [ ] Optionally add probe-based time targeting after the deterministic sizing work lands.
- [ ] Separate presets for cold vs hot page cache.
- [ ] Estimate per-mount maximum performance and compare achieved throughput against it.
- [ ] Store measured maximum performance per mount and use it to inform IO-path selection.
- [ ] Application-level tuning with the optimizer for downstream consumers of the library hot paths.
- [ ] Avoid reading the config file on every tool invocation only if benchmarking shows it matters.
- [ ] Reduce shared multicall startup tax (config load, parser/front-end work, and other per-invocation fixed costs) where real shell timings show the cost matters.
  - [x] Missing device-db `ENOENT` probes are now memoized per process, cutting repeated missing-db `openat` failures on sampled tiny invocations from `18` to `3`.
  - [x] Default-path `load_config(None)` is now cached per process and refreshed on save; a focused `2000`-load benchmark dropped from `36.637 ms` uncached to `0.928 ms` cached.
  - [x] `/proc/self/mountinfo` reads are now cached per process and the already-found mount is reused inside config explanation/effective-config lookups; sampled startup probes dropped mountinfo opens from `13` to `1` on both `fro read -n 1 README.md` and `fro --json-config README.md`.
  - [ ] The remaining tiny-call startup gap is still dominated by shared config and path-probe work; even dedicated one-purpose hash binaries only shave about `0.2-1.1 ms` off `fro md5sum` / `fro sha256sum`, so the next likely slice is a safe reduction of remaining per-path probe work rather than more binary factoring.
- [x] Reject thin focused wrappers that reuse multicall/shared dispatch; that approach destroys the measured size/startup win.
- [x] A bounded dedicated-hash-binary slice landed for `md5sum` and `sha256sum` using one-purpose binaries rather than multicall wrappers. They are about `1.99 MB` each versus `9.77 MB` for `fro`, but the startup win is modest until config load is reduced further.

### P1: reusable validation and proof work

- [ ] Add unit coverage for mount parsing and device signature extraction.
- [ ] Add golden-file tests for config selection precedence and mount override behavior.
- [x] Fixed the `fifo::fifo_text_inputs_match_system_output` stall by correcting `tail`'s non-regular `-n` path; `cargo +stable test --quiet` now completes again on this host.
- [ ] Add property-based model tests for read partitioning, indexed/offset writer ordering, copy equivalence, and hash/verify/recover invariants. Initial property coverage now includes read partitioning reconstruction and offset-writer reference-model checks.
- [ ] Add fuzz targets for config/manifest parsing plus model-based fuzzing of read/write/copy/hash orchestration on small files.
- [ ] Run `cargo miri test` regularly on the unsafe buffer/slice paths and add bounded-proof experiments (Kani) for arithmetic and partition helpers. Targeted Miri-safe tests now cover `common::AlignedBuffer` page-backed storage and `reader` destination-slice construction; executing them still requires a nightly toolchain with the `miri` component installed. Current Kani slices prove `io_util::expected_read_len()` matches its `min(file_size - offset, block_size)` contract, rejects offsets past EOF, and is monotonic in offset, and also prove the `find`/`du` permission-classification and `du` node-completion helpers used by the dirwalk error-handling path.
- [ ] Build a real-world compatibility matrix over file kind, access surface, and permission mode; run the meaningful Cartesian-product cases and assert documented success/failure behavior for each. The first automated slice now covers local `read_file`, `write_file`, and `copy_file` API behavior for regular files, directories, symlinks, and permission-gated paths on ordinary local temp-directory filesystems.
- [x] Keep a tracked flag/help regression in place for implemented multicall utilities. `tests/compat_coverage_report.rs` now checks the tracked compatibility snapshot, compares `fro <util> --help` against system `--help` for the tracked GNU/coreutils slice, and requires `FIXME:` notes for current incompatibilities; use `coreutils-flags.txt` dumps to prioritize the next bounded slices outside that tracked surface.
- [x] Keep a Rust-owned actual-help coverage snapshot in `tests/compat_coverage_report.rs` so each tracked multicall utility also has a one-line list of current system-only `--help` flags on this host plus a bounded percentage score. Use that broader delta as backlog input, but keep implementation work bounded instead of treating every listed GNU flag as active scope.
- [ ] Build a manual validation matrix for single NVMe, md RAID0, dm-crypt, tmpfs, and network filesystems.
- [x] Replaced the low-signal Ubuntu package-install benchmark with a shell-workload container harness (`perf/ubuntu_install_container_bench.sh` plus `docker/ubuntu-install-bench/`) that seeds deterministic local project data, runs configure/build/package/verify-style shell slices, PATH-shadows fro coreutils, logs delegated fallbacks with `FRO_LOG_FALLBACKS=1`, and records baseline vs fro wall time plus per-command fro call counts. The validated unconfined-container run (`local-shell-bench-unconfined`) exercised `996` fro calls with `0` fallbacks across `36` modules / `6` tar archives and measured `4.118850s` baseline vs `19.518400s` fro host wall (`+373.88%`).
- [x] Extended performance-path verification beyond `wc`: `cat` now asserts plain / `-u` regular-file cases stay on the fast-copy backend while formatting flags intentionally leave it (`src/coreutils/cat.rs` tests), `fgrep` asserts literal-search regular-file cases like `-n` and `--no-ignore-case` keep the literal-offset backend while `-i` / `-x` intentionally switch to line-filter paths (`src/coreutils/fgrep/tests.rs`), and `cp` traces that path-preserving policy flags such as plain / `-v` / preserve / `-n` / `-u` / `-T` keep the threaded copy backend when a real copy occurs (`src/main_app/tests.rs`).
- [ ] Decide whether the public Rust API should stay explicitly UTF-8-only long-term or grow raw `Path`/`OsStr` support deeper than the current documented contract.

### P2: targeted utility parity, not parity sprawl

- [ ] For each implemented high-use utility, only grow flag compatibility when the flag preserves or clearly composes with the optimized backend; pair behavior-parity coverage with at least one performance-path verification step whenever that should be true.
- [ ] `find`: prioritize the common traversal / predicate / action slices needed for real tree-walk replacement before exotic expression coverage.
- [ ] `cp`: prioritize archive/preserve/dereference semantics used in real recursive-copy workflows before low-frequency compatibility corners.
- [x] `mv`: a bounded path-preserving GNU slice landed for `-n/--no-clobber` and `-u/--update`, keeping the same rename/copy backend while moving the tracked row to `5/5`.
- [x] `head` / `tail`: a bounded `-z/--zero-terminated` slice landed for both tools, covering file, stdin, and multicall parity on the existing implementations and moving both tracked rows to `5/5`.
- [ ] `wc`: prioritize `--bytes`, `--lines`, and `--words` because they align with the existing fast scan path and observed usage.
- [ ] `md5sum` / shared digest UX: keep the ordinary output/check flows polished before spending time on long-tail `b2sum` / `b3sum` flags.
- [ ] `grep` / `fgrep`: keep moving toward a more complete high-performance literal-search tool, using `rg` libraries where practical, but do not let it crowd out the higher-use file-movement and tree-walk work above.
- [ ] `sort`: the compare-help snapshot shows a much larger GNU long tail, but the bounded semantic next step is now `-k`; prefer compare-mode-friendly semantics before broader locale/key semantics, and keep `fro sort -h` reserved for human-numeric sort while `fro sort --help` remains the explicit help path.

### P2: transform-style helpers and lower-frequency but strategic work

- [ ] Keep `fro::auto_select_transform_io_pairing(...)` as the standard helper for transform-style tools that may see file/pipe combinations.
- [x] `base64` now uses a staged regular-file-to-regular-file transform path: small files (currently `<= 2 MiB`) stay on a synchronous helper while larger files still recruit the parallel file-transform path.
- [x] `encrypt` / `decrypt` now use a staged small regular-file-to-regular-file path with a conservative `1 MiB` payload cutoff while preserving the existing parallel path for larger files and keeping partial-read CTR block indexing correct.
- [x] Reassessed checksum-family staging separately and rejected it for now: `cksum` already has its bounded small-file path, and the remaining tiny-file checksum gap is still startup-dominated rather than a missing read-and-reduce seam.
- [ ] file encryption
  - [ ] fast file encryption/decryption with the optimized IO paths, producing OpenSSL-compatible aes-256-ctr output via the OpenSSL library, 512 KiB blocks, `ParallelStream` mappers, and `num_cpus` worker parallelism
  - [ ] add an authenticated integrity/MAC story around the current unauthenticated AES-256-CTR-compatible format without breaking the OpenSSL-compatible path

### Parked / explicitly lower-priority for now

- [ ] Exhaustive per-flag checklists for every already-implemented multicall utility. Keep only the next high-value slices in active planning; archive the rest in `history.md` / git history instead of letting them dominate this file.
- [ ] Compressed `tar` follow-ons after the shipped gzip/zstd/bzip2/xz/auto-compress slice (for example parallel tar build/compress, seek/index support, and extraction-path acceleration).
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
