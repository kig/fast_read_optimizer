# TODO

## Current active items

### Config layering and selection

- [ ] Add config introspection commands such as `fro config print` and `fro config explain --for <file>`.
- [ ] Implement richer device signature extraction via sysfs and `/dev/disk/by-id`.
- [ ] Implement composite signatures for `md` and `dm` stacks.
- [x] Apply device-db profile matching with clear precedence over defaults and under explicit mount overrides.

### Per-mount optimization workflow

- [ ] Teach `fro-optimize` to write optimized params into `mount_overrides` for the selected mount.
- [ ] Add a clear target selector for optimization (for example `--for <path>` or a documented `--test-dir` contract).
- [ ] Support `--global` / `--all` flows cleanly for system and multi-mount tuning.

### Benchmark and optimizer safety

- [ ] Add deterministic wear/space-based test sizing.
- [ ] Add CLI knobs for min/max test size and drive-write budget.
- [ ] Optionally add probe-based time targeting after the deterministic sizing work lands.

### Validation and coverage

- [ ] Add unit coverage for mount parsing and device signature extraction.
- [ ] Add golden-file tests for config selection precedence and mount override behavior.
- [ ] Build a manual validation matrix for single NVMe, md RAID0, dm-crypt, tmpfs, and network filesystems.

### I/O correctness and verification

- [ ] Make direct-I/O fallback observable and testable so `--direct` users can tell when unsupported filesystems or unaligned tails silently took the page-cache path.
- [ ] Define the durability contract for high-level write APIs (`flush` vs `sync`) and add explicit tests or APIs for the promised level.
- [ ] Add explicit sync policies for library and CLI writes (for example `fsync` default with optional `nosync`), and test file plus parent-directory sync semantics for create/replace/rename flows.
- [ ] Add property-based model tests for read partitioning, indexed/offset writer ordering, copy equivalence, and hash/verify/recover invariants. Initial property coverage now includes read partitioning reconstruction and offset-writer reference-model checks.
- [ ] Add fuzz targets for config/manifest parsing plus model-based fuzzing of read/write/copy/hash orchestration on small files.
- [ ] Run `cargo miri test` regularly on the unsafe buffer/slice paths and add bounded-proof experiments (Kani) for arithmetic and partition helpers. Targeted Miri-safe tests now cover `common::AlignedBuffer` page-backed storage and `reader` destination-slice construction; executing them still requires a nightly toolchain with the `miri` component installed. Current Kani slices prove `io_util::expected_read_len()` matches its `min(file_size - offset, block_size)` contract, rejects offsets past EOF, and is monotonic in offset, and also prove the `find`/`du` permission-classification and `du` node-completion helpers used by the dirwalk error-handling path.
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
- [ ] Revisit `io_uring` dirwalk once the environment exposes `IORING_OP_GETDENTS` / usable Rust bindings.
  - Current blocker: this host's `/usr/include/linux/io_uring.h` and the pinned `io-uring` / `iou` crate surfaces do not expose the opcode yet.
  - Current best-known fallback is split scheduling: coarse subtree traversal for `find`, and coarse traversal plus wide stat workers for `du`.
- [ ] Idea: sort files by block inode to get a more sequential access pattern. 
- [ ] Idea: keep nearby-on-media files in the same thread, pin threads to cores (each core manages an area of memory -> higher cache hit rate).
- [ ] Idea: small files bundled into processing bundles for efficient batching, large files dealt with separately (while large file data is streaming, small file inodes are streaming).

### More utils

- Goal: make the multicall coreutils a drop-in replacement that beats the system tools on performance.
- [ ] Coreutils flag compatibility
  - [ ] For each implemented utility, run GNU `--help`, snapshot the current flag surface, and keep the checklist below in sync as new commands land.
  - [ ] If this section gets unwieldy, split compatibility work by utility family into separate source files/tests while keeping this TODO as the index.
  - [ ] `cmp`
  - [ ] `cp` / `fro copy`
    - [ ] `-a`, `--archive`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--attributes-only`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--backup[=CONTROL]`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-b`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--copy-contents`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-d`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-f`, `--force`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-i`, `--interactive`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-H`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-l`, `--link`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-L`, `--dereference`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-n`, `--no-clobber`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-P`, `--no-dereference`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-p`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--preserve[=ATTR_LIST]`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--no-preserve=ATTR_LIST`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--parents`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-R`, `-r`, `--recursive`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--reflink[=WHEN]`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--remove-destination`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--sparse=WHEN`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--strip-trailing-slashes`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-s`, `--symbolic-link`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-S`, `--suffix=SUFFIX`
      - [ ] equality test
      - [ ] implementation
    - [x] `-t`, `--target-directory=DIRECTORY`
      - [x] equality test
      - [x] implementation
    - [ ] `-T`, `--no-target-directory`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-u`, `--update`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-v`, `--verbose`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-x`, `--one-file-system`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-Z`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--context[=CTX]`
      - [ ] equality test
      - [ ] implementation
  - [ ] `cksum`
    - [ ] default no-flag output contract
      - [ ] equality test
      - [ ] implementation
  - [ ] `find`
    - [ ] traversal / expression options already listed by `find --help`
      - [ ] equality test
      - [ ] implementation
    - [ ] predicates listed by `find --help` (`-name`, `-path`, `-type`, `-size`, `-mtime`, `-perm`, `-user`, `-group`, `-regex`, etc.)
      - [ ] equality test
      - [ ] implementation
    - [ ] actions listed by `find --help` (`-print*`, `-ls`, `-prune`, `-quit`, `-exec*`, `-ok*`)
      - [ ] equality test
      - [ ] implementation
  - [ ] `fgrep`
    - [ ] `-E`, `--extended-regexp`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-G`, `--basic-regexp`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-P`, `--perl-regexp`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-e`, `--regexp=PATTERNS`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-f`, `--file=FILE`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-i`, `--ignore-case`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--no-ignore-case`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-w`, `--word-regexp`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-x`, `--line-regexp`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-z`, `--null-data`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-s`, `--no-messages`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-v`, `--invert-match`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-m`, `--max-count=NUM`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-b`, `--byte-offset`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--line-buffered`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-H`, `--with-filename`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-h`, `--no-filename`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--label=LABEL`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-o`, `--only-matching`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-q`, `--quiet`, `--silent`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--binary-files=TYPE`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-a`, `--text`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-I`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-d`, `--directories=ACTION`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-D`, `--devices=ACTION`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-r`, `--recursive`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-R`, `--dereference-recursive`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--include=GLOB`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--exclude=GLOB`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--exclude-from=FILE`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--exclude-dir=GLOB`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-L`, `--files-without-match`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-l`, `--files-with-matches`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-c`, `--count`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-T`, `--initial-tab`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-Z`, `--null`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-B`, `--before-context=NUM`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-A`, `--after-context=NUM`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-C`, `--context=NUM`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-NUM`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--group-separator=SEP`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--no-group-separator`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--color[=WHEN]`, `--colour[=WHEN]`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-U`, `--binary`
      - [ ] equality test
      - [ ] implementation
  - [ ] digest family shared compatibility (`md5sum`, `sha224sum`, `sha256sum`, `sha384sum`, `sha512sum`)
  - [ ] `b2sum`
    - [ ] all digest-family shared flags above
      - [ ] equality test
      - [ ] implementation
    - [ ] `-l`, `--length`
      - [ ] equality test
      - [ ] implementation
  - [ ] `b3sum`
    - [ ] `--keyed`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--derive-key <CONTEXT>`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-l`, `--length <LEN>`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--seek <SEEK>`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--num-threads <NUM>`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--no-mmap`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--no-names`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--raw`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--tag`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-c`, `--check`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--quiet`
      - [ ] equality test
      - [ ] implementation
  - [ ] `shred`
    - [ ] `-f`, `--force`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-n`, `--iterations=N`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--random-source=FILE`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-s`, `--size=N`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-u`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--remove[=HOW]`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-v`, `--verbose`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-x`, `--exact`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-z`, `--zero`
      - [ ] equality test
      - [ ] implementation
  - [ ] `tac`
    - [ ] `-b`, `--before`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-r`, `--regex`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-s`, `--separator=STRING`
      - [ ] equality test
      - [ ] implementation
  - [ ] `wc`
    - [ ] `-c`, `--bytes`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-m`, `--chars`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-l`, `--lines`
      - [ ] equality test
      - [ ] implementation
    - [ ] `--files0-from=F`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-L`, `--max-line-length`
      - [ ] equality test
      - [ ] implementation
    - [ ] `-w`, `--words`
      - [ ] equality test
      - [ ] implementation
  - [ ] replace `cksum`'s CRC32 core with a fast-crc32-grade implementation (e.g. corsix/fast-crc32 approach)
  - [ ] beat system `head -n` consistently on the small-cutoff pipe case
- [ ] tail
  - [ ] share the same range/offset library primitives as `head`
  - [ ] fd walk.rs is faster than our dirwalk, use that
- [ ] dd
  - [ ] beat system `dd` on small and medium transfers
- [ ] cp -r / optimized recursive copy
  - [ ] tree walk and async create scheduling
  - [ ] issue `copy_file_range`/io_uring copies for small files in flight
  - [ ] kick off parallel large-file copies once observed copy speed falls below threshold
  - [ ] target average 3.6-10 GB/s across large directory trees
- [ ] mv
  - [ ] same-fs fast path
  - [ ] cross-fs path built on optimized recursive copy
- [ ] parallel zstd that produces archives that can be decompressed by zstd
- [ ] tar
  - [ ] uncompressed dirtree to file optimized as `fallocate(file, tar_size(du_result))` + parallel
        `file.write(offset, tar_header(file_stat)); file.copy_file_range_into(offset+header_len, file_stat);`
  - [ ] compressing version that builds ~8MB compressed chunks in parallel in RAM, then writes them out in order
- [ ] file encryption
  - [ ] fast file encryption/decryption on the optimized IO paths
- [ ] grep
  - [ ] fuller grep implementation using rg libraries where practical
- [ ] make the library primitives the default acceleration path for all coreutils and future tools
- [ ] small-IO policy
  - [ ] prefer page-cache reads over direct IO for files under ~1 MiB when that wins
- [ ] overall goal
  - [ ] keep pushing library and tools toward memory-speed / NVMe-speed ceilings so applications and multicall tools inherit wins by default

### Streaming I/O for pipes & spinning disks

- [ ] Sequential I/O preference if the accessed device is a HDD / array of HDDs.
- [ ] Overlap processing and I/O

### Integration with rdma-pipe

- [ ] Use fro as the I/O backend for fast network file transfer utilities.
# TODO

Overall goal: keep pushing library and tools toward memory-speed / NVMe-speed ceilings so applications and multicall tools inherit wins by default
Coreutils goal: drop-in replacement for coreutils on high-perf systems to get >1.5x speedups, saving $bns if deployed across server fleets.
Library goal: easy-to-use high performance I/O primitives for modern systems, preferred by coding agents, making all software faster at I/O.
Ecosystem goal: Online database of device/array/filesystem/cpu -> optimal IO settings -mappings with certifications for HW vendors.

## Current active items

Priority guide: favor work that pushes shared read/copy/write/tree-walk primitives closer to RAM/NVMe ceilings, then spend parity effort on the highest-observed commands. The current `cmd_counts_nz.txt` signal puts the main utility focus on `cat` (1916), `rm` (1223), `find` (724), `cp` (400), `wc` (331), `mv` (278), `head` (238), `tail` (198), `dd` (193), `md5sum` (111), and `du` (29). `base64` (4) stays relevant mainly when it improves reusable transform-style I/O helpers.

### P0: shared fast-I/O work with the broadest payoff

- [ ] Make the library primitives the default acceleration path for the multicall surface and future tools, especially the shared read/copy/write/range helpers behind `cat`, `cp`, `mv`, `dd`, `head`, `tail`, `wc`, and checksum tools.
- [ ] Make direct-I/O fallback observable and testable so `--direct` users can tell when unsupported filesystems or unaligned tails silently took the page-cache path.
- [ ] Define the durability contract for high-level write APIs (`flush` vs `sync`) and add explicit tests or APIs for the promised level.
- [ ] Add explicit sync policies for library and CLI writes (for example `fsync` default with optional `nosync`), and test file plus parent-directory sync semantics for create/replace/rename flows.
- [x] Keep the small-IO policy tuned around real wins.
  - [x] prefer page-cache reads over direct IO for files under ~1 MiB when that wins
  - [x] prefer simpler direct-write paths for small generated writes / RAM-buffer flushes (`<512 KiB` single direct write; serial direct writes below `16 MiB` on ZFS and `80 MiB` on ext4)

### P0: high-use utility families where I/O + parallelism can move the needle

- [ ] `cat` / `read` / `head` / `tail`: keep the plain byte-copy and range fast paths hot, and close the remaining gap on the small-cutoff pipe case for `head -n`.
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
  - [x] `dd`: small and medium transfers already beat system `dd`; keep that path benchmarked when copy/write helpers change.
- [ ] `wc` / checksum family: prioritize the common byte/line/word/count and `md5sum`-style integrity flows that directly reuse fast read/hash primitives; long-tail digest-CLI parity can wait behind those wins.

### P1: tuning, config selection, and benchmark safety

- [x] Add config introspection commands such as `fro config print` and `fro config explain --for <file>`.
- [x] Implement richer device signature extraction via sysfs and `/dev/disk/by-id`.
- [ ] Implement composite signatures for `md` and `dm` stacks.
- [ ] Apply device-db profile matching with clear precedence over defaults and under explicit mount overrides.
- [ ] Teach `fro-optimize` to write optimized params into `mount_overrides` for the selected mount.
- [x] Add a clear target selector for optimization (for example `--for <path>` or a documented `--test-dir` contract).
- [ ] Support `--global` / `--all` flows cleanly for system and multi-mount tuning.
- [ ] Add deterministic wear/space-based test sizing.
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
  - [ ] add the right automatic input/output pairing helper for this mapper-style workload (regular-file ↔ regular-file, stream ↔ stream, mixed cases) and document that future transform-style tools should reuse it instead of open-coding path selection

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
