# Verification Plan

This document defines a correctness-focused verification plan for `fro`.

The goal is not "some tests pass"; the goal is a validity argument with as few holes as practical for a Linux file-I/O tool that users may trust with large, valuable files.

## Scope

In scope:

- block-partitioned reads
- sequential and offset writes
- copy, diff, hash, verify, and recover behavior
- high-level Rust APIs in `src/api.rs`
- unsafe buffer/slice construction and direct-I/O fallback behavior

Out of scope for the initial proof:

- kernel correctness
- filesystem/device firmware correctness
- power-loss guarantees beyond the API's explicit durability contract
- performance claims

## Architectural review

These are the highest-value correctness issues and proof holes found during review.

### 1. Unchecked offset arithmetic

`src/reader.rs` computes offsets with unchecked multiplication/addition:

- `block_offset(thread_base, block_id, num_threads, block_size)` at `src/reader.rs:139-141`

This is a proof hole because the read partitioning argument depends on offsets being computed without overflow. The same category also matters anywhere offsets are combined with lengths or converted to `usize`.

Required hardening:

- use checked arithmetic for block/offset calculations
- use checked `u64 -> usize` conversion before slicing/allocation
- fail explicitly on overflow instead of wrapping

Status:

- implemented in `src/reader.rs`, `src/block_hash.rs`, `src/stream.rs`, and `src/writer.rs`
- covered with explicit overflow-boundary tests for read offsets, destination-buffer offsets, hash offsets, and writer end offsets

Formal contribution:

- Premise 1: every derived offset is now computed with `checked_mul`/`checked_add`, and every `u64 -> usize` destination conversion is checked before unsafe slice construction.
- Premise 2: each failure branch returns `InvalidInput` (or another explicit error) instead of continuing with wrapped arithmetic.
- Therefore, for this arithmetic layer, `(successful return) -> (all derived offsets used by the operation were representable and in-bounds for the checked surface)`.
- This discharges the main overflow hole for the partitioning and destination-slice arguments, so later proofs may reason over mathematical offsets instead of wrapped machine arithmetic at these call sites.

### 2. Partial-read acceptance in the read paths

The read visitor/map/load loops currently treat a positive CQE result as a valid logical block completion:

- `src/reader.rs:621-632`
- `src/reader.rs:532-545`
- `src/reader.rs:443-446`

That is acceptable only if the implementation can prove that short reads are impossible for the submitted request shape. Today that proof is not encoded in the code or tests, so this remains a correctness hole.

Required hardening:

- either retry short reads until the requested logical block is complete
- or fail the operation on any unexpected short read
- add tests with injected short completions

Status:

- implemented as explicit error-on-short-read validation in `src/reader.rs` and the coupled block-hash reader in `src/block_hash.rs`
- shared completion checks now live in `src/io_util.rs`, so reader, mapper, visitor, loader, and block-hash paths apply the same predicate

Formal contribution:

- Premise 1: for every completion, the code now computes the expected logical byte count for that `(file_size, offset, block_size)` triple.
- Premise 2: the operation continues only if `actual_len == expected_len`; otherwise it returns `UnexpectedEof`.
- Therefore, `(successful logical block completion) -> (the full requested logical bytes for that block were delivered)`.
- This removes the previous ambiguity where any positive CQE result could masquerade as a valid block, which is the key local lemma needed for the read-partitioning and hash-integrity arguments.

Remaining gap:

- the validator now has injected unit coverage over synthetic completion lengths, including `0..=255`, `n * 17` beyond `1024`, negative values, and exact/near-block-size boundaries
- a later, stronger step would still be a live CQE fault-injection seam, but the current injected-length tests already prove the shared decision rule over the intended fault domain

### 3. Unsafe destination slicing depends on caller-side checks

`output_slice_mut()` builds raw mutable slices:

- `src/reader.rs:1223-1225`

The current call sites do check `current_offset as usize + len > output.len` before using it:

- `src/reader.rs:414-423`
- `src/reader.rs:451-460`

That is better than unchecked slicing, but the proof still relies on every caller preserving the precondition and on the `u64 -> usize` cast being valid.

Required hardening:

- move bounds validation into a checked helper
- test large-boundary cases under Miri and normal unit/property tests

### 4. Offset-writer semantics are only partially specified

Offset writes are bounds-checked against `total_size`:

- `src/stream.rs:362-370`

But the API does not currently prove full coverage of `[0, total_size)` for fixed-size outputs, and the intended meaning of gaps is not spelled out as a contract. This is a semantics hole, not necessarily a bug.

Required hardening:

- define whether gaps are valid, forbidden, or mode-dependent
- if forbidden, track written ranges and reject incomplete outputs
- if valid, model and test sparse semantics explicitly

Status:

- fixed-size offset writes are now documented as deterministic gap-filling writes, not completeness-enforcing writes
- with `truncate = true`, the destination is prepared to `total_size` and unwritten regions read back as zeroes
- with `truncate = false`, existing bytes outside caller-written ranges are preserved and the file is extended only if needed

Formal contribution:

- Premise 1: the offset-writer API now states the meaning of an unwritten range instead of leaving it implicit.
- Premise 2: tests cover both branches of the contract: zero-filled gaps for default fixed-size outputs and preserved untouched bytes for `truncate = false`.
- Therefore, `(successful offset-write return) -> (every byte in the output is either caller-written data or bytes defined by the documented gap policy)`.
- This closes the semantics hole for callers and for later verification work, because "missing write" is no longer an undefined state in the model.

### 5. Durability semantics are under-specified

The high-level writer flushes pending work and then calls `File::flush()`:

- sequential writer: `src/writer.rs:125-138`
- offset writer: `src/writer.rs:312-317`

`flush()` is not the same thing as `sync_all()`. Some lower-level write/copy paths do call `sync_all()` in specific cases:

- `src/writer.rs:706-712`

This means the codebase currently has multiple write-completion notions. That is a proof hole until the API contract states what "done" means for each path.

Required hardening:

- document which APIs guarantee visibility only vs durability
- add explicit sync-capable APIs if durability is promised
- test the documented contract, not an implicit one

### 5a. Recommended durability model

SQLite's main lesson is not just "call `fsync` a lot". The useful pattern is:

- define a small number of named durability modes
- place sync points deliberately
- make the user's selected durability level explicit in both API and CLI semantics
- treat metadata durability (directory entries, rename visibility) separately from file-content durability

For `fro`, the most practical first model is:

- `sync = full` (recommended default): explicit `fsync`/`sync_all` semantics for successful write/copy/replace flows
- `sync = none` (explicit opt-out): performance mode with no durability guarantee beyond process-local completion

Optionally later:

- `sync = data`: file-data durability without the strongest metadata guarantees where the platform exposes that distinction cleanly

The important point is not the exact names. The important point is that "completed write" must stop being an ambiguous term.

### 5b. File and directory sync obligations

If `fro` creates, truncates, replaces, or renames files, the proof needs separate obligations for:

- file contents
- file size
- directory entry durability

SQLite is exemplary here because it models the ordering assumptions explicitly. For `fro`, that translates into:

- sync the file after durable write completion
- sync the parent directory when create/replace/rename semantics matter
- document which operations promise crash-safe replacement vs only durable file contents

Without directory-sync semantics, a "successful" durable copy can still have a metadata hole after crash.

### 5c. Proposed verified mode

A useful higher-assurance user-facing mode would be a verified copy/write flow along the lines of:

1. compute or load source block hashes
2. copy the data
3. `fsync` the destination
4. if redundancy or manifests are available, run `recover`/repair logic if needed
5. `fsync` again after any repair writes
6. optionally `verify` as a final postcondition check

As a user-facing shorthand, this could be exposed as a "verified" mode for copy/write-oriented operations.

Conceptually:

- `normal copy`: best-effort data movement
- `durable copy`: copy + sync contract
- `verified copy`: copy + sync + integrity postcondition

For the library, the equivalent should likely be explicit APIs or options rather than hidden behavior.

### 5d. Verification obligations for durable and verified modes

For `sync = full`, the required proof obligations are:

- successful return implies the file sync step completed
- if the operation created/replaced/renamed a path, the required parent-directory sync step completed
- failures surface whether they occurred before or after data reached the durable phase

For `verified` mode, the required proof obligations are:

- successful return implies destination bytes match the source bytes according to the documented verifier
- if repair was needed, the repaired bytes also passed the same verification rule
- intermediate artifacts and partial-failure states are documented

This does not prove power-loss correctness against a broken kernel or device. It does prove that `fro` executed its side of the durability protocol.

### 6. Direct-I/O fallback is correctness-relevant and hard to observe

Direct open helpers fall back to the page-cache file descriptor on unsupported `O_DIRECT` cases:

- `src/io_util.rs:5-44`

That behavior is reasonable for portability, but when a user asked for direct I/O it affects semantics and benchmarking interpretation. The current proof hole is not "fallback exists"; it is "fallback may happen without a visible contract."

Required hardening:

- make fallback observable in reports/logs/results
- add tests covering unsupported-direct and unaligned-tail behavior

## Verification strategy

We want multiple layers:

1. deterministic unit tests for local invariants
2. property-based tests for algebraic/model invariants
3. small exhaustive state exploration for tiny files and operation sequences
4. fuzzing for parser/state-machine robustness
5. unsafe-code checking via Miri
6. bounded proof experiments for arithmetic/partition helpers

## Subsystem obligations

Each subsystem needs a local proof obligation and a concrete verification method.

### A. Read partitioning

Property:

- for any valid `(file_size, block_size, num_threads)`, the scheduled block offsets cover every byte in `[0, file_size)` exactly once, in-bounds, with no overlap and no omission

Evidence required:

- pure helper tests for offset generation
- property tests over randomized sizes/threads/block sizes
- small exhaustive enumeration over tiny files and tiny thread counts

Suggested approach:

- extract or mirror the partition logic into a pure helper
- use `proptest` for QuickCheck-style generation and shrinking
- add a SmallCheck-style exhaustive test that enumerates tiny parameter sets (for example file sizes `0..=64 KiB`, threads `1..=4`, block sizes from a small set)

Progress:

- the arithmetic precondition for this property is now enforced in production code
- initial property coverage now reconstructs small files from `visit_file_blocks()` results over randomized file sizes, block sizes, queue depths, and thread counts
- remaining work is to prove coverage/non-overlap/ordering algebraically and with broader property/exhaustive tests

### B. Read completion semantics

Property:

- a logical block is either fully delivered with the correct bytes or the operation fails

Evidence required:

- injected short-read tests
- tests for final partial block behavior
- tests for direct-to-buffer loader correctness

Suggested approach:

- introduce a test seam around completion handling so unit tests can feed synthetic CQE lengths
- model the expected file bytes with a plain `Vec<u8>`

Progress:

- the completion policy is now explicit: partial completions are rejected instead of being treated as successful logical blocks
- next evidence step is an injected-CQE test seam to show the policy fires in the live `io_uring` loops

### C. Unsafe buffer/slice usage

Property:

- every raw slice created by the code is in-bounds, non-overlapping where required, and valid for the lifetime of the I/O operation

Evidence required:

- unit/property tests for checked-slice helpers
- `cargo miri test` on unsafe-heavy modules
- targeted tests for alignment, empty inputs, tail blocks, and large boundary values

Suggested approach:

- minimize the surface of raw slice creation
- prove preconditions in safe wrappers and make the unsafe core tiny
- this has started in `reader.rs` by funneling destination-offset validation through `checked_output_offset()` before `output_slice_mut()`

### D. Sequential and indexed writing

Property:

- bytes appear at the correct offsets in the final file, with no duplication, reordering, truncation, or silent short writes

Evidence required:

- model-based tests that compare the writer output to a reference in-memory model
- randomized out-of-order indexed block delivery
- duplicate-index and missing-index failure tests

Suggested approach:

- use temp files plus a reference `Vec<u8>`
- generate randomized block arrival orders and block sizes

Progress:

- indexed/sequential writer model checks are still open; the current property layer covers read partitioning and fixed-size offset writes first

### E. Offset writing / sparse semantics

Property:

- the final file contents match the declared semantics for offset writes, including gaps, overwrites, and preservation of existing data when `truncate = false`

Evidence required:

- property tests over random offset-write sequences against a reference sparse model
- small exhaustive tests for tiny files and operation sequences
- explicit contract tests for `bytes_written` and `written_extent`

Suggested approach:

- build a reference model using `Vec<Option<u8>>` or a byte vector plus initialization bitmap
- compare temp-file bytes against the model after every generated sequence

Progress:

- randomized offset-write sequences are now checked against a zero-filled reference model for the default fixed-size writer path

### F. Copy equivalence

Property:

- for any supported mode combination, successful copy outputs the exact source bytes at the destination

Evidence required:

- property tests over random file contents, sizes, and range copies
- tests across page-cache/direct combinations
- tests for via-memory and streaming paths against the same oracle

Suggested approach:

- generate small random files
- compare `fro` copy outputs against plain `std::fs` copies or a memory model

### G. Hash / verify / recover correctness

Property:

- `hash` produces manifests matching a reference implementation
- `verify` accepts clean files and rejects corrupted ones
- `recover` reconstructs bytes consistent with the chosen quorum rule

Evidence required:

- randomized block corruption tests
- property tests over manifest round-trips and recovery scenarios
- fuzzing of manifest/config parsing

Suggested approach:

- use small random files with random corruption maps
- compare recovered bytes against the known-good reference file
- fuzz JSON manifests and config files with `cargo-fuzz`

### H. API/CLI semantic equivalence

Property:

- the high-level API and CLI commands preserve the same correctness semantics as their underlying core paths

Evidence required:

- paired tests: API result vs CLI result vs plain `std` model
- random small-file end-to-end tests

Suggested approach:

- for a generated scenario, run the API, run the CLI, and compare outputs and expected failure classes

## Tooling plan

### Property-based testing

Preferred stack:

- `proptest` for the main QuickCheck-style layer because it supports shrinking and richer generators
- manual exhaustive enumeration for the SmallCheck-style layer on small parameter spaces

Core property suites to add first:

1. read partitioning and bounds
2. offset-writer sparse model equivalence
3. copy/range-copy equivalence
4. hash/verify/recover model equivalence

### Fuzzing

Preferred stack:

- `cargo-fuzz` / libFuzzer

Initial fuzz targets:

1. config JSON parsing
2. sidecar manifest parsing
3. block-hash recovery inputs (bounded-size model harness)
4. API sequences on small temp files: read/write/copy/offset-write/range-copy

The most valuable fuzz harnesses are model-based: they should compare `fro` behavior to a reference model, not just seek crashes.

### Miri

Run regularly on:

- `reader`
- `common::AlignedBuffer`
- stream/write tests involving unsafe buffer handling

Miri is especially valuable here because the code uses raw pointers, `mmap`, and manual slice construction.

Current Miri-safe surface:

- `common::tests::aligned_buffer_page_storage_is_aligned_and_mutable`
- `reader::tests::output_slice_mut_writes_only_checked_window`
- `reader::tests::output_slice_mut_preserves_non_overlapping_regions`

These tests are aimed at the unsafe buffer/slice surface that does not depend on `io_uring` execution. In this environment, the nightly Miri toolchain is not fully available on the active Rust installation, so the current deliverable is a focused Miri target plus the command needed to execute it when nightly with `miri` is installed:

```bash
cargo +nightly miri test --lib common::tests::aligned_buffer_page_storage_is_aligned_and_mutable -- --exact
cargo +nightly miri test --lib reader::tests::output_slice_mut_writes_only_checked_window -- --exact
cargo +nightly miri test --lib reader::tests::output_slice_mut_preserves_non_overlapping_regions -- --exact
```

### Bounded proof experiments

Most promising candidates:

- checked offset/length arithmetic
- file partition coverage/non-overlap helpers
- small pure range-merging helpers for sparse write accounting

Possible tools:

- Kani for bounded model checking of pure helper logic
- Creusot or Prusti for contract-style proofs if the helper logic is kept pure and isolated

These tools are likely most useful once the hot-path arithmetic and partition logic are factored into small pure helpers.

## Real-world compatibility matrix

The property/model/fuzz layers prove core logic. They do not prove that `fro` behaves sanely on the kinds of paths and permission situations users actually hand it.

We therefore also want a real-world compatibility matrix over:

- file kind
- access surface / backing environment
- permission mode

Conceptually, the matrix starts from:

```rust
#[derive(Debug, Clone, Copy)]
pub enum FileType {
    Regular, Directory, Symlink, CharDevice, BlockDevice, Fifo, Socket
}

#[derive(Debug, Clone, Copy)]
pub enum AccessType {
    LocalExt4, LocalTmpfs, ProcFS, SysFS, NFS, Samba, NVMeoF, Stdin, Stdout, Stderr
}

const PERMISSIONS: [u32; 6] = [
    0o644,
    0o755,
    0o600,
    0o444,
    0o777,
    0o000,
];
```

### Why this layer exists

The correctness argument otherwise has a major integration hole:

- core logic may be correct
- parsing may be correct
- unsafe memory use may be correct
- yet the library may still behave badly on directories, FIFOs, symlinks, read-only files, procfs entries, pipes, or non-local filesystems

For a file-I/O tool, these user-facing semantic mismatches are correctness failures, even when memory safety is intact.

### Matrix policy

We do **not** run the raw Cartesian product blindly.

Instead, each candidate combination must be classified as one of:

1. **supported and expected to succeed**
2. **supported and expected to fail with a specific error class**
3. **unsupported by contract**
4. **environment-dependent / only runnable in specialized CI or manual labs**

This keeps the matrix meaningful instead of producing noise from impossible cases like:

- `Stdout × Directory`
- `Stdin × 0o755`
- `SysFS × writable regular-file semantics`

### Required semantics per axis

#### File kind

For each file kind, define whether each operation (`read`, `grep`, `write`, `copy`, `diff`, `hash`, `verify`, `recover`, API helpers) is:

- valid
- invalid but gracefully rejected
- conditionally valid

At minimum:

- `Regular`: must be the fully supported baseline
- `Directory`: must be rejected cleanly everywhere a byte-stream file is required
- `Symlink`: contract must say whether operations follow symlinks or reject them
- `CharDevice` / `BlockDevice`: contract must say whether these are supported as stream/file-like inputs
- `Fifo` / `Socket`: contract must distinguish streaming byte sources from seekable-file requirements

#### Access surface

Each access surface changes semantics:

- `LocalExt4`, `LocalTmpfs`: baseline local cases
- `ProcFS`, `SysFS`: special kernel virtual files with unusual metadata, length, and seek semantics
- `NFS`, `Samba`, `NVMeoF`: remote or network-backed semantics; direct-I/O and durability behavior may differ
- `Stdin`, `Stdout`, `Stderr`: stream endpoints, not normal path-based files

The plan should test only the operations that claim to support each surface.

#### Permissions

Permissions should not just be "can open / cannot open".

They should validate:

- open/read failures are surfaced correctly
- write attempts on read-only objects fail explicitly
- metadata-only success does not masquerade as I/O success
- directory permissions interact correctly with file creation and replacement

### Verification obligations for the matrix

For each meaningful matrix point, define:

1. setup procedure
2. operations that should be attempted
3. expected result class
4. expected invariants after success or failure

Expected result classes should be coarse and stable:

- success with exact output bytes
- rejected with `InvalidInput`-class behavior
- rejected with permission-denied behavior
- rejected as unsupported direct-I/O / unsupported filesystem behavior
- rejected with environment-specific transient or transport behavior where documented (for example `EWOULDBLOCK`, `ETIMEDOUT`, or similar remote-filesystem / network-storage failures)
- skipped because the environment is unavailable

### Recommended execution layers

#### Layer 1: deterministic local matrix in ordinary CI

These should run on every developer machine and normal CI:

- regular files on local filesystem
- directories
- symlinks
- FIFOs
- stdin/stdout/stderr cases where the API/CLI claims support
- permission variations that can be created as the current user

This is the highest-value matrix because it is portable and catches many user-facing failures.

#### Layer 2: privileged/local-host matrix

These likely require root or host setup:

- char devices
- block devices
- explicit direct-I/O behavior on known filesystems

These can run in privileged CI or manual validation labs.

#### Layer 3: environment-specific matrix

These should be treated as optional/manual or specialized CI:

- `tmpfs`
- `procfs`
- `sysfs`
- `NFS`
- `Samba`
- `NVMeoF`

The plan should record them as named obligations, even if not always runnable.

For these environments, tests must not assume local-filesystem error behavior. The matrix needs an "environment-specific acceptable result" channel so transient or transport-flavored failures such as `EWOULDBLOCK` or `ETIMEDOUT` are classified separately from logic failures.

### How to combine this with property-based testing

The matrix and property layers serve different purposes:

- property tests prove abstract invariants over generated data
- the matrix proves the implementation's contract matches reality on concrete OS/file-type/environment combinations

The combined argument is:

1. model/property tests show the logic is correct when given valid abstract inputs
2. fuzzing shows malformed inputs do not break the program
3. the compatibility matrix shows that real OS objects are correctly classified into "supported", "rejected", or "environment-dependent"

Without the matrix, the proof still has a large "real file semantics" hole.

### Suggested first-pass matrix

Implement first:

- `Regular × {LocalExt4, LocalTmpfs} × PERMISSIONS`
- `Directory × {LocalExt4, LocalTmpfs} × PERMISSIONS`
- `Symlink × {LocalExt4, LocalTmpfs} × PERMISSIONS`
- `Fifo × {LocalExt4, LocalTmpfs} × relevant PERMISSIONS`
- `{Stdin, Stdout, Stderr}` for the operations that are supposed to support streams

Then extend to:

- `ProcFS`
- `SysFS`
- privileged device cases
- network filesystems

### Remaining holes after this matrix work

Even with the matrix, these holes remain:

- kernel/filesystem bugs below userspace
- remote filesystem behaviors not available in CI
- privilege-dependent device semantics not exercised in unprivileged environments
- crash-consistency and power-loss behavior unless tested separately

## How the evidence composes into a validity proof

We want the following argument:

1. If partitioning proofs show every planned read/write range is in-bounds and non-overlapping where required,
2. and completion-semantic tests show the implementation never silently accepts truncated logical blocks,
3. and model-based transformation tests show read/write/copy/hash/recover results match a reference model,
4. and parser fuzzing plus Miri show the unsafe and input-decoding surfaces do not introduce undefined behavior or obvious malformed-input crashes,
5. and the durability contract is explicit and tested,
6. and durable / verified modes have explicit sync and postcondition obligations,

then `fro` is valid for its documented Linux file-I/O semantics within the stated assumptions below.

This does not give a machine-checked end-to-end formal proof, but it does give a layered validity argument with small, named trust assumptions instead of one large implicit hole.

## Remaining holes after the planned work

Even after executing this plan, these holes remain:

### Trusted platform hole

We still trust:

- Linux kernel behavior
- `io_uring` correctness
- filesystem correctness
- storage hardware / firmware correctness

### Crash-consistency hole

Without a dedicated crash harness or stronger API contract, we cannot fully prove power-loss behavior for general writes. We can only prove conformance to the documented sync contract.

Even after adding `sync = full`, there is still a gap between:

- "the program issued the required sync calls successfully"
- and "the platform actually honored them correctly under sudden power loss"

That gap is inherent unless the validation program includes crash-injection or platform-specific fault testing.

### Environment-specific direct-I/O hole

Different filesystems and mounts may treat `O_DIRECT` support and alignment rules differently. We can reduce this with tests and observability, but not eliminate it in one portable proof.

### Performance-path hole

Fast paths that are correct on one platform may expose different edge behavior on another. The proof must stay scoped to correctness, not throughput.

### 32-bit / extreme-size hole

If the project continues to care about non-64-bit targets or extremely large files, conversion and allocation boundaries must be treated as a first-class proof surface.

## Recommended execution order

1. checked arithmetic and checked conversions
2. short-read handling
3. offset-writer semantics decision
4. explicit sync-mode and verified-mode contract design
5. first property suites (`partitioning`, `offset writes`, `copy equivalence`)
6. `cargo-fuzz` harnesses for config and manifests
7. Miri in CI or janitor
8. bounded-proof experiments on pure helpers
