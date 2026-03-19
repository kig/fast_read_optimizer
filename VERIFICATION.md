# Verification Status

This file is the reader-facing entry point for `fro` verification work.

For the full architectural review, proof obligations, and remaining trust holes, see [`docs/verification.md`](docs/verification.md).

## Goal

The verification goal is not just "tests pass." The goal is a validity argument for a Linux large-file I/O tool: if an operation reports success, the implementation should have satisfied the documented safety, integrity, and semantic obligations for that operation.

## What has been completed so far

The following proof-relevant items are implemented and covered by tests:

- checked offset arithmetic and checked `u64 -> usize` conversions for read and write offset paths
- explicit rejection of short read completions instead of treating any positive CQE as a valid logical block
- injected unit tests for the shared short-read validator, covering:
  - lengths `0..=255`
  - `n * 17` lengths past `1024`
  - negative synthetic completions
  - `block_size - 1`, `block_size`, and `block_size + 1`
- documented and tested offset-writer gap semantics:
  - default fixed-size outputs zero-fill unwritten gaps
  - `truncate = false` preserves existing untouched bytes
- shared janitor checks and a tracked pre-commit hook

## Why these items matter

Formal logic summary:

- If derived offsets are checked before use, then successful execution implies the code did not rely on wrapped arithmetic.
- If logical block completion is accepted only when `actual_len == expected_len`, then successful execution implies full logical block delivery rather than an arbitrary positive prefix.
- If unwritten output ranges have a documented meaning, then successful offset writes imply every output byte is either caller-written data or data defined by the documented gap policy.

Together, these close three high-value proof holes:

1. arithmetic overflow in partitioning and destination slicing
2. silent acceptance of partial read completions
3. implicit / undefined semantics for offset-write gaps

## What is still open

The main remaining verification items are:

- make direct-I/O fallback observable and testable
- define and implement explicit durability contracts (`flush` vs durable sync)
- add sync-policy coverage for file and parent-directory semantics
- add property-based tests for partitioning, copy equivalence, and hash / verify / recover invariants
- add fuzzing targets
- run Miri and bounded-proof experiments
- build the real-world compatibility matrix across file type, access surface, and permissions

## How to validate the current state

Repository checks used for the current verification work:

```bash
cargo test --quiet
cargo run --quiet --manifest-path janitor/Cargo.toml -- all
```

## Current reference document

The full outline, subsystem obligations, evidence plan, and remaining holes live in [`docs/verification.md`](docs/verification.md).
