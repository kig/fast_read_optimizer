# Copilot instructions for `fast_read_optimizer`

## Build & run

- Build release binaries:
  - `cargo build --release`
- Run the main tool (CLI is `fro`):
  - `./target/release/fro --help`
  - Example: `./target/release/fro read --direct -n 1 <file>`
  - Example: `./target/release/fro grep --no-direct -n 1 <pattern> <file>`

### “Test”/benchmark runner (performance regression suite)

This repo’s main automated verification is the benchmark runner binary:

- Run the full suite:
  - `./target/release/fro-benchmark`
- Run a single benchmark (prefix match on the printed benchmark name):
  - `./target/release/fro-benchmark 'read (auto, hot)'`
  - `./target/release/fro-benchmark 'grep (direct)'`
- Run a subset (prefix match):
  - `./target/release/fro-benchmark read`
  - `./target/release/fro-benchmark grep`
- Choose where temp files are created (use a specific mount/device):
  - `./target/release/fro-benchmark --test-dir /mnt/nvme`
- Use a large enough working set when validating read-side performance on fast storage:
  - `./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB`
  - On the NVMe array used during recent tuning, `4GB` was necessary to get stable and representative read-family numbers.
- Re-run `fro-benchmark` after every hot-path edit before trusting optimizer output:
  - if the code path slowed down, `fro-optimize` will happily tune the slower implementation and save bad parameters.

### Tuning / generating `fro.json`

- Optimize and save best-found params to `fro.json`:
  - `./target/release/fro-optimize --test-dir /mnt/nvme read grep diff write copy`
- `fro` also supports saving settings for one mode via `-s/--save`, but only when forcing a mode (`--direct` or `--no-direct`):
  - `./target/release/fro read --direct -s -n 100 <file>`

## High-level architecture (big picture)

- `fro` is a single binary that **always wraps each operation in a hill-climb optimizer** (`src/optimizer.rs`).
  - `-n <iterations>` controls how many optimizer iterations are run; use `-n 1` to just run once using the current config-derived params.
- The actual IO operations are in:
  - `src/reader.rs`: striped multi-threaded reads; optional substring search via `memchr::memmem::Finder` (the `grep` mode).
  - `src/writer.rs`: parallel writes and copy (read+write pipeline); uses `posix_fallocate` to pre-size/allocate output files.
  - `src/differ.rs`: parallel file diff / dual read bench; can stop early on mismatch unless `bench_only`.
- Concurrency model:
  - One OS thread per stripe worker; each worker creates its **own io_uring** (`iou::IoUring::new(1024)`) and drives SQE/CQE submission locally.
  - Queue depth (`qd`) is implemented as “in-flight ops per worker” and is part of the tuned parameter set.
- IO mode selection:
  - `--direct` / `--no-direct` force direct vs page cache.
  - `--auto` uses `mincore` (see `src/mincore.rs`) to detect whether the file looks cached and chooses direct vs page-cache defaults accordingly.
  - `write`/`copy` can override the *write* side independently via `--direct-write`, `--no-direct-write`, `--auto-write`.

## Key repo-specific conventions

- When generating test data or scratch files for validation, prefer the repo's own utilities over ad hoc shell/Python generators when practical:
  - use `./target/release/fro write --create <size> <path>` for sized files
  - use `./target/release/fro dd if=<src> of=<dst> ...` for offset/partial copy semantics
  - use `./target/release/fro copy`, `fro cat`, and related multicalls to exercise the actual optimized paths under test
  - reserve external generators for cases the repo tools cannot express directly
- `fro.json` is the source of truth for tuned params (per tool × {direct,page_cache}): see `src/config.rs`.
  - `AppConfig::load("fro.json")` auto-creates a default `fro.json` if missing.
  - Prefer regenerating via `fro-optimize` (or `fro ... -s`) rather than hand-editing.
- Direct I/O safety rules are enforced in hot paths:
  - Direct reads/writes are only used when offsets are 4K-aligned and the request length is a full aligned block; otherwise the code falls back to the non-`O_DIRECT` FD.
  - Buffers for uring IO are 4096-aligned via `common::AlignedBuffer`.
- `fro-benchmark` and `fro-optimize` create large temp files (default 4 GiB) under `--test-dir`; plan runs accordingly.
- Profiling/perf workflow is part of the project’s working style:
  - See `GEMINI.md` for rules like “no optimization without evidence” and how to back claims with `perf`/`strace`/assembly.
  - `perf/` contains helper scripts that run `perf stat` against `./target/release/fro ...` (paths inside those scripts may be environment-specific).
  - See `docs/profiling.md` for the repo’s measurement workflow and the branch-specific caveats around hot page-cache tests, cold writes, and low-overhead tracing.

## Profiling and optimization workflow

- Measure first, then tune:
  - run `cargo build --release`
  - run `./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB --no-fail`
  - identify which family regressed (`read` / `grep` / `diff` / `write` / `copy`) before touching code
- Use the lightest tool that can answer the question:
  - start with benchmark throughput and `perf stat`
  - use `strace -f -c` to count syscall batching / `io_uring_enter`
  - use `perf record` / `perf report` only after you know which path is slow
- Beware observer effect:
  - heavy tracing can distort IO measurements on modern NVMe arrays
  - the linked Tanel Poder articles are the model here: always consider whether the instrumentation itself is consuming CPU, cache, or synchronization budget
- Interpret hot vs cold carefully:
  - hot page-cache read tests should prewarm the file and then measure the steady-state run
  - cold cached writes often measure page-cache filling and writeback side effects more than raw device throughput
  - if the goal is raw write-path throughput, force direct writes and compare that separately
- Auto-write expectations:
  - for cold write benchmarking, `auto` should land on the direct-write path when that is the faster mode for the target
  - if `write (auto, cold)` behaves like cached writes, verify the code path before re-tuning config

## Performance review protocol for agentic turns

When a user asks about performance, throughput, or regressions, follow this protocol before making claims or proposing fixes:

- **State the proposition precisely before reasoning from numbers.**
  - Example: distinguish `archive build throughput`, `write engine throughput`, `msync/fsync durability cost`, and `end-to-end wall time`.
  - Do not compare `bytes / (build + write + sync)` against a claim that was about raw write throughput.

- **Decompose end-to-end timings into named phases.**
  - For any benchmark path that contains multiple substantial stages, print and analyze at least:
    - setup/allocation time
    - payload/build/copy time
    - write submission/completion time
    - durability barriers such as `msync`, `fsync`, `sync_all`, directory sync
  - Do not attribute a slow total to one subsystem until the phase split shows that subsystem dominates.

- **Check for hidden O(n) work before blaming the fast path.**
  - Inspect the benchmark/helper code for:
    - cloning large buffers
    - extra `Vec` materialization
    - repeated allocation/zeroing
    - avoidable `mmap`/`munmap`
    - extra format conversion or manifest rebuilding inside the timed region
  - If a benchmark reports unexpectedly low throughput, audit for accidental pre-copy or pre-processing overhead before speculating about IO limits.

- **Validate warm vs cold explicitly.**
  - If the first run differs sharply from reruns, record both and label them.
  - Do not present a cold first-run result as the canonical steady-state number for RAM or hot-cache paths.
  - For hot-path claims, require at least one warm rerun before drawing conclusions.

- **Compare against an in-repo baseline on the same mount and size.**
  - When a result seems wrong, run the closest existing benchmark or command on the same target path, same data size, and same IO mode.
  - Prefer comparisons like:
    - `bench-tar-archive ram-write` vs `fro write --create ...`
    - `tar` large-file path vs `fro cp -r`
    - mmap-file path vs existing write/mmap benches
  - Do not compare across different mounts, cache states, or durability semantics unless that difference is the point of the experiment.

- **Use stdlib or ad hoc programs only as controls, never as the primary proof for `fro`.**
  - A standalone `std::io::BufReader` / `BufWriter` test program does not exercise the tuned `fro` path and therefore cannot prove a `fro` regression or optimization.
  - External microprograms may be used as sanity checks or controls, but label them explicitly as external baselines.
  - Do not infer anything about `fro` performance from an external control until the same claim is reproduced on the real in-tree `fro` path with matching mount, size, cache state, and sync semantics.

- **Verify that the intended optimized API is actually the one being exercised.**
  - Before concluding that an optimized path is slow, inspect the call chain and confirm:
    - the benchmark reaches the intended helper
    - the helper is configured with the mount-specific tuned params
    - the IO mode (`direct`/`page-cache`/`auto`) is the one being claimed
    - direct-IO alignment rules are not silently forcing fallback behavior

- **Treat helper semantics as part of the benchmark contract.**
  - If a public helper is used as a benchmarked primitive, audit whether it performs extra work beyond the name's apparent promise.
  - Example: `write_buffer_range(...)` must not clone the entire source slice when the benchmark claim is about RAM-to-file write throughput.

- **Treat `/dev/null` and in-memory targets as upper bounds, not substitutes for real-target claims.**
  - `/dev/null` can show payload-generation or read-side ceiling behavior, but it does not measure archive-file destination behavior.
  - RAM-only assembly can show what the copy/build path can do without destination writeback, but it does not prove real-file tar performance.
  - Use these as diagnostic ceilings and explicitly label them as such.

- **Prefer first-class in-tree commands over temporary harnesses when presenting final numbers.**
  - Temporary benchmark crates or one-off binaries are acceptable for fast exploration.
  - But before presenting a result as the current project behavior, reproduce it through the closest committed command or helper in the repository.
  - If the temporary harness and the in-tree command disagree, investigate the semantic difference before drawing conclusions.

- **Re-state the exact benchmark intent when the user is distinguishing between similar paths.**
  - In this repo, “serialize a tar archive in userspace” and “use the cp-r style read path into precomputed tar offsets in RAM” are different propositions.
  - If the user is asking for the latter, do not substitute the former just because both produce tar bytes.
  - When there are multiple plausible benchmark meanings, explicitly name the one being implemented before measuring.

- **Do not delay the compile-and-run proof for new benchmark surfaces.**
  - If a new command or benchmark mode is being added, compile it and run it early rather than stacking more reasoning on unvalidated code.
  - Late validation increases the chance of reasoning from a path that does not yet build or whose CLI semantics are still wrong.

- **Keep measurement semantics aligned across comparisons.**
  - Match at least the following before comparing throughput numbers:
    - source payload size
    - target mount/device
    - cache temperature
    - direct/page-cache mode
    - whether durability work (`msync`/`fsync`/directory sync) is included
    - whether data is copied, generated, or reused in memory
  - If one of these differs, name that mismatch explicitly instead of presenting the numbers as directly comparable.

- **Use formal contradiction checks before accepting a surprising result.**
  - If measured throughput contradicts a validated baseline or known-good path, explicitly test the alternatives:
    - either the code path differs,
    - or the timed region includes extra work,
    - or cache/durability conditions differ.
  - Resolve which proposition is false before proposing an optimization.

- **Only write performance conclusions that survive cross-checks.**
  - A conclusion is not ready to present until it is consistent with:
    - the phase timing split,
    - the helper/code inspection,
    - the same-mount baseline,
    - and the warm/cold labeling.

### Critique of this protocol

- This protocol adds overhead and can slow down agent turns.
  - Response: use the cheapest checks that falsify the bad explanation first: inspect the timed helper, split phases, and run one same-mount baseline before escalating to `perf`/`strace`.

- It can still miss errors caused by benchmark code that mutates across turns.
  - Response: after each benchmark-path edit, rerun the nearest baseline immediately so any semantic drift is caught while context is fresh.

- It may overfocus on throughput and miss correctness or semantics differences.
  - Response: always pair performance comparisons with semantic checks: same payload size, same target semantics, same sync/durability behavior, same cache mode.

- It can become a long checklist that agents cargo-cult without prioritizing.
  - Response: apply the protocol in falsification order: first verify path identity and timing semantics, then same-mount baseline, then warm/cold state, and only then escalate to heavier tracing.

- It may bias agents toward excessive benchmarking of controls rather than the product path.
  - Response: require that every external control or temporary harness be paired with a reproduction on the real `fro` path before using it in a final conclusion.
