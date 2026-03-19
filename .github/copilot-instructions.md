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
