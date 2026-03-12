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
