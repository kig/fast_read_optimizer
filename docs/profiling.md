# Profiling and performance workflow

This project is performance-sensitive enough that the measurement method matters almost as much as the code.

The short version:

- benchmark after every hot-path edit
- use a large enough working set to avoid fake wins
- separate page-cache behavior from direct-IO behavior
- keep tracing overhead low enough that the profiler is not the bottleneck
- only run the optimizer after the code path itself is healthy

## Baseline workflow

Build release binaries first:

```bash
cargo build --release
```

Run the benchmark suite on the target mount/device:

```bash
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB --no-fail
```

Why `--test-size 4GB`?

- on the fast NVMe array used during recent tuning, smaller tests made the read-family results too optimistic or too noisy
- `4GB` was large enough to expose the real steady-state behavior of `read`, `grep`, `diff`, and `dual-read-bench`

If you only want one family:

```bash
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB read
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB grep
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB diff
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB write
./target/release/fro-benchmark --test-dir /mnt/nvme --test-size 4GB copy
```

## Hot path development rule

If you change a hot code path, rerun `fro-benchmark` before trusting `fro-optimize`.

Reason:

- `fro-optimize` only finds the best parameters for the current implementation
- if the implementation regressed, the optimizer will tune the slower code path and can make the saved config look “optimized” while overall throughput is still worse

In practice:

1. edit the code
2. rebuild release
3. rerun the affected benchmark family
4. only then run `fro-optimize` if the code path still looks healthy

## Choosing the right IO mode for the question

### Direct IO

Use direct IO when you want to measure raw device throughput with minimal page-cache interference.

Examples:

```bash
./target/release/fro read --direct -n 1 /mnt/nvme/bigfile
./target/release/fro write --direct-write -n 1 /mnt/nvme/out.bin
./target/release/fro copy --direct --direct-write -n 1 /mnt/nvme/in.bin /mnt/nvme/out.bin
```

This is the right mode when the question is:

- how fast is the storage device itself?
- how fast is the direct-IO code path?
- did a code change hurt the raw read or write pipeline?

### Page cache

Use page-cache mode when you want to measure cached steady-state behavior or CPU/memory overheads.

Examples:

```bash
./target/release/fro read --no-direct -n 1 /mnt/nvme/bigfile
./target/release/fro grep --no-direct -n 1 needle /mnt/nvme/bigfile
./target/release/fro diff --no-direct -n 1 /mnt/nvme/a.bin /mnt/nvme/b.bin
```

For hot-cache tests, explicitly warm the file first and then measure the steady-state run.

### Cold cached writes

Interpret cold cached writes carefully.

Large cached writes can benchmark page-cache fill, dirty-page accounting, and writeback side effects more than the storage device itself. On the NVMe array used during recent work, this was a major reason that cached cold-write numbers looked far worse than direct-write numbers.

If your question is “how fast is the write path really?”, compare against forced direct writes:

```bash
./target/release/fro write --direct-write -n 1 /mnt/nvme/out.bin
```

If your question is “what does the cached write behavior look like for a real application?”, then the lower cached number may still be valid, but it should not be confused with raw device throughput.

## Profiling workflow

### 1. `perf stat`

Use `perf stat` first to decide whether the bottleneck is CPU-side or IO/memory-side.

Examples:

```bash
perf stat ./target/release/fro read --direct -n 1 /mnt/nvme/bigfile
perf stat ./target/release/fro grep --no-direct -n 1 needle /mnt/nvme/bigfile
```

Things to look at:

- cycles
- instructions
- IPC
- cache misses
- TLB misses

General interpretation:

- high throughput with low IPC usually means the CPU is waiting on IO or memory
- high IPC with lower throughput suggests the code is instruction-bound
- large cache or TLB miss rates may point to bad block sizing or poor locality

### 2. `strace -f -c`

Use `strace -f -c` to check whether batching is still working:

```bash
strace -f -c ./target/release/fro read --direct -n 1 /mnt/nvme/bigfile
strace -f -c ./target/release/fro copy --no-direct --direct-write -n 1 /mnt/nvme/in.bin /mnt/nvme/out.bin
```

Focus on:

- `io_uring_enter`
- `futex`
- `mmap` / `munmap`
- unexpected metadata syscalls

If `io_uring_enter` grows sharply after a code change, that often means batching or queue utilization regressed.

### 3. `perf record` / `perf report`

Once you know which family regressed, sample the hot path:

```bash
perf record -F 999 -g --call-graph dwarf -- ./target/release/fro read --no-direct -n 1 /mnt/nvme/bigfile
perf report
```

This is where you answer:

- is time in userspace or kernel?
- is the bottleneck in read submission, completion handling, search logic, comparison logic, or synchronization?
- did a helper added for correctness move into the hot loop?

### 4. Assembly / annotate

If the hot function is compute-bound, inspect assembly:

```bash
perf annotate --stdio
```

Or use `objdump -d` for deeper inspection.

This is mainly useful for `grep`, hashing, and compare loops where vectorization or unrolling matters.

## Keep instrumentation overhead low

The two Tanel Poder articles below are the model for the repo’s profiling discipline:

- <https://tanelpoder.com/posts/optimizing-ebpf-biolatency-accounting/>
- <https://tanelpoder.com/posts/11m-iops-with-10-ssds-on-amd-threadripper-pro-workstation/>

Main lessons applied here:

- always ask whether the measurement tool is distorting the result
- prefer lower-overhead counters and sampling before high-frequency tracing
- when monitoring millions of IOs per second, global shared accounting can become the bottleneck
- modern fast storage often shifts the limit from disk media to CPU, memory bandwidth, cache traffic, or synchronization

That means:

- do not jump straight to heavy tracing
- avoid over-instrumenting high-frequency per-IO events
- compare observed throughput with CPU cost, not just wall-clock time

## Optimizer workflow

Run the optimizer only after the code path itself is healthy:

```bash
./target/release/fro-optimize --test-dir /mnt/nvme read grep diff write copy
```

Remember the config search order:

1. `-c <path>`
2. `$FRO_CONFIG`
3. `~/.fro/fro.json`
4. `/etc/fro.json`

If you are comparing repo-local changes and want the benchmark run to use a specific config file, pass it explicitly or set `FRO_CONFIG`. Otherwise you may end up tuning one config and benchmarking another.

## Branch-specific lessons from recent work

- use `--test-size 4GB` when validating the read-family on the fast NVMe array
- benchmark immediately after code-path edits so regressions do not get hidden behind later re-tuning
- for cold writes, distinguish “cached write behavior” from “raw write-path throughput”
- mixed-mode copy (`--no-direct --direct-write`) must be validated explicitly because read-side and write-side directness are independent concerns
