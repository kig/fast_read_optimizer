# Gemini AI Instructions

*   **No Optimization Without Evidence:** If you have no evidence to show that a particular edit will improve performance, don't do it unless it retains performance and simplifies the code.
*   **No Performance Speculation:** All claims regarding performance bottlenecks, memory access patterns, hardware limits (like PCIe or RAM bandwidth), or caching behaviors must be backed by concrete profiling and tracing (e.g., `perf`, `strace`, `valgrind`, and reading assembly).
*   **Benchmarks are End-to-End only:** Remember that benchmarks only measure e2e timings; they do *not* tell you what actually happened beneath the surface.
*   **Trace and Analyze:** To explain why a routine is fast or slow, trace its syscalls and read its assembly output to give an accurate, factual description of how the software physically functions.
*   **Use the Optimizer:** There is an optimizer to find the best-performing IO settings for the current build. Use it. Do not edit the result file directly.

## Performance Analysis & Documentation

### 1. Perf: Hardware Counter Interpretation
Use `perf stat` to establish the fundamental hardware bottleneck.
*   **Cycles & Instructions (IPC):** If IPC is low (< 1.0) while throughput is high, the CPU is stalling on memory/IO. If IPC is high (> 2.0), the routine is instruction-bound (execution ports are saturated).
*   **Cache Misses:** High LLC (Last Level Cache) misses during `O_DIRECT` are expected but should be baselined against `dual-read-bench` to distinguish between DMA bus contention and CPU-to-RAM bandwidth limits.
*   **TLB Misses:** High `dTLB-load-misses` indicates that memory access patterns are jumping across page boundaries too frequently for the MMU to keep up.

**Project Script Guide:**
*   `perf_stat.sh`: Monitors TLB and pipeline stalls. Use to check if address translation is a bottleneck.
*   `perf_stat2.sh`: Monitors cache hierarchy. Use to identify memory bandwidth saturation (LLC misses).
*   `perf_stat_grep.sh`: Focused on compute-heavy `memchr` search logic.

### 2. Strace: Syscall Batching & Overhead
Use `strace -f -c` to profile the interaction between the application and the kernel.
*   **io_uring_enter:** The primary syscall for `io_uring`. In an optimized build, the count of `io_uring_enter` should be significantly lower than the total IO requests. If the counts are 1:1, the `peek_for_cqe` batching logic is broken or failing to find ready events.
*   **Futex/Synchronization:** High time in `futex` indicates thread contention or starvation.
*   **Memory Management:** Check for excessive `mmap`/`munmap` calls in the hot path, which indicates buffer reuse logic is failing.

### 3. Profiling: Identifying Hotspots
Use `perf record -F 999 -g --call-graph dwarf` followed by `perf report`.
*   **Hot Paths:** Identify the specific functions consuming cycles. 
*   **Kernel vs. User:** Differentiate between time spent in the kernel (likely DMA/driver overhead) vs. userspace (likely search/comparison logic).

### 4. Assembler Interpretation
Use `perf annotate --stdio` or `objdump -d` to verify compiler output.
*   **Vectorization:** Search for `v`-prefixed instructions (e.g., `vpcmpeqb`, `vpand`). If these are absent in `diff` or `grep`, the compiler failed to use AVX2/AVX-512.
*   **Loop Unrolling:** Verify if the hot loop is unrolled. If the assembly contains many identical vector instructions in a row, the compiler has successfully unrolled the loop.
*   **Stalls:** Look for instructions with high cycle counts in `perf annotate`. For example, a `vmovdqu` with high cycles indicates a load stall (waiting for RAM). A `vpcmpeqb` with high cycles suggests an execution port bottleneck.
