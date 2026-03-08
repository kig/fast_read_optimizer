# Gemini AI Instructions

*   **No Performance Speculation:** All claims regarding performance bottlenecks, memory access patterns, hardware limits (like PCIe or RAM bandwidth), or caching behaviors must be backed by concrete profiling and tracing (e.g., `perf`, `strace`, `valgrind`, or reading assembly).
*   **Benchmarks are End-to-End only:** Remember that benchmarks only measure e2e timings; they do *not* tell you what actually happened beneath the surface.
*   **Trace and Analyze:** Before explaining "why" a piece of software is fast or slow, trace its syscalls and read its assembly output to give an accurate, factual description of how the software physically functions.