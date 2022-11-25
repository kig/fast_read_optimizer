# fast_read_optimizer

fast_read_optimizer is a tool to find the fastest way to read a file into memory, targeted at NVMe and page cache.

It reads the file in stripes using multiple threads, and uses a stochastic hill climb optimizer to find the 
best-performing combination of thread count, block size and queue depth.

This is useful if you want to write e.g. a grep implementation that runs at 20 GB/s off NVMe or 50 GB/s off page cache.

## Usage

```bash
cargo build --release

# Direct I/O uncached read
$ target/release/fast_read_optimizer --direct filename
Opening file filename for reading
Reading 6442450944 bytes
...
Read 6443499008 bytes in 0.321657352 s, 20.0 GB/s, t=15 bs=3072, qd=1

# Use page cache
$ target/release/fast_read_optimizer filename
Opening file filename for reading
Reading 6442450944 bytes
...
Read 6442450944 bytes in 0.07575304 s, 85.0 GB/s, t=32 bs=424, qd=2

# Search for a pattern (basically grep -UFboa pattern filename)
$ target/release/fast_read_optimizer --direct pattern filename
...
Found pattern at 934539552
Found pattern at 2945568459
Read 6443289600 bytes in 0.351962814 s, 18.3 GB/s, t=17 bs=3840, qd=4

$ target/release/fast_read_optimizer pattern filename | grep GB # to keep only the performance numbers
...
Read 6450193920 bytes in 0.139828309 s, 46.1 GB/s, t=32 bs=416, qd=1
```

### License

MIT