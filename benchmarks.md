# Benchmarks

This file records ad hoc benchmark results gathered during development in this session.

## Environment and datasets

Unless noted otherwise, commands were run from `/repos/fast_read_optimizer` on Linux.

Datasets used:

- Mixed large-file tree: `/data/ilmari_cache/fro-test/tar-mixedbench`
  - `blob-512m.bin` = `512 MiB`
  - `blob-1g-a.bin` = `1 GiB`
  - `blob-1g-b.bin` = `1 GiB`
  - Total payload bytes: `2.5 GiB` (`2684354560` bytes)
- Small-file tree: `/data/ilmari_cache/fro-test/tar-smallbench`
  - `10,000 x 4 KiB` files across 40 directories
  - Total payload bytes: `39.06 MiB` (`40960000` bytes)

## Mixed large-file tree

### Current tar baselines

`fro tar -cf /dev/null /data/ilmari_cache/fro-test/tar-mixedbench`

- `0.12s` in one run
- `0.35s` in a verbose tracking run

The verbose `/dev/null` tracking run showed:

- `sendfile_streams=1`
- no real destination writes

This is the fast streaming baseline without archive-file writeback cost.

`fro tar -cf /data/ilmari_cache/fro-test/tar-mixedbench.tar /data/ilmari_cache/fro-test/tar-mixedbench`

- `4.21s` before the verbose tracker
- `4.33s` with verbose tracking

The verbose real-target tracking run showed:

- `mt_copy_jobs=3`
- `mt_copy_threads=12`

for most of the run, confirming the large-file tar path is already issuing three parallel threaded copy jobs into one archive file.

### Comparison against system tar and fro cp -r

Real target under `/data/ilmari_cache/fro-test/`:

- `fro tar` -> `4.210s` -> `0.594 GiB/s`
- `system tar` -> `4.890s` -> `0.511 GiB/s`
- `fro cp -r` -> `0.430s` -> `5.814 GiB/s`

Interpretation:

- `fro tar` is faster than `system tar` on this mixed real-target case.
- `fro tar` is still far below `fro cp -r`, so the remaining bottleneck is the single-archive-file destination behavior, not lack of parallel large-file scheduling.

### Tar-to-RAM benchmark

Intent: approximate the “optimal tar” roofline by precomputing tar offsets and using the threaded reader path to copy file payload blocks directly into a preallocated archive buffer in RAM, with headers written separately.

Temporary harness command:

```bash
cargo run --quiet --manifest-path /tmp/fro-tar-ram-bench/Cargo.toml
```

Temporary harness result:

- `tar_to_ram_s=0.210585`
- `bytes=2684354560`
- `11.872 GiB/s`

Built-in command:

```bash
./target/release/fro bench-tar-archive ram /data/ilmari_cache/fro-test/tar-mixedbench
```

Built-in command results:

- cold-ish first run: `1.8901s` -> `1.323 GiB/s`
- warm rerun: `0.2667s` -> `9.374 GiB/s`
- warm rerun: `0.2567s` -> `9.741 GiB/s`

Interpretation:

- The first-class command reaches the same general range as the temporary harness once the source data and benchmark path are warm.
- The large cold/warm spread means cache state matters a lot here; warm reruns are the more useful roofline comparison for this RAM-only path.

### Tar-to-RAM plus file write benchmark

Built-in command:

```bash
./target/release/fro bench-tar-archive ram-write /data/ilmari_cache/fro-test/tar-mixedbench /data/ilmari_cache/fro-test/tar-mixedbench.tar
```

Result:

- before fixing write-buffer pre-copy:
  - build: `0.2947s` -> `8.482 GiB/s` payload
  - write: `2.4923s` -> `1.003 GiB/s` archive
  - total: `2.7870s`
- after fixing `write_buffer_range(...)` to share the input buffer instead of cloning it:
  - build: `0.2612s` -> `9.573 GiB/s` payload
  - write: `0.2640s` -> `9.469 GiB/s` archive
  - sync: `0.0003s`
  - total: `0.5255s`

Interpretation:

- The RAM archive build phase stays fast and remains much closer to the desired “cp-r into memory” behavior.
- The earlier `~1.0 GiB/s` write figure was bogus because `write_buffer_range(...)` cloned the entire archive buffer before issuing writes, effectively benchmarking `memcpy + write`.
- After removing that extra copy, the archive flush lands in the expected optimized-write range.

### Plain fro write baseline on same target

Command:

```bash
./target/release/fro write --create 2684354560 /data/ilmari_cache/fro-test/tar-write-baseline.bin
```

Result:

- `0.2490s` -> `10.8 GB/s`

Interpretation:

- After the `write_buffer_range(...)` fix, `bench-tar-archive ram-write` and the plain `fro write` baseline are in the same throughput class on the same mount.

### Tar directly into mmap-backed file benchmark

Built-in command:

```bash
./target/release/fro bench-tar-archive mmap-file /data/ilmari_cache/fro-test/tar-mixedbench /data/ilmari_cache/fro-test/tar-mixedbench.tar
```

Result:

- build into mmap: `2.8461s` -> `0.878 GiB/s` payload
- `msync`: `1.6175s`
- total: `4.4636s`

Interpretation:

- Writing directly into the mapping was slower than the RAM-only and RAM-plus-write variants on this dataset.
- This path ends up in roughly the same range as ordinary real-target tar, so mmap does not currently bypass the single-archive-file bottleneck in a useful way here.

Interpretation:

- Tar payload assembly into RAM is much faster than writing the archive to a real file.
- It is also slower than the aspirational `30+ GB/s` roofline, so there is still room to optimize the current RAM-path benchmark if it becomes a first-class tool.

## Small-file tree

Real target under `/data/ilmari_cache/fro-test/`:

- `fro tar` -> `0.160s` -> `0.238 GiB/s`
- `system tar` -> `0.170s` -> `0.224 GiB/s`
- `fro cp -r` -> `0.050s` -> `0.763 GiB/s`

Interpretation:

- The reusable hugepage-backed slab writer brought `fro tar` back ahead of `system tar` on this small-file real-target case.
- `fro cp -r` still has a large lead.

## Small-file syscall profile

For the valid `10,000 x 4 KiB` tree:

`system tar -cf /dev/null ...` aggregated `strace -c`:

- about `20563` syscalls total
- dominated by `newfstatat`
- very few `write` calls

`fro tar -cf /dev/null ...` aggregated `strace -c`:

- about `40459` syscalls total
- about `10049 openat`
- about `10042 statx`
- about `10010 read`
- about `10049 close`
- only `90 write`

Interpretation:

- After write batching, the small-file gap is dominated by per-file metadata/open/read/close churn, not write syscall count.

## Notes

- A one-file `strace` of GNU tar to a real archive showed the expected pattern:
  - `newfstatat` for metadata
  - `openat` + `read` for payload
  - one larger buffered `write` for tar output
- That confirms GNU tar is not reading payload with metadata syscalls; it is batching user-space output well.
