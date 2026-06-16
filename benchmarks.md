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

## `dd` small/medium transfer notes

Commands were run from `/repos/fast_read_optimizer` against local files under `target/dd-perf-baseline/` after `cargo build --release --quiet`.

Datasets:

- `small.bin`: `64 KiB`
- `medium.bin`: `8 MiB`

Before the dd-only change, `fro dd` delegated both whole-file and ranged copies to the tuned threaded copy path, which was expensive for small and medium transfers:

- `fro dd if=small.bin of=... status=none`: about `12.9 ms` median
- `fro dd if=small.bin of=... bs=4K count=16 status=none`: about `12.0 ms` median
- `fro dd if=medium.bin of=... status=none`: about `12.7 ms` median
- `fro dd if=medium.bin of=... bs=64K count=128 status=none`: about `12.7 ms` median
- `fro dd if=small.bin of=... bs=4K count=8 skip=2 seek=1 conv=notrunc status=none`: about `13.2 ms` median
- `fro dd if=medium.bin of=... bs=64K count=64 skip=8 seek=4 conv=notrunc status=none`: about `15.7 ms` median

Direct copy-strategy measurements on the same host showed:

- `64 KiB` and `1 MiB`: one-call `copy_file_range` was fastest
- `4 MiB` to `16 MiB`: chunked `copy_file_range` beat the threaded path
- the threaded copy path stayed best only once transfers grew beyond this “small/medium” range

After switching dd to reuse those existing copy primitives for page-cache small/medium copies:

- `fro dd if=small.bin of=... status=none`: about `4.0 ms` median
- `fro dd if=small.bin of=... bs=4K count=16 status=none`: about `4.1 ms` median
- `fro dd if=medium.bin of=... status=none`: about `9.7 ms` median
- `fro dd if=medium.bin of=... bs=64K count=128 status=none`: about `10.3 ms` median
- `fro dd if=small.bin of=... bs=4K count=8 skip=2 seek=1 conv=notrunc status=none`: about `4.3 ms` median
- `fro dd if=medium.bin of=... bs=64K count=64 skip=8 seek=4 conv=notrunc status=none`: about `11.1 ms` median

The `/dev/null` path was intentionally left unchanged by this edit; it remained roughly flat (`~9.1 ms` for `64 KiB`, `~9.5 ms` for `8 MiB`) because the change was scoped to reusable copy-path selection for real-file dd transfers.

## Multicall flag benchmark notes

These notes are here so future benchmark sessions keep behavior-parity work separate from execution-path regressions.

- Benchmark plain and flagged forms separately. A new GNU-style flag may preserve the tuned backend, or it may intentionally force extra transformation/scanning work.
- `cat`
  - plain `cat`, `-u`, and IO-mode selectors are the copy-style baseline
  - `-n`, `-b`, `-s`, `-E`, `-T`, `-v`, `-A`, `-e`, and `-t` force ordered line transformation; compare them to plain `cat`, not to the `fro read` ceiling
- `fgrep`
  - `-n`, `-i`, `--no-ignore-case`, and `-x` should stay in the same literal-search family
  - measure them on the same dataset/cache state as plain `fgrep`, because the extra CPU cost is in normalization/line handling rather than in losing the read path
- `wc`
  - `wc -c` on regular files can be metadata-only, so it is not a scan benchmark
  - `wc -c` on pipes can be a splice-count benchmark
  - `-l/-w/-m/-L` are the right cases when validating the streaming counting path
- `head` / `tail`
  - regular-file byte/range cases and stream cases are different propositions; measure them separately
  - `tail` on a non-seekable stream may need whole-input buffering/scanning that regular files avoid
  - for `head -c N`, compare regular-file prefix runs to `bin/wc` only up to the split point and compare pipe runs to `cat | head -c N` / splice-style prefixes, not to full-stream scans
- `cp` / `mv`
  - separate rename-only / skip-policy benchmarks from actual byte-moving benchmarks
  - if a policy flag still results in a real copy, verify that throughput is still governed by the normal copy engine rather than a slower compatibility shim

- When a new parity flag lands, add a short benchmark note or helper/path verification note alongside the behavior tests. Equality-only coverage does not tell future agents whether a fast path was preserved.

## `rm -r` and `mv` benchmarks

Commands were run on the same datasets described above, with source trees copied into `/data/ilmari_cache/fro-test/rm-mv-bench`.

Filesystem note:

- `/data` is `ext4`
- `/tmp` is on the root `zfs` pool

So `mv` from `/data/...` to `/tmp/...` is a real cross-filesystem move and exercises the copy+remove fallback rather than plain `rename(2)`.

### Recursive remove (`rm -r`)

Commands:

```bash
./target/release/fro rm -r /data/ilmari_cache/fro-test/rm-mv-bench/rm-small-fro
rm -rf /data/ilmari_cache/fro-test/rm-mv-bench/rm-small-sys

./target/release/fro rm -r /data/ilmari_cache/fro-test/rm-mv-bench/rm-mixed-fro
rm -rf /data/ilmari_cache/fro-test/rm-mv-bench/rm-mixed-sys
```

Results:

- small-file tree:
  - `fro rm -r`: `0.07s`
  - `system rm -rf`: `0.20s`
- mixed tree:
  - `fro rm -r`: `0.81s`
  - `system rm -rf`: `0.81s`

Interpretation:

- `fro rm -r` is materially faster on the metadata-heavy small-file tree.
- On the mixed tree, `fro` and system `rm` were effectively tied in this run.

### Move (`mv`)

#### Same-filesystem rename path

Commands:

```bash
./target/release/fro mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-small-same-src /data/ilmari_cache/fro-test/rm-mv-bench/mv-small-same-dst
./target/release/fro mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-mixed-same-src /data/ilmari_cache/fro-test/rm-mv-bench/mv-mixed-same-dst
```

Results:

- same-fs small tree: `0.00s`
- same-fs mixed tree: `0.00s`

Interpretation:

- As expected, same-filesystem `mv` is just `rename(2)` and is effectively instantaneous at this scale.

#### Cross-filesystem move path (`/data` -> `/tmp`)

Commands:

```bash
./target/release/fro mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-small-cross-src /tmp/fro-mv-small-cross-dst
mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-small-cross-sys-src /tmp/fro-mv-small-cross-sys-dst

./target/release/fro mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-mixed-cross-src /tmp/fro-mv-mixed-cross-dst
mv /data/ilmari_cache/fro-test/rm-mv-bench/mv-mixed-cross-sys-src /tmp/fro-mv-mixed-cross-sys-dst
```

Results:

- small-file tree:
  - `fro mv` cross-fs: `0.15s`
  - `system mv` cross-fs: `1.12s`
- mixed tree:
  - `fro mv` cross-fs: `2.18s`
  - `system mv` cross-fs: `1.64s`

Interpretation:

- On the small-file tree, `fro mv` is much faster than system `mv`.
- On the mixed tree, current `fro mv` loses to system `mv`, so the cross-filesystem large-tree move path still has meaningful optimization headroom.

## Notes

- A one-file `strace` of GNU tar to a real archive showed the expected pattern:
  - `newfstatat` for metadata
  - `openat` + `read` for payload
  - one larger buffered `write` for tar output
- That confirms GNU tar is not reading payload with metadata syscalls; it is batching user-space output well.

## Coreutils flag-parity benchmark slice (`wc` + `tail`)

Practical first slice for recently added flag-parity work:

- `wc -l -w -m -L`
  - chosen because these flags force a real scan and are already covered by parity tests
  - `wc -c` was intentionally excluded because regular-file GNU `wc` can answer it from metadata
- `tail -n 4096`
  - chosen as the line-oriented regular-file case
- `tail -c 65536`
  - chosen as the byte-oriented regular-file case

Harness added:

```bash
./perf/coreutils_flag_parity_slice.py --size 256MiB --repeat 5
```

What it does:

- creates `target/coreutils-flag-parity-bench/wc-tail-text.txt`
- verifies output parity before timing:
  - `fro wc --no-direct -l -w -m -L`
  - `wc -l -w -m -L`
  - `fro tail -n 4096`
  - `tail -n 4096`
  - `fro tail -c 65536`
  - `tail -c 65536`
- includes uutils via the local `coreutils` multicall only when that subcommand is actually available

Local availability during this run:

- GNU system tools present: `wc`, `tail`, `grep`, `cp`
- uutils present via `/home/ilmari/.cargo/bin/coreutils`
  - `coreutils wc`: available
  - `coreutils tail`: available
  - `coreutils grep`: **not** available in this environment, so `fgrep` was not included in this first measured slice

Measured on a `256 MiB` generated text fixture under hot page cache, best of `5` runs:

### `wc -l -w -m -L`

| Command | Seconds | Effective GB/s |
| --- | ---: | ---: |
| `fro wc --no-direct` | `0.5454` | `0.49` |
| `system wc` | `1.0489` | `0.26` |
| `uutils wc` | `0.9946` | `0.27` |

### `tail -n 4096`

Tail is reported as latency instead of throughput because the regular-file implementation does not read the full file once it can identify the suffix boundary.

| Command | Seconds | Best ms |
| --- | ---: | ---: |
| `fro tail -n 4096` | `0.1185` | `118.46` |
| `system tail -n 4096` | `0.0037` | `3.68` |
| `uutils tail -n 4096` | `0.0049` | `4.90` |

### `tail -c 65536`

| Command | Seconds | Best ms |
| --- | ---: | ---: |
| `fro tail -c 65536` | `0.0056` | `5.56` |
| `system tail -c 65536` | `0.0023` | `2.33` |
| `uutils tail -c 65536` | `0.0045` | `4.48` |

Interpretation:

- This is a useful first parity-performance slice because it covers both:
  - a scan-heavy flag family (`wc -l/-w/-m/-L`)
  - a regular-file suffix/range family (`tail -n`, `tail -c`)
- `fro wc --no-direct` beat both GNU `wc` and the available uutils `wc` on this generated text fixture.
- `fro tail` is still behind both GNU `tail` and the available uutils `tail` on these regular-file cases, especially the line-count form.
- The current implementation work is focused on replacing full-file newline-offset collection with reverse suffix scanning for regular files and bounded trailing windows for stream inputs; re-run this slice after tail changes to quantify the remaining gap.
- `fgrep` remains a good next slice, but the local uutils multicall does not expose `grep`, so that comparison should be added only where a real uutils `grep` is present or with system-only notes.

## Head/tail roofline slice (`bin/wc` + cat-style pipe ceiling)

Practical roofline slice for the current head/tail multicall surfaces:

- `head -n 4096` on a regular file
  - roofline comparator: `bin/wc -l` on the exact emitted prefix up to the split
- `tail -n 4096` on a regular file
  - roofline comparator: `bin/wc -l` on the full file, because suffix discovery still depends on finding the split point in the source
- `tail -c 65536` on a pipe
  - roofline comparators:
    - `bin/cat >/dev/null` as the cat-style splice / vmsplice ceiling
    - `bin/cat | bin/wc -c >/dev/null` as a scan-like pipe baseline

Harness added:

```bash
python3 perf/head_tail_roofline_slice.py --size 256MiB --repeat 5
```

What it does:

- creates `target/head-tail-roofline-bench/head-tail-text.txt`
- materializes the exact `head -n 4096` prefix into `target/head-tail-roofline-bench/head-n4096-prefix.txt`
- verifies output parity before timing:
  - `fro head -n 4096`
  - `head -n 4096`
  - `fro tail -n 4096`
  - `tail -n 4096`
  - `fro tail -c 65536`
  - `tail -c 65536`
  - `bin/cat ... | fro tail -c 65536`
  - `bin/cat ... | tail -c 65536`

Measured on branch `coreutils-multicall`, current working tree `85a7cb8 (dirty working tree)`, using a `256 MiB` generated text fixture under hot page cache, best of `5` runs:

### `head -n 4096` regular file

| Command | Seconds | Best ms | Relative to `bin/wc -l prefix` |
| --- | ---: | ---: | ---: |
| `fro head -n 4096` | `0.0140` | `14.05` | `3.71x` |
| `system head -n 4096` | `0.0034` | `3.42` | `0.90x` |
| `bin/wc -l prefix` | `0.0038` | `3.79` | `1.00x` |

### `tail -n 4096` regular file

| Command | Seconds | Best ms | Relative to `bin/wc -l full file` |
| --- | ---: | ---: | ---: |
| `fro tail -n 4096` | `0.0042` | `4.23` | `0.10x` |
| `system tail -n 4096` | `0.0028` | `2.79` | `0.06x` |
| `bin/wc -l full file` | `0.0441` | `44.12` | `1.00x` |

### `tail -c 65536` regular file

| Command | Seconds | Best ms |
| --- | ---: | ---: |
| `fro tail -c 65536` | `0.0052` | `5.16` |
| `system tail -c 65536` | `0.0029` | `2.95` |

### `tail -c 65536` pipe path

| Command | Seconds | Effective GB/s | Relative to `bin/cat >/dev/null` |
| --- | ---: | ---: | ---: |
| `bin/cat \| fro tail -c 65536` | `0.1095` | `2.45` | `1.69x` |
| `bin/cat \| system tail -c 65536` | `0.2054` | `1.31` | `3.17x` |
| `bin/cat >/dev/null` | `0.0647` | `4.15` | `1.00x` |
| `bin/cat \| bin/wc -c >/dev/null` | `0.0921` | `2.92` | `1.42x` |

Interpretation:

- `fro head -n 4096` is currently well above the prefix-count roofline and slower than GNU `head` on this hot regular-file case.
- `fro tail -n 4096` is much faster than a full-file `bin/wc` scan, which is the expected shape for a seekable suffix operation, but it still trails GNU `tail`.
- `fro tail -c 65536` on a pipe beats GNU `tail` on the measured pipe case, but it is still below the local cat-style ceiling and slightly below the local `bin/cat | bin/wc -c` pipe baseline.
- These numbers are measurements of the current working tree state captured above; re-run the harness after any head/tail implementation changes rather than carrying the ratios forward.

### Follow-up: `head -n` prefix-scan slice

After switching `head -n` from the generic ordered-visitor path to a smaller blocking reader that stops immediately after the split line is emitted, the same local `256 MiB` / best-of-`5` harness produced:

| Command | Seconds | Best ms | Relative to `bin/wc -l prefix` |
| --- | ---: | ---: | ---: |
| `fro head -n 4096` | `0.0151` | `15.05` | `1.96x` |
| `system head -n 4096` | `0.0037` | `3.73` | `0.49x` |
| `bin/wc -l prefix` | `0.0077` | `7.66` | `1.00x` |

Additional pipe spot-check on the same warmed fixture:

| Command | Seconds |
| --- | ---: |
| `bin/cat \| fro head -n 4096` | `0.0185` |
| `bin/cat \| system head -n 4096` | `0.0126` |
| `bin/cat >/dev/null` | `0.0752` |

Interpretation:

- The practical win here is path reuse and early stop on the line-prefix path: `fro head -n 4096` no longer keeps scanning after the split, and the small-cutoff pipe case improved sharply from the earlier `~0.1208s` spot-check to `~0.0185s`.
- The regular-file roofline gap remains: this slice improves the cutoff behavior but does not yet catch GNU `head` or the `bin/wc -l prefix` comparator on hot regular files.
- Next likely slice, if continuing head-only work, is reducing the remaining regular-file setup/output overhead around the small emitted prefix rather than broadening the algorithm again.

## `tail -c` pipe-window benchmark note

For the byte-mode pipe path, keep the measurement focused on non-seekable stdin and report latency only.

- Suggested fixture shape:
  - generate `target/coreutils-tail-pipe-bench/tail-pipe.bin`
  - sizes worth re-checking:
    - input: `4 MiB`
    - suffix counts: `64 KiB`, `1 MiB`, `1.5 MiB`
- Verify parity before timing:
  - `cat target/coreutils-tail-pipe-bench/tail-pipe.bin | ./target/release/fro tail -c 65536`
  - `cat target/coreutils-tail-pipe-bench/tail-pipe.bin | tail -c 65536`
  - repeat for `1048576` and `1572865`
- Interpretation guardrails:
  - `tail -c` on a pipe still must consume the full input; the optimization target is bounded retained memory and cheaper suffix retention, not early completion
  - for counts `<= 1 MiB`, retain a `1 MiB` internal window
  - for larger counts, retain a reused window rounded up to `ceil(count / 1 MiB) * 1 MiB`
  - report both command latency and the retained-window size used by the implementation

### Head/tail pipe byte-path update

- `head -c` on FIFOs/stdin now stays on the splice-limited path instead of falling back to buffered block visits.
- `tail -c` on FIFOs/stdin now has two byte-mode fast paths:
  - `<= 1 MiB`: keep a bounded internal pipe/window, splice leading bytes to `/dev/null`, then splice the retained suffix to stdout.
  - `> 1 MiB`: keep a reused page-aligned circular window rounded to a multiple of `1 MiB`, then emit only the final suffix after EOF.
- Regular-file byte roofline comparisons should continue to use `bin/wc` as the scan baseline for the corresponding prefix/suffix split; these pipe changes target the `cat|tail` and `cat|head` splice roofline instead.
