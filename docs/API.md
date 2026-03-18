# API notes for the `fro` crate

This document is for Rust users of the crate API.

The crate currently exports these public modules:

- `fro::config`
- `fro::block_hash`

It also re-exports:

- `fro::IOMode`

And now exposes:

- `fro::reader`
- `fro::stream`
- `fro::writer`

The public API is useful, but it is still closely tied to the CLI implementation and should be treated as evolving rather than as a polished long-term stability contract.

At the high-level convenience layer, the most commonly useful helpers are now:

- `fro::read_file(...)`
- `fro::write_file(...)`
- `fro::write_file_range(...)`
- `fro::copy_file(...)`
- `fro::copy_file_via_memory(...)`

If you are an agent generating Rust code, the fastest path is usually:

1. `load_config(None)` or `load_config(Some(path))`
2. choose `IOMode::{Auto, Direct, PageCache}`
3. use `fro::reader` for "load a whole file" or "map a function over blocks"
4. use `fro::stream::{ParallelFile, ParallelWriter}` for blockwise transforms
5. use `fro::writer::SequentialWriter` only when you need to append trailers, sidecars, or other variable-length metadata after the main data path

That mirrors the example programs and keeps you on the same tuned IO path family as the CLI.

## Crate name

Package / crate name:

```toml
[dependencies]
fro = { path = "../fast_read_optimizer" }
```

## `fro::config`

Use this module when you want to:

- load the same config files the CLI uses
- inspect or modify direct / page-cache tuning
- resolve per-path overrides the same way `fro` does

Important public types:

- `IOParams`
- `ModeConfig`
- `AppConfig`
- `ConfigBundleV1`
- `MountOverrides`
- `AppConfigPatch`
- `ModeConfigPatch`
- `DeviceDbConfig`
- `LoadedConfig`

Important functions:

- `load_config(path: Option<&str>) -> LoadedConfig`
- `default_user_config_path()`
- `default_system_config_path()`
- `resolve_default_config_path()`

Typical pattern:

```rust
use fro::config::load_config;

let cfg = load_config(None);
let read_direct = cfg.get_params("read", true);
let read_cached = cfg.get_params("read", false);
```

If you care about mount-aware behavior, prefer:

```rust
let params = cfg.get_params_for_path("read", true, "/mnt/fast/bigfile.dat");
```

Notes:

- `LoadedConfig::get_params_for_path` is the API that most closely matches CLI behavior
- path-based selection is command-specific in the CLI, so if you are reproducing CLI semantics, choose the same context path the CLI would use

## `fro::block_hash`

Use this module when you want to:

- hash a file into configurable block hashes
- persist or inspect manifest sidecars
- verify a file against sidecars
- recover corrupted blocks from one or more full-file replicas
- inspect recovery decisions programmatically

Core types:

- `BlockHashManifest`
- `BlockRecoveryDecision`
- `BlockRecoveryBasis`
- `BlockRecoveryFailure`
- `VerifyReport`
- `RecoverReport`
- `RecoverMode`

Core functions:

- `default_hash_base(filename)`
- `save_manifest_replicas(base, manifest)`
- `load_manifest_replicas(base)`
- `hash_file_blocks(...)`
- `hash_file_to_replicas(...)`
- `verify_file_with_replicas(...)`
- `recover_file_with_copies(...)`
- `recover_block_hash(...)`

### Manifest model

`BlockHashManifest` contains:

- `file_size`
- `block_size`
- `bytes_hashed`
- `block_hashes`
- `hash_of_hashes`

Current default block size:

```rust
fro::block_hash::BLOCK_HASH_SIZE == 1024 * 1024
```

The actual block size used for a manifest is stored in `BlockHashManifest::block_size`.

### Verification and recovery reports

`VerifyReport` answers:

- how many blocks were checked
- how many manifests were loaded
- which blocks are bad

`RecoverReport` answers:

- how many bytes were hashed
- how many blocks were repaired
- how many files were repaired
- how many sidecar sets were refreshed
- whether `RecoverMode::Fast` stayed on the fast path
- which blocks still failed

### Recover modes

- `RecoverMode::Standard`
  - repair only the first file
- `RecoverMode::Fast`
  - first try a target-only scrub using the target's own sidecars
  - fall back to full multi-file recovery if needed
- `RecoverMode::InPlaceAll`
  - repair every input file and refresh healthy sidecars

The CLI is still the main way most users will interact with hash / verify / recover, but the basic execution helpers are now directly usable from the crate because `fro::IOMode` is re-exported.

## `fro::reader`

Use this module when you want to reuse the crate's tuned read-side parameter selection and file loading helpers from your own program.

Useful items:

- `ResolvedReadParams`
- `LoadedFile`
- `ReaderBlock<'_>`
- `resolve_reader_params(...)`
- `resolve_reader_params_for_mode(...)`
- `load_file_to_memory(...)`
- `load_file_to_memory_for_mode(...)`
- `map_file_blocks(...)`
- `map_file_blocks_for_mode(...)`
- `visit_file_blocks(...)`
- `visit_file_blocks_for_mode(...)`

Typical pattern:

```rust
use fro::config::load_config;
use fro::reader::load_file_to_memory_for_mode;
use fro::IOMode;

let cfg = load_config(None);
let loaded = load_file_to_memory_for_mode(&cfg, "read", "/mnt/fast/model.bin", IOMode::Auto)?;
println!("loaded {} bytes", loaded.bytes_read);
# Ok::<(), Box<dyn std::error::Error>>(())
```

If you want to process each block immediately in the worker thread, prefer the visitor-style API:

```rust
use fro::config::load_config;
use fro::reader::visit_file_blocks_for_mode;
use fro::IOMode;

let cfg = load_config(None);
let (bytes_read, _file_size, _params) =
    visit_file_blocks_for_mode(&cfg, "read", "/mnt/fast/model.bin", IOMode::Auto, |block| {
        println!("block {}: {} bytes", block.block_index, block.data.len());
        Ok::<_, std::io::Error>(())
    })?;
println!("processed {} bytes", bytes_read);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The examples directory now includes end-to-end programs built on this surface:

- `examples/model_loader.rs`
- `examples/hw_sha256.rs`
- `examples/parallel_zstd.rs`

Use them as templates:

- `model_loader.rs`
  - best starting point when you want "read the whole file into memory as fast as possible"
  - shows `load_file_to_memory_for_mode(...)`
- `hw_sha256.rs`
  - best starting point when you want "run a pure function on each block as it is read"
  - shows `map_file_blocks_for_mode(...)`
- `parallel_zstd.rs`
  - best starting point when you want "read/process/write a blockwise container format"
  - shows `ParallelFile`, `ParallelWriter`, appended metadata, and the matching decompression path

## `fro::stream`

Use this module when you want the "fewest tokens" path to a fast block pipeline:

- iterate fixed-size input blocks in parallel
- read arbitrary byte ranges from a shared input file
- write variable-size outputs by block index
- write fixed-size outputs by exact destination offset

Useful items:

- `ParallelFile`
- `ParallelWriter`
- `BlockRange`
- `ParallelReadReport`
- `ParallelWriteReport`

Mental model:

- `ParallelFile` is the input side
  - `foreach_block_parallel(...)` means "read fixed-size blocks on the tuned fro read path, then call me for each block"
  - `foreach_index_parallel(...)` means "run N independent indexed jobs in parallel"
  - `read_range(...)` means "fetch this exact byte range from the file"
- `ParallelWriter` is the output side
  - `indexed(...)` + `write_at_index(...)` is for variable-size output blocks that must land in block order
  - `fixed_size(...)` + `write_at_offset(...)` is for precise in-place or scatter-style writes where the destination offset is already known

Typical pattern:

```rust
use fro::config::load_config;
use fro::stream::{ParallelFile, ParallelWriter};
use fro::IOMode;
use zstd::bulk::compress;

let cfg = load_config(None);
let input = ParallelFile::open(&cfg, "read", "/tmp/in.bin", IOMode::Auto)?;
let block_size = 1024 * 1024;
let block_count = input.block_count(block_size)?;
let output = ParallelWriter::indexed(&cfg, "write", "/tmp/out.bin", IOMode::Auto, block_count)?;
let output_for_blocks = output.clone();

let read = input.foreach_block_parallel(block_size, move |chunk_index, raw_bytes| {
    let compressed = compress(raw_bytes, 3).map_err(std::io::Error::other)?;
    output_for_blocks.write_at_index(chunk_index, compressed)
})?;
let written = output.finish()?;
println!("read {} bytes, wrote {} bytes", read.bytes_read, written.bytes_written);
# Ok::<(), Box<dyn std::error::Error>>(())
```

This is the right template for:

- blockwise compression
- per-block encryption with variable output size
- chunked upload staging
- building your own framed container formats

For metadata-driven formats such as block decompression, use `ParallelFile::foreach_index_parallel(...)` plus `ParallelFile::read_range(...)` and `ParallelWriter::write_at_offset(...)`.

That looks like:

```rust
use fro::config::load_config;
use fro::stream::{ParallelFile, ParallelWriter};
use fro::IOMode;

let cfg = load_config(None);
let input = ParallelFile::open(&cfg, "read", "/tmp/in.container", IOMode::Auto)?;
let output = ParallelWriter::fixed_size(&cfg, "write", "/tmp/out.bin", IOMode::Auto, total_size)?;
let output_for_jobs = output.clone();
let input_for_jobs = input.clone();

input.foreach_index_parallel(block_count, move |i| {
    let compressed = input_for_jobs.read_range(offsets[i].offset, offsets[i].len as usize)?;
    let plain = decode_block(&compressed)?;
    output_for_jobs.write_at_offset(i as u64 * block_size, plain)
})?;
drop(output_for_jobs);
let report = output.finish()?;
println!("wrote {} bytes", report.bytes_written);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Important current semantics:

- `foreach_block_parallel(block_size, ...)` currently uses your requested `block_size` as the actual read size too
- it still uses the configured/tuned `num_threads` and `qd` for that mode under the hood
- it does **not** yet have a separate internal "read in one size, expose another size" rechunking layer

So if you ask for `1 MiB` blocks, the current implementation will actually issue `1 MiB` reads using the mode-selected thread count and queue depth.

### Agent cookbook

If you are writing code generation prompts for agents, these are good defaults:

- whole-file load:
  - copy `examples/model_loader.rs`
- per-block stateless transform:
  - copy `examples/hw_sha256.rs`
- variable-size block container:
  - copy the compression half of `examples/parallel_zstd.rs`
- indexed metadata-driven reconstruction:
  - copy the decompression half of `examples/parallel_zstd.rs`

In practice, the shortest reliable instruction is often:

> "Use `ParallelFile` + `ParallelWriter` like `examples/parallel_zstd.rs`, with `write_at_index` for variable-size transformed blocks and `write_at_offset` for fixed-size reconstruction."

## `fro::writer`

Use this module when you want to reuse the crate's write-side tuning or append output through the same io_uring-backed path family as the CLI.

Useful items:

- `ResolvedWriteParams`
- `resolve_writer_params_for_mode(...)`
- `SequentialWriter`

Typical pattern:

```rust
use fro::config::load_config;
use fro::writer::{resolve_writer_params_for_mode, SequentialWriter};
use fro::IOMode;

let cfg = load_config(None);
let params = resolve_writer_params_for_mode(&cfg, "write", "/tmp/out.bin", IOMode::Auto);
let mut out = SequentialWriter::create("/tmp/out.bin", params.qd, params.block_size, IOMode::Auto)?;
out.append(b"hello")?;
out.flush()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`SequentialWriter` uses `io_uring`, the selected write queue depth, and a write-side landing block size. In direct-capable mode it batches appended data until it has a full aligned landing chunk, submits those writes through the direct file descriptor, and only falls back to the page-cache file descriptor for the final unaligned tail.

## Stability notes

This crate's public modules are real, but they are still closely aligned with the CLI and benchmark tooling.

That means:

- type and function names may still evolve
- sidecar format may still evolve
- some lower-level helpers are public mainly because they are useful inside the project, not because they are already a fully curated external API

If you want a stronger library contract later, the likely path is:

- keep `fro` as the CLI package
- define a more intentionally curated library layer on top of the current internals
