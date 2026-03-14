# API notes for the `fro` crate

This document is for Rust users of the crate API.

The crate currently exports two public modules:

- `fro::config`
- `fro::block_hash`

The public API is useful, but it is still closely tied to the CLI implementation and should be treated as evolving rather than as a polished long-term stability contract.

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

### Current external-use caveat

The crate exports `fro::block_hash`, but the operational helpers currently take `IOMode`, and `IOMode` is not re-exported as part of the public crate surface.

That means:

- manifest types and recovery-decision types are externally visible
- the end-to-end block-hash execution helpers are not yet a polished external embedding API
- today, most users should prefer the CLI for hash / verify / recover workflows

In practice, most external users should currently prefer the CLI over embedding the internals directly.

## Stability notes

This crate's public modules are real, but they are still closely aligned with the CLI and benchmark tooling.

That means:

- type and function names may still evolve
- sidecar format may still evolve
- some lower-level helpers are public mainly because they are useful inside the project, not because they are already a fully curated external API

If you want a stronger library contract later, the likely path is:

- keep `fro` as the CLI package
- define a more intentionally curated library layer on top of the current internals
