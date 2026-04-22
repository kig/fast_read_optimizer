# fast_read_optimizer (fro)

fast_read_optimizer ("fro") is a Rust crate and command-line tool for high-throughput large-file I/O. Use it to read, copy, search, hash, verify, and repair large files, and to benchmark or tune I/O parameters for specific hardware.

Quick start

- Build (release):
  - cargo build --release
- Show help:
  - ./target/release/fro --help
- Install locally:
  - cargo install --path .

Common commands

- fro read [--direct|--no-direct|--auto] -n 1 <file>
- fro hash <file>            # create block-hash sidecars
- fro verify <file>          # verify file against sidecars
- fro recover target backup...  # repair using replicas
- fro grep <pattern> <file>
- fro copy in.bin out.bin [--verify]

Tuning and benchmarking

- Tune for a mount: fro-optimize --test-dir /mnt/nvme read grep diff write copy
- Run regressions: fro-benchmark --test-dir /mnt/fast

Note: optimizer and benchmark runs perform substantial writes. Use --test-size and --iters (for example: --test-size 64MiB --iters 5) to reduce wear during development.

Library and examples

- Public modules: fro::reader, fro::writer, fro::stream, fro::config
- See docs/API.md and the examples/ directory for usage samples.

Contributing

- Enable hooks: git config core.hooksPath .githooks
- Run tests: cargo test --quiet
- Run janitor checks: cargo run --quiet --manifest-path janitor/Cargo.toml -- all

Further reading

See docs/profiling.md, GEMINI.md, and VERIFICATION.md for profiling, verification, and performance workflows.

License

MIT
