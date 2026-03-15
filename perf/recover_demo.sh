#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fro_bin="${FRO_BIN:-$repo_root/target/release/fro}"
test_dir="${1:-/mnt/fast/fro-test}"
size="${SIZE:-64MiB}"
recover_flags="${RECOVER_FLAGS:---in-place-all --no-direct -n 1 -v}"

mkdir -p "$test_dir"

pristine="$test_dir/pristine.bin"
copy1="$test_dir/copy-1.bin"
copy2="$test_dir/copy-2.bin"
copy3="$test_dir/copy-3.bin"
copy4="$test_dir/copy-4.bin"

echo "Building release binary..."
cargo build --release --manifest-path "$repo_root/Cargo.toml"

echo "Creating pristine file with fro write --create $size ..."
"$fro_bin" write --create "$size" --no-direct -n 1 "$pristine"

echo "Cloning pristine file with fro copy ..."
"$fro_bin" copy -n 1 "$pristine" "$copy1"
"$fro_bin" copy -n 1 "$pristine" "$copy2"
"$fro_bin" copy -n 1 "$pristine" "$copy3"
"$fro_bin" copy -n 1 "$pristine" "$copy4"

echo "Hashing demo copies ..."
for path in "$copy1" "$copy2" "$copy3" "$copy4"; do
    "$fro_bin" hash --no-direct -n 1 "$path"
done

echo "Corrupting demo copies ..."
"$repo_root/perf/corrupt_recover_demo_copies.sh" "$test_dir"

echo "Showing verify failures before recovery ..."
for path in "$copy1" "$copy2" "$copy3" "$copy4"; do
    "$fro_bin" verify --no-direct -n 1 "$path" || true
done

echo "Running recover with flags: $recover_flags"
"$fro_bin" recover $recover_flags "$copy1" "$copy2" "$copy3" "$copy4"

echo "Comparing repaired files to pristine ..."
for path in "$copy1" "$copy2" "$copy3" "$copy4"; do
    cmp -s "$pristine" "$path"
    echo "$(basename "$path"): match"
done

echo "Demo complete under $test_dir"
