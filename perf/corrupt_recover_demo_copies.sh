#!/usr/bin/env bash
set -euo pipefail

test_dir="${1:?usage: corrupt_recover_demo_copies.sh <test-dir>}"
block_size=$((1024 * 1024))

corrupt_at() {
    local file="$1"
    local offset="$2"
    local bytes="$3"
    printf "%b" "$bytes" | dd of="$file" bs=1 seek="$offset" conv=notrunc status=none
}

copy1="$test_dir/copy-1.bin"
copy2="$test_dir/copy-2.bin"
copy3="$test_dir/copy-3.bin"
copy4="$test_dir/copy-4.bin"

for path in "$copy1" "$copy2" "$copy3" "$copy4"; do
    if [[ ! -f "$path" ]]; then
        echo "missing expected file: $path" >&2
        exit 1
    fi
done

# Corrupt different 1 MiB blocks in each copy so that another clean source still
# exists for every damaged block.
corrupt_at "$copy1" $((0 * block_size + 17)) '\x11\x22\x33\x44\x55\x66\x77\x88'
corrupt_at "$copy1" $((5 * block_size + 23)) '\x89\x9a\xab\xbc\xcd\xde\xef\x01'
corrupt_at "$copy1" $((9 * block_size + 31)) '\x02\x13\x24\x35\x46\x57\x68\x79'

corrupt_at "$copy2" $((1 * block_size + 19)) '\xaa\xbb\xcc\xdd\xee\xff\x10\x20'
corrupt_at "$copy2" $((6 * block_size + 27)) '\x30\x40\x50\x60\x70\x80\x90\xa0'
corrupt_at "$copy2" $((10 * block_size + 35)) '\xb0\xc0\xd0\xe0\xf0\x01\x12\x23'

corrupt_at "$copy3" $((2 * block_size + 21)) '\xfe\xdc\xba\x98\x76\x54\x32\x10'
corrupt_at "$copy3" $((7 * block_size + 29)) '\x0f\x1e\x2d\x3c\x4b\x5a\x69\x78'
corrupt_at "$copy3" $((11 * block_size + 37)) '\x87\x96\xa5\xb4\xc3\xd2\xe1\xf0'

corrupt_at "$copy4" $((3 * block_size + 25)) '\x5a\x5b\x5c\x5d\x5e\x5f\x60\x61'
corrupt_at "$copy4" $((8 * block_size + 33)) '\x62\x63\x64\x65\x66\x67\x68\x69'
corrupt_at "$copy4" $((12 * block_size + 39)) '\x6a\x6b\x6c\x6d\x6e\x6f\x70\x71'

echo "Corrupted demo copies under $test_dir"
