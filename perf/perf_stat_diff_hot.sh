FILE_A="${FILE_A:-/mnt/fast/5GB_file.dat}"
FILE_B="${FILE_B:-/mnt/fast/5GB_file_copy.dat}"
./target/release/fro diff --no-direct -n 3 -v "$FILE_A" "$FILE_B"
perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fro diff --no-direct -n 1 -v "$FILE_A" "$FILE_B"
