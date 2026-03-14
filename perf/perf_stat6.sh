FILE_A="${FILE_A:-/mnt/fast/5GB_file.dat}"
FILE_B="${FILE_B:-/mnt/fast/5GB_file_copy.dat}"
perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads,cycles,instructions ./target/release/fro dual-read-bench --direct -n 1 -v "$FILE_A" "$FILE_B"
