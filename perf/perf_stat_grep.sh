FILE_A="${FILE_A:-/mnt/fast/5GB_file.dat}"
perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fro grep --direct -n 1 -v needle "$FILE_A"
