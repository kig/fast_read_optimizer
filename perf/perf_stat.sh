FILE_A="${FILE_A:-/mnt/fast/5GB_file.dat}"
FILE_B="${FILE_B:-/mnt/fast/5GB_file_copy.dat}"
perf stat ./target/release/fro diff --direct -n 1 -v "$FILE_A" "$FILE_B"
