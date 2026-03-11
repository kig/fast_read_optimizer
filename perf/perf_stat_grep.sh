perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fro grep --direct -n 1 -v needle /data/repos/sadtalker/checkpoints.tar.gz
