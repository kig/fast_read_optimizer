perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fast_read_optimizer grep --direct -n 1 -v needle /data/repos/sadtalker/checkpoints.tar.gz
