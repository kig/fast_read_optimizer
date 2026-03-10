perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fast_read_optimizer read --direct -n 1 -v /data/repos/sadtalker/checkpoints.tar.gz
