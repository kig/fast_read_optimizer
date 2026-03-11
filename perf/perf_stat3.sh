vmtouch -e /data/repos/sadtalker/checkpoints.tar.gz /data/repos/sadtalker/checkpoints.tar.gz.e
perf stat -e cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads ./target/release/fro diff --no-direct -n 1 -v /data/repos/sadtalker/checkpoints.tar.gz /data/repos/sadtalker/checkpoints.tar.gz.e

