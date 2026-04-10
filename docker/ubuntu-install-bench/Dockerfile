FROM ubuntu:24.04

COPY target/release/fro /usr/local/bin/fro
COPY docker/ubuntu-install-bench/entrypoint.sh /usr/local/bin/ubuntu-install-bench
COPY docker/ubuntu-install-bench/fro-coreutil-wrapper.sh /opt/fro-coreutils/fro-coreutil-wrapper.sh
COPY docker/ubuntu-install-bench/shell-workload.sh /opt/fro-bench/shell-workload.sh

RUN chmod +x \
        /usr/local/bin/fro \
        /usr/local/bin/ubuntu-install-bench \
        /opt/fro-coreutils/fro-coreutil-wrapper.sh \
        /opt/fro-bench/shell-workload.sh \
    && mkdir -p /opt/fro-coreutils/bin \
    && for cmd in \
        base64 cat cmp cp cksum dd decrypt du encrypt fgrep find head md5sum mv rm shred sort \
        tac tail tar wc b2sum b3sum sha224sum sha256sum sha384sum sha512sum; do \
        ln -sf /opt/fro-coreutils/fro-coreutil-wrapper.sh "/opt/fro-coreutils/bin/${cmd}"; \
    done

ENTRYPOINT ["/usr/local/bin/ubuntu-install-bench"]
