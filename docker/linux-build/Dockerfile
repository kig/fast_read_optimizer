FROM rust:1.93-slim

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        pkg-config \
        ca-certificates \
        git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /repo

# Copy repo into container
COPY . /repo

# Use size-optimized Rust flags for minimized binaries
ENV RUSTFLAGS="-C opt-level=s -C lto -C codegen-units=1 -C panic=abort -C link-arg=-s"

# Build release
RUN cargo build --release

CMD ["/bin/bash"]
