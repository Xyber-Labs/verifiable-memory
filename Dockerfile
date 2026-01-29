# Multi-stage build for the Verifiable DB API service (api_server)
#
# Note:
# - this repo's Cargo.lock is lockfile v4
# - some dependencies require Cargo support for Rust Edition 2024
# So we build with a newer Rust/Cargo. Some deps require rustc >= 1.88.
FROM rust:1.88-slim AS builder

WORKDIR /app

# Build deps (openssl, pkg-config for some crates)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first (better layer caching)
COPY Cargo.toml Cargo.lock ./

# Copy source
COPY src ./src
COPY migrations ./migrations

# Build release API binary
RUN cargo build --release --bin api_server

FROM debian:bookworm-slim AS runtime

WORKDIR /app

# Runtime deps: CA certs, libssl, libpq
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -m -u 1000 appuser

# Copy binary
COPY --from=builder /app/target/release/api_server /app/api_server

# Copy migrations for runtime-loaded SQLx migrator (used by /bootstrap/migrate)
COPY --from=builder /app/migrations /app/migrations

# Create expected Solana config dir for the app user (we'll mount id.json here)
RUN mkdir -p /home/appuser/.config/solana && chown -R appuser:appuser /home/appuser /app

USER appuser

EXPOSE 3000

CMD ["./api_server"]