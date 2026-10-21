FROM rust:1.89-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY contracts ./contracts
RUN cargo build --release -p marketplace-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/marketplace-service /usr/local/bin/marketplace-service
# The store connector worker runs from the same image with its own command:
# `connector-worker` (needs CONNECTORS_ENABLED=true).
COPY --from=builder /build/target/release/connector-worker /usr/local/bin/connector-worker
# The commit is a runtime env, set after the compile stage so a new SHA does
# not invalidate the cargo build cache. `/health` reports it as `commit`.
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}
# A platform that injects PORT is bridged to BIND_ADDR, which the service reads, at startup.
ENV RUST_LOG=info
CMD ["sh", "-c", "BIND_ADDR=0.0.0.0:${PORT:-8080} exec marketplace-service"]
