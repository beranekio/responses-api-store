FROM rust:1-bookworm AS builder
WORKDIR /app

# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY proto ./proto

RUN cargo build --release -p responses-api-store-server

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/responses-api-store /responses-api-store

EXPOSE 50051
ENTRYPOINT ["/responses-api-store"]