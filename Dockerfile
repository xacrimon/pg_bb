FROM rust:1.82-bullseye AS builder
RUN rustup toolchain install nightly-2024-06-07
RUN rustup default nightly-2024-06-07
COPY . /app
WORKDIR /app
RUN cargo build --release

FROM postgres:16.4-bullseye
COPY --from=builder /app/target/release/pg_pitr /usr/local/bin/pg_pitr
