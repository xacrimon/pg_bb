FROM rust:1.82-bullseye AS chef
RUN rustup toolchain install nightly-2024-06-07
RUN rustup default nightly-2024-06-07
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --profile dev --recipe-path recipe.json
COPY . .
RUN cargo build

FROM postgres:16.4-bullseye
COPY --from=builder /app/target/debug/pg_pitr /usr/local/bin/pg_pitr
