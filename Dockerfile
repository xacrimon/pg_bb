FROM rust:1.82-bullseye AS builder
COPY . /app
WORKDIR /app
RUN cargo build

FROM postgres:16.4-bullseye
COPY --from=builder /app/target/debug/pg_pitr /usr/local/bin/pg_pitr
