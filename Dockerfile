FROM rust:1.82-bullseye AS builder
COPY . /app
WORKDIR /app
RUN cargo build --release

FROM postgres:16.4-bullseye
COPY --from=builder /app/target/release/pgpitr /usr/local/bin/pgpitr
