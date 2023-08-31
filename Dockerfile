FROM rust:bookworm as builder
WORKDIR /usr/src/app

# Cache toolchain as docker intermediate image
COPY ./rust-toolchain.toml .

RUN rustup component add rustfmt
RUN apt-get update && apt-get install cmake -y

COPY ./Cargo.toml ./Cargo.lock ./
COPY ./crates ./crates
RUN ls /usr/src/app/

RUN cargo install --locked --path ./crates/cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/cryo /app/cryo

WORKDIR /app

ENTRYPOINT ["/app/cryo"]
