FROM rust:bookworm as base

RUN apt update && yes | apt install protobuf-compiler

WORKDIR /src

VOLUME . ~/.cargo/registry/cache

#RUN cargo new --bin /app

#COPY Cargo.toml Cargo.toml
#COPY Cargo.lock Cargo.lock

#RUN cargo build
#RUN rm src/*

COPY . .

RUN cargo build

FROM debian:bookworm as release

WORKDIR /app

COPY --from=base /src/target /app

ENTRYPOINT ["sleep", "infinity"]