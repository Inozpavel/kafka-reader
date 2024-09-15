FROM rust:1.81-slim as base

RUN apt update && yes | apt install protobuf-compiler gcc make

WORKDIR /src

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim as release

WORKDIR /app

RUN apt update && yes | apt install protobuf-compiler

COPY --from=base /src/appsettings.toml ./appsettings.toml
COPY --from=base /src/target/release/kafka_reader_api ./kafka_reader_api

ENV APP__PORT=80
ENTRYPOINT ["kafka_reader_api"]