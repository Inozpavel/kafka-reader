FROM rust AS base

RUN apt update && yes | apt install protobuf-compiler cmake

WORKDIR /src

RUN cargo fetch

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim AS release

WORKDIR /app

RUN apt update && apt install -y protobuf-compiler

COPY --from=base /src/appsettings.toml ./appsettings.toml
COPY --from=base /src/target/release/kafka_reader_api ./kafka_reader_api

ENV APP__PORT=80
ENTRYPOINT ["./kafka_reader_api"]