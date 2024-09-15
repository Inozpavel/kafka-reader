FROM rust:bookworm as base

RUN apt update && yes | apt install protobuf-compiler

WORKDIR /src

COPY . .

RUN cargo build --release

FROM debian:bookworm as release

WORKDIR /app

COPY --from=base /src/target /app

ENV APP__PORT=80
ENTRYPOINT ["kafka_reader_api"]