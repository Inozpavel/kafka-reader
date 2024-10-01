# kafka-reader

Web api service for debugging, reading, producing and filtering kafka messages in different formats

## Features

Service has grpc api with streaming endpoint for
messages ([proto file contract](kafka_reader_api/src/protos/kafka_service.proto)). Grpc service supports
reflection

Supported message decode formats, separated for key and body:

+ Ignore (null)
+ String
+ Hex
+ Protobuf
    + Single proto file (add to request)

Supported topic read start settings:

+ From beginning
+ From latest
+ From today
+ From time

Supported topic read limit filters:

+ No limit - steaming will be infinite, will parse new messages
+ Message count - certain message count from all partition in sum
+ To time

Supported value filters for key and body:

+ Filter kind
    + String
    + Regex

+ Conditions:
    + Contains
    + Not contains

Supported produce formats:

+ String
+ Base64
+ Hex
+ Protobuf

Supported security protocols:

+ Plaintext
+ Ssl

## Settings

Environment variables:

`APP__HOST` - listener ip
`APP__PORT` - listener port
`RUST_LOG` = log level, format: https://docs.rs/env_logger/latest/env_logger/#enabling-logging

## Build from source

Install rust https://www.rust-lang.org/tools/install

### Windows

```bash
git clone https://github.com/microsoft/vcpkg
cd vcpkg
.\bootstrap-vcpkg.bat

# Add vcpkg.exe to system PATH variable
vcpkg install librdkafka
vcpkg install zlib
vcpkg install --triplet=x64-windows-static openssl

set VCPKG_ROOT=c:\path\to\vcpkg\installed

vcpkg integrate install

cargo build --release
```

### Linux

```bash
sudo apt install opensll cmake

cargo build --release
```

App will be available in ./target/release