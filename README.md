# kafka-reader

Web api service for reading and filtering kafka messages in different formats

Service has grpc api with streaming endpoint for
messages ([proto file contract](kafka_reader_api/src/protos/reader_service.proto)). Grpc service supports
reflection

Supported message decode formats (key and body):

+ Ignore bytes (null)
+ String
+ Hex
+ Protobuf
    + Single proto file (add to request)

Supported topic read start settings:

+ From beginning
+ From latest
+ From today

+ Supported topic read filters:

Not yet supported