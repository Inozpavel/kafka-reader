# kafka-reader

Web api service for reading and filtering kafka messages in different formats

Service has grpc api with streaming endpoint for
messages ([proto file contract](kafka_reader_api/src/protos/reader_service.proto)). Grpc service supports
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

All can be filtered with regex or string

Conditions:

+ Contains
+ Not contains

+ Not yet supported