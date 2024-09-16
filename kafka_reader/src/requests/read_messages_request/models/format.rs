#[derive(Debug)]
pub enum Format {
    Ignore,
    String,
    Hex,
    Base64,
    Protobuf(ProtobufDecodeWay),
}

#[derive(Debug)]
pub enum ProtobufDecodeWay {
    SingleProtoFile(SingleProtoFile),
}

#[derive(Debug)]
pub struct SingleProtoFile {
    pub message_type_name: String,
    pub file: String,
}
