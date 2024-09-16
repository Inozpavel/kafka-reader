#[derive(Debug)]
pub enum Format {
    Ignore,
    String,
    Hex,
    Base64,
    Protobuf(ProtoConvertData),
}

#[derive(Debug)]
pub enum ProtoConvertData {
    RawProto(String),
}
