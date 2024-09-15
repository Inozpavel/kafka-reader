#[derive(Debug)]
pub enum Format {
    Ignore,
    String,
    Hex,
    Protobuf(ProtoConvertData),
}

#[derive(Debug)]
pub enum ProtoConvertData {
    RawProto(String),
}
