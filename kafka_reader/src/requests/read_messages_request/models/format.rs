#[derive(Debug)]
pub enum Format {
    String,
    Hex,
    Protobuf(ProtoConvertData),
}

#[derive(Debug)]
pub enum ProtoConvertData {
    RawProto(String),
}
