pub enum Format {
    String,
    Hex,
    Protobuf(ProtoConvertData),
}

pub enum ProtoConvertData {
    RawProto(String),
}
