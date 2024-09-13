pub enum Format {
    String,
    Hex,
    Protobuf(ProtoConvertData),
}

pub enum StartFrom {
    Beginning,
    Latest,
    Today,
}

pub enum ProtoConvertData {
    RawProto(String),
}
