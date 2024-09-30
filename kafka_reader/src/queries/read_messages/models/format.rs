#[derive(Debug)]
pub enum Format {
    String,
    Hex,
    Base64,
    Protobuf(ProtobufDecodeWay),
}

#[derive(Debug)]
pub enum ProtobufDecodeWay {
    SingleProtoFile(SingleProtoFile),
    TarArchive(ProtoTarArchive),
}

#[derive(Debug)]
pub struct SingleProtoFile {
    pub message_type_name: String,
    pub file: String,
}

#[derive(Debug)]
pub struct ProtoTarArchive {
    pub message_type_name: String,
    pub archive_bytes: Vec<u8>,
    pub target_file_path: String,
}
