fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes: &[&str] = &[];
    tonic_build::configure()
        .file_descriptor_set_path("./descriptor.bin")
        .compile(&["./src/protos/reader_service.proto"], includes)?;

    tonic_build::configure().compile(&["./src/protos/message.proto"], includes)?;

    Ok(())
}
