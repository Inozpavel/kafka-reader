use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes: &[&str] = &[];

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("reader_service_descriptor.bin"))
        .build_client(false)
        .compile(
            &[
                "./src/protos/reader_service.proto",
                "./src/protos/message.proto",
            ],
            includes,
        )?;

    tonic_build::configure().compile(&["./src/protos/message.proto"], includes)?;

    Ok(())
}
