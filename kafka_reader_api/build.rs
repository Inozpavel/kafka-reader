use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes: &[&str] = &[];

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("reader_service_descriptor.bin"))
        .build_client(false)
        .compile_protos(
            &["./src/protos/kafka_service.proto", "./tests/snazzy.proto"],
            includes,
        )?;

    Ok(())
}
