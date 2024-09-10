fn main() -> std::io::Result<()> {
    tonic_build::configure().compile(&["src/message.proto"], &["src/"])?;

    Ok(())
}
