use crate::reader_api;
use crate::reader_api::ReaderService;
use anyhow::Context;
use tonic::transport::Server;
use tracing::info;

pub async fn run_until_stopped() -> Result<(), anyhow::Error> {
    let address = "127.0.0.1:50002"
        .parse()
        .context("While parsing socket address")?;
    info!("Listening {address}");
    let service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(reader_api::proto::FILE_DESCRIPTOR_SET)
        .build_v1alpha()
        .context("While building reflection service")?;

    Server::builder()
        .add_service(service)
        .add_service(reader_api::proto::kafka_reader_server::KafkaReaderServer::new(ReaderService))
        .serve(address)
        .await
        .context("While service port")?;

    Ok(())
}
