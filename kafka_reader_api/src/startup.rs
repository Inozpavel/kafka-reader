use crate::app_config::AppConfig;
use crate::reader_api::{proto, ReaderService};
use anyhow::Context;
use tonic::transport::Server;
use tracing::info;

pub async fn run_until_stopped(config: AppConfig) -> Result<(), anyhow::Error> {
    let address = format!("{}:{}", config.host, config.port)
        .parse()
        .context("While parsing socket address")?;

    let service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1alpha()
        .context("While building reflection service")?;

    info!("Listening {address}");

    Server::builder()
        .add_service(service)
        .add_service(proto::KafkaReaderServer::new(ReaderService))
        .serve(address)
        .await
        .context("While listening service address")?;

    Ok(())
}
