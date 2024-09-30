use crate::api::kafka_service::proto::kafka_service_server::KafkaServiceServer;
use crate::api::kafka_service::KafkaService;
use crate::app_config::AppConfig;
use anyhow::Context;
use tonic::body::BoxBody;
use tonic::transport::Server;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug_span, info};

pub async fn run_until_stopped(config: AppConfig) -> Result<(), anyhow::Error> {
    let address = format!("{}:{}", config.host, config.port)
        .parse()
        .context("While parsing socket address")?;

    let service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(crate::api::kafka_service::proto::FILE_DESCRIPTOR_SET)
        .build_v1alpha()
        .context("While building reflection service")?;

    info!("Starting on {address}");

    Server::builder()
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(
            TraceLayer::new_for_grpc().make_span_with(|r: &http::Request<BoxBody>| {
                let request_id = r
                    .headers()
                    .get("x-request-id")
                    .map_or("None", |x| x.to_str().unwrap_or("None"));
                debug_span!("grpc-request", request_id)
            }),
        )
        .add_service(service)
        .add_service(KafkaServiceServer::new(KafkaService))
        .serve(address)
        .await
        .context("While listening service address")?;

    Ok(())
}
