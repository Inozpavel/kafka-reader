#![warn(unused_imports)]

use anyhow::Context;
use kafka_reader_api::app_config::AppConfig;
use kafka_reader_api::startup::run_until_stopped;
use prost::bytes::BytesMut;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod snazzy {
    use tonic::include_proto;

    include_proto!("snazzy.items");
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(
                    std::env::var("RUST_LOG")
                        .unwrap_or("info,kafka_reader_api=debug,kafka_reader=debug,proto_bytes_to_json_string_converter=debug,prost_build=trace".to_owned())
                        .as_str(),
                ),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = AppConfig::build().context("While building app config")?;

    let shirt = snazzy::Shirt {
        size: 26123,
        color: "Red123".into(),
    };
    let mut serialized = BytesMut::new();
    prost::Message::encode(&shirt, &mut serialized)?;
    info!("Serialized: {:02X}", serialized);

    run_until_stopped(config).await?;

    Ok(())
}
