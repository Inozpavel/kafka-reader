#![warn(unused_imports)]
#![deny(clippy::clone_on_copy)]
#![deny(forgetting_copy_types)]
#![deny(clippy::style)]

use anyhow::Context;
use kafka_reader_api::app_config::AppConfig;
use kafka_reader_api::startup::run_until_stopped;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let log_level = std::env::var("RUST_LOG").unwrap_or(
        "info,kafka_reader_api,kafka_reader=trace,proto_json_converter=debug,prost_build=trace,protobuf_parse=trace"
            .to_owned(),
    );

    println!("Log level: {}", log_level);
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(log_level),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = AppConfig::build().context("While building app config")?;

    run_until_stopped(config).await?;

    Ok(())
}
