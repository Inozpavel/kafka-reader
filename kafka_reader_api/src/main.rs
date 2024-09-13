use kafka_reader_api::startup::run_until_stopped;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(
                    std::env::var("RUST_LOG")
                        .unwrap_or("info,proto_experiments=debug".to_owned())
                        .as_str(),
                ),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    run_until_stopped().await?;

    Ok(())
}
