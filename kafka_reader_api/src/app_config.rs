use anyhow::Context;
use config::Config;
use serde::Deserialize;
use tracing::info;

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
}

impl AppConfig {
    pub fn build() -> Result<Self, anyhow::Error> {
        let config = Config::builder()
            .add_source(config::File::with_name("appsettings"))
            .add_source(config::Environment::with_prefix("App").separator("__"))
            .build()
            .context("While building config")?;

        let deserialized_config = config
            .try_deserialize()
            .context("While deserializing config")?;

        info!("App config: {deserialized_config:?}");

        Ok(deserialized_config)
    }
}
