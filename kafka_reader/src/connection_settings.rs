use crate::consumer::SecurityProtocol;
use anyhow::bail;
use rdkafka::ClientConfig;

#[derive(Debug)]
pub struct ConnectionSettings {
    pub brokers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl TryFrom<&ConnectionSettings> for ClientConfig {
    type Error = anyhow::Error;

    fn try_from(value: &ConnectionSettings) -> Result<Self, Self::Error> {
        if value.brokers.is_empty() {
            bail!("No brokers specified")
        }

        let mut config = ClientConfig::new();

        let brokers_string = value.brokers.join(",");
        config
            .set("bootstrap.servers", brokers_string)
            .set("security.protocol", value.security_protocol.to_string());

        if let Ok(value) = std::env::var("RD_KAFKA_DEBUG") {
            config.set("debug", value);
        }

        Ok(config)
    }
}
