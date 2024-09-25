use crate::consumer::SecurityProtocol;
use anyhow::bail;
use rdkafka::ClientConfig;

#[derive(Debug)]
pub struct KafkaConnectionSettings {
    pub brokers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl TryFrom<&KafkaConnectionSettings> for ClientConfig {
    type Error = anyhow::Error;

    fn try_from(value: &KafkaConnectionSettings) -> Result<Self, Self::Error> {
        if value.brokers.is_empty() {
            bail!("No brokers specified")
        }

        let mut config = ClientConfig::new();

        let brokers_string = value.brokers.join(",");
        config
            .set("bootstrap.servers", brokers_string)
            // .set("debug", "all")
            .set("security.protocol", value.security_protocol.to_string());

        Ok(config)
    }
}
