use crate::consumer::{AutoOffsetReset, SecurityProtocol};
use anyhow::{bail, Context};
use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};

pub struct ConsumerWrapper {
    consumer: StreamConsumer,
}

impl ConsumerWrapper {
    pub fn create_for_consuming(
        brokers: &[String],
        security_protocol: SecurityProtocol,
        group: &str,
        auto_offset_reset: AutoOffsetReset,
    ) -> Result<Self, anyhow::Error> {
        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let consumer: StreamConsumer =
            Self::create_common_config(brokers, security_protocol, Some(group))
                .context("While creating common config")?
                .set("auto.offset.reset", auto_offset_reset.to_string())
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "10000")
                .set("enable.auto.commit", "true")
                .set("enable.auto.offset.store", "false")
                .set("auto.commit.interval.ms", "4000")
                .set("message.max.bytes", "1000000000")
                .set("receive.message.max.bytes", "2147483647")
                .set("heartbeat.interval.ms", "1000")
                .create()?;

        Ok(Self { consumer })
    }

    pub fn create_for_non_consuming(
        brokers: &[String],
        security_protocol: SecurityProtocol,
        group: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let consumer: StreamConsumer =
            Self::create_common_config(brokers, security_protocol, group)
                .context("While creating common config")?
                .create()?;

        Ok(Self { consumer })
    }

    fn create_common_config(
        brokers: &[String],
        security_protocol: SecurityProtocol,
        group: Option<&str>,
    ) -> Result<ClientConfig, anyhow::Error> {
        if brokers.is_empty() {
            bail!("No brokers specified")
        }
        let mut config = ClientConfig::new();

        let brokers_string = brokers.join(",");
        config
            .set("bootstrap.servers", brokers_string)
            // .set("debug", "all")
            .set("security.protocol", security_protocol.to_string());

        if let Some(group) = group {
            config.set("group.id", group);
        }

        Ok(config)
    }
}

impl DerefMut for ConsumerWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.consumer
    }
}

impl Deref for ConsumerWrapper {
    type Target = StreamConsumer;

    fn deref(&self) -> &Self::Target {
        &self.consumer
    }
}
