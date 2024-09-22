use crate::consumer::{AutoOffsetReset, SecurityProtocol};
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
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
    ) -> Result<Self, KafkaError> {
        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let consumer: StreamConsumer = Self::create_common_config(brokers, security_protocol, Some(group))
            .set("auto.offset.reset", auto_offset_reset.to_string())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.commit.interval.ms", "4000")
            .set("message.max.bytes", "1000000000")
            .set("receive.message.max.bytes", "2147483647")
            // .set("debug", "all")
            .set("heartbeat.interval.ms", "1000")
            .create()?;

        Ok(Self { consumer })
    }

    pub fn create_for_non_consuming(
        brokers: &[String],
        security_protocol: SecurityProtocol,
        group: Option<&str>,
    ) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer =
            Self::create_common_config(brokers, security_protocol, group).create()?;

        Ok(Self { consumer })
    }

    fn create_common_config(
        brokers: &[String],
        security_protocol: SecurityProtocol,
        group: Option<&str>,
    ) -> ClientConfig {
        let mut config = ClientConfig::new();

        let brokers_string = brokers.join(",");
        config
            .set("bootstrap.servers", brokers_string)
            // .set("debug", "all")
            .set("security.protocol", security_protocol.to_string());

        if let Some(group) = group {
            config.set("group.id", group);
        }

        config
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
