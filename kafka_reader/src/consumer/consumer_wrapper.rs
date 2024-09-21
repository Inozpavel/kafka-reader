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
        group: &str,
        auto_offset_reset: AutoOffsetReset,
        security_protocol: SecurityProtocol,
    ) -> Result<Self, KafkaError> {
        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let consumer: StreamConsumer = Self::create_common_config(brokers, security_protocol)
            .set("auto.offset.reset", auto_offset_reset.to_string())
            .set("group.id", group)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.commit.interval.ms", "4000")
            .set("message.max.bytes", "1000000000")
            .set("receive.message.max.bytes", "2147483647")
            .create()?;

        Ok(Self { consumer })
    }

    pub fn create_for_non_consuming(
        brokers: &[String],
        security_protocol: SecurityProtocol,
    ) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer =
            Self::create_common_config(brokers, security_protocol).create()?;

        Ok(Self { consumer })
    }

    fn create_common_config(
        brokers: &[String],
        security_protocol: SecurityProtocol,
    ) -> ClientConfig {
        let mut config = ClientConfig::new();

        let brokers_string = brokers.join(",");
        config
            .set("bootstrap.servers", brokers_string)
            // .set("debug", "")
            .set("security.protocol", security_protocol.to_string());

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
