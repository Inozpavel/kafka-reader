use crate::consumer::{AutoOffsetReset, SecurityProtocol};
use anyhow::Context;
use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

pub struct ConsumerWrapper {
    consumer: StreamConsumer,
}

impl ConsumerWrapper {
    pub fn create(
        brokers: &[String],
        auto_offset_reset: AutoOffsetReset,
        security_protocol: SecurityProtocol,
    ) -> Result<Self, anyhow::Error> {
        let offset_reset = match auto_offset_reset {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
        };

        let protocol = match security_protocol {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl"
        };
        let brokers_string = brokers.join(",");

        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_string)
            .set("auto.offset.reset", offset_reset)
            .set("group.id", Uuid::now_v7().to_string())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.commit.interval.ms", "4000")
            .set("message.max.bytes", "1000000000")
            .set("receive.message.max.bytes", "2147483647")
            .set("security.protocol", protocol)
            // .set("debug", "")
            .create()
            .context("While creating a Kafka client config file")?;

        Ok(Self { consumer })
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
