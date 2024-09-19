use crate::consumer::{AutoOffsetReset, SecurityProtocol};
use anyhow::Context;
use rdkafka::consumer::StreamConsumer;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};

pub struct ConsumerWrapper {
    consumer: StreamConsumer,
}

impl ConsumerWrapper {
    pub fn create(
        brokers: &[String],
        group: String,
        auto_offset_reset: AutoOffsetReset,
        security_protocol: SecurityProtocol,
    ) -> Result<Self, anyhow::Error> {
        let brokers_string = brokers.join(",");

        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_string)
            .set("auto.offset.reset", auto_offset_reset.to_string())
            .set("group.id", group)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.commit.interval.ms", "4000")
            .set("message.max.bytes", "1000000000")
            .set("receive.message.max.bytes", "2147483647")
            .set("security.protocol", security_protocol.to_string())
            // .set("debug", "")
            .create()
            .context("While creating kafka StreamConsumer")?;

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
