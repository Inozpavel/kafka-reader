use crate::consumer_settings::AutoOffsetReset;
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
    ) -> Result<Self, anyhow::Error> {
        let offset_reset = match auto_offset_reset {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
        };

        let brokers_string = brokers.join(",");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_string)
            .set("auto.offset.reset", offset_reset)
            .set("group.id", Uuid::now_v7().to_string())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "4000")
            .set("message.max.bytes", "1000000000")
            .set("receive.message.max.bytes", "2147483647")
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
