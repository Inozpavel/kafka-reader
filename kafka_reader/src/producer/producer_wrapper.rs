use crate::connection_settings::KafkaConnectionSettings;
use anyhow::Context;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};

pub struct ProducerWrapper {
    producer: FutureProducer,
}

impl ProducerWrapper {
    pub fn create(
        kafka_connection_settings: &KafkaConnectionSettings,
    ) -> Result<Self, anyhow::Error> {
        let mut config = ClientConfig::try_from(kafka_connection_settings)?;
        let producer: FutureProducer = config
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "0")
            .create()
            .context("While creating a kafka FutureProducer")?;

        Ok(Self { producer })
    }
}

impl DerefMut for ProducerWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.producer
    }
}

impl Deref for ProducerWrapper {
    type Target = FutureProducer;

    fn deref(&self) -> &Self::Target {
        &self.producer
    }
}
