use crate::consumer::SecurityProtocol;
use anyhow::Context;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};

pub struct ProducerWrapper {
    producer: FutureProducer,
}

impl ProducerWrapper {
    pub fn create(
        brokers: Vec<String>,
        security_protocol: SecurityProtocol,
    ) -> Result<Self, anyhow::Error> {
        let servers = brokers.join(",");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set("security.protocol", security_protocol.to_string())
            // .set("debug", "")
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
