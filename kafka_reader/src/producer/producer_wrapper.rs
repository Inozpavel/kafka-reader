use crate::connection_settings::ConnectionSettings;
use crate::consumer::PartitionOffset;
use anyhow::{bail, Context};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};
use std::time::Duration;
use tracing::error;

pub struct ProducerWrapper {
    producer: FutureProducer,
}

impl ProducerWrapper {
    pub fn create(kafka_connection_settings: &ConnectionSettings) -> Result<Self, anyhow::Error> {
        let mut config = ClientConfig::try_from(kafka_connection_settings)?;
        let producer: FutureProducer = config
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "0")
            .create()
            .context("While creating kafka FutureProducer")?;

        Ok(Self { producer })
    }

    pub async fn produce_message<'a, T>(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&[u8]>,
        body: Option<&[u8]>,
        headers: Option<T>,
        timeout: Option<Duration>,
    ) -> Result<PartitionOffset, anyhow::Error>
    where
        T: IntoIterator<Item = (&'a str, &'a [u8])>,
        T::IntoIter: ExactSizeIterator<Item = T::Item>,
    {
        let kafka_headers = headers.and_then(|headers_map| {
            let iter = headers_map.into_iter();
            let items_count = iter.len();
            if items_count == 0 {
                None
            } else {
                Some(iter.fold(
                    OwnedHeaders::new_with_capacity(items_count),
                    |acc, (key, value)| {
                        acc.insert(Header {
                            key,
                            value: Some(value),
                        })
                    },
                ))
            }
        });

        let record = FutureRecord {
            topic,
            partition,
            timestamp: None,
            key,
            payload: body,
            headers: kafka_headers,
        };

        let send_result = self.producer.send(record, timeout).await;

        let partition_offset = match send_result {
            Ok((partition, offset)) => PartitionOffset::new(partition, Some(offset)),
            Err((kafka_error, _message)) => {
                error!("Produce error {:?}", kafka_error);
                bail!("{:?}", kafka_error)
            }
        };
        Ok(partition_offset)
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
