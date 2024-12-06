use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::connection_settings::ConnectionSettings;
use crate::consumer::{AutoOffsetReset, PartitionOffset};
use crate::contexts::build_context::build_main_client_context;
use crate::contexts::token_client_context::MainClientContext;
use crate::queries::get_topic_partitions_with_offsets::MinMaxOffset;
use anyhow::Context;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

// https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
pub struct ConsumerWrapper {
    consumer: StreamConsumer<MainClientContext<Box<dyn BearerTokenProvider>>>,
}

impl ConsumerWrapper {
    pub fn create_for_consuming(
        kafka_connection_settings: &ConnectionSettings,
        group: &str,
        auto_offset_reset: AutoOffsetReset,
    ) -> Result<Self, anyhow::Error> {
        let context = build_main_client_context(kafka_connection_settings);

        let consumer: StreamConsumer<MainClientContext<Box<dyn BearerTokenProvider>>> =
            Self::create_common_config(kafka_connection_settings, Some(group))
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
                .create_with_context(context)?;

        Ok(Self { consumer })
    }

    pub fn create_for_non_consuming(
        kafka_connection_settings: &ConnectionSettings,
        group: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let context = build_main_client_context(kafka_connection_settings);
        let consumer: StreamConsumer<MainClientContext<Box<dyn BearerTokenProvider>>> =
            Self::create_common_config(kafka_connection_settings, group)
                .context("While creating common config")?
                .create_with_context(context)?;

        Ok(Self { consumer })
    }

    fn create_common_config(
        kafka_connection_settings: &ConnectionSettings,
        group: Option<&str>,
    ) -> Result<ClientConfig, anyhow::Error> {
        let mut config = ClientConfig::try_from(kafka_connection_settings)?;

        if let Some(group) = group {
            config.set("group.id", group);
        }

        Ok(config)
    }

    pub fn get_topic_partitions_count(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> Result<i32, anyhow::Error> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), timeout)
            .context("While fetching topic metadata")?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|x| x.name() == topic)
            .context("Requested topic wasn't found")?;

        Ok(topic_metadata.partitions().len() as i32)
    }

    pub fn get_partition_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Option<Duration>,
    ) -> Result<MinMaxOffset, anyhow::Error> {
        let (min, max) = self
            .consumer
            .fetch_watermarks(topic, partition, timeout)
            .context("While fetching watermarks")?;

        Ok(MinMaxOffset {
            min_offset: min,
            max_offset: max,
        })
    }

    pub fn get_topic_offsets(
        &self,
        topic: &str,
        partitions_count: i32,
        timeout: Option<Duration>,
    ) -> Result<Vec<PartitionOffset>, anyhow::Error> {
        let partitions = (0..partitions_count)
            .try_fold(
                TopicPartitionList::with_capacity(partitions_count as usize),
                |mut acc, p| {
                    acc.add_partition_offset(topic, p, Offset::Invalid)
                        .context("While adding partitions")?;
                    Result::<_, anyhow::Error>::Ok(acc)
                },
            )
            .context("While building partitions list")?;

        let commited = self
            .consumer
            .committed_offsets(partitions, timeout)
            .context("While fetching commited offsets")?;

        let offsets = commited
            .elements()
            .iter()
            .map(|e| {
                let offset = e
                    .offset()
                    .to_raw()
                    .and_then(|x| if x >= 0 { None } else { Some(x) });

                PartitionOffset::new(e.partition(), offset)
            })
            .collect::<Vec<_>>();

        Ok(offsets)
    }

    pub fn get_all_partitions_watermarks(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> Result<HashMap<i32, MinMaxOffset>, anyhow::Error> {
        let partitions_counts = self
            .get_topic_partitions_count(topic, timeout)
            .context("While fetching partitions count")?;

        (0..partitions_counts)
            .map(|partition| {
                let watermarks = self
                    .get_partition_watermarks(topic, partition, timeout)
                    .context("While fetching watermarks for topic")?;

                Result::<_, anyhow::Error>::Ok((partition, watermarks))
            })
            .collect()
    }
}

impl DerefMut for ConsumerWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.consumer
    }
}

impl Deref for ConsumerWrapper {
    type Target = StreamConsumer<MainClientContext<Box<dyn BearerTokenProvider>>>;

    fn deref(&self) -> &Self::Target {
        &self.consumer
    }
}
