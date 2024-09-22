use crate::consumer::ConsumerWrapper;
use crate::queries::get_topic_partitions_with_offsets::response::{
    GetTopicPartitionsWithOffsetsQueryResponseInternal, TopicPartitionWithOffsetsInternal,
};
use crate::queries::get_topic_partitions_with_offsets::{GetTopicPartitionsWithOffsetsQueryInternal, MinMaxOffset};
use anyhow::{bail, Context};
use rayon::prelude::*;
use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;
use std::time::Duration;
use tracing::trace;

pub async fn get_topic_partition_offsets(
    query: GetTopicPartitionsWithOffsetsQueryInternal,
) -> Result<GetTopicPartitionsWithOffsetsQueryResponseInternal, anyhow::Error> {
    let handle = tokio::task::spawn_blocking(move || build_response(query));

    handle.await.context("While joining handle")?
}

fn build_response(
    query: GetTopicPartitionsWithOffsetsQueryInternal,
) -> Result<GetTopicPartitionsWithOffsetsQueryResponseInternal, anyhow::Error> {
    let consumer =
        ConsumerWrapper::create_for_non_consuming(&query.brokers, query.security_protocol, None)
            .context("While creating consumer")?;

    let metadata = consumer
        .fetch_metadata(Some(&query.topic), Timeout::After(Duration::from_secs(5)))
        .with_context(|| format!("While fetching topic '{}' metadata", query.topic))?;

    trace!(
        "Got metadata for topic partition offset from broker. Count: {:?}",
        metadata.topics().len()
    );

    if metadata.topics().len() != 1 {
        bail!(
            "Incorrect topics count returned, expected 1, got: {}",
            metadata.topics().len()
        )
    }

    let topic = metadata.topics().first().unwrap();

    trace!(
        "Got data for topic. Name: {}, partitions: {}",
        topic.name(),
        topic.partitions().len()
    );
    let partitions = topic
        .partitions()
        .iter()
        .map(|x| x.id())
        .collect::<Vec<_>>();

    let results = partitions
        .into_par_iter()
        .map(|x| fetch_topic_partition_offset(&consumer, &query.topic, x))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("While fetching watermarks for topic {}", query.topic))?;

    Ok(GetTopicPartitionsWithOffsetsQueryResponseInternal {
        partitions: results,
    })
}
fn fetch_topic_partition_offset(
    consumer: &ConsumerWrapper,
    topic: &str,
    partition: i32,
) -> Result<TopicPartitionWithOffsetsInternal, anyhow::Error> {
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(5)))
        .with_context(|| {
            format!(
                "While fetching watermarks for topic {} and partition {}",
                topic, partition
            )
        })?;

    Ok(TopicPartitionWithOffsetsInternal {
        id: partition,
        offsets: MinMaxOffset {
            min_offset: low,
            max_offset: high,
        },
    })
}
