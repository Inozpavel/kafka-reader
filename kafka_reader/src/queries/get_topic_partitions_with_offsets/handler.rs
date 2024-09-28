use crate::consumer::ConsumerWrapper;
use crate::queries::get_topic_partitions_with_offsets::response::GetTopicPartitionsWithOffsetsQueryResponseInternal;
use crate::queries::get_topic_partitions_with_offsets::GetTopicPartitionsWithOffsetsQueryInternal;
use anyhow::Context;
use std::time::Duration;

pub async fn get_topic_partition_offsets(
    query: GetTopicPartitionsWithOffsetsQueryInternal,
) -> Result<GetTopicPartitionsWithOffsetsQueryResponseInternal, anyhow::Error> {
    let handle = tokio::task::spawn_blocking(move || build_response(query));

    handle.await.context("While joining handle")?
}

fn build_response(
    query: GetTopicPartitionsWithOffsetsQueryInternal,
) -> Result<GetTopicPartitionsWithOffsetsQueryResponseInternal, anyhow::Error> {
    let consumer = ConsumerWrapper::create_for_non_consuming(&query.connection_settings, None)
        .context("While creating consumer")?;

    let partitions_offsets = consumer
        .get_all_partitions_watermarks(&query.topic, Some(Duration::from_secs(5)))
        .context("While fetching all partitions watermarks")?;

    Ok(GetTopicPartitionsWithOffsetsQueryResponseInternal { partitions_offsets })
}
