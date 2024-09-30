use kafka_reader::queries::get_topic_partitions_with_offsets::{GetTopicPartitionsWithOffsetsQueryInternal, GetTopicPartitionsWithOffsetsQueryResponseInternal};
use crate::api::converters::proto_connection_setting_to_internal;
use crate::api::kafka_service::proto::get_topic_partitions_with_offsets::{GetTopicPartitionsWithOffsetsQuery, GetTopicPartitionsWithOffsetsQueryResponse, PartitionDataWatermarksDto};

pub fn proto_get_topic_partition_offsets_internal(
    model: GetTopicPartitionsWithOffsetsQuery,
) -> Result<GetTopicPartitionsWithOffsetsQueryInternal, anyhow::Error> {
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;
    Ok(GetTopicPartitionsWithOffsetsQueryInternal {
        connection_settings,
        topic: model.topic,
    })
}

pub fn topic_partition_offsets_to_proto_response(
    model: GetTopicPartitionsWithOffsetsQueryResponseInternal,
) -> GetTopicPartitionsWithOffsetsQueryResponse {
    let partitions = model
        .partitions_offsets
        .into_iter()
        .map(|(partition, offsets)| PartitionDataWatermarksDto {
            id: partition,
            min_offset: offsets.min_offset,
            max_offset: offsets.max_offset,
            messages_count: offsets.messages_count(),
        })
        .collect();

    GetTopicPartitionsWithOffsetsQueryResponse { partitions }
}