use crate::api::converters::shared::proto_connection_setting_to_internal;
use crate::api::kafka_service::proto::get_cluster_metadata::{
    GetClusterMetadataQuery, GetClusterMetadataQueryResponse, KafkaBrokerMetadataDto,
    KafkaTopicMetadataDto,
};
use kafka_reader::queries::get_cluster_metadata::{
    GetClusterMetadataQueryInternal, GetClusterMetadataQueryInternalResponse,
};
use rayon::prelude::*;

pub fn proto_get_cluster_metadata_to_internal(
    model: GetClusterMetadataQuery,
) -> Result<GetClusterMetadataQueryInternal, anyhow::Error> {
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;

    Ok(GetClusterMetadataQueryInternal {
        connection_settings,
    })
}

pub fn kafka_cluster_metadata_to_proto_response(
    model: GetClusterMetadataQueryInternalResponse,
) -> GetClusterMetadataQueryResponse {
    let brokers = model
        .brokers
        .into_par_iter()
        .map(|x| KafkaBrokerMetadataDto {
            port: x.port as u32,
            host: x.host,
        })
        .collect();

    let topics = model
        .topics
        .into_par_iter()
        .map(|x| KafkaTopicMetadataDto {
            topic_name: x.topic_name,
            partitions_count: x.partitions_count as u32,
        })
        .collect();

    GetClusterMetadataQueryResponse { brokers, topics }
}
