use crate::consumer::ConsumerWrapper;
use crate::queries::get_cluster_metadata::request::GetClusterMetadataQueryInternal;
use crate::queries::get_cluster_metadata::response::{
    BrokerMetadata, GetClusterMetadataQueryInternalResponse, KafkaTopicMetadata,
};
use anyhow::Context;
use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;
use std::time::Duration;

pub async fn get_cluster_metadata(
    request: GetClusterMetadataQueryInternal,
) -> Result<GetClusterMetadataQueryInternalResponse, anyhow::Error> {
    let handle = tokio::task::spawn_blocking(move || {
        let client =
            ConsumerWrapper::create_for_non_consuming(&request.brokers, request.security_protocol, None)
                .context("While creating admin client")?;

        let metadata = client
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
            .context("While fetching metadata")?;

        let brokers = metadata
            .brokers()
            .iter()
            .map(|broker| BrokerMetadata {
                host: broker.host().to_owned(),
                port: broker.port() as u16,
            })
            .collect::<Vec<_>>();

        let topics = metadata
            .topics()
            .iter()
            .map(|topic| KafkaTopicMetadata {
                topic_name: topic.name().to_owned(),
                partitions_count: topic.partitions().len(),
            })
            .collect::<Vec<_>>();

        let metadata = GetClusterMetadataQueryInternalResponse { topics, brokers };
        Result::<_, anyhow::Error>::Ok(metadata)
    });

    let metadata = handle.await.context("While joining blocking handle")??;
    Ok(metadata)
}
