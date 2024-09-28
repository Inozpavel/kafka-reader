use crate::connection_settings::ConnectionSettings;
use crate::consumer::ConsumerWrapper;
use crate::queries::get_topic_lags::query::GetTopicLagsQueryInternal;
use crate::queries::get_topic_lags::response::{
    GroupInfo, GroupTopicLags, PartitionOffsetWithLag, ReadLagsResult,
};
use crate::queries::get_topic_partitions_with_offsets::MinMaxOffset;
use anyhow::Context;
use rdkafka::consumer::Consumer;
use rdkafka::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info_span, Instrument};

pub async fn get_topic_lags(
    query: GetTopicLagsQueryInternal,
    cancellation_token: CancellationToken,
) -> Result<Receiver<ReadLagsResult>, anyhow::Error> {
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::task::spawn(
        async move {
            if let Err(e) = write_lags_to_channel(query, tx.clone(), cancellation_token).await {
                let _ = tx.send(ReadLagsResult::BrokerError(e)).await;
            }
        }
        .instrument(info_span!("Writing lags to channel").or_current()),
    );

    Ok(rx)
}

async fn write_lags_to_channel(
    query: GetTopicLagsQueryInternal,
    tx: Sender<ReadLagsResult>,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let consumer = Arc::new(
        ConsumerWrapper::create_for_non_consuming(&query.connection_settings, None)
            .context("While creating consumer")?,
    );

    let topic = Arc::new(query.topic);
    let consumer_copy = consumer.clone();
    let groups_handle = tokio::task::spawn_blocking(move || get_consumer_groups(&consumer_copy));

    let consumer_copy = consumer.clone();
    let topic_copy = topic.clone();
    let topic_partitions_handle =
        tokio::task::spawn_blocking(move || get_topic_partitions(&consumer_copy, &topic_copy));

    let groups = select! {
        groups = groups_handle => {
            groups.context("While joining handle with groups")??
        }
        _ = cancellation_token.cancelled() => {
            return Ok(())
        }
    };

    let topic_partitions = select! {
        topic_partitions = topic_partitions_handle => {
            topic_partitions.context("While joining handle with topic partitions")??
        }
        _ = cancellation_token.cancelled() => {
            return Ok(())
        }
    };

    let consumer_copy = consumer.clone();
    let topic_copy = topic.clone();

    let topic_partitions_clone = topic_partitions.clone();
    let watermarks = tokio::task::spawn_blocking(move || {
        get_watermarks(&consumer_copy, &topic_copy, &topic_partitions_clone)
    })
    .await
    .context("While joining handle with watermarks")??;

    let watermarks = Arc::new(watermarks);

    let kafka_settings = Arc::new(query.connection_settings);
    for group in groups {
        let topic_partitions = topic_partitions.clone();
        let kafka_settings = kafka_settings.clone();
        let topic = topic.clone();
        let tx = tx.clone();
        let watermarks = watermarks.clone();

        tokio::task::spawn(async move {
            let Ok(lags_result) = tokio::task::spawn_blocking(move || {
                get_group_topic_offsets(&kafka_settings, watermarks, topic_partitions, group, topic)
            })
            .await
            .inspect_err(|e| error!("{:?}", e)) else {
                return;
            };

            let _ = match lags_result {
                Ok(Some(lag)) => tx.send(ReadLagsResult::GroupTopicLag(lag)).await,
                Ok(None) => Ok(()),
                Err(e) => tx.send(ReadLagsResult::BrokerError(e)).await,
            };
        });
    }

    Ok(())
}
fn get_consumer_groups(consumer: &ConsumerWrapper) -> Result<Vec<GroupInfo>, anyhow::Error> {
    let groups_list = consumer
        .fetch_group_list(None, Duration::from_secs(5))
        .context("While fetching groups")?;

    let groups = groups_list
        .groups()
        .iter()
        .map(|x| GroupInfo {
            state: x.state().to_owned(),
            id: x.name().to_owned(),
        })
        .collect::<Vec<_>>();

    Ok(groups)
}

fn get_watermarks(
    consumer: &ConsumerWrapper,
    topic: &str,
    topic_partitions: &TopicPartitionList,
) -> Result<HashMap<i32, MinMaxOffset>, anyhow::Error> {
    let mut watermarks = HashMap::new();
    for partition in topic_partitions.elements() {
        let (min, max) = consumer
            .fetch_watermarks(topic, partition.partition(), Duration::from_secs(1))
            .context("While fetching watermarks")?;

        watermarks.insert(
            partition.partition(),
            MinMaxOffset {
                max_offset: max,
                min_offset: min,
            },
        );
    }

    Ok(watermarks)
}

fn get_topic_partitions(
    consumer: &ConsumerWrapper,
    topic: &str,
) -> Result<TopicPartitionList, anyhow::Error> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(1))
        .context("While fetching metadata")?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .context("Topic not found in metadata")?;

    let mut topic_partition_list = TopicPartitionList::new();
    for partition in topic_metadata.partitions() {
        topic_partition_list
            .add_partition_offset(topic, partition.id(), Offset::Invalid)
            .context("While adding partition")?;
    }

    Ok(topic_partition_list)
}

fn get_group_topic_offsets(
    kafka_connection_settings: &ConnectionSettings,
    partitions_watermarks: Arc<HashMap<i32, MinMaxOffset>>,
    topic_partitions: TopicPartitionList,
    group: GroupInfo,
    topic: Arc<String>,
) -> Result<Option<GroupTopicLags>, anyhow::Error> {
    let consumer =
        ConsumerWrapper::create_for_non_consuming(kafka_connection_settings, Some(&group.id))?;
    let committed_list = consumer
        .committed_offsets(topic_partitions, Duration::from_secs(15))
        .context("While fetching committed offsets")?;
    let committed_list_elements = committed_list.elements_for_topic(&topic);

    if committed_list_elements.is_empty()
        || committed_list_elements
            .iter()
            .all(|x| matches!(x.offset(), Offset::Invalid))
    {
        return Ok(None);
    }
    let mut lags = Vec::with_capacity(committed_list.elements().len());
    for x in committed_list_elements {
        let offset_value = x
            .offset()
            .to_raw()
            .and_then(|x| if x >= 0 { Some(x) } else { None });

        let watermarks = partitions_watermarks.get(&x.partition());
        let lag = match (watermarks, offset_value) {
            (Some(min_max_offset), None) => min_max_offset.messages_count(),
            (Some(min_max_offset), Some(v)) => min_max_offset.max_offset - v,
            (None, _) => 0,
        };
        let partition_with_lag = PartitionOffsetWithLag {
            partition: x.partition(),
            offset: offset_value,
            lag,
        };
        lags.push(partition_with_lag)
    }

    let group_topic_lags = GroupTopicLags {
        group_info: group,
        lags,
        topic,
    };
    debug!("Got group lags: {:?}", group_topic_lags);

    Ok(Some(group_topic_lags))
}
