use crate::connection_settings::ConnectionSettings;
use crate::consumer::ConsumerWrapper;
use crate::queries::get_topic_lags::query::GetTopicLagsQueryInternal;
use crate::queries::get_topic_lags::response::{
    GroupInfo, GroupTopicLags, PartitionOffsetWithLag, ReadLagsResult,
};
use crate::queries::get_topic_partitions_with_offsets::MinMaxOffset;
use anyhow::Context;
use rdkafka::consumer::Consumer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, Instrument, Span};

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
        .instrument(Span::current()),
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

    let groups = select! {
        groups = groups_handle => {
            groups.context("While joining handle with groups")??
        }
        _ = cancellation_token.cancelled() => {
            return Ok(())
        }
    };

    let topic_copy = topic.clone();

    let partitions_offsets =
        consumer.get_all_partitions_watermarks(&topic_copy, Some(Duration::from_secs(5)))?;

    let watermarks = Arc::new(partitions_offsets);

    let kafka_settings = Arc::new(query.connection_settings);
    for group in groups {
        let kafka_settings = kafka_settings.clone();
        let topic = topic.clone();
        let tx = tx.clone();
        let watermarks = watermarks.clone();

        tokio::task::spawn(async move {
            let Ok(lags_result) = tokio::task::spawn_blocking(move || {
                get_group_topic_offsets(&kafka_settings, watermarks, group, topic)
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

fn get_group_topic_offsets(
    kafka_connection_settings: &ConnectionSettings,
    partitions_watermarks: Arc<HashMap<i32, MinMaxOffset>>,
    group: GroupInfo,
    topic: Arc<String>,
) -> Result<Option<GroupTopicLags>, anyhow::Error> {
    let consumer =
        ConsumerWrapper::create_for_non_consuming(kafka_connection_settings, Some(&group.id))?;

    let partitions_count = consumer
        .get_topic_partitions_count(&topic, Some(Duration::from_secs(5)))
        .context("While fetching partitions_count")?;
    let partition_offsets = consumer
        .get_topic_offsets(&topic, partitions_count, Some(Duration::from_secs(5)))
        .context("While fetching group lag")?;

    if partition_offsets.is_empty() || partition_offsets.iter().all(|x| x.offset().is_none()) {
        return Ok(None);
    }

    let mut lags = vec![];

    for partition_offset in partition_offsets {
        let watermarks = partitions_watermarks.get(partition_offset.partition());
        let lag = match (watermarks, partition_offset.offset()) {
            (Some(min_max_offset), None) => min_max_offset.messages_count(),
            (Some(min_max_offset), Some(v)) => min_max_offset.max_offset - v,
            (None, _) => 0,
        };
        let partition_with_lag = PartitionOffsetWithLag {
            partition_offset,
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
