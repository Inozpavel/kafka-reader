use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Context;
use chrono::Utc;
use rdkafka::bindings::rd_kafka_ListConsumerGroupOffsets;
use rdkafka::consumer::Consumer;
use rdkafka::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;
use crate::admin::AdminWrapper;
use crate::consumer::{ConsumerWrapper, PartitionOffset, SecurityProtocol};
use crate::queries::get_topic_lags::query::GetTopicLagsQueryInternal;
use crate::queries::get_topic_partitions_with_offsets::MinMaxOffset;

pub async fn get_topic_lags(query: GetTopicLagsQueryInternal) -> Result<Receiver<GroupTopicLags>, anyhow::Error> {
    let consumer = ConsumerWrapper::create_for_non_consuming(&query.brokers, query.security_protocol, None)?;
    let groups_list = consumer.fetch_group_list(None, Timeout::After(Duration::from_secs(5))).context("While fetching groups")?;
    let groups = groups_list.groups().iter().map(|x| GroupInfo {
        state: x.state().to_owned(),
        id: x.name().to_owned(),
    }).collect::<Vec<_>>();

    let metadata = consumer
        .fetch_metadata(Some(&query.topic), Duration::from_secs(1))
        .expect("Failed to fetch metadata");

    let topic_metadata = metadata.topics().iter().find(|t| t.name() == query.topic)
        .expect("Topic not found in metadata");
    let mut tpl = TopicPartitionList::new();
    for partition in topic_metadata.partitions() {
        tpl.add_partition_offset(&query.topic, partition.id(), Offset::Invalid)
            .expect("Failed to add partition");
    }

    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let mut watermarks = HashMap::new();
    for partition in topic_metadata.partitions() {
        let (min, max) = consumer.fetch_watermarks(&query.topic, partition.id(), Duration::from_secs(1))?;

        watermarks.insert(partition.id(), MinMaxOffset {
            max_offset: max,
            min_offset: min,
        });
    }

    let brokers = Arc::new(query.brokers.clone());
    let topic = Arc::new(query.topic.clone());
    let watermarks = Arc::new(watermarks);
    for group in groups {
        let tpl = tpl.clone();
        let brokers = brokers.clone();
        let topic = topic.clone();
        let tx = tx.clone();
        let watermarks = watermarks.clone();
        tokio::task::spawn_blocking(move || {
            get_group_topic_offsets(watermarks, tpl, brokers, query.security_protocol, group, topic, tx).with_context(|| format!("While fetching group offsets"))
        });
    }
    Ok(rx)
}

fn get_group_topic_offsets(partitions_watermarks: Arc<HashMap<i32, MinMaxOffset>>,
                           tpl: TopicPartitionList, brokers: Arc<Vec<String>>, security_protocol: SecurityProtocol, group: GroupInfo, topic: Arc<String>,
                           tx: Sender<GroupTopicLags>) -> Result<Option<GroupTopicLags>, anyhow::Error> {
    let consumer = ConsumerWrapper::create_for_non_consuming(&*brokers, security_protocol, Some(&group.id))?;
    let committed_list = consumer.committed_offsets(tpl, Duration::from_secs(15)).context("While fetching committed offsets")?;
    let committed_list_elements = committed_list.elements_for_topic(&*topic);

    if committed_list_elements.is_empty()
        || committed_list_elements.iter().all(|x| matches!(x.offset(), Offset::Invalid)) {
        return Ok(None);
    }
    let mut lags = Vec::with_capacity(committed_list.elements().len());
    for x in committed_list_elements {
        // info!("g {:?} t {} p {} o {:?}", group, x.topic(), x.partition(), x.offset());

        let offset_value = x.offset().to_raw().and_then(|x| if x >= 0 { Some(x) } else { None });

        let watermarks = partitions_watermarks.get(&x.partition());
        let lag = match (watermarks, offset_value) {
            (Some(min_max_offset), None) => min_max_offset.messages_count(),
            (Some(min_max_offset), Some(v)) => min_max_offset.max_offset - v,
            (None, _) => 0
        };
        let partition_with_lag = PartitionOffsetWithLag {
            partition: x.partition(),
            offset: offset_value,
            lag,
        };
        lags.push(partition_with_lag)
    }

    info!("Lags: {:?}", lags);
    let lags = GroupTopicLags {
        group_info: group,
        lags,
    };

    Ok(Some(lags))
}

#[derive(Debug)]
pub struct GroupInfo {
    id: String,
    state: String,
}

#[derive(Debug)]
pub struct GroupTopicLags {
    group_info: GroupInfo,
    lags: Vec<PartitionOffsetWithLag>,
}

#[derive(Debug)]
pub struct PartitionOffsetWithLag {
    partition: i32,
    offset: Option<i64>,
    lag: i64,
}

impl PartitionOffsetWithLag {
    pub fn set_lag(&mut self, lag: i64) {
        self.lag = lag
    }
}