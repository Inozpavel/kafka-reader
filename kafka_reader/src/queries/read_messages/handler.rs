use crate::consumer::{AutoOffsetReset, ConsumerWrapper, PartitionOffset};
use crate::queries::read_messages::{
    ConsumeCounters, ConsumeError, ConvertError, FilterCondition, FilterKind, Format, KafkaMessage,
    MessagesCounters, ProtobufDecodeWay, ReadLimit, ReadMessagesQueryInternal,
    ReadMessagesQueryInternalResponse, StartFrom, ValueFilter,
};
use crate::utils::create_holder;
use anyhow::{anyhow, bail, Context};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::BytesMut;
use proto_json_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, trace, Instrument};
use uuid::Uuid;

type ChannelItem = ReadMessagesQueryInternalResponse;

#[tracing::instrument(skip_all)]
pub async fn run_read_messages_to_channel(
    query: ReadMessagesQueryInternal,
    cancellation_token: CancellationToken,
) -> Result<Receiver<ChannelItem>, anyhow::Error> {
    let offset_reset = match query.start_from {
        StartFrom::Beginning | StartFrom::Time(_) => AutoOffsetReset::Earliest,
        StartFrom::Latest => AutoOffsetReset::Latest,
    };
    let group = format!("kafka-reader-{}", Uuid::now_v7());
    let consumer_wrapper =
        ConsumerWrapper::create_for_consuming(&query.connection_settings, &group, offset_reset)
            .context("While creating consumer")?;

    let query = Arc::new(query);

    let query_clone = query.clone();
    let partitions_count = tokio::task::spawn_blocking(move || {
        consumer_wrapper
            .get_topic_partitions_count(&query_clone.topic, Some(Duration::from_secs(5)))
            .context("While fetching partitions count")
    })
    .await??;

    let (tx, rx) = tokio::sync::mpsc::channel(128);
    let holder = Arc::new(RwLock::new(None));
    let topic = Arc::new(query.topic.clone());
    let counters = Arc::new(ConsumeCounters {
        message_count_check_counter: AtomicU64::new(0),
        returned_messages_counter: AtomicU64::new(0),
        read_messages_counter: AtomicU64::new(0),
    });

    debug!(
        "Starting {} consumers for topic {}",
        partitions_count, topic
    );

    if partitions_count == 0 {
        bail!("Topic {} wasn't found", topic)
    }
    for _ in 0..partitions_count {
        let consumer_wrapper =
            ConsumerWrapper::create_for_consuming(&query.connection_settings, &group, offset_reset)
                .context("While creating consumer")?;

        let topic = topic.clone();
        let query = query.clone();
        let tx = tx.clone();
        let counters = counters.clone();
        let holder = holder.clone();
        let cancellation_token = cancellation_token.clone();

        let future = consume_topic(
            topic,
            query,
            Arc::new(consumer_wrapper),
            tx,
            counters,
            holder,
            cancellation_token,
        )
        .instrument(info_span!("Consuming topic").or_current());
        tokio::task::spawn(future);
    }

    let counters_future = async move {
        let mut last_counter = MessagesCounters {
            returned_message_count: 0,
            read_message_count: 0,
        };

        loop {
            select! {
                _ = sleep(Duration::from_millis(500)) => {
                     let counter = MessagesCounters {
                        read_message_count: counters.read_messages_counter.load(Ordering::Relaxed),
                        returned_message_count: counters.returned_messages_counter.load(Ordering::Relaxed),
                    };
                    if last_counter != counter {
                        last_counter = counter;
                        let _ = tx.send(ChannelItem::MessagesCounters(counter)).await;
                    }

                },
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }
    .instrument(info_span!("Updating counters").or_current());

    tokio::task::spawn(counters_future);

    Ok(rx)
}

async fn consume_topic(
    topic: Arc<String>,
    query: Arc<ReadMessagesQueryInternal>,
    consumer_wrapper: Arc<ConsumerWrapper>,
    tx: Sender<ChannelItem>,
    counters: Arc<ConsumeCounters>,
    holder: Arc<RwLock<Option<ProtoDescriptorPreparer>>>,
    cancellation_token: CancellationToken,
) {
    if let Err(error) = consumer_wrapper
        .subscribe(&[&topic])
        .context("While subscribing to topic")
    {
        let _ = tx.send(ChannelItem::Error(ConsumeError { error })).await;
        return;
    }

    loop {
        let cancellation_token = cancellation_token.clone();
        let holder = holder.clone();

        let message_result = select! {
            msg = consumer_wrapper.recv() => {
                msg
            }
            _ = cancellation_token.cancelled() => {
                info!("Consuming was cancelled");
                break
            }
        };

        let Ok(message) = message_result else {
            let error = message_result.unwrap_err();
            error!(
                "Error while reading message from kafka consumer: {:?}",
                error
            );
            let _ = tx
                .send(ChannelItem::Error(ConsumeError {
                    error: error.into(),
                }))
                .await;

            return;
        };

        trace!(
            "New message. Topic: '{}', partition: {}, offset: {}",
            message.topic(),
            message.partition(),
            message.offset(),
        );

        counters
            .read_messages_counter
            .fetch_add(1, Ordering::Relaxed);

        let converted_message = convert_message(
            message,
            query.key_format.as_ref(),
            query.body_format.as_ref(),
            holder,
        )
        .await;

        let consumer_wrapper = consumer_wrapper.clone();
        let counters = counters.clone();
        let query = query.clone();
        let topic = topic.clone();
        let tx = tx.clone();
        let handle_future = async move {
            let partition_offset = converted_message.partition_offset;
            let handled = handle_message_result(
                converted_message,
                query,
                tx.clone(),
                topic.clone(),
                counters,
                cancellation_token,
            )
            .await;

            if handled {
                if let Err(e) = consumer_wrapper.store_offset(
                    &topic,
                    *partition_offset.partition(),
                    *partition_offset.offset(),
                ) {
                    error!(
                        "Error while storing offset to consumer. Topic {}, metadata: {:?}. {:?}",
                        topic, partition_offset, e
                    );
                }
            }
        }
        .instrument(info_span!("Handling kafka message result"));
        tokio::task::spawn(handle_future);
    }
}

async fn convert_message<'a>(
    message: BorrowedMessage<'a>,
    key_format: Option<&Format>,
    body_format: Option<&Format>,
    holder: Arc<RwLock<Option<ProtoDescriptorPreparer>>>,
) -> KafkaMessage {
    let milliseconds = message.timestamp().to_millis().unwrap_or(0).unsigned_abs();
    let timestamp = chrono::DateTime::UNIX_EPOCH + Duration::from_millis(milliseconds);
    let partition_offset = PartitionOffset::new(message.partition(), message.offset());

    let key_result = message.key_view::<[u8]>();
    let key = message_part_to_string("key", key_result, key_format, &holder)
        .await
        .map_err(|e| ConvertError {
            error: e,
            partition_offset,
        });

    let body_result = message.payload_view::<[u8]>();
    let body = message_part_to_string("body", body_result, body_format, &holder)
        .await
        .map_err(|e| ConvertError {
            error: e,
            partition_offset,
        });

    let headers = message.headers().map(|h| {
        h.iter()
            .map(|header| {
                (
                    header.key.to_owned(),
                    header
                        .value
                        .map_or_else(|| "".to_owned(), |v| String::from_utf8_lossy(v).to_string()),
                )
            })
            .collect::<HashMap<_, _>>()
    });
    KafkaMessage {
        partition_offset,
        timestamp,
        key,
        body,
        headers,
    }
}

async fn handle_message_result(
    message: KafkaMessage,
    query: Arc<ReadMessagesQueryInternal>,
    tx: Sender<ChannelItem>,
    topic: Arc<String>,
    counters: Arc<ConsumeCounters>,
    cancellation_token: CancellationToken,
) -> bool {
    if !check_start_condition(&message, &query.start_from) {
        return false;
    }
    if query.key_filter.is_some() || query.body_filter.is_some() {
        let filter_passed =
            check_message_part_filter(&message, |m| &m.key, query.key_filter.as_ref())
                || check_message_part_filter(&message, |m| &m.body, query.body_filter.as_ref());

        if !filter_passed {
            return false;
        }
    }

    if !check_limit_condition(
        &message,
        &counters.message_count_check_counter,
        &query.limit,
    ) {
        cancellation_token.cancel();
        return false;
    }

    let partition_offset = message.partition_offset;

    match tx
        .send(ReadMessagesQueryInternalResponse::KafkaMessage(message))
        .await
    {
        Ok(_) => {
            counters
                .returned_messages_counter
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            error!(
                "Error while sending message to channel. Topic {}, metadata: {:?}. {}",
                topic, partition_offset, e
            );
            cancellation_token.cancel();
        }
    };

    true
}

fn check_start_condition(message: &KafkaMessage, start_from: &StartFrom) -> bool {
    match start_from {
        StartFrom::Beginning | StartFrom::Latest => true,
        StartFrom::Time(time) => message.timestamp >= time.time(),
    }
}

fn check_limit_condition(
    message: &KafkaMessage,
    message_check_counter: &AtomicU64,
    limit: &ReadLimit,
) -> bool {
    match limit {
        ReadLimit::NoLimit => true,
        ReadLimit::MessageCount(count) => {
            let last_value = message_check_counter.fetch_add(1, Ordering::Relaxed);

            last_value < *count
        }
        ReadLimit::ToTime(date) => message.timestamp <= *date,
    }
}

fn check_message_part_filter<F>(
    message: &KafkaMessage,
    part_fn: F,
    value_filter: Option<&ValueFilter>,
) -> bool
where
    F: Fn(&KafkaMessage) -> &Result<Option<String>, ConvertError>,
{
    let Some(filter) = value_filter else {
        return false;
    };

    let part = part_fn(message);
    let Ok(part) = part else {
        return false;
    };

    let Some(value) = part else {
        return false;
    };

    match &filter.filter_kind {
        FilterKind::String(s) => match filter.filter_condition {
            FilterCondition::Contains => value.contains(s),
            FilterCondition::NotContains => !value.contains(s),
        },
        FilterKind::Regex(r) => match filter.filter_condition {
            FilterCondition::Contains => r.is_match(value),
            FilterCondition::NotContains => !r.is_match(value),
        },
    }
}

async fn message_part_to_string(
    part_name: &'static str,
    part: Option<Result<&[u8], ()>>,
    format: Option<&Format>,
    descriptor_holder: &RwLock<Option<ProtoDescriptorPreparer>>,
) -> Result<Option<String>, anyhow::Error> {
    let Some(bytes_result) = part else {
        return Ok(None);
    };
    let Ok(bytes) = bytes_result else {
        return Err(anyhow!(
            "Error for viewing {} message bytes {:?}",
            part_name,
            bytes_result.unwrap_err()
        ));
    };

    let result_string = bytes_to_string(bytes, format, descriptor_holder)
        .await
        .with_context(|| format!("While converting {} bytes to json string", part_name))?;

    Ok(result_string)
}

async fn bytes_to_string(
    bytes: &[u8],
    format: Option<&Format>,
    holder: &RwLock<Option<ProtoDescriptorPreparer>>,
) -> Result<Option<String>, anyhow::Error> {
    let Some(format) = format else {
        return Ok(None);
    };
    let converted = match format {
        Format::String => Some(String::from_utf8_lossy(bytes).to_string()),
        Format::Hex => Some(format!("{:02X}", BytesMut::from(bytes))),
        Format::Base64 => Some(BASE64_STANDARD.encode(bytes)),
        Format::Protobuf(ref protobuf_decode_way) => match protobuf_decode_way {
            ProtobufDecodeWay::SingleProtoFile(single_proto_file) => {
                create_holder(holder, single_proto_file)
                    .await
                    .context("While creating descriptor holder")?;
                let read_guard = holder.read().await;
                let preparer = read_guard.as_ref().expect("Poisoned rwlock");
                let json = proto_bytes_to_json_string(
                    bytes,
                    &single_proto_file.message_type_name,
                    preparer,
                )
                .context("While converting proto bytes to json")?;
                Some(json)
            }
        },
    };

    Ok(converted)
}
