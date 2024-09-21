use crate::consumer::{
    AutoOffsetReset, BrokerError, ConsumerWrapper, KafkaMessage, PartitionOffset, ReadResult,
};
use crate::error::ConvertError;
use crate::queries::read_messages::{
    FilterCondition, FilterKind, Format, ProtobufDecodeWay, ReadLimit, ReadMessagesQueryInternal,
    StartFrom, ValueFilter,
};
use crate::utils::create_holder;
use anyhow::{anyhow, bail, Context};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::BytesMut;
use proto_json_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace};
use uuid::Uuid;

type ChannelItem = ReadResult;

pub async fn run_read_messages_to_channel(
    request: ReadMessagesQueryInternal,
    cancellation_token: CancellationToken,
) -> Result<Receiver<ChannelItem>, anyhow::Error> {
    if request.brokers.is_empty() {
        bail!("No brokers specified")
    }
    let offset_reset = match request.start_from {
        StartFrom::Beginning | StartFrom::Time(_) => AutoOffsetReset::Earliest,
        StartFrom::Latest => AutoOffsetReset::Latest,
    };
    let group = format!("kafka-reader-{}", Uuid::now_v7());
    let consumer_wrapper = ConsumerWrapper::create_for_consuming(
        &request.brokers,
        &group,
        offset_reset,
        request.security_protocol,
    )
    .context("While creating consumer")?;
    let consumer_wrapper = Arc::new(consumer_wrapper);

    let request = Arc::new(request);
    consumer_wrapper
        .subscribe(&[&request.topic])
        .context("While subscribing to topic")?;

    let holder = Arc::new(RwLock::new(None));
    let topic = Arc::new(request.topic.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let token = cancellation_token.clone();
    tokio::task::spawn(async move {
        let check_messages_counter = Arc::new(AtomicU64::new(0));
        let read_messages_counter = Arc::new(AtomicU64::new(0));
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
            if message_result.is_ok() {
                read_messages_counter.fetch_add(1, Ordering::Relaxed);
            };
            let converted_message = match message_result {
                Ok(message) => convert_message(
                    message,
                    request.key_format.as_ref(),
                    request.body_format.as_ref(),
                    holder,
                ),
                Err(e) => {
                    error!("Error while reading message from kafka consumer: {:?}", e);
                    let _ = tx
                        .send(ChannelItem::BrokerError(BrokerError {
                            message: format!("{:?}", e),
                        }))
                        .await;
                    return;
                }
            }
            .await;

            tokio::task::spawn(handle_message_result(
                converted_message,
                request.clone(),
                tx.clone(),
                topic.clone(),
                consumer_wrapper.clone(),
                check_messages_counter.clone(),
                cancellation_token,
            ));
        }
    });

    tokio::task::spawn(async move {
        loop {
            select! {
                _ = sleep(Duration::from_nanos(300)) => {

                }
                _ = token.cancelled() => {

                }
            }
        }
    });
    Ok(rx)
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
    trace!(
        "New message. Topic: '{}', {:?}. ",
        message.topic(),
        partition_offset
    );
    let key_result = message.key_view::<[u8]>();
    let key = message_part_to_string("key", key_result, key_format, &holder)
        .await
        .map_err(|e| ConvertError {
            error: e,
            partition_offset,
        });
    let payload_result = message.payload_view::<[u8]>();
    let body = message_part_to_string("body", payload_result, body_format, &holder)
        .await
        .map_err(|e| ConvertError {
            error: e,
            partition_offset,
        });

    KafkaMessage {
        partition_offset,
        timestamp,
        key,
        body,
        headers: None,
    }
}

async fn handle_message_result(
    message: KafkaMessage,
    request: Arc<ReadMessagesQueryInternal>,
    tx: Sender<ChannelItem>,
    topic: Arc<String>,
    consumer_wrapper: Arc<ConsumerWrapper>,
    message_check_counter: Arc<AtomicU64>,
    cancellation_token: CancellationToken,
) {
    if !check_start_condition(&message, &request.start_from) {
        return;
    }
    if request.key_filter.is_some() || request.body_filter.is_some() {
        let filter_passed =
            check_message_part_filter(&message, |m| &m.key, request.key_filter.as_ref())
                || check_message_part_filter(&message, |m| &m.body, request.body_filter.as_ref());

        if !filter_passed {
            return;
        }
    }

    if !check_limit_condition(&message, &message_check_counter, &request.limit) {
        cancellation_token.cancel();
        return;
    }

    let partition_offset = message.partition_offset;
    if let Err(e) = tx.send(ReadResult::KafkaMessage(message)).await {
        error!(
            "Error while sending message to channel. Topic {}, metadata: {:?}. {}",
            topic, partition_offset, e
        );
        cancellation_token.cancel();
    }

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
