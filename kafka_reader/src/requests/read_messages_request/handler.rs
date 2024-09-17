use crate::consumer::{
    AutoOffsetReset, ConsumerWrapper, KafkaMessage, MessageMetadata, PartitionOffset,
};
use crate::error::ConvertError;
use crate::requests::read_messages_request::{
    FilterCondition, FilterKind, Format, ProtobufDecodeWay, ReadLimit, ReadMessagesRequest,
    StartFrom, ValueFilter,
};
use anyhow::{anyhow, bail, Context};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::BytesMut;
use chrono::Duration;
use proto_json_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

type ChannelItem = Option<KafkaMessage>;

pub async fn run_read_messages_to_channel(
    request: ReadMessagesRequest,
    cancellation_token: CancellationToken,
) -> Result<Receiver<ChannelItem>, anyhow::Error> {
    if request.brokers.is_empty() {
        bail!("No brokers specified")
    }
    let offset_reset = match request.start_from {
        StartFrom::Beginning | StartFrom::Time(_) => AutoOffsetReset::Earliest,
        StartFrom::Latest => AutoOffsetReset::Latest,
    };
    let consumer_wrapper =
        ConsumerWrapper::create(&request.brokers, offset_reset, request.security_protocol)
            .context("While creating consumer")?;
    let consumer_wrapper = Arc::new(consumer_wrapper);

    let request = Arc::new(request);
    consumer_wrapper
        .subscribe(&[&request.topic])
        .context("While subscribing to topic")?;

    let holder = Arc::new(RwLock::new(None));
    let topic = Arc::new(request.topic.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::task::spawn(async move {
        let check_messages_counter = Arc::new(AtomicU64::new(0));
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

            let converted_message = match message_result {
                Ok(message) => {
                    convert_message(message, &request.key_format, &request.body_format, holder)
                }
                Err(e) => {
                    error!("Error while reading message from kafka consumer: {:?}", e);
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

    Ok(rx)
}

async fn convert_message<'a>(
    message: BorrowedMessage<'a>,
    key_format: &Format,
    body_format: &Format,
    holder: Arc<RwLock<Option<ProtoDescriptorPreparer>>>,
) -> ChannelItem {
    let milliseconds = message.timestamp().to_millis().unwrap_or(0);
    let timestamp = chrono::DateTime::UNIX_EPOCH + Duration::milliseconds(milliseconds);

    let partition_offset = PartitionOffset::new(message.partition(), message.offset());
    debug!(
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

    let message = KafkaMessage {
        partition_offset,
        timestamp,
        key,
        body,
        headers: None,
    };

    Some(message)
}

async fn handle_message_result(
    message: ChannelItem,
    request: Arc<ReadMessagesRequest>,
    tx: Sender<ChannelItem>,
    topic: Arc<String>,
    consumer_wrapper: Arc<ConsumerWrapper>,
    message_check_counter: Arc<AtomicU64>,
    cancellation_token: CancellationToken,
) {
    if !check_start_condition(&message, &request.start_from) {
        return;
    }

    if let Some(value) = &message {
        let filter_passed =
            check_message_part_filter(value, |m| &m.key, request.key_filter.as_ref())
                || check_message_part_filter(value, |m| &m.body, request.body_filter.as_ref());

        if !filter_passed {
            return;
        }
    }

    if !check_limit_condition(&message, &message_check_counter, &request.limit) {
        cancellation_token.cancel();
        return;
    }

    let metadata = MessageMetadata::from(&message);
    if let Err(e) = tx.send(message).await {
        error!(
            "Error while sending message to channel. Topic {}, metadata: {:?}. {}",
            topic, metadata, e
        );
        cancellation_token.cancel();
    }

    if let Err(e) = consumer_wrapper.store_offset(&topic, metadata.partition(), metadata.offset()) {
        error!(
            "Error while storing offset to consumer. Topic {}, metadata: {:?}. {:?}",
            topic, metadata, e
        );
    }
}

fn check_start_condition(message: &ChannelItem, start_from: &StartFrom) -> bool {
    let Some(message_value) = message else {
        return true;
    };
    match start_from {
        StartFrom::Beginning | StartFrom::Latest => true,
        StartFrom::Time(time) => message_value.timestamp >= time.time(),
    }
}

fn check_limit_condition(
    message: &ChannelItem,
    message_check_counter: &AtomicU64,
    limit: &ReadLimit,
) -> bool {
    let Some(message_value) = message else {
        return true;
    };
    match limit {
        ReadLimit::NoLimit => true,
        ReadLimit::MessageCount(count) => {
            let last_value = message_check_counter.fetch_add(1, Ordering::Relaxed);

            last_value < *count
        }
        ReadLimit::ToTime(date) => message_value.timestamp <= *date,
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
        return true;
    };

    let part = part_fn(message);
    let Ok(part) = part else {
        return true;
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
    format: &Format,
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
    format: &Format,
    holder: &RwLock<Option<ProtoDescriptorPreparer>>,
) -> Result<Option<String>, anyhow::Error> {
    let converted = match format {
        Format::Ignore => None,
        Format::String => Some(String::from_utf8_lossy(bytes).to_string()),
        Format::Hex => Some(format!("{:02X}", BytesMut::from(bytes))),
        Format::Base64 => Some(BASE64_STANDARD.encode(bytes)),
        Format::Protobuf(ref protobuf_decode_way) => match protobuf_decode_way {
            ProtobufDecodeWay::SingleProtoFile(single_proto_file) => {
                if holder.read().expect("Panic in read scope").is_none() {
                    let mut new_descriptor_holder =
                        ProtoDescriptorPreparer::new(single_proto_file.file.to_string(), vec![]);
                    new_descriptor_holder
                        .prepare()
                        .await
                        .context("While initializing ProtoDescriptorPreparer")?;
                    *holder.write().expect("Panic in write scope") = Some(new_descriptor_holder);
                }
                let read_guard = holder.read().expect("Panic in read scope");
                let preparer = read_guard.as_ref().unwrap();
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
