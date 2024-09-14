use crate::error::{ConsumeError, ConvertError};
use crate::read_messages_request::ReadMessagesRequest;
use crate::read_messages_request::{Format, ProtoConvertData, StartFrom};
use anyhow::{anyhow, bail, Context};
use bytes::BytesMut;
use chrono::Duration;
use proto_bytes_to_json_string_converter::{proto_bytes_to_json_string, ProtoDescriptorHolder};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use crate::consumer::{AutoOffsetReset, ConsumerWrapper, KafkaMessage, MessageMetadata, PartitionOffset};

pub async fn run_read_messages_to_channel(
    request: ReadMessagesRequest,
    cancellation_token: CancellationToken,
) -> Result<Receiver<Result<Option<KafkaMessage>, ConvertError>>, anyhow::Error> {
    if request.brokers.is_empty() {
        bail!("No brokers specified")
    }
    let offset_reset = match request.start_from {
        StartFrom::Beginning | StartFrom::Today => AutoOffsetReset::Earliest,
        StartFrom::Latest => AutoOffsetReset::Latest,
    };
    let mut consumer_wrapper = ConsumerWrapper::create(&request.brokers, offset_reset)
        .context("While creating consumer")?;

    let topic = request.topic.clone();
    consumer_wrapper
        .subscribe(&[&topic])
        .context("While subscribing to topic")?;

    let mut preparer = None;
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::task::spawn(async move {
        loop {
            let message_result = select! {
                msg = read_message(&consumer_wrapper, &request.format, &request.format, &mut preparer) => {
                    msg
                }
                _ = cancellation_token.cancelled() => {
                    info!("Consuming was cancelled");
                    break
                }
            };

            if let Err(()) =
                handle_message_result(message_result, &tx, &topic, &mut consumer_wrapper).await
            {
                break;
            }
        }
    });

    Ok(rx)
}

async fn read_message(
    consumer: &StreamConsumer,
    key_format: &Format,
    body_format: &Format,
    holder: &mut Option<ProtoDescriptorHolder>,
) -> Result<Option<KafkaMessage>, ConsumeError> {
    let msg = consumer
        .recv()
        .await
        .context("While consuming message")
        .map_err(ConsumeError::RdKafkaError)?;

    let milliseconds = msg.timestamp().to_millis().unwrap_or(0);
    let timestamp = chrono::DateTime::UNIX_EPOCH + Duration::milliseconds(milliseconds);

    let partition_offset = PartitionOffset::new(msg.partition(), msg.offset());

    debug!(
        "New message. Topic: '{}', {:?}. ",
        msg.topic(),
        partition_offset
    );

    let key_result = msg.key_view::<[u8]>();
    let key = message_part_to_string("key", key_result, key_format, holder)
        .await
        .map_err(|e| {
            ConsumeError::ConvertError(ConvertError {
                error: e,
                partition_offset,
            })
        })?;
    let payload_result = msg.payload_view::<[u8]>();
    let body = message_part_to_string("body", payload_result, body_format, holder)
        .await
        .map_err(|e| {
            ConsumeError::ConvertError(ConvertError {
                error: e,
                partition_offset,
            })
        })?;

    let message = KafkaMessage {
        partition_offset,
        timestamp,
        key,
        body,
        headers: None,
    };

    Ok(Some(message))
}

async fn handle_message_result(
    message_result: Result<Option<KafkaMessage>, ConsumeError>,
    tx: &Sender<Result<Option<KafkaMessage>, ConvertError>>,
    topic: &str,
    consumer_wrapper: &mut ConsumerWrapper,
) -> Result<(), ()> {
    match message_result {
        Ok(message) => {
            let metadata = MessageMetadata::from(&message);
            if let Err(e) = tx.send(Ok(message)).await {
                error!(
                    "Error while sending message to channel. Topic {}, metadata: {:?}. {:?}",
                    topic, metadata, e
                );
                return Err(());
            }

            if let Err(e) =
                consumer_wrapper.store_offset(&topic, metadata.partition(), metadata.offset())
            {
                error!(
                    "Error while storing offset to consumer. Topic {}, metadata: {:?}. {:?}",
                    topic, metadata, e
                );
            }

            Ok(())
        }
        Err(e) => {
            match e {
                ConsumeError::RdKafkaError(e) => {
                    error!("Error while reading message from kafka consumer: {:?}", e)
                }
                ConsumeError::ConvertError(e) => {
                    if let Err(e) = tx.send(Err(e)).await {
                        error!(
                            "Error while sending message to channel. Topic {}, metadata: {:?}",
                            topic, e
                        );

                        return Err(());
                    }
                }
            }
            Ok(())
        }
    }
}

async fn message_part_to_string(
    part_name: &'static str,
    part: Option<Result<&[u8], ()>>,
    format: &Format,
    descriptor_holder: &mut Option<ProtoDescriptorHolder>,
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

    let result_string = bytes_to_string(bytes, &format, descriptor_holder)
        .await
        .with_context(|| format!("While converting {} bytes to json string", part_name))?;

    Ok(Some(result_string))
}

async fn bytes_to_string(
    bytes: &[u8],
    format: &Format,
    holder: &mut Option<ProtoDescriptorHolder>,
) -> Result<String, anyhow::Error> {
    let converted = match format {
        Format::String => Ok(String::from_utf8_lossy(bytes).to_string()),
        Format::Hex => Ok(format!("{:02X}", BytesMut::from(bytes))),
        Format::Protobuf(ref convert) => match convert {
            ProtoConvertData::RawProto(proto_file) => {
                if holder.is_none() {
                    let new_descriptor_holder =
                        ProtoDescriptorHolder::from_single_file(proto_file.as_bytes())
                            .await
                            .context("While creating descriptor holder")?;
                    *holder = Some(new_descriptor_holder);
                }
                let preparer = holder.as_mut().map(|x| x.preparer()).unwrap();
                let json_from_proto = proto_bytes_to_json_string(bytes, preparer).await;
                json_from_proto
            }
        },
    }
        .context("While converting body")?;

    Ok(converted)
}
