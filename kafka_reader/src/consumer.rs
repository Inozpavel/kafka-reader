use crate::message::KafkaMessage;
use crate::message_metadata::MessageMetadata;
use crate::read_messages_request::ReadMessagesRequest;
use crate::read_messages_request::{Format, ProtoConvertData, StartFrom};
use anyhow::{bail, Context};
use bytes::BytesMut;
use proto_bytes_to_json_string_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

pub async fn read_messages_to_channel(
    request: ReadMessagesRequest,
    cancellation_token: CancellationToken,
) -> Result<Receiver<Option<KafkaMessage>>, anyhow::Error> {
    if request.brokers.is_empty() {
        bail!("No brokers specified")
    }
    let consumer = create_consumer(&request.brokers, &request.start_from)
        .context("While creating consumer")?;
    let topic = request.topic.clone();

    consumer
        .subscribe(&[&topic])
        .context("While subscribing to topic")?;

    let mut preparer = None;
    let (tx, rx) = tokio::sync::mpsc::channel::<Option<KafkaMessage>>(128);
    tokio::task::spawn(async move {
        loop {
            let message_result = select! {
                msg = read_message(&consumer, &request.format, &mut preparer) => {
                    msg
                }
                _ = cancellation_token.cancelled() => {
                    info!("Consuming was cancelled");
                    break
                }
            };

            match message_result {
                Ok(message) => {
                    let metadata = MessageMetadata::from(&message);
                    if let Err(e) = tx.send(message).await {
                        error!(
                            "Error while sending message to channel. Topic {}, metadata: {:?}. :{}",
                            topic, metadata, e
                        );
                        break;
                    }

                    if let Err(e) =
                        consumer.store_offset(&topic, *metadata.partition(), *metadata.offset())
                    {
                        error!("Error while storing offset to consumer. Topic {}, metadata: {:?}. {:?}. ",  topic, metadata, e);
                    }
                }
                Err(e) => {
                    error!("Error while reading message from kafka consumer: {:?}", e);
                }
            }
        }
    });

    Ok(rx)
}

async fn read_message(
    consumer: &StreamConsumer,
    format: &Format,
    preparer: &mut Option<ProtoDescriptorPreparer<&str>>,
) -> Result<Option<KafkaMessage>, anyhow::Error> {
    let msg = consumer.recv().await?;
    debug!("New message");

    let Some(bytes) = msg.payload_view::<[u8]>() else {
        return Ok(None);
    };

    let Ok(bytes) = bytes else {
        error!("Error viewing kafka message bytes {:?}", bytes.unwrap_err());
        return Ok(None);
    };
    let body = bytes_to_string(bytes, &format, preparer)
        .await
        .context("While converting bytes to json string")?;

    let message = KafkaMessage {
        partition: msg.partition(),
        offset: msg.offset(),
        key: None,
        body: Some(body),
        headers: None,
    };

    Ok(Some(message))
}

async fn bytes_to_string(
    bytes: &[u8],
    format: &Format,
    preparer: &mut Option<ProtoDescriptorPreparer<&str>>,
) -> Result<String, anyhow::Error> {
    let converted = match format {
        Format::String => Ok(String::from_utf8_lossy(bytes).to_string()),
        Format::Hex => Ok(format!("{:02X}", BytesMut::from(bytes))),
        Format::Protobuf(ref convert) => match convert {
            ProtoConvertData::RawProto(proto_file) => {
                if preparer.is_none() {
                    *preparer = Some(ProtoDescriptorPreparer::new("", vec![]));
                }
                let preparer = preparer.as_mut().unwrap();
                let json_from_proto = proto_bytes_to_json_string(bytes, preparer).await;
                json_from_proto
            }
        },
    }
    .context("While converting body")?;

    Ok(converted)
}

fn create_consumer(
    brokers: &[String],
    start_from: &StartFrom,
) -> Result<StreamConsumer, anyhow::Error> {
    let offset_reset = match start_from {
        StartFrom::Beginning | StartFrom::Today => "earliest",
        StartFrom::Latest => "latest",
    };
    let brokers_string = brokers.join(",");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers_string)
        .set("auto.offset.reset", offset_reset)
        .set("group.id", Uuid::now_v7().to_string())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()
        .context("While creating a Kafka client config file")?;

    Ok(consumer)
}
