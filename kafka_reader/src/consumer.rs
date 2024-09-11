use crate::format::{Format, ProtoConvertData};
use crate::message::KafkaMessage;
use anyhow::{bail, Context};
use proto_bytes_to_json_string_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio::sync::mpsc::Receiver;
use tracing::error;

pub async fn read_messages_to_channel(
    brokers: Vec<String>,
    topic: String,
    format: Format,
) -> Result<Receiver<Option<KafkaMessage>>, anyhow::Error> {
    if brokers.is_empty() {
        bail!("No brokers specified")
    }
    let brokers_string = brokers.join(",");
    let consumer = create_consumer(brokers_string).context("While creating consumer")?;
    consumer
        .subscribe(&[&topic])
        .context("While subscribing to topic")?;

    let mut preparer = None;
    let (tx, rx) = tokio::sync::mpsc::channel::<Option<KafkaMessage>>(128);
    tokio::task::spawn(async move {
        loop {
            match consumer.recv().await {
                Err(e) => {
                    error!("Kafka error: {}", e);
                }
                Ok(msg) => {
                    let Some(bytes) = msg.payload_view::<[u8]>() else {
                        if let Err(e) = tx
                            .send(None)
                            .await
                            .context("While sending None message to channel")
                        {
                            error!("While sending None message to channel: {}", e);
                        };

                        break;
                    };

                    let Ok(bytes) = bytes else {
                        error!("Error viewing kafka message bytes {:?}", bytes.unwrap_err());
                        continue;
                    };

                    let body_result = bytes_to_string(bytes, &format, &mut preparer).await;

                    match body_result {
                        Ok(body) => {
                            let message = KafkaMessage {
                                key: None,
                                body: Some(body),
                                headers: None,
                            };

                            tx.send(Some(message)).await.unwrap();
                            consumer.store_offset_from_message(&msg);
                        }
                        Err(e) => error!("Error on converting bytes to json string: {:?}", e),
                    }
                }
            }
        }
    });

    Ok(rx)
}

async fn bytes_to_string(
    bytes: &[u8],
    format: &Format,
    preparer: &mut Option<ProtoDescriptorPreparer<&str>>,
) -> Result<String, anyhow::Error> {
    let converted = match format {
        Format::String => Ok(String::from_utf8_lossy(bytes).to_string()),
        Format::Hex => Ok(format!("{:X?}", bytes)),
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

fn create_consumer(brokers: String) -> Result<StreamConsumer, anyhow::Error> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "1")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()
        .context("While creating a Kafka client config file")?;
    Ok(consumer)
}
