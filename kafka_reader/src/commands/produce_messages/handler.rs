use crate::commands::produce_messages::response::{
    ProduceMessagesCommandInternalResponse, ProducedMessageInfo,
};
use crate::commands::produce_messages::{ProduceMessage, ProduceMessagesCommandInternal};
use crate::producer::ProducerWrapper;
use crate::queries::read_messages::{Format, ProtobufDecodeWay};
use crate::utils::create_holder;
use anyhow::{anyhow, Context};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use proto_json_converter::{json_string_to_proto_bytes, ProtoDescriptorPreparer};
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureRecord;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument};

pub async fn produce_messages_to_topic(
    mut request: ProduceMessagesCommandInternal,
    cancellation_token: CancellationToken,
) -> Result<Receiver<ProduceMessagesCommandInternalResponse>, anyhow::Error> {
    let preparer = Arc::new(RwLock::new(None));
    let producer =
        ProducerWrapper::create(&request.connection_settings).context("While creating producer")?;

    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let mut messages = vec![];
    std::mem::swap(&mut request.messages, &mut messages);

    let request = Arc::new(request);
    let producer = Arc::new(producer);
    for message in messages.into_iter() {
        let tx = tx.clone();
        let request = request.clone();
        let preparer = preparer.clone();
        let cancellation_token = cancellation_token.clone();
        let producer = producer.clone();
        let future = async move {
            if let Err(e) = produce_message(
                message,
                request,
                preparer,
                producer,
                tx.clone(),
                cancellation_token,
            )
            .await
            {
                let _ = tx
                    .send(ProduceMessagesCommandInternalResponse::Error(e))
                    .await;
            };
        }
        .instrument(info_span!("Producing message").or_current());

        tokio::task::spawn(future);
    }
    Ok(rx)
}

async fn produce_message(
    message: ProduceMessage,
    request: Arc<ProduceMessagesCommandInternal>,
    preparer: Arc<RwLock<Option<ProtoDescriptorPreparer>>>,
    producer: Arc<ProducerWrapper>,
    tx: Sender<ProduceMessagesCommandInternalResponse>,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let key_bytes = to_bytes(
        message.key,
        request.key_format.as_ref().unwrap_or(&Format::String),
        &preparer,
    )
    .await
    .context("While converting key to bytes")?;

    let body_bytes = to_bytes(
        message.body,
        request.body_format.as_ref().unwrap_or(&Format::String),
        &preparer,
    )
    .await
    .context("While converting body to bytes")?;

    let headers = if message.headers.is_empty() {
        None
    } else {
        Some(message.headers.iter().fold(
            OwnedHeaders::new_with_capacity(message.headers.len()),
            |acc, (key, value)| {
                acc.insert(Header {
                    key: key.as_str(),
                    value: Some(value.as_str()),
                })
            },
        ))
    };

    let record = FutureRecord {
        topic: &request.topic,
        partition: message.partition,
        timestamp: None,
        key: key_bytes.as_ref(),
        payload: body_bytes.as_ref(),
        headers,
    };

    let send_result = select! {
        send_result = producer.send(record, Duration::from_secs(5)) => {
            Some(send_result)
        }
        _ = cancellation_token.cancelled() => {
            info!("Producing to topic {} was cancelled", request.topic);
            None
        }
    };

    let Some(result) = send_result else {
        return Ok(());
    };

    let message = match result {
        Ok((partition, offset)) => {
            ProduceMessagesCommandInternalResponse::ProducedMessageInfo(ProducedMessageInfo {
                offset,
                partition,
            })
        }
        Err((kafka_error, _message)) => {
            error!("Produce error {:?}", kafka_error);
            ProduceMessagesCommandInternalResponse::Error(anyhow!("{:?}", kafka_error))
        }
    };

    let _ = tx.send(message).await;

    Ok(())
}

async fn to_bytes(
    s: Option<String>,
    format: &Format,
    preparer: &RwLock<Option<ProtoDescriptorPreparer>>,
) -> Result<Option<Vec<u8>>, anyhow::Error> {
    let Some(s) = s else {
        return Ok(None);
    };

    let bytes = match format {
        Format::String => s.to_bytes().to_vec(),
        Format::Hex => vec![],
        Format::Base64 => BASE64_STANDARD
            .decode(s.as_bytes())
            .context("While decoding base64")?,
        Format::Protobuf(decode_way) => match decode_way {
            ProtobufDecodeWay::SingleProtoFile(single_file) => {
                create_holder(preparer, single_file)
                    .await
                    .context("While creating ProtoDescriptorPreparer")?;
                let read_guard = preparer.read().await;
                let preparer = read_guard.as_ref().expect("Poisoned rwlock");
                json_string_to_proto_bytes(&s, &single_file.message_type_name, preparer)
                    .context("While converting json to proto bytes")?
            }
        },
    };

    Ok(Some(bytes))
}
