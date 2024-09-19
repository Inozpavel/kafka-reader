use crate::producer::ProducerWrapper;
use crate::requests::produce_messages_request::ProduceMessagesRequest;
use crate::requests::read_messages_request::{Format, ProtobufDecodeWay};
use crate::utils::create_holder;
use anyhow::Context;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use proto_json_converter::{json_string_to_proto_bytes, ProtoDescriptorPreparer};
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use tokio::select;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub async fn produce_messages_to_topic(
    request: ProduceMessagesRequest,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let preparer = RwLock::new(None);
    let producer = ProducerWrapper::create(request.brokers, request.security_protocol)
        .context("While creating producer")?;

    for message in request.messages.into_iter() {
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

        let mut headers = None;
        if !message.headers.is_empty() {
            let mut new_headers = OwnedHeaders::new_with_capacity(message.headers.len());

            for (header_key, header_value) in &message.headers {
                new_headers = new_headers.insert(Header {
                    key: header_key.as_str(),
                    value: Some(header_value.as_bytes()),
                })
            }

            headers = Some(new_headers);
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
            send_result = producer.send(record, Timeout::Never) => {
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

        match result {
            Ok((partition, offset)) => {}
            Err((kafka_error, message)) => {
                error!(?kafka_error, ?message)
            }
        }
    }
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
