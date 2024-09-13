use crate::reader_api::proto::read_messages::message_format::proto_format::DecodeWay;
use crate::reader_api::proto::read_messages::message_format::StringFormat;
use crate::reader_api::proto::read_messages::start_from::FromBeginning;
use anyhow::anyhow;
use kafka_reader::consumer::read_messages_to_channel;
use kafka_reader::message::KafkaMessage;
use kafka_reader::read_messages_request::ReadMessagesRequest;
use kafka_reader::read_messages_request::{Format, ProtoConvertData, StartFrom};
use proto::read_messages::message_format::Format as ProtoMessageFormatVariant;
use proto::read_messages::start_from::From as ProtoStartFromVariant;
use proto::read_messages::MessageFormat as ProtoMessageFormat;
use proto::read_messages::StartFrom as ProtoStartFrom;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Response, Status};
use tracing::debug;

pub mod proto {
    tonic::include_proto!("kafka_reader_api");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("reader_service_descriptor");
}

pub struct ReaderService;

#[tonic::async_trait]
impl proto::kafka_reader_server::KafkaReader for ReaderService {
    type ReadMessagesStream =
        Box<dyn Stream<Item = Result<proto::read_messages::Response, Status>> + Send + Unpin>;

    async fn read_messages(
        &self,
        request: Request<proto::read_messages::Request>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        debug!("New request: {:?}", request);

        let request = request.into_inner();

        let format = proto_format_to_format(request.format)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;
        let start_from = proto_start_from_to_from(request.start_from)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;

        let read_request = ReadMessagesRequest {
            topic: request.topic,
            format,
            start_from,
            brokers: request.brokers,
        };
        debug!("Mapped request: {:?}", read_request);

        let create_consumer_result = read_messages_to_channel(read_request).await;

        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(map_to_response);
                Ok(Response::new(Box::new(map)))
            }
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }
}

fn proto_format_to_format(
    message_format: Option<ProtoMessageFormat>,
) -> Result<Format, anyhow::Error> {
    let proto_format = message_format
        .and_then(|x| x.format)
        .unwrap_or(ProtoMessageFormatVariant::StringFormat(StringFormat {}));

    let format = match proto_format {
        ProtoMessageFormatVariant::StringFormat(_) => Format::String,
        ProtoMessageFormatVariant::HexFormat(_) => Format::Hex,
        ProtoMessageFormatVariant::ProtoFormat(protobuf_data) => match protobuf_data
            .decode_way
            .ok_or(anyhow!("Protobuf format can't be none"))?
        {
            DecodeWay::RawProtoFile(file) => {
                Format::Protobuf(ProtoConvertData::RawProto(file.proto))
            }
        },
    };

    Ok(format)
}

fn proto_start_from_to_from(
    message_format: Option<ProtoStartFrom>,
) -> Result<StartFrom, anyhow::Error> {
    let proto_from = message_format
        .and_then(|x| x.from)
        .unwrap_or(ProtoStartFromVariant::Beginning(FromBeginning {}));

    let from = match proto_from {
        ProtoStartFromVariant::Beginning(_) => StartFrom::Beginning,
        ProtoStartFromVariant::Latest(_) => StartFrom::Latest,
        ProtoStartFromVariant::Today(_) => StartFrom::Today,
    };

    Ok(from)
}
fn map_to_response(
    message: Option<KafkaMessage>,
) -> Result<proto::read_messages::Response, Status> {
    match message {
        None => Ok(proto::read_messages::Response {
            kafka_message: None,
        }),
        Some(value) => Ok(proto::read_messages::Response {
            kafka_message: Some(proto::read_messages::KafkaMessage {
                key: value.key,
                body: value.body,
                headers: value.headers.unwrap_or_default(),
            }),
        }),
    }
}
