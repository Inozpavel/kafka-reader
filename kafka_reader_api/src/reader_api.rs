use anyhow::{anyhow};
use kafka_reader::consumer::read_messages_to_channel;
use kafka_reader::format::{Format, ProtoConvertData};
use kafka_reader::message::KafkaMessage;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Response, Status};

use crate::reader_api::proto::read_messages::message_format::protobuf::DecodeWay;
use crate::reader_api::proto::read_messages::MessageFormat;
use proto::read_messages::message_format::Format as ProtoFormat;

pub mod proto {
    tonic::include_proto!("kafka_reader_api");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("reader_service_descriptor");
}

pub struct ReaderService;

#[tonic::async_trait]
impl proto::kafka_reader_server::KafkaReader for ReaderService {
    type ReadMessagesStream =
    Box<dyn Stream<Item=Result<proto::read_messages::Response, Status>> + Send + Unpin>;

    async fn read_messages(
        &self,
        request: Request<proto::read_messages::Request>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        let request = request.into_inner();
        let format = proto_format_to_format(request.format)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;

        let create_consumer_result =
            read_messages_to_channel(request.brokers, request.topic, format).await;

        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(map_to_response);
                Ok(Response::new(Box::new(map)))
            }
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }
}

fn proto_format_to_format(message_format: Option<MessageFormat>) -> Result<Format, anyhow::Error> {
    match message_format
        .and_then(|x| x.format)
        .unwrap_or(ProtoFormat::StringFormat(Default::default()))
    {
        ProtoFormat::StringFormat(_) => Ok(Format::String),
        ProtoFormat::HexFormat(_) => Ok(Format::Hex),
        ProtoFormat::ProtobufFormat(protobuf_data) => match protobuf_data
            .decode_way
            .ok_or(anyhow!("Protobuf format can't be none"))?
        {
            DecodeWay::RawProtoFile(file) => {
                Ok(Format::Protobuf(ProtoConvertData::RawProto(file.proto)))
            }
        },
    }
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
