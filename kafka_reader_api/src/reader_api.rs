use crate::reader_api::proto::read_messages::message_format::proto_format::DecodeWay;
use crate::reader_api::proto::read_messages::message_format::StringFormat;
use crate::reader_api::proto::read_messages::read_limit::{
    Limit as ProtoReadLimitVariant, NoLimit,
};
use crate::reader_api::proto::read_messages::start_from::FromBeginning;
use crate::reader_api::proto::read_messages::ReadLimit as ProtoReadLimit;
use crate::util::StreamDataExtension;
use anyhow::anyhow;
use chrono::DateTime;
use kafka_reader::consumer::KafkaMessage;
use kafka_reader::error::ConvertError;
use kafka_reader::message_read::run_read_messages_to_channel;
use kafka_reader::read_messages_request::{Format, ProtoConvertData, StartFrom};
use kafka_reader::read_messages_request::{ReadLimit, ReadMessagesRequest};
use proto::read_messages::message_format::Format as ProtoMessageFormatVariant;
use proto::read_messages::start_from::From as ProtoStartFromVariant;
use proto::read_messages::MessageFormat as ProtoMessageFormat;
use proto::read_messages::StartFrom as ProtoStartFrom;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
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
        let limit = proto_limit_to_limit(request.limit)
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;

        let read_request = ReadMessagesRequest {
            topic: request.topic,
            brokers: request.brokers,
            format,
            start_from,
            limit,
        };
        debug!("Mapped request: {:?}", read_request);

        let cancellation_token = CancellationToken::new();
        let guard = cancellation_token.clone().drop_guard();

        let create_consumer_result =
            run_read_messages_to_channel(read_request, cancellation_token).await;
        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(map_to_response);
                Ok(Response::new(Box::new(map.with_data(guard))))
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

fn proto_limit_to_limit(limit: Option<ProtoReadLimit>) -> Result<ReadLimit, anyhow::Error> {
    let proto_limit = limit
        .and_then(|x| x.limit)
        .unwrap_or(ProtoReadLimitVariant::NoLimit(NoLimit {}));

    let limit = match proto_limit {
        ProtoReadLimitVariant::NoLimit(_) => ReadLimit::NoLimit,
        ProtoReadLimitVariant::MessageCount(c) => ReadLimit::MessageCount(c.count),
        ProtoReadLimitVariant::ToDate(d) => {
            let proto_to_date = d.date.ok_or(anyhow!("To date limit can't be null"))?;
            let date = DateTime::from_timestamp(proto_to_date.seconds, proto_to_date.nanos as u32)
                .ok_or(anyhow!("Can't convert datetime"))?;
            ReadLimit::ToDate(date)
        }
    };

    Ok(limit)
}

fn map_to_response(
    message: Result<Option<KafkaMessage>, ConvertError>,
) -> Result<proto::read_messages::Response, Status> {
    let message = message.map_err(|e| Status::invalid_argument(format!("{:?}", e)))?;
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
