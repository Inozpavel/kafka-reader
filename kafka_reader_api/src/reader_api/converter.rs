use super::*;

use crate::reader_api::proto;
use anyhow::anyhow;
use chrono::DateTime;
use kafka_reader::consumer::KafkaMessage;
use kafka_reader::error::ConvertError;
use kafka_reader::requests::read_messages_request::{
    Format, ProtoConvertData, ReadLimit, ReadMessagesRequest, StartFrom,
};
use tonic::Status;

pub fn proto_request_to_read_request(
    request: proto::Request,
) -> Result<ReadMessagesRequest, anyhow::Error> {
    let format = proto_format_to_format(request.format)?;
    let start_from = proto_start_from_to_from(request.start_from)?;
    let limit = proto_limit_to_limit(request.limit)?;

    let result = ReadMessagesRequest {
        topic: request.topic,
        brokers: request.brokers,
        format,
        start_from,
        limit,
    };

    Ok(result)
}
fn proto_format_to_format(message_format: Option<ProtoFormat>) -> Result<Format, anyhow::Error> {
    let proto_format =
        message_format
            .and_then(|x| x.format)
            .unwrap_or(ProtoFormatVariant::StringFormat(
                proto::message_format::StringFormat {},
            ));

    let format = match proto_format {
        ProtoFormatVariant::Ignore(_) => Format::Ignore,
        ProtoFormatVariant::StringFormat(_) => Format::String,
        ProtoFormatVariant::HexFormat(_) => Format::Hex,
        ProtoFormatVariant::ProtoFormat(protobuf_data) => {
            match protobuf_data
                .decode_way
                .ok_or(anyhow!("Protobuf format can't be none"))?
            {
                proto::message_format::proto_format::DecodeWay::RawProtoFile(file) => {
                    Format::Protobuf(ProtoConvertData::RawProto(file.proto))
                }
            }
        }
    };

    Ok(format)
}

fn proto_start_from_to_from(
    message_format: Option<ProtoStartFrom>,
) -> Result<StartFrom, anyhow::Error> {
    let proto_from =
        message_format
            .and_then(|x| x.from)
            .unwrap_or(ProtoStartFromVariant::Beginning(
                proto::start_from::FromBeginning {},
            ));

    let from = match proto_from {
        ProtoStartFromVariant::Beginning(_) => StartFrom::Beginning,
        ProtoStartFromVariant::Latest(_) => StartFrom::Latest,
        ProtoStartFromVariant::Today(_) => StartFrom::Day(chrono::Utc::now().date_naive()),
    };

    Ok(from)
}

fn proto_limit_to_limit(limit: Option<ProtoReadLimit>) -> Result<ReadLimit, anyhow::Error> {
    let proto_limit = limit
        .and_then(|x| x.limit)
        .unwrap_or(ProtoReadLimitVariant::NoLimit(
            proto::read_limit::NoLimit {},
        ));

    let limit = match proto_limit {
        ProtoReadLimitVariant::NoLimit(_) => ReadLimit::NoLimit,
        ProtoReadLimitVariant::MessageCount(c) => ReadLimit::MessageCount(c.count),
        proto::read_limit::Limit::ToDate(d) => {
            let proto_date = d.date.ok_or(anyhow!("To date limit can't be null"))?;
            let date = DateTime::from_timestamp(proto_date.seconds, proto_date.nanos as u32)
                .ok_or(anyhow!("Can't convert datetime"))?;
            ReadLimit::ToDate(date.date_naive())
        }
    };

    Ok(limit)
}

pub fn response_to_proto_response(
    message: Result<Option<KafkaMessage>, ConvertError>,
) -> Result<proto::Response, Status> {
    let message = message.map_err(|e| Status::invalid_argument(format!("{:?}", e)))?;
    match message {
        None => Ok(proto::Response {
            kafka_message: None,
        }),
        Some(value) => Ok(proto::Response {
            kafka_message: Some(proto::KafkaMessage {
                key: value.key.unwrap_or_else(|e| Some(format!("{:?}", e))),
                body: value.body.unwrap_or_else(|e| Some(format!("{:?}", e))),
                headers: value.headers.unwrap_or_default(),
            }),
        }),
    }
}
