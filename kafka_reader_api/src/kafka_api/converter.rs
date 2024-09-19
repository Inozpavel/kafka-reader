use super::*;

use crate::kafka_api::proto;
use crate::kafka_api::proto::security_protocol::{PlaintextProtocol, Protocol};
use crate::time_util::{DateTimeConvert, ProtoTimestampConvert};
use anyhow::{anyhow, Context};
use kafka_reader::consumer::{KafkaMessage, SecurityProtocol};
use kafka_reader::requests::produce_messages_request::{ProduceMessage, ProduceMessagesRequest};
use kafka_reader::requests::read_messages_request::{
    FilterCondition, FilterKind, Format, MessageTime, ProtobufDecodeWay, ReadLimit,
    ReadMessagesRequest, SingleProtoFile, StartFrom, ValueFilter,
};
use regex::Regex;
use tonic::Status;

pub fn proto_read_request_to_internal_request(
    request: proto::read_messages::Request,
) -> Result<ReadMessagesRequest, anyhow::Error> {
    let key_format =
        proto_format_to_format(request.key_format).context("While converting key_format")?;
    let body_format =
        proto_format_to_format(request.body_format).context("While converting body_format")?;

    let key_value_filter = proto_value_filter_to_filter(request.key_filter)?;
    let body_value_filter = proto_value_filter_to_filter(request.body_filter)?;

    let start_from = proto_start_from_to_from(request.start_from)?;
    let limit = proto_limit_to_limit(request.limit)?;
    let security_protocol = proto_security_protocol_to_protocol(request.security_protocol);

    let result = ReadMessagesRequest {
        topic: request.topic,
        brokers: request.brokers,
        key_format,
        body_format,
        key_filter: key_value_filter,
        body_filter: body_value_filter,
        start_from,
        limit,
        security_protocol,
    };

    Ok(result)
}

pub fn proto_produce_request_to_internal_request(
    request: proto::produce_messages::Request,
) -> Result<ProduceMessagesRequest, anyhow::Error> {
    let key_format =
        proto_format_to_format(request.key_format).context("While converting key_format")?;
    let body_format =
        proto_format_to_format(request.body_format).context("While converting body_format")?;

    let security_protocol = proto_security_protocol_to_protocol(request.security_protocol);
    let messages = request
        .messages
        .into_iter()
        .map(proto_produce_message_to_internal_message)
        .collect();
    let result = ProduceMessagesRequest {
        topic: request.topic,
        brokers: request.brokers,
        key_format,
        body_format,
        security_protocol,
        messages,
    };

    Ok(result)
}

fn proto_produce_message_to_internal_message(model: ProtoProduceMessage) -> ProduceMessage {
    ProduceMessage {
        body: model.body,
        key: model.key,
        headers: model.headers,
        partition: model.partition,
    }
}

fn proto_security_protocol_to_protocol(
    protocol: Option<ProtoSecurityProtocol>,
) -> SecurityProtocol {
    let proto_protocol =
        protocol
            .and_then(|x| x.protocol)
            .unwrap_or(ProtoSecurityProtocolVariant::Plaintext(
                PlaintextProtocol {},
            ));
    match proto_protocol {
        Protocol::Plaintext(_) => SecurityProtocol::Plaintext,
        Protocol::Ssl(_) => SecurityProtocol::Ssl,
    }
}

fn proto_value_filter_to_filter(
    filter: Option<ProtoValueFilter>,
) -> Result<Option<ValueFilter>, anyhow::Error> {
    let Some(value) = filter else {
        return Ok(None);
    };
    let proto_filter_kind = value
        .filter_kind
        .context("FilterKind can't be null")?
        .kind
        .context("FilterKind filter can't be null")?;

    let filter_kind = match proto_filter_kind {
        ProtoFilterKindVariant::StringValue(s) => FilterKind::String(s.value),
        ProtoFilterKindVariant::RegexValue(regex) => {
            let regex = Regex::new(&regex.value).context("While creating regex")?;
            FilterKind::Regex(regex)
        }
    };

    let proto_condition = value
        .condition
        .context("FilterKind can't be null")?
        .condition
        .context("FilterKind filter can't be null")?;

    let filter_condition = match proto_condition {
        ProtoFilterCondition::Contains(_) => FilterCondition::Contains,
        ProtoFilterCondition::NotContains(_) => FilterCondition::NotContains,
    };

    Ok(Some(ValueFilter {
        filter_condition,
        filter_kind,
    }))
}
fn proto_format_to_format(
    message_format: Option<ProtoFormat>,
) -> Result<Option<Format>, anyhow::Error> {
    let Some(proto_format) = message_format else {
        return Ok(None);
    };

    let Some(proto_format_variant) = proto_format.format else {
        return Ok(None);
    };
    let format = match proto_format_variant {
        ProtoFormatVariant::StringFormat(_) => Format::String,
        ProtoFormatVariant::HexFormat(_) => Format::Hex,
        ProtoFormatVariant::Base64Format(_) => Format::Base64,
        ProtoFormatVariant::ProtoFormat(protobuf_data) => {
            match protobuf_data
                .decode_way
                .ok_or_else(|| anyhow!("Protobuf format can't be none"))?
            {
                proto::message_format::proto_format::DecodeWay::RawProtoFile(single_file) => {
                    Format::Protobuf(ProtobufDecodeWay::SingleProtoFile(SingleProtoFile {
                        file: single_file.file,
                        message_type_name: single_file.message_type_name,
                    }))
                }
            }
        }
    };

    Ok(Some(format))
}

fn proto_start_from_to_from(
    message_format: Option<ProtoStartFrom>,
) -> Result<StartFrom, anyhow::Error> {
    let proto_from =
        message_format
            .and_then(|x| x.from)
            .unwrap_or(ProtoStartFromVariant::FromBeginning(
                proto::start_from::FromBeginning {},
            ));

    let from = match proto_from {
        ProtoStartFromVariant::FromBeginning(_) => StartFrom::Beginning,
        ProtoStartFromVariant::FromLatest(_) => StartFrom::Latest,
        ProtoStartFromVariant::FromToday(_) => StartFrom::Time(MessageTime::today()),
        ProtoStartFromVariant::FromTime(time) => {
            let message_time = time
                .day
                .context("From time filter can't be null")?
                .to_date_time();
            StartFrom::Time(message_time.into())
        }
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
        proto::read_limit::Limit::ToTime(d) => {
            let time = d
                .time
                .ok_or_else(|| anyhow!("ToTime limit can't be null"))?
                .to_date_time();
            ReadLimit::ToTime(time)
        }
    };

    Ok(limit)
}

pub fn response_to_proto_response(
    message: Option<KafkaMessage>,
) -> Result<proto::Response, Status> {
    match message {
        None => Ok(proto::Response {
            kafka_message: None,
        }),
        Some(value) => Ok(proto::Response {
            kafka_message: Some(proto::KafkaMessage {
                partition: *value.partition_offset.partition(),
                offset: *value.partition_offset.offset(),
                timestamp: Some(value.timestamp.to_proto_timestamp()),
                key: value.key.unwrap_or_else(|e| Some(format!("{:?}", e))),
                body: value.body.unwrap_or_else(|e| Some(format!("{:?}", e))),
                headers: value.headers.unwrap_or_default(),
            }),
        }),
    }
}
