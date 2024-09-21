use crate::kafka_api::proto;
use crate::kafka_api::proto::get_cluster_metadata::GetClusterMetadataQuery;
use crate::kafka_api::proto::message_format_dto::proto_format_dto;
use crate::kafka_api::proto::produce_messages::{ProduceMessageDto, ProduceMessagesCommand};
use crate::kafka_api::proto::read_limit_dto::NoLimitDto;
use crate::kafka_api::proto::security_protocol_dto::PlaintextProtocolDto;
use crate::kafka_api::proto::start_from_dto::FromBeginningDto;
use crate::kafka_api::proto::value_filter_dto::condition;
use crate::kafka_api::proto::{
    filter_kind_dto, message_format_dto, read_limit_dto, read_messages_query_response,
    security_protocol_dto, start_from_dto, MessageFormatDto, ReadLimitDto, ReadMessagesQuery,
    ReadMessagesQueryResponse, SecurityProtocolDto, StartFromDto, ValueFilterDto,
};
use crate::time_util::{DateTimeConvert, ProtoTimestampConvert};
use anyhow::{anyhow, Context};
use kafka_reader::consumer::{ReadResult, SecurityProtocol};
use kafka_reader::requests::get_cluster_metadata::GetClusterMetadataQueryInternal;
use kafka_reader::requests::produce_messages::{ProduceMessage, ProduceMessagesCommandInternal};
use kafka_reader::requests::read_messages::{
    FilterCondition, FilterKind, Format, MessageTime, ProtobufDecodeWay, ReadLimit,
    ReadMessagesQueryInternal, SingleProtoFile, StartFrom, ValueFilter,
};
use regex::Regex;
use tonic::Status;

pub fn proto_read_messages_to_internal(
    model: ReadMessagesQuery,
) -> Result<ReadMessagesQueryInternal, anyhow::Error> {
    let key_format =
        proto_format_to_format(model.key_format).context("While converting key_format")?;
    let body_format =
        proto_format_to_format(model.body_format).context("While converting body_format")?;

    let key_value_filter = proto_value_filter_to_filter(model.key_filter)?;
    let body_value_filter = proto_value_filter_to_filter(model.body_filter)?;

    let start_from = proto_start_from_to_from(model.start_from)?;
    let limit = proto_limit_to_limit(model.limit)?;
    let security_protocol = proto_security_protocol_to_protocol(model.security_protocol);

    let result = ReadMessagesQueryInternal {
        topic: model.topic,
        brokers: model.brokers,
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

pub fn proto_produce_messages_to_internal(
    model: ProduceMessagesCommand,
) -> Result<ProduceMessagesCommandInternal, anyhow::Error> {
    let key_format =
        proto_format_to_format(model.key_format).context("While converting key_format")?;
    let body_format =
        proto_format_to_format(model.body_format).context("While converting body_format")?;

    let security_protocol = proto_security_protocol_to_protocol(model.security_protocol);
    let messages = model
        .messages
        .into_iter()
        .map(proto_produce_message_to_internal)
        .collect();
    let result = ProduceMessagesCommandInternal {
        topic: model.topic,
        brokers: model.brokers,
        key_format,
        body_format,
        security_protocol,
        messages,
    };

    Ok(result)
}

pub fn proto_get_cluster_metadata_to_internal(
    model: GetClusterMetadataQuery,
) -> GetClusterMetadataQueryInternal {
    let security_protocol = proto_security_protocol_to_protocol(model.security_protocol);
    GetClusterMetadataQueryInternal {
        brokers: model.brokers,
        security_protocol,
    }
}

fn proto_produce_message_to_internal(model: ProduceMessageDto) -> ProduceMessage {
    ProduceMessage {
        body: model.body,
        key: model.key,
        headers: model.headers,
        partition: model.partition,
    }
}

fn proto_security_protocol_to_protocol(model: Option<SecurityProtocolDto>) -> SecurityProtocol {
    let proto_protocol =
        model
            .and_then(|x| x.protocol)
            .unwrap_or(security_protocol_dto::Protocol::Plaintext(
                PlaintextProtocolDto {},
            ));
    match proto_protocol {
        security_protocol_dto::Protocol::Plaintext(_) => SecurityProtocol::Plaintext,
        security_protocol_dto::Protocol::Ssl(_) => SecurityProtocol::Ssl,
    }
}

fn proto_value_filter_to_filter(
    model: Option<ValueFilterDto>,
) -> Result<Option<ValueFilter>, anyhow::Error> {
    let Some(value) = model else {
        return Ok(None);
    };
    let proto_filter_kind = value
        .filter_kind
        .context("FilterKind can't be null")?
        .kind
        .context("FilterKind filter can't be null")?;

    let filter_kind = match proto_filter_kind {
        filter_kind_dto::Kind::StringValue(s) => FilterKind::String(s.value),
        filter_kind_dto::Kind::RegexValue(regex) => {
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
        condition::Condition::Contains(_) => FilterCondition::Contains,
        condition::Condition::NotContains(_) => FilterCondition::NotContains,
    };

    Ok(Some(ValueFilter {
        filter_condition,
        filter_kind,
    }))
}
fn proto_format_to_format(
    model: Option<MessageFormatDto>,
) -> Result<Option<Format>, anyhow::Error> {
    let Some(proto_format) = model else {
        return Ok(None);
    };

    let Some(proto_format_variant) = proto_format.format else {
        return Ok(None);
    };
    let format = match proto_format_variant {
        message_format_dto::Format::StringFormat(_) => Format::String,
        message_format_dto::Format::HexFormat(_) => Format::Hex,
        message_format_dto::Format::Base64Format(_) => Format::Base64,
        message_format_dto::Format::ProtoFormat(protobuf_data) => {
            match protobuf_data
                .decode_way
                .ok_or_else(|| anyhow!("Protobuf format can't be none"))?
            {
                proto_format_dto::DecodeWay::RawProtoFile(single_file) => {
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

fn proto_start_from_to_from(model: Option<StartFromDto>) -> Result<StartFrom, anyhow::Error> {
    let proto_from = model
        .and_then(|x| x.from)
        .unwrap_or(start_from_dto::From::FromBeginning(FromBeginningDto {}));

    let from = match proto_from {
        start_from_dto::From::FromBeginning(_) => StartFrom::Beginning,
        start_from_dto::From::FromLatest(_) => StartFrom::Latest,
        start_from_dto::From::FromToday(_) => StartFrom::Time(MessageTime::today()),
        start_from_dto::From::FromTime(time) => {
            let message_time = time
                .day
                .context("From time filter can't be null")?
                .to_date_time();
            StartFrom::Time(message_time.into())
        }
    };

    Ok(from)
}

fn proto_limit_to_limit(model: Option<ReadLimitDto>) -> Result<ReadLimit, anyhow::Error> {
    let proto_limit = model
        .and_then(|x| x.limit)
        .unwrap_or(read_limit_dto::Limit::NoLimit(NoLimitDto {}));
    let limit = match proto_limit {
        read_limit_dto::Limit::NoLimit(_) => ReadLimit::NoLimit,
        read_limit_dto::Limit::MessageCount(c) => ReadLimit::MessageCount(c.count),
        read_limit_dto::Limit::ToTime(d) => {
            let time = d
                .time
                .ok_or_else(|| anyhow!("ToTime limit can't be null"))?
                .to_date_time();
            ReadLimit::ToTime(time)
        }
    };

    Ok(limit)
}

pub fn read_result_to_proto_response(
    model: ReadResult,
) -> Result<ReadMessagesQueryResponse, Status> {
    let variant = match model {
        ReadResult::KafkaMessage(kafka_message) => {
            read_messages_query_response::Response::KafkaMessage(proto::KafkaMessageDto {
                partition: *kafka_message.partition_offset.partition(),
                offset: *kafka_message.partition_offset.offset(),
                timestamp: Some(kafka_message.timestamp.to_proto_timestamp()),
                key: kafka_message
                    .key
                    .unwrap_or_else(|e| Some(format!("{:?}", e))),
                body: kafka_message
                    .body
                    .unwrap_or_else(|e| Some(format!("{:?}", e))),
                headers: kafka_message.headers.unwrap_or_default(),
            })
        }
        ReadResult::MessagesCounters(message_counters) => {
            read_messages_query_response::Response::Counters(proto::MessagesCountersDto {
                read_count: message_counters.read_message_count,
                returned_count: message_counters.returned_message_count,
            })
        }
        ReadResult::BrokerError(broker_error) => {
            read_messages_query_response::Response::BrokerError(proto::BrokerErrorDto {
                message: format!("{:?}", broker_error.message),
            })
        }
    };
    Ok(ReadMessagesQueryResponse {
        response: Some(variant),
    })
}

// pub fn kafka_cluster_metadata_to_proto_response(metadata: KafkaClusterMetadata) -> {
//
// }
