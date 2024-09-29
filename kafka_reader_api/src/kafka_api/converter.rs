use crate::kafka_api::proto::get_cluster_metadata::{
    GetClusterMetadataQuery, GetClusterMetadataQueryResponse, KafkaBrokerMetadataDto,
    KafkaTopicMetadataDto,
};
use crate::kafka_api::proto::get_topic_lags::{
    get_topic_lags_query_response, GetTopicLagsQuery, GetTopicLagsQueryResponse, GroupLagsDto,
    GroupTopicLagDto, GroupTopicPartitionLagDto,
};
use crate::kafka_api::proto::get_topic_partitions_with_offsets::{
    GetTopicPartitionsWithOffsetsQuery, GetTopicPartitionsWithOffsetsQueryResponse,
    PartitionDataWatermarksDto,
};
use crate::kafka_api::proto::message_format_dto::proto_format_dto;
use crate::kafka_api::proto::message_format_dto::proto_format_dto::bytes_or_base64_data_dto::Data;
use crate::kafka_api::proto::message_format_dto::proto_format_dto::BytesOrBase64DataDto;
use crate::kafka_api::proto::produce_messages::{
    produce_messages_command_response, DeliveryResultDto, ProduceMessageDto,
    ProduceMessagesCommand, ProduceMessagesCommandResponse,
};
use crate::kafka_api::proto::read_limit_dto::NoLimitDto;
use crate::kafka_api::proto::security_protocol_dto::PlaintextProtocolDto;
use crate::kafka_api::proto::start_from_dto::FromBeginningDto;
use crate::kafka_api::proto::value_filter_dto::condition;
use crate::kafka_api::proto::{
    filter_kind_dto, message_format_dto, read_limit_dto, read_messages_query_response,
    security_protocol_dto, start_from_dto, ConnectionSettingsDto, ErrorDto, KafkaMessageDto,
    MessageFormatDto, MessagesCountersDto, ReadLimitDto, ReadMessagesQuery,
    ReadMessagesQueryResponse, SecurityProtocolDto, StartFromDto, ValueFilterDto,
};
use crate::time_util::{DateTimeConvert, ProtoTimestampConvert};
use anyhow::{anyhow, bail, Context};
use base64::prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD, BASE64_URL_SAFE};
use base64::Engine;
use kafka_reader::commands::produce_messages::{
    ProduceMessage, ProduceMessagesCommandInternal, ProduceMessagesCommandInternalResponse,
};
use kafka_reader::connection_settings::ConnectionSettings;
use kafka_reader::consumer::SecurityProtocol;
use kafka_reader::queries::get_cluster_metadata::{
    GetClusterMetadataQueryInternal, GetClusterMetadataQueryInternalResponse,
};
use kafka_reader::queries::get_topic_lags::{
    GetTopicLagsQueryInternal, GroupTopicLags, ReadLagsResult,
};
use kafka_reader::queries::get_topic_partitions_with_offsets::{
    GetTopicPartitionsWithOffsetsQueryInternal, GetTopicPartitionsWithOffsetsQueryResponseInternal,
};
use kafka_reader::queries::read_messages::{
    FilterCondition, FilterKind, Format, MessageTime, ProtoTarArchive, ProtobufDecodeWay,
    ReadLimit, ReadMessagesQueryInternal, ReadMessagesQueryInternalResponse, SingleProtoFile,
    StartFrom, ValueFilter,
};
use rayon::prelude::*;
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
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;

    let result = ReadMessagesQueryInternal {
        topic: model.topic,
        connection_settings,
        key_format,
        body_format,
        key_filter: key_value_filter,
        body_filter: body_value_filter,
        start_from,
        limit,
    };

    Ok(result)
}

fn proto_connection_setting_to_internal(
    model: Option<ConnectionSettingsDto>,
) -> Result<ConnectionSettings, anyhow::Error> {
    let Some(model) = model else {
        bail!("Connection settings can't be null")
    };
    let security_protocol = proto_security_protocol_to_protocol(model.security_protocol);
    Ok(ConnectionSettings {
        brokers: model.brokers,
        security_protocol,
    })
}

pub fn proto_get_lags_to_internal(
    model: GetTopicLagsQuery,
) -> Result<GetTopicLagsQueryInternal, anyhow::Error> {
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;

    Ok(GetTopicLagsQueryInternal {
        topic: model.topic,
        connection_settings,
        group_search: model.group_search,
    })
}

pub fn proto_produce_messages_to_internal(
    model: ProduceMessagesCommand,
) -> Result<ProduceMessagesCommandInternal, anyhow::Error> {
    let key_format =
        proto_format_to_format(model.key_format).context("While converting key_format")?;
    let body_format =
        proto_format_to_format(model.body_format).context("While converting body_format")?;

    let messages = model
        .messages
        .into_par_iter()
        .map(proto_produce_message_to_internal)
        .collect();

    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;
    let result = ProduceMessagesCommandInternal {
        connection_settings,
        topic: model.topic,
        key_format,
        body_format,
        messages,
    };

    Ok(result)
}

pub fn proto_get_cluster_metadata_to_internal(
    model: GetClusterMetadataQuery,
) -> Result<GetClusterMetadataQueryInternal, anyhow::Error> {
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;

    Ok(GetClusterMetadataQueryInternal {
        connection_settings,
    })
}

pub fn proto_get_topic_partition_offsets_internal(
    model: GetTopicPartitionsWithOffsetsQuery,
) -> Result<GetTopicPartitionsWithOffsetsQueryInternal, anyhow::Error> {
    let connection_settings = proto_connection_setting_to_internal(model.connection_settings)?;
    Ok(GetTopicPartitionsWithOffsetsQueryInternal {
        connection_settings,
        topic: model.topic,
    })
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
    let format =
        match proto_format_variant {
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
                    proto_format_dto::DecodeWay::TarArchive(tar_archive) => {
                        Format::Protobuf(ProtobufDecodeWay::TarArchive(ProtoTarArchive {
                            target_file_path: tar_archive.target_file_path,
                            archive_bytes: match tar_archive.data.and_then(|x| x.data) {
                                None => bail!("Archive data can't be null"),
                                Some(data) => match data {
                                    Data::DataBytes(bytes) => bytes,
                                    Data::DataBase64(base64) => BASE64_STANDARD
                                        .decode(base64)
                                        .context("While decoding base 64 archive bytes")?,
                                },
                            },
                            message_type_name: tar_archive.message_type_name,
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
    model: ReadMessagesQueryInternalResponse,
) -> ReadMessagesQueryResponse {
    let variant = match model {
        ReadMessagesQueryInternalResponse::KafkaMessage(kafka_message) => {
            read_messages_query_response::Response::KafkaMessage(KafkaMessageDto {
                partition: *kafka_message.partition_offset.partition(),
                offset: kafka_message.partition_offset.offset().unwrap_or(-1),
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
        ReadMessagesQueryInternalResponse::MessagesCounters(message_counters) => {
            read_messages_query_response::Response::Counters(MessagesCountersDto {
                read_count: message_counters.read_message_count,
                returned_count: message_counters.returned_message_count,
            })
        }
        ReadMessagesQueryInternalResponse::Error(error) => {
            read_messages_query_response::Response::Error(ErrorDto {
                message: format!("{:?}", error.error),
            })
        }
    };
    ReadMessagesQueryResponse {
        response: Some(variant),
    }
}

pub fn topic_lag_result_to_proto_response(
    model: ReadLagsResult,
) -> Result<GetTopicLagsQueryResponse, Status> {
    let variant = match model {
        ReadLagsResult::GroupTopicLag(lags) => {
            let response = group_lags_to_response(lags);
            get_topic_lags_query_response::Response::Lags(response)
        }
        ReadLagsResult::BrokerError(e) => {
            get_topic_lags_query_response::Response::Error(ErrorDto {
                message: format!("{:?}", e),
            })
        }
    };
    Ok(GetTopicLagsQueryResponse {
        response: Some(variant),
    })
}

fn group_lags_to_response(model: GroupTopicLags) -> GroupLagsDto {
    let partition_lag_dto = model
        .lags
        .into_iter()
        .map(|x| GroupTopicPartitionLagDto {
            lag: x.lag,
            partition: *x.partition_offset.partition(),
        })
        .collect();

    let dto = GroupTopicLagDto {
        group_id: model.group_info.id,
        group_state: model.group_info.state,
        topic: model.topic.to_string(),
        partition_lag_dto,
    };
    GroupLagsDto {
        group_topic_lags: vec![dto],
    }
}

pub fn produce_message_result_to_response(
    model: ProduceMessagesCommandInternalResponse,
) -> ProduceMessagesCommandResponse {
    let variant = match model {
        ProduceMessagesCommandInternalResponse::Error(e) => {
            produce_messages_command_response::Response::Error(ErrorDto {
                message: format!("{:?}", e),
            })
        }
        ProduceMessagesCommandInternalResponse::ProducedMessageInfo(info) => {
            produce_messages_command_response::Response::DeliveryResult(DeliveryResultDto {
                partition: *info.partition(),
                offset: info.offset().unwrap_or(-1),
            })
        }
    };

    ProduceMessagesCommandResponse {
        response: Some(variant),
    }
}
pub fn kafka_cluster_metadata_to_proto_response(
    model: GetClusterMetadataQueryInternalResponse,
) -> GetClusterMetadataQueryResponse {
    let brokers = model
        .brokers
        .into_par_iter()
        .map(|x| KafkaBrokerMetadataDto {
            port: x.port as u32,
            host: x.host,
        })
        .collect();

    let topics = model
        .topics
        .into_par_iter()
        .map(|x| KafkaTopicMetadataDto {
            topic_name: x.topic_name,
            partitions_count: x.partitions_count as u32,
        })
        .collect();

    GetClusterMetadataQueryResponse { brokers, topics }
}

pub fn topic_partition_offsets_to_proto_response(
    model: GetTopicPartitionsWithOffsetsQueryResponseInternal,
) -> GetTopicPartitionsWithOffsetsQueryResponse {
    let partitions = model
        .partitions_offsets
        .into_iter()
        .map(|(partition, offsets)| PartitionDataWatermarksDto {
            id: partition,
            min_offset: offsets.min_offset,
            max_offset: offsets.max_offset,
            messages_count: offsets.messages_count(),
        })
        .collect();

    GetTopicPartitionsWithOffsetsQueryResponse { partitions }
}
