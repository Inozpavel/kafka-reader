use crate::api::converters::shared::{
    proto_connection_setting_to_internal, proto_format_to_format,
};
use crate::api::kafka_service::proto::read_limit_dto::NoLimitDto;
use crate::api::kafka_service::proto::start_from_dto::FromBeginningDto;
use crate::api::kafka_service::proto::value_filter_dto::condition;
use crate::api::kafka_service::proto::{
    filter_kind_dto, read_limit_dto, read_messages_query_response, start_from_dto, ErrorDto,
    KafkaMessageDto, MessagesCountersDto, ReadLimitDto, ReadMessagesQuery,
    ReadMessagesQueryResponse, StartFromDto, ValueFilterDto,
};
use crate::time_util::{DateTimeConvert, ProtoTimestampConvert};
use anyhow::{anyhow, Context};
use kafka_reader::queries::read_messages::{
    FilterCondition, FilterKind, MessageTime, ReadLimit, ReadMessagesQueryInternal,
    ReadMessagesQueryInternalResponse, StartFrom, ValueFilter,
};
use regex::Regex;

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
    let limit = proto_read_limit_to_limit(model.limit)?;
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

fn proto_read_limit_to_limit(model: Option<ReadLimitDto>) -> Result<ReadLimit, anyhow::Error> {
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
