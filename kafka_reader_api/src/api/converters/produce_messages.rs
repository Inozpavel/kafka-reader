use crate::api::converters::shared::{
    proto_connection_setting_to_internal, proto_format_to_format,
};
use crate::api::kafka_service::proto::produce_messages::{
    produce_messages_command_response, DeliveryResultDto, ProduceMessageDto,
    ProduceMessagesCommand, ProduceMessagesCommandResponse,
};
use crate::api::kafka_service::proto::ErrorDto;
use anyhow::Context;
use kafka_reader::commands::produce_messages::{
    ProduceMessage, ProduceMessagesCommandInternal, ProduceMessagesCommandInternalResponse,
};
use rayon::prelude::*;

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

fn proto_produce_message_to_internal(model: ProduceMessageDto) -> ProduceMessage {
    ProduceMessage {
        body: model.body,
        key: model.key,
        headers: model.headers,
        partition: model.partition,
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
