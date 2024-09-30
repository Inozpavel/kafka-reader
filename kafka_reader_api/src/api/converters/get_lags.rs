use crate::api::converters::shared::proto_connection_setting_to_internal;
use crate::api::kafka_service::proto::get_topic_lags::{
    get_topic_lags_query_response, GetTopicLagsQuery, GetTopicLagsQueryResponse, GroupLagsDto,
    GroupTopicLagDto, GroupTopicPartitionLagDto,
};
use crate::api::kafka_service::proto::ErrorDto;
use kafka_reader::queries::get_topic_lags::{
    GetTopicLagsQueryInternal, GroupTopicLags, ReadLagsResult,
};
use tonic::Status;

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
