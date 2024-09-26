use crate::error::ApplicationError;
use crate::kafka_api::converter::read_result_to_proto_response;
use crate::kafka_api::proto::get_cluster_metadata::{
    GetClusterMetadataQuery, GetClusterMetadataQueryResponse,
};
use crate::kafka_api::proto::get_topic_lags::{GetTopicLagsQuery, GetTopicLagsQueryResponse};
use crate::kafka_api::proto::get_topic_partitions_with_offsets::{
    GetTopicPartitionsWithOffsetsQuery, GetTopicPartitionsWithOffsetsQueryResponse,
};
use crate::kafka_api::proto::produce_messages::{
    ProduceMessagesCommand, ProduceMessagesCommandResponse,
};
use crate::kafka_api::proto::{ReadMessagesQuery, ReadMessagesQueryResponse};
use crate::kafka_api::{
    kafka_cluster_metadata_to_proto_response, produce_message_result_to_response, proto,
    proto_get_cluster_metadata_to_internal, proto_get_lags_to_internal,
    proto_get_topic_partition_offsets_internal, proto_produce_messages_to_internal,
    proto_read_messages_to_internal, topic_lag_result_to_proto_response,
    topic_partition_offsets_to_proto_response,
};
use crate::util::StreamDataExtension;
use kafka_reader::commands::produce_messages::produce_messages_to_topic;
use kafka_reader::queries::get_cluster_metadata::get_cluster_metadata;
use kafka_reader::queries::get_topic_lags::get_topic_lags;
use kafka_reader::queries::get_topic_partitions_with_offsets::get_topic_partition_offsets;
use kafka_reader::queries::read_messages::run_read_messages_to_channel;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;

#[derive(Debug)]
pub struct KafkaService;

#[tonic::async_trait]
impl proto::KafkaService for KafkaService {
    type ReadMessagesStream =
        Box<dyn Stream<Item = Result<ReadMessagesQueryResponse, Status>> + Send + Unpin>;

    #[tracing::instrument(skip_all)]
    async fn read_messages(
        &self,
        request: Request<ReadMessagesQuery>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        let proto_read_request = request.into_inner();
        debug!("New request: {:?}", proto_read_request);

        let query = proto_read_messages_to_internal(proto_read_request)
            .map_err(ApplicationError::InvalidArgument)?;

        debug!("Mapped request: {:?}", query);

        let cancellation_token = CancellationToken::new();
        let guard = cancellation_token.clone().drop_guard();

        let rx = run_read_messages_to_channel(query, cancellation_token)
            .await
            .map_err(ApplicationError::InvalidArgument)?;

        let stream = ReceiverStream::new(rx)
            .map(read_result_to_proto_response)
            .map(Ok)
            .with_data(guard);

        Ok(Response::new(Box::new(stream)))
    }

    type ProduceMessagesStream =
        Box<dyn Stream<Item = Result<ProduceMessagesCommandResponse, Status>> + Send + Unpin>;

    #[tracing::instrument(skip_all)]
    async fn produce_messages(
        &self,
        request: Request<ProduceMessagesCommand>,
    ) -> Result<Response<Self::ProduceMessagesStream>, Status> {
        debug!("New request: {:?}", request);
        let proto_produce_request = request.into_inner();

        let command = proto_produce_messages_to_internal(proto_produce_request)
            .map_err(ApplicationError::InvalidArgument)?;

        debug!("Mapped request: {:?}", command);
        let cancellation_token = CancellationToken::new();
        let guard = cancellation_token.clone().drop_guard();

        let rx = produce_messages_to_topic(command, cancellation_token.clone())
            .await
            .map_err(ApplicationError::InvalidArgument)?;

        let stream = ReceiverStream::new(rx)
            .map(produce_message_result_to_response)
            .map(Ok)
            .with_data(guard);

        Ok(Response::new(Box::new(stream)))
    }

    #[tracing::instrument(skip_all)]
    async fn get_cluster_metadata(
        &self,
        request: Request<GetClusterMetadataQuery>,
    ) -> Result<Response<GetClusterMetadataQueryResponse>, Status> {
        let proto_request = request.into_inner();

        let query = proto_get_cluster_metadata_to_internal(proto_request)
            .map_err(ApplicationError::InvalidArgument)?;

        let response = get_cluster_metadata(query)
            .await
            .map_err(ApplicationError::InvalidArgument)?;

        let proto_response = kafka_cluster_metadata_to_proto_response(response);

        Ok(Response::new(proto_response))
    }

    #[tracing::instrument(skip_all)]
    async fn get_topic_partitions_with_offsets(
        &self,
        request: Request<GetTopicPartitionsWithOffsetsQuery>,
    ) -> Result<Response<GetTopicPartitionsWithOffsetsQueryResponse>, Status> {
        let proto_request = request.into_inner();

        let query = proto_get_topic_partition_offsets_internal(proto_request)
            .map_err(ApplicationError::InvalidArgument)?;

        let response = get_topic_partition_offsets(query)
            .await
            .map_err(ApplicationError::InvalidArgument)?;

        let proto_response = topic_partition_offsets_to_proto_response(response);
        Ok(Response::new(proto_response))
    }

    type GetTopicLagsStream =
        Box<dyn Stream<Item = Result<GetTopicLagsQueryResponse, Status>> + Send + Unpin>;

    #[tracing::instrument(skip_all)]
    async fn get_topic_lags(
        &self,
        request: Request<GetTopicLagsQuery>,
    ) -> Result<Response<Self::GetTopicLagsStream>, Status> {
        let proto_request = request.into_inner();
        let query =
            proto_get_lags_to_internal(proto_request).map_err(ApplicationError::InvalidArgument)?;

        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        let rx = get_topic_lags(query, token)
            .await
            .map_err(ApplicationError::InvalidArgument)?;

        let stream = ReceiverStream::new(rx)
            .map(topic_lag_result_to_proto_response)
            .with_data(guard);
        Ok(Response::new(Box::new(stream)))
    }
}
