use crate::kafka_api::converter::response_to_proto_response;
use crate::kafka_api::{
    proto, proto_produce_request_to_internal_request, proto_read_request_to_internal_request,
};
use crate::util::StreamDataExtension;
use kafka_reader::requests::produce_messages_request::produce_messages_to_topic;
use kafka_reader::requests::read_messages_request::run_read_messages_to_channel;
use tokio::select;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;

pub struct KafkaService;

#[tonic::async_trait]
impl proto::KafkaService for KafkaService {
    type ReadMessagesStream =
        Box<dyn Stream<Item = Result<proto::Response, Status>> + Send + Unpin>;

    async fn read_messages(
        &self,
        request: Request<proto::Request>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        let proto_read_request = request.into_inner();
        debug!("New request: {:?}", proto_read_request);

        let internal_request = proto_read_request_to_internal_request(proto_read_request)
            .map_err(|e| Status::invalid_argument(format!("{:?}", e)))?;
        debug!("Mapped request: {:?}", internal_request);

        let cancellation_token = CancellationToken::new();
        let guard = cancellation_token.clone().drop_guard();

        let create_consumer_result =
            run_read_messages_to_channel(internal_request, cancellation_token).await;

        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(response_to_proto_response);
                Ok(Response::new(Box::new(map.with_data(guard))))
            }
            Err(e) => Err(Status::invalid_argument(format!("{:?}", e))),
        }
    }

    async fn produce_messages(
        &self,
        request: Request<proto::produce_messages::Request>,
    ) -> Result<Response<proto::produce_messages::Response>, Status> {
        debug!("New request: {:?}", request);
        let proto_produce_request = request.into_inner();

        let internal_request = proto_produce_request_to_internal_request(proto_produce_request)
            .map_err(|e| Status::invalid_argument(format!("{:?}", e)))?;

        debug!("Mapped request: {:?}", internal_request);
        let cancellation_token = CancellationToken::new();
        let _drop_guard = cancellation_token.clone().drop_guard();

        select! {
            result = produce_messages_to_topic(internal_request, cancellation_token.clone()) =>{
                result.map_err(|e| Status::invalid_argument(format!("{:?}", e)))?
            }
            _ = cancellation_token.cancelled() =>{
                return Err(Status::cancelled("Request was cancelled"));
            }
        }

        Ok(Response::new(proto::produce_messages::Response {
            delivery_results: vec![],
        }))
    }
}
