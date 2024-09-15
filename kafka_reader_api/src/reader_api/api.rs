use crate::reader_api::converter::response_to_proto_response;
use crate::reader_api::{proto, proto_request_to_read_request};
use crate::util::StreamDataExtension;
use kafka_reader::requests::read_messages_request::run_read_messages_to_channel;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;

pub struct ReaderService;

#[tonic::async_trait]
impl proto::KafkaReader for ReaderService {
    type ReadMessagesStream =
        Box<dyn Stream<Item = Result<proto::Response, Status>> + Send + Unpin>;

    async fn read_messages(
        &self,
        request: Request<proto::Request>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        debug!("New request: {:?}", request);

        let read_request = proto_request_to_read_request(request.into_inner())
            .map_err(|e| Status::invalid_argument(format!("{:?}", e)))?;
        debug!("Mapped request: {:?}", read_request);

        let cancellation_token = CancellationToken::new();
        let guard = cancellation_token.clone().drop_guard();

        let create_consumer_result =
            run_read_messages_to_channel(read_request, cancellation_token).await;

        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(response_to_proto_response);
                Ok(Response::new(Box::new(map.with_data(guard))))
            }
            Err(e) => Err(Status::invalid_argument(format!("{:?}", e))),
        }
    }
}
