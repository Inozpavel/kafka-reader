use kafka_reader::consumer::read_messages_to_channel;
use kafka_reader::format::Format;
use kafka_reader::message::KafkaMessage;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Response, Status};

pub mod rpc_service {
    tonic::include_proto!("kafka_reader_api");
}

#[derive(Default)]
struct ReaderService;

#[tonic::async_trait]
impl rpc_service::kafka_reader_server::KafkaReader for ReaderService {
    type ReadMessagesStream =
    Box<dyn Stream<Item=Result<rpc_service::read_messages::Response, Status>> + Send + Unpin>;

    async fn read_messages(
        &self,
        request: Request<rpc_service::read_messages::Request>,
    ) -> Result<Response<Self::ReadMessagesStream>, Status> {
        let request = request.into_inner();
        let create_consumer_result =
            read_messages_to_channel(vec![], request.topic, Format::Hex).await;

        match create_consumer_result {
            Ok(rx) => {
                let map = ReceiverStream::new(rx).map(map_to_response);
                Ok(Response::new(Box::new(map)))
            }
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }
}

fn map_to_response(
    message: Option<KafkaMessage>,
) -> Result<rpc_service::read_messages::Response, Status> {
    match message {
        None => Ok(rpc_service::read_messages::Response {
            kafka_message: None,
        }),
        Some(value) => Ok(rpc_service::read_messages::Response {
            kafka_message: Some(rpc_service::read_messages::KafkaMessage {
                key: value.key,
                body: value.body,
                headers: value.headers.unwrap(),
            }),
        }),
    }
}
