mod api;
mod converter;

pub use api::*;
pub use converter::*;

use proto::filter_kind::Kind as ProtoFilterKindVariant;
use proto::message_format::Format as ProtoFormatVariant;
use proto::read_limit::Limit as ProtoReadLimitVariant;
use proto::start_from::From as ProtoStartFromVariant;
use proto::value_filter::condition::Condition as ProtoFilterCondition;
use proto::MessageFormat as ProtoFormat;
use proto::ReadLimit as ProtoReadLimit;
use proto::StartFrom as ProtoStartFrom;
use proto::ValueFilter as ProtoValueFilter;
use proto::SecurityProtocol as ProtoSecurityProtocol;
use proto::security_protocol::Protocol as ProtoSecurityProtocolVariant;

pub mod proto {
    pub use kafka_reader_server::*;
    pub use read_messages::*;
    tonic::include_proto!("kafka_reader_api");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("reader_service_descriptor");
}
