mod api;
mod converter;

pub use api::*;
pub use converter::*;

pub mod proto {
    pub use kafka_service_server::*;
    pub use read_messages::*;
    tonic::include_proto!("kafka_reader_api");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("reader_service_descriptor");
}
