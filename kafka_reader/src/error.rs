use crate::message_metadata::PartitionOffset;

#[derive(Debug)]
pub enum ConsumeError {
    RdKafkaError(anyhow::Error),
    ConvertError(ConvertError),
}

#[derive(Debug)]
pub struct ConvertError {
    pub error: anyhow::Error,
    pub partition_offset: PartitionOffset,
}
