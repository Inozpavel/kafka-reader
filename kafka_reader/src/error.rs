use crate::consumer::PartitionOffset;

#[derive(Debug)]
pub struct ConvertError {
    pub error: anyhow::Error,
    pub partition_offset: PartitionOffset,
}
