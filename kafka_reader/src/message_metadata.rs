use crate::message::KafkaMessage;
use getset::Getters;
#[derive(Debug, Getters, Copy, Clone)]
#[getset(get = "pub")]
pub struct PartitionOffset {
    partition: i32,
    offset: i64,
}

impl PartitionOffset {
    pub fn new(partition: i32, offset: i64) -> PartitionOffset {
        Self { offset, partition }
    }
}
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct MessageMetadata {
    partition_offset: PartitionOffset,
    is_null: bool,
}

impl MessageMetadata {
    pub fn partition(&self) -> i32 {
        self.partition_offset.partition
    }

    pub fn offset(&self) -> i64 {
        self.partition_offset.offset
    }
}

impl From<&Option<KafkaMessage>> for MessageMetadata {
    fn from(value: &Option<KafkaMessage>) -> Self {
        let partition = value
            .as_ref()
            .map(|x| x.partition_offset.partition)
            .unwrap_or_default();
        let offset = value
            .as_ref()
            .map(|x| x.partition_offset.offset)
            .unwrap_or_default();
        let is_null = value.is_none();

        let partition_offset = PartitionOffset::new(partition, offset);

        Self {
            is_null,
            partition_offset,
        }
    }
}