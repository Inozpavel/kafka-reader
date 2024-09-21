pub struct GetTopicPartitionsWithOffsetsQueryResponseInternal {
    pub partitions: Vec<TopicPartitionWithOffsetsInternal>,
}

pub struct TopicPartitionWithOffsetsInternal {
    pub id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
}

impl TopicPartitionWithOffsetsInternal {
    pub fn messages_count(&self) -> i64 {
        self.max_offset.max(0) - self.min_offset.max(0)
    }
}
