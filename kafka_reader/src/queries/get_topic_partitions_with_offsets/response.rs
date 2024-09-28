use std::collections::HashMap;

pub struct GetTopicPartitionsWithOffsetsQueryResponseInternal {
    pub partitions_offsets: HashMap<i32, MinMaxOffset>,
}

pub struct MinMaxOffset {
    pub min_offset: i64,
    pub max_offset: i64,
}

impl MinMaxOffset {
    pub fn messages_count(&self) -> i64 {
        self.max_offset.max(0) - self.min_offset.max(0)
    }
}
