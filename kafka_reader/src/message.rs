use crate::message_metadata::PartitionOffset;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

pub struct KafkaMessage {
    pub partition_offset: PartitionOffset,
    pub timestamp: DateTime<Utc>,
    pub key: Option<String>,
    pub body: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}
