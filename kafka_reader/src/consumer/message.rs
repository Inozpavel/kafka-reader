use chrono::{DateTime, Utc};
use std::collections::HashMap;
use crate::consumer::PartitionOffset;

pub struct KafkaMessage {
    pub partition_offset: PartitionOffset,
    pub timestamp: DateTime<Utc>,
    pub key: Option<String>,
    pub body: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}
