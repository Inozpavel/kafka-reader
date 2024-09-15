use crate::consumer::PartitionOffset;
use crate::error::ConvertError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

pub struct KafkaMessage {
    pub partition_offset: PartitionOffset,
    pub timestamp: DateTime<Utc>,
    pub key: Result<Option<String>, ConvertError>,
    pub body: Result<Option<String>, ConvertError>,
    pub headers: Option<HashMap<String, String>>,
}
