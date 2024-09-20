use crate::consumer::PartitionOffset;
use crate::error::ConvertError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug)]
pub enum ReadResult {
    KafkaMessage(KafkaMessage),
    MessagesCounters(MessagesCounters),
    BrokerError(BrokerError),
}

#[derive(Debug)]
pub struct KafkaMessage {
    pub partition_offset: PartitionOffset,
    pub timestamp: DateTime<Utc>,
    pub key: Result<Option<String>, ConvertError>,
    pub body: Result<Option<String>, ConvertError>,
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug)]
pub struct MessagesCounters {
    pub read_message_count: u64,
    pub returned_message_count: u64,
}

#[derive(Debug)]
pub struct BrokerError {
    pub message: String,
}
