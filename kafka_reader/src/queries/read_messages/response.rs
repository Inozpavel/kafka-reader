use crate::consumer::PartitionOffset;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug)]
pub enum ReadMessagesQueryInternalResponse {
    KafkaMessage(KafkaMessage),
    MessagesCounters(MessagesCounters),
    Error(ConsumeError),
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
pub struct ConvertError {
    pub error: anyhow::Error,
    pub partition_offset: PartitionOffset,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct MessagesCounters {
    pub read_message_count: u64,
    pub returned_message_count: u64,
}

#[derive(Debug)]
pub struct ConsumeError {
    pub error: anyhow::Error,
}
