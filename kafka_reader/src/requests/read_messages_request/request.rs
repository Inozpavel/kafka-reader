use crate::requests::read_messages_request::{Format, ReadLimit, StartFrom};

#[derive(Debug)]
pub struct ReadMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key_format: Format,
    pub body_format: Format,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
}
