use crate::requests::read_messages_request::{Format, ReadLimit, StartFrom};

#[derive(Debug)]
pub struct ReadMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub format: Format,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
}