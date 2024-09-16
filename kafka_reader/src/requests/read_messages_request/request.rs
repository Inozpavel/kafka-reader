use crate::consumer::SecurityProtocol;
use crate::requests::read_messages_request::{Format, ReadLimit, StartFrom, ValueFilter};

#[derive(Debug)]
pub struct ReadMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key_format: Format,
    pub body_format: Format,
    pub key_value_filter: Option<ValueFilter>,
    pub body_value_filter: Option<ValueFilter>,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
    pub security_protocol: SecurityProtocol,
}
