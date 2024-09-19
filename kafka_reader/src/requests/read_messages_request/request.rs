use crate::consumer::SecurityProtocol;
use crate::requests::read_messages_request::{Format, ReadLimit, StartFrom, ValueFilter};

#[derive(Debug)]
pub struct ReadMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key_format: Option<Format>,
    pub body_format: Option<Format>,
    pub key_filter: Option<ValueFilter>,
    pub body_filter: Option<ValueFilter>,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
    pub security_protocol: SecurityProtocol,
}
