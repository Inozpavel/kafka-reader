use crate::consumer::SecurityProtocol;
use crate::requests::read_messages_request::Format;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProduceMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key_format: Option<Format>,
    pub body_format: Option<Format>,
    pub security_protocol: SecurityProtocol,
    pub messages: Vec<ProduceMessage>,
}

#[derive(Debug)]
pub struct ProduceMessage {
    pub key: Option<String>,
    pub body: Option<String>,
    pub partition: Option<i32>,
    pub headers: HashMap<String, String>,
}
