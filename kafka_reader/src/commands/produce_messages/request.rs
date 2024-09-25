use crate::connection_settings::KafkaConnectionSettings;
use crate::queries::read_messages::Format;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProduceMessagesCommandInternal {
    pub connection_settings: KafkaConnectionSettings,
    pub topic: String,
    pub key_format: Option<Format>,
    pub body_format: Option<Format>,
    pub messages: Vec<ProduceMessage>,
}

#[derive(Debug)]
pub struct ProduceMessage {
    pub key: Option<String>,
    pub body: Option<String>,
    pub partition: Option<i32>,
    pub headers: HashMap<String, String>,
}
