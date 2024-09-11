use std::collections::HashMap;

pub struct KafkaMessage {
    pub key: Option<String>,
    pub body: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}
