use std::collections::HashMap;

pub struct KafkaMessage {
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub body: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}
