use crate::consumer::SecurityProtocol;

pub struct GetTopicPartitionsWithOffsetsQueryInternal {
    pub brokers: Vec<String>,
    pub security_protocol: SecurityProtocol,
    pub topic: String,
}
