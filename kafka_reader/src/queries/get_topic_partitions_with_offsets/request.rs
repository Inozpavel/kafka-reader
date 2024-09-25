use crate::connection_settings::KafkaConnectionSettings;

pub struct GetTopicPartitionsWithOffsetsQueryInternal {
    pub connection_settings: KafkaConnectionSettings,
    pub topic: String,
}
