use crate::connection_settings::ConnectionSettings;

pub struct GetTopicPartitionsWithOffsetsQueryInternal {
    pub connection_settings: ConnectionSettings,
    pub topic: String,
}
