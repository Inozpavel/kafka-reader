use crate::connection_settings::KafkaConnectionSettings;

#[derive(Debug)]
pub struct GetTopicLagsQueryInternal {
    pub connection_settings: KafkaConnectionSettings,
    pub topic: String,
    pub group_search: Option<String>,
}
