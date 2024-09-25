use crate::connection_settings::ConnectionSettings;

#[derive(Debug)]
pub struct GetTopicLagsQueryInternal {
    pub connection_settings: ConnectionSettings,
    pub topic: String,
    pub group_search: Option<String>,
}
