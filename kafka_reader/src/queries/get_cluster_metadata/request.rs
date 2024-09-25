use crate::connection_settings::KafkaConnectionSettings;

#[derive(Debug)]
pub struct GetClusterMetadataQueryInternal {
    pub connection_settings: KafkaConnectionSettings,
}
