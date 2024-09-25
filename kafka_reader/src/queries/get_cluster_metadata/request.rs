use crate::connection_settings::ConnectionSettings;

#[derive(Debug)]
pub struct GetClusterMetadataQueryInternal {
    pub connection_settings: ConnectionSettings,
}
