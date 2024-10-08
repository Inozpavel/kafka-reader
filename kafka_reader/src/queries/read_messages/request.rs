use crate::connection_settings::ConnectionSettings;
use crate::queries::read_messages::{Format, ReadLimit, StartFrom, ValueFilter};

#[derive(Debug)]
pub struct ReadMessagesQueryInternal {
    pub connection_settings: ConnectionSettings,
    pub topic: String,
    pub key_format: Option<Format>,
    pub body_format: Option<Format>,
    pub key_filter: Option<ValueFilter>,
    pub body_filter: Option<ValueFilter>,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
}
