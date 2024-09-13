use crate::message::KafkaMessage;
use getset::Getters;

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct MessageMetadata {
    partition: i32,
    offset: i64,
    is_null: bool,
}

impl From<&Option<KafkaMessage>> for MessageMetadata {
    fn from(value: &Option<KafkaMessage>) -> Self {
        let partition = value.as_ref().map(|x| x.partition).unwrap_or_default();
        let offset = value.as_ref().map(|x| x.offset).unwrap_or_default();
        let is_null = value.is_none();

        Self {
            is_null,
            partition,
            offset,
        }
    }
}
