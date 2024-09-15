use crate::requests::read_messages_request::{FilterCondition, FilterKind};

#[derive(Debug)]
pub struct ValueFilter {
    pub filter_kind: FilterKind,
    pub filter_condition: FilterCondition,
}
