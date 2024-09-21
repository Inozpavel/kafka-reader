use crate::requests::read_messages::{FilterCondition, FilterKind};

#[derive(Debug)]
pub struct ValueFilter {
    pub filter_kind: FilterKind,
    pub filter_condition: FilterCondition,
}
