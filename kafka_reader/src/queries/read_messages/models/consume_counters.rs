use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct ConsumeCounters {
    pub returned_messages_counter: AtomicU64,
    pub read_messages_counter: AtomicU64,
    pub message_count_check_counter: AtomicU64,
}
