use chrono::{DateTime, Utc};

#[derive(Debug, Copy, Clone)]
pub enum ReadLimit {
    NoLimit,
    MessageCount(u64),
    ToTime(DateTime<Utc>),
}
