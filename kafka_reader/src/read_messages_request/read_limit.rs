use chrono::{DateTime, Utc};

#[derive(Debug, Copy, Clone)]
pub enum ReadLimit {
    MessageCount(u64),
    ToDate(DateTime<Utc>),
}
