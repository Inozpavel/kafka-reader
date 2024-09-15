use chrono::NaiveDate;

#[derive(Debug, Copy, Clone)]
pub enum ReadLimit {
    NoLimit,
    MessageCount(u64),
    ToDate(NaiveDate),
}
