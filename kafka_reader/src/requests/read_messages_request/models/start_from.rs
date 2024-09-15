use chrono::NaiveDate;

#[derive(Debug, Copy, Clone)]
pub enum StartFrom {
    Beginning,
    Latest,
    Day(NaiveDate),
}
