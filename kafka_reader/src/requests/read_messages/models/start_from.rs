use chrono::{DateTime, Utc};

#[derive(Debug, Copy, Clone)]
pub enum StartFrom {
    Beginning,
    Latest,
    Time(MessageTime),
}

#[derive(Debug, Copy, Clone)]
pub struct MessageTime {
    time: DateTime<Utc>,
}

impl MessageTime {
    pub fn today() -> Self {
        let now = Utc::now().date_naive().into();
        let today = DateTime::from_naive_utc_and_offset(now, Utc);
        Self { time: today }
    }

    pub fn time(&self) -> DateTime<Utc> {
        self.time
    }
}

impl From<DateTime<Utc>> for MessageTime {
    fn from(value: DateTime<Utc>) -> Self {
        Self { time: value }
    }
}
