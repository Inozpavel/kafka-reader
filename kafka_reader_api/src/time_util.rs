use chrono::{DateTime, TimeDelta, Timelike, Utc};
use prost_types::Timestamp;

pub trait DateTimeConvert {
    fn to_date_time(&self) -> DateTime<Utc>;
}

impl DateTimeConvert for Timestamp {
    fn to_date_time(&self) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
            + TimeDelta::seconds(self.seconds)
            + TimeDelta::nanoseconds(self.nanos as i64)
    }
}

pub trait ProtoTimestampConvert {
    fn to_proto_timestamp(&self) -> Timestamp;
}

impl ProtoTimestampConvert for DateTime<Utc> {
    fn to_proto_timestamp(&self) -> Timestamp {
        Timestamp {
            nanos: self.nanosecond() as i32,
            seconds: self.timestamp(),
        }
    }
}
