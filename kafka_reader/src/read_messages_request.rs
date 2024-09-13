mod format;
mod read_limit;
mod start_from;

pub use format::*;
pub use read_limit::*;
pub use start_from::*;

#[derive(Debug)]
pub struct ReadMessagesRequest {
    pub brokers: Vec<String>,
    pub topic: String,
    pub format: Format,
    pub start_from: StartFrom,
    pub limit: ReadLimit,
}
