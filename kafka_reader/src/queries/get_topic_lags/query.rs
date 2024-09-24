use crate::consumer::SecurityProtocol;

#[derive(Debug)]
pub struct GetTopicLagsQueryInternal {
    pub brokers: Vec<String>,
    pub topic: String,
    pub group_search: Option<String>,
    pub security_protocol: SecurityProtocol,
}
