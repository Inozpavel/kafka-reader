use std::sync::Arc;

#[derive(Debug)]
pub enum ReadLagsResult {
    GroupTopicLag(GroupTopicLags),
    BrokerError(anyhow::Error),
}

#[derive(Debug)]
pub struct GroupInfo {
    pub id: String,
    pub state: String,
}

#[derive(Debug)]
pub struct GroupTopicLags {
    pub group_info: GroupInfo,
    pub topic: Arc<String>,
    pub lags: Vec<PartitionOffsetWithLag>,
}

#[derive(Debug)]
pub struct PartitionOffsetWithLag {
    pub partition: i32,
    pub offset: Option<i64>,
    pub lag: i64,
}

impl PartitionOffsetWithLag {
    pub fn set_lag(&mut self, lag: i64) {
        self.lag = lag
    }
}
