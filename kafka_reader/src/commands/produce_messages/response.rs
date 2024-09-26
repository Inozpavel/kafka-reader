#[derive(Debug)]
pub enum ProduceMessagesCommandInternalResponse {
    Error(anyhow::Error),
    ProducedMessageInfo(ProducedMessageInfo),
}

#[derive(Debug)]
pub struct ProducedMessageInfo {
    pub partition: i32,
    pub offset: i64,
}
