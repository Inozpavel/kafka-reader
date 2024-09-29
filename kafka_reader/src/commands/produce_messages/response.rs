use crate::consumer::PartitionOffset;

#[derive(Debug)]
pub enum ProduceMessagesCommandInternalResponse {
    Error(anyhow::Error),
    ProducedMessageInfo(PartitionOffset),
}
