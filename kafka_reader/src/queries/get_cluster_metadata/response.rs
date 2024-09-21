#[derive(Debug)]
pub struct GetClusterMetadataQueryInternalResponse {
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<KafkaTopicMetadata>,
}

#[derive(Debug)]
pub struct KafkaTopicMetadata {
    pub topic_name: String,
    pub partitions_count: usize,
}

#[derive(Debug)]
pub struct BrokerMetadata {
    pub host: String,
    pub port: u16,
}
