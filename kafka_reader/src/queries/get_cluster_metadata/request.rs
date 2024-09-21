use crate::consumer::SecurityProtocol;

#[derive(Debug)]
pub struct GetClusterMetadataQueryInternal {
    pub brokers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}
