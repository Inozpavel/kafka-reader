use getset::Getters;

#[derive(Debug, Getters, Copy, Clone)]
#[getset(get = "pub")]
pub struct PartitionOffset {
    partition: i32,
    offset: Option<i64>,
}

impl PartitionOffset {
    pub fn new(partition: i32, offset: Option<i64>) -> PartitionOffset {
        Self { offset, partition }
    }
}
