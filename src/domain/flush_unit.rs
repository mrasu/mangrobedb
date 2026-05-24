use crate::domain::partition::Partition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FlushUnit {
    pub stream: i64,
    pub partition: Partition,
}

impl FlushUnit {
    pub fn new(stream: i64, partition: Partition) -> Self {
        Self { stream, partition }
    }

    pub fn matches(&self, stream: i64, partition: Partition) -> bool {
        self.stream == stream && self.partition == partition
    }
}
