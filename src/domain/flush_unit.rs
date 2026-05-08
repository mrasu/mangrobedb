#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FlushUnit {
    pub stream_id: i32,
    pub partition_time: i64,
}

impl FlushUnit {
    pub fn new(stream_id: i32, partition_time: i64) -> Self {
        Self {
            stream_id,
            partition_time,
        }
    }

    pub fn matches(&self, stream_id: i32, partition_time: i64) -> bool {
        self.stream_id == stream_id && self.partition_time == partition_time
    }
}
