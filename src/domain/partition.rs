#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Partition {
    TimeMicrosecond(i64),
    Int64(i64),
}

impl Partition {
    pub fn path_value(&self) -> String {
        match self {
            Partition::TimeMicrosecond(value) => {
                chrono::DateTime::<chrono::Utc>::from_timestamp_micros(*value)
                    .map(|time| time.format("%Y%m%d_%H%M%S").to_string())
                    .unwrap_or_else(|| value.to_string())
            }
            Partition::Int64(value) => value.to_string(),
        }
    }
}
