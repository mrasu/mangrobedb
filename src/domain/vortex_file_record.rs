use crate::domain::flush_unit::FlushUnit;
use crate::domain::statistics::FileStatistics;
use anyhow::anyhow;
use arrow::array::RecordBatch;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct VortexFileRecord {
    name: String,
    flush_unit: FlushUnit,
    batch_record: RecordBatch,
}

impl VortexFileRecord {
    pub fn new(name: String, flush_unit: FlushUnit, batch_record: RecordBatch) -> Self {
        Self {
            name,
            flush_unit,
            batch_record,
        }
    }

    pub fn path(&self) -> Result<String, anyhow::Error> {
        let partition_time = DateTime::<Utc>::from_timestamp_micros(self.flush_unit.partition_time)
            .ok_or_else(|| anyhow!("invalid partition time: {}", self.flush_unit.partition_time))?
            .format("%Y%m%d_%H%M%S");

        Ok(format!(
            "stream_id={}/partition_time={}/{}",
            self.flush_unit.stream_id, partition_time, self.name
        ))
    }

    pub fn calculate_statistics(&self) -> FileStatistics {
        FileStatistics::calculate(&self.batch_record)
    }

    pub fn partition_time_micros(&self) -> i64 {
        self.flush_unit.partition_time
    }

    pub fn batch_record(&self) -> &RecordBatch {
        &self.batch_record
    }
}
