use crate::domain::flush_unit::FlushUnit;
use crate::domain::partition::Partition;
use crate::domain::statistics::FileStatistics;
use arrow::array::RecordBatch;

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
        Ok(format!(
            "stream={}/partition={}/{}",
            self.flush_unit.stream,
            self.flush_unit.partition.path_value(),
            self.name
        ))
    }

    pub fn calculate_statistics(&self) -> FileStatistics {
        FileStatistics::calculate(&self.batch_record)
    }

    pub fn partition(&self) -> Partition {
        self.flush_unit.partition
    }

    pub fn batch_record(&self) -> &RecordBatch {
        &self.batch_record
    }
}
