use crate::domain::flush_unit::FlushUnit;
use arrow::array::RecordBatch;

#[derive(Debug)]
pub struct FlushUnitRecord {
    flush_unit: FlushUnit,
    batch_record: RecordBatch,
}

impl FlushUnitRecord {
    pub fn new(flush_unit: FlushUnit, batch_record: RecordBatch) -> Self {
        Self {
            flush_unit,
            batch_record,
        }
    }

    pub fn flush_unit(&self) -> &FlushUnit {
        &self.flush_unit
    }

    pub fn batch_record(&self) -> &RecordBatch {
        &self.batch_record
    }
}
