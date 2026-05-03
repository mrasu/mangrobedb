use crate::domain::table_schema::TableSchema;
use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use chrono::{DateTime, Utc};

pub struct FileBatch {
    schema: TableSchema,
    // Only vortex is supported now.
    file_records: Vec<VortexFileRecord>,
}

pub struct VortexFileRecord {
    name: String,
    flush_unit: FlushUnit,
    batch_record: RecordBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FlushUnit {
    pub stream_id: i32,
    pub partition_time: i64,
}

impl FileBatch {
    pub fn new(schema: TableSchema, file_records: Vec<VortexFileRecord>) -> Self {
        Self {
            schema,
            file_records,
        }
    }

    pub fn print_record_batch(&self) -> Result<(), anyhow::Error> {
        for file_record in &self.file_records {
            println!("import table={}", self.schema.name);
            println!("file={}", file_record.name);
            println!("schema={:?}", file_record.batch_record.schema());
            println!("rows={}", file_record.batch_record.num_rows());

            let formatted = pretty_format_batches(std::slice::from_ref(&file_record.batch_record))
                .map_err(|error| anyhow!(error))?;
            println!("{formatted}");
        }

        Ok(())
    }
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
