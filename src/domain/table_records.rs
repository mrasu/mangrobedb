use crate::domain::table_schema::TableSchema;
use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

pub struct TableRecords {
    schema: TableSchema,
    batch_records: Vec<RecordBatch>,
}

impl TableRecords {
    pub fn new(schema: TableSchema, batch_records: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batch_records,
        }
    }

    pub fn print_record_batch(&self) -> Result<(), anyhow::Error> {
        for batch in &self.batch_records {
            println!("import table={}", self.schema.name);
            println!("schema={:?}", batch.schema());
            println!("rows={}", batch.num_rows());

            let formatted = pretty_format_batches(std::slice::from_ref(batch))
                .map_err(|error| anyhow!(error))?;
            println!("{formatted}");
        }

        Ok(())
    }
}
