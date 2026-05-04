pub mod service;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct QueryOutput {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}
