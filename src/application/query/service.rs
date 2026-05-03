use std::sync::Arc;

use crate::application::query::QueryOutput;
use crate::application::query::error::QueryError;
use arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Default)]
pub struct QueryService;

impl QueryService {
    pub fn new() -> Self {
        Self
    }

    pub fn query(&self, sql: &str) -> Result<QueryOutput, QueryError> {
        println!("DoGet query sql: {sql}");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("stream_id", DataType::Int32, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("user", DataType::Utf8, false),
            Field::new(
                "posted_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![11, 12, 13])),
                Arc::new(Int32Array::from(vec![0, 0, 0])),
                Arc::new(StringArray::from(vec![
                    "doget-one",
                    "doget-two",
                    "doget-three",
                ])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_777_623_200_000_000,
                    1_777_626_800_000_000,
                    1_777_627_800_000_000,
                ])),
            ],
        )?;

        Ok(QueryOutput {
            schema,
            batches: vec![batch],
        })
    }
}
