use crate::application::datafusion::query::codec::create_table::from_domain_create_table;
use crate::application::error::ApplicationError;
use crate::domain::table_schema::TableSchema;
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub(super) fn convert_table_schema_to_response_batch(
    table: &TableSchema,
) -> Result<RecordBatch, ApplicationError> {
    let create_table_sql = from_domain_create_table(table);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "statement",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![create_table_sql])) as ArrayRef],
    )?;

    Ok(batch)
}
