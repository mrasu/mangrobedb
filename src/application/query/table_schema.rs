use crate::application::error::ApplicationError;
use crate::domain::file::FileFormat;
use crate::domain::table_schema::{ExternalLocation, PublicColumnDefinition, TableSchema};
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;

pub(super) fn convert_table_schema_to_response_batch(
    table: &TableSchema,
) -> Result<RecordBatch, ApplicationError> {
    let location = location_uri(&table.location);
    let format = file_format_label(&table.format);
    let columns_json = columns_json(&table.public_columns);

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, false),
        Field::new("columns_json", DataType::Utf8, false),
        Field::new("stream_column", DataType::Utf8, false),
        Field::new("partition_column", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![table.table_name.clone()])) as ArrayRef,
            Arc::new(StringArray::from(vec![location])) as ArrayRef,
            Arc::new(StringArray::from(vec![format])) as ArrayRef,
            Arc::new(StringArray::from(vec![columns_json])) as ArrayRef,
            Arc::new(StringArray::from(vec![table.stream_column()])) as ArrayRef,
            Arc::new(StringArray::from(vec![table.partition_column()])) as ArrayRef,
            Arc::new(StringArray::from(vec![table.comment.clone()])) as ArrayRef,
        ],
    )?;

    Ok(batch)
}

fn location_uri(location: &ExternalLocation) -> String {
    if location.prefix.is_empty() {
        format!("s3://{}", location.bucket)
    } else {
        format!("s3://{}/{}", location.bucket, location.prefix)
    }
}

fn file_format_label(format: &FileFormat) -> &'static str {
    match format {
        FileFormat::Vortex => "VORTEX",
    }
}

fn columns_json(columns: &[PublicColumnDefinition]) -> String {
    let columns = columns
        .iter()
        .map(|column| {
            json!({
                "name": column.name,
                "data_type": column.data_type.display_label(),
                "nullable": column.nullable,
                "comment": column.comment,
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_string(&columns).expect("table columns JSON serialization should not fail")
}
