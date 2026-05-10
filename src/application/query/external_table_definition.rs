use crate::application::error::ApplicationError;
use crate::domain::port::catalog::{
    ColumnDataType, ExternalLocation, ExternalTableDefinition, FileFormat, PartitionField,
    PartitionTransform, TableColumn, TimeUnit,
};
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;

pub(super) fn convert_external_table_definition_to_response_batch(
    table: &ExternalTableDefinition,
) -> Result<RecordBatch, ApplicationError> {
    let location = location_uri(&table.location);
    let format = file_format_label(&table.format);
    let columns_json = columns_json(&table.columns);
    let partition_fields_json = partition_fields_json(&table.partition_fields);

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, false),
        Field::new("columns_json", DataType::Utf8, false),
        Field::new("partition_fields_json", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![table.table_name.clone()])) as ArrayRef,
            Arc::new(StringArray::from(vec![location])) as ArrayRef,
            Arc::new(StringArray::from(vec![format])) as ArrayRef,
            Arc::new(StringArray::from(vec![columns_json])) as ArrayRef,
            Arc::new(StringArray::from(vec![partition_fields_json])) as ArrayRef,
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

fn columns_json(columns: &[TableColumn]) -> String {
    let columns = columns
        .iter()
        .map(|column| {
            json!({
                "name": column.name,
                "data_type": column_data_type_label(&column.data_type),
                "nullable": column.nullable,
                "comment": column.comment,
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_string(&columns).expect("table columns JSON serialization should not fail")
}

fn partition_fields_json(partition_fields: &[PartitionField]) -> String {
    let partition_fields = partition_fields
        .iter()
        .map(|field| {
            json!({
                "source_column": field.source_column,
                "destination_column": field.destination_column,
                "transform": partition_transform_label(field.transform),
                "result_type": column_data_type_label(&field.result_type),
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_string(&partition_fields)
        .expect("partition fields JSON serialization should not fail")
}

fn partition_transform_label(transform: PartitionTransform) -> &'static str {
    match transform {
        PartitionTransform::Identity => "IDENTITY",
    }
}

fn column_data_type_label(data_type: &ColumnDataType) -> &'static str {
    match data_type {
        ColumnDataType::Bool => "BOOL",
        ColumnDataType::Int32 => "INT32",
        ColumnDataType::Int64 => "INT64",
        ColumnDataType::Float64 => "FLOAT64",
        ColumnDataType::String => "STRING",
        ColumnDataType::Date => "DATE",
        ColumnDataType::Time(TimeUnit::Second) => "TIME_SECOND",
        ColumnDataType::Time(TimeUnit::Millisecond) => "TIME_MILLISECOND",
        ColumnDataType::Time(TimeUnit::Microsecond) => "TIME_MICROSECOND",
        ColumnDataType::Time(TimeUnit::Nanosecond) => "TIME_NANOSECOND",
    }
}
