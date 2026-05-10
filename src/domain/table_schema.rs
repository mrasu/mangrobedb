use crate::application::datafusion::column::{INTERNAL_COLUMN_PREFIX, to_internal_column_name};
use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use anyhow::anyhow;
use arrow::array::{Int32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::marker::PhantomData;
use thiserror::Error;

pub fn create_default_stream_id_mapping() -> TableMapping {
    TableMapping::new(
        PublicColumnDefinition::new("stream_id", DataType::Int32),
        InternalColumnDefinition::new(to_internal_column_name("stream_id"), DataType::Int32),
        MappingStrategy::Copy,
    )
}

pub fn create_default_partition_mapping() -> TableMapping {
    TableMapping::new(
        ColumnDefinition::new(
            "posted_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        InternalColumnDefinition::new(
            to_internal_column_name("partition_time"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        MappingStrategy::ToHour,
    )
}

#[derive(Debug, Error)]
pub enum TableSchemaError {
    #[error("required column is missing: {column_name}")]
    MissingColumn { column_name: String },
    #[error("incompatible type for column {column_name}: expected {expected}, got {actual}")]
    IncompatibleColumnType {
        column_name: String,
        expected: String,
        actual: String,
    },
    #[error("unsupported data type. {data_type}")]
    UnsupportedArrowDataType { data_type: DataType },
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub bucket: String,
    pub path_prefix: String,

    public_columns: Vec<ColumnDefinition<Public>>,

    stream_id_mapping: TableMapping,
    partition_time_mapping: TableMapping,
}

#[derive(Debug, Clone)]
pub struct Internal;

#[derive(Debug, Clone)]
pub struct Public;

#[derive(Debug, Clone)]
pub struct ColumnDefinition<T> {
    pub name: String,
    data_type: DataType,
    _marker: PhantomData<T>,
}

pub type InternalColumnDefinition = ColumnDefinition<Internal>;
pub type PublicColumnDefinition = ColumnDefinition<Public>;

pub struct AddMissingPublicColumnsResult {
    pub schema: TableSchema,
    pub schema_changed: bool,
}

// TODO: remove TableSchema and replace with ExternalTableDefinition, then support stream_id_mapping and partition_time_mapping
impl TableSchema {
    pub fn new(
        table_name: String,
        bucket: String,
        path_prefix: String,
        public_columns: Vec<PublicColumnDefinition>,
        // stream_id_mapping: TableMapping,
        // partition_time_mapping: TableMapping,
    ) -> Self {
        Self {
            table_name,
            bucket,
            path_prefix,
            public_columns,
            stream_id_mapping: create_default_stream_id_mapping(),
            partition_time_mapping: create_default_partition_mapping(),
        }
    }

    pub fn add_missing_public_columns_if_required(
        &self,
        arrow_schema: &Schema,
    ) -> Result<AddMissingPublicColumnsResult, TableSchemaError> {
        let mut updated_schema = self.clone();
        let mut schema_changed = false;

        for field in arrow_schema.fields() {
            if updated_schema.public_column(field.name()).is_none() {
                updated_schema
                    .public_columns
                    .push(PublicColumnDefinition::new(
                        field.name(),
                        field.data_type().clone(),
                    ));
                schema_changed = true;
            }
        }

        Ok(AddMissingPublicColumnsResult {
            schema: updated_schema,
            schema_changed,
        })
    }

    pub fn validate_columns(&self, arrow_schema: &Schema) -> Result<(), TableSchemaError> {
        self.stream_id_mapping.validate_schema(arrow_schema)?;
        self.partition_time_mapping.validate_schema(arrow_schema)?;

        for field in arrow_schema.fields() {
            self.validate_column(field)?;
        }

        Ok(())
    }

    fn validate_column(&self, field: &Field) -> Result<(), TableSchemaError> {
        if let Some(column) = self.public_column(field.name()) {
            return column.validate_compatible(field);
        }

        Ok(())
    }

    fn public_column(&self, name: &str) -> Option<&PublicColumnDefinition> {
        self.public_columns
            .iter()
            .find(|column| column.name == name)
    }

    pub fn stream_id_mapping(&self) -> &TableMapping {
        &self.stream_id_mapping
    }

    pub fn partition_time_mapping(&self) -> &TableMapping {
        &self.partition_time_mapping
    }

    pub fn public_columns(&self) -> &[PublicColumnDefinition] {
        &self.public_columns
    }

    pub fn is_acceptable_column_name_for_public(name: &str) -> bool {
        !name.starts_with(INTERNAL_COLUMN_PREFIX)
    }

    pub fn stream_id_array<'a>(
        &self,
        record: &'a RecordBatch,
    ) -> Result<&'a Int32Array, anyhow::Error> {
        let column_name = &self.stream_id_mapping.dst_column_ref().name;
        let index = record.schema().index_of(column_name)?;
        record
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow!("internal stream id column must be Int32"))
    }

    pub fn partition_time_array<'a>(
        &self,
        record: &'a RecordBatch,
    ) -> Result<&'a TimestampMicrosecondArray, anyhow::Error> {
        let column_name = &self.partition_time_mapping.dst_column_ref().name;
        let index = record.schema().index_of(column_name)?;
        record
            .column(index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| anyhow!("internal partition time column must be TimestampMicrosecond"))
    }
}

impl<T> ColumnDefinition<T> {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validate_compatible(&self, field: &Field) -> Result<(), TableSchemaError> {
        if self.is_compatible(field.data_type()) {
            return Ok(());
        }

        Err(TableSchemaError::IncompatibleColumnType {
            column_name: field.name().clone(),
            expected: expected_type(&self.data_type),
            actual: format!("{:?}", field.data_type()),
        })
    }

    fn is_compatible(&self, data_type: &DataType) -> bool {
        match (&self.data_type, data_type) {
            (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
            (expected, actual) => expected == actual,
        }
    }
}

impl<T> ColumnDefinition<T> {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            _marker: PhantomData,
        }
    }
}

impl PublicColumnDefinition {}

fn expected_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Timestamp(_, _) => "Timestamp".to_string(),
        other => format!("{other:?}"),
    }
}
