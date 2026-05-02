use crate::domain::table_mapping::{MappingStrategy, TableMapping};
use arrow::datatypes::{DataType, Field, Schema};
use std::marker::PhantomData;
use thiserror::Error;

const INTERNAL_COLUMN_PREFIX: &str = "__mangrobe__";

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
    #[error("not implemented. message: {message}")]
    NotImplemented { message: String },
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
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

impl TableSchema {
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

        // TODO: add columns
        Err(TableSchemaError::NotImplemented {
            message: "adding columns dynamically".to_owned(),
        })
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

    pub fn is_acceptable_column_name_for_public(name: &str) -> bool {
        !name.starts_with(INTERNAL_COLUMN_PREFIX)
    }
}

impl<T> ColumnDefinition<T> {
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
    fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
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

// TODO: remove
pub const DUMMY_TABLE: &str = "dummy_table";

// TODO: remove dummy schema
pub fn initial_dummy_table_schema() -> TableSchema {
    TableSchema {
        name: DUMMY_TABLE.to_string(),
        public_columns: vec![
            PublicColumnDefinition::new("id", DataType::Int32),
            PublicColumnDefinition::new("stream_id", DataType::Int32),
            PublicColumnDefinition::new("message", DataType::Utf8),
            PublicColumnDefinition::new("user", DataType::Utf8),
            ColumnDefinition::new(
                "posted_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
        ],

        stream_id_mapping: TableMapping::new(
            PublicColumnDefinition::new("stream_id", DataType::Int32),
            InternalColumnDefinition::new("__mangrobe__stream_id", DataType::Int32),
            MappingStrategy::Copy,
        ),
        partition_time_mapping: TableMapping::new(
            ColumnDefinition::new(
                "posted_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
            InternalColumnDefinition::new(
                "__mangrobe__partition_time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
            MappingStrategy::ToHour,
        ),
    }
}
