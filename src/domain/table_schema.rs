use crate::application::datafusion::column::INTERNAL_COLUMN_PREFIX;
use crate::domain::column_data_type::{ColumnDataType, TimeUnit};
use crate::domain::file::FileFormat;
use anyhow::anyhow;
use arrow::array::{Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType as ArrowDatType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::marker::PhantomData;
use thiserror::Error;

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
    UnsupportedArrowDataType { data_type: ArrowDatType },
    #[error("stream_column {stream_column} does not match any column in the table")]
    NoMatchingStreamColumn { stream_column: String },
    #[error(
        "stream_column {stream_column} is invalid. data type of stream_column must be INT64 but {data_type}"
    )]
    InvalidStreamDataType {
        stream_column: String,
        data_type: ColumnDataType,
    },
    #[error("partition_column {partition_column} does not match any column in the table")]
    NoMatchingPartitionColumn { partition_column: String },
    #[error(
        "partition_column {partition_column} is invalid. data type of partition_column must be {} or {} but {data_type}",
        ColumnDataType::Timestamp(TimeUnit::Microsecond),
        ColumnDataType::Int64
    )]
    InvalidPartitionDataType {
        partition_column: String,
        data_type: ColumnDataType,
    },
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub location: ExternalLocation,
    pub format: FileFormat,

    pub public_columns: Vec<ColumnDefinition<Public>>,

    // stream MUST be i64.
    stream_ref: StreamColumnReference,
    // partition MUST be Time(Microsecond) or i64.
    partition_ref: PartitionColumnReference,

    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Public;

#[derive(Debug, Clone)]
pub struct ColumnDefinition<T> {
    pub name: String,
    pub data_type: ColumnDataType,
    pub nullable: bool,
    pub comment: Option<String>,
    _marker: PhantomData<T>,
}

pub type PublicColumnDefinition = ColumnDefinition<Public>;

pub struct AddMissingPublicColumnsResult {
    pub schema: TableSchema,
    pub schema_changed: bool,
}

#[derive(Debug, Clone)]
struct StreamColumnReference {
    name: String,
}

impl StreamColumnReference {
    fn try_new(columns: &[PublicColumnDefinition], name: String) -> Result<Self, TableSchemaError> {
        let Some(col) = columns.iter().find(|col| col.name == name) else {
            return Err(TableSchemaError::NoMatchingStreamColumn {
                stream_column: name.clone(),
            });
        };

        if col.data_type != ColumnDataType::Int64 {
            return Err(TableSchemaError::InvalidStreamDataType {
                stream_column: name.clone(),
                data_type: col.data_type.clone(),
            });
        }

        Ok(StreamColumnReference { name })
    }
}

#[derive(Debug, Clone)]
struct PartitionColumnReference {
    name: String,
    data_type: ColumnDataType,
}

impl PartitionColumnReference {
    fn try_new(columns: &[PublicColumnDefinition], name: String) -> Result<Self, TableSchemaError> {
        let Some(col) = columns.iter().find(|col| col.name == name) else {
            return Err(TableSchemaError::NoMatchingPartitionColumn {
                partition_column: name.clone(),
            });
        };

        if col.data_type != ColumnDataType::Timestamp(TimeUnit::Microsecond)
            && col.data_type != ColumnDataType::Int64
        {
            return Err(TableSchemaError::InvalidPartitionDataType {
                partition_column: name.clone(),
                data_type: col.data_type.clone(),
            });
        }

        Ok(PartitionColumnReference {
            name,
            data_type: col.data_type.clone(),
        })
    }
}

impl TableSchema {
    pub fn try_new(
        table_name: String,
        location: ExternalLocation,
        format: FileFormat,
        public_columns: Vec<PublicColumnDefinition>,
        stream_column: String,
        partition_column: String,
        comment: Option<String>,
    ) -> Result<Self, TableSchemaError> {
        let stream_ref = StreamColumnReference::try_new(&public_columns, stream_column.clone())?;

        let partition_ref =
            PartitionColumnReference::try_new(&public_columns, partition_column.clone())?;

        Ok(Self {
            table_name,
            location,
            format,
            public_columns,
            stream_ref,
            partition_ref,
            comment,
        })
    }

    pub fn add_missing_public_columns_if_required(
        &self,
        arrow_schema: &Schema,
    ) -> Result<AddMissingPublicColumnsResult, TableSchemaError> {
        let mut updated_schema = self.clone();
        let mut schema_changed = false;

        for field in arrow_schema.fields() {
            if updated_schema.find_public_column(field.name()).is_none() {
                let data_type = ColumnDataType::try_from(field.data_type().clone()).map_err({
                    |_err| TableSchemaError::UnsupportedArrowDataType {
                        data_type: field.data_type().clone(),
                    }
                })?;

                updated_schema
                    .public_columns
                    .push(PublicColumnDefinition::new(
                        field.name(),
                        data_type,
                        true,
                        None,
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
        if arrow_schema.fields.find(&self.stream_column()).is_none() {
            return Err(TableSchemaError::NoMatchingStreamColumn {
                stream_column: self.stream_column().clone(),
            });
        }

        for field in arrow_schema.fields() {
            self.validate_column(field)?;
        }

        self.validate_stream(arrow_schema)?;
        self.validate_partition(arrow_schema)?;

        Ok(())
    }

    fn validate_column(&self, field: &Field) -> Result<(), TableSchemaError> {
        if let Some(column) = self.find_public_column(field.name()) {
            return column.validate_compatibility(field);
        }

        Ok(())
    }

    fn validate_stream(&self, arrow_schema: &Schema) -> Result<(), TableSchemaError> {
        let Some(stream_column) = self.find_public_column(&self.stream_column()) else {
            return Err(TableSchemaError::NoMatchingStreamColumn {
                stream_column: self.stream_column(),
            });
        };

        self.validate_column_matched(stream_column, arrow_schema)?;

        Ok(())
    }

    fn validate_partition(&self, arrow_schema: &Schema) -> Result<(), TableSchemaError> {
        let Some(partition_column) = self.find_public_column(&self.partition_column()) else {
            return Err(TableSchemaError::NoMatchingPartitionColumn {
                partition_column: self.partition_column(),
            });
        };

        self.validate_column_matched(partition_column, arrow_schema)?;

        Ok(())
    }

    fn validate_column_matched(
        &self,
        table_column: &PublicColumnDefinition,
        arrow_schema: &Schema,
    ) -> Result<(), TableSchemaError> {
        let Some((_, arrow_field)) = arrow_schema.fields.find(&table_column.name) else {
            return Err(TableSchemaError::MissingColumn {
                column_name: table_column.name.clone(),
            });
        };

        if table_column.arrow_data_type() != arrow_field.data_type().clone() {
            return Err(TableSchemaError::IncompatibleColumnType {
                column_name: table_column.name.clone(),
                expected: table_column.data_type.display_label().to_string(),
                actual: arrow_field.data_type().to_string(),
            });
        }

        Ok(())
    }

    fn find_public_column(&self, name: &str) -> Option<&PublicColumnDefinition> {
        self.public_columns
            .iter()
            .find(|column| column.name == name)
    }

    pub fn public_columns(&self) -> &[PublicColumnDefinition] {
        &self.public_columns
    }

    pub fn stream_column(&self) -> String {
        self.stream_ref.name.clone()
    }

    pub fn partition_column(&self) -> String {
        self.partition_ref.name.clone()
    }

    pub fn partition_data_type(&self) -> &ColumnDataType {
        &self.partition_ref.data_type
    }

    pub fn is_acceptable_column_name_for_public(name: &str) -> bool {
        !name.starts_with(INTERNAL_COLUMN_PREFIX)
    }

    pub fn stream_array<'a>(
        &self,
        record: &'a RecordBatch,
    ) -> Result<&'a Int64Array, anyhow::Error> {
        let index = record.schema().index_of(&self.stream_column())?;

        record
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("internal stream column must be Int64"))
    }

    pub fn partition_array<'a>(
        &self,
        record: &'a RecordBatch,
    ) -> Result<&'a TimestampMicrosecondArray, anyhow::Error> {
        let index = record.schema().index_of(&self.partition_column())?;
        record
            .column(index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| anyhow!("internal partition time column must be TimestampMicrosecond"))
    }

    pub fn partition_int64_array<'a>(
        &self,
        record: &'a RecordBatch,
    ) -> Result<&'a Int64Array, anyhow::Error> {
        let index = record.schema().index_of(&self.partition_column())?;
        record
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("internal partition column must be Int64"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLocation {
    pub bucket: String,
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl ExternalLocation {
    pub fn new(
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
    ) -> Self {
        Self {
            bucket,
            prefix,
            endpoint,
            region,
        }
    }
}

impl<T> ColumnDefinition<T> {
    pub fn arrow_data_type(&self) -> ArrowDatType {
        match self.data_type {
            ColumnDataType::Bool => ArrowDatType::Boolean,
            ColumnDataType::Int32 => ArrowDatType::Int32,
            ColumnDataType::Int64 => ArrowDatType::Int64,
            ColumnDataType::Float64 => ArrowDatType::Float64,
            ColumnDataType::String => ArrowDatType::Utf8,
            ColumnDataType::Date => ArrowDatType::Date64,
            ColumnDataType::Timestamp(unit) => match unit {
                TimeUnit::Second => ArrowDatType::Timestamp(Second, None),
                TimeUnit::Millisecond => ArrowDatType::Timestamp(Millisecond, None),
                TimeUnit::Microsecond => ArrowDatType::Timestamp(Microsecond, None),
                TimeUnit::Nanosecond => ArrowDatType::Timestamp(Nanosecond, None),
            },
        }
    }

    fn validate_compatibility(&self, field: &Field) -> Result<(), TableSchemaError> {
        if self.is_compatible(field.data_type()) {
            return Ok(());
        }

        Err(TableSchemaError::IncompatibleColumnType {
            column_name: field.name().clone(),
            expected: format!("{:?}", self.data_type),
            actual: format!("{:?}", field.data_type()),
        })
    }

    fn is_compatible(&self, data_type: &ArrowDatType) -> bool {
        let Ok(column_data_type) = data_type.clone().try_into() else {
            return false;
        };
        match (self.data_type.clone(), column_data_type) {
            (ColumnDataType::Timestamp(_), ColumnDataType::Timestamp(_)) => true,
            (expected, actual) => expected == actual,
        }
    }
}

impl<T> ColumnDefinition<T> {
    pub fn new(
        name: impl Into<String>,
        data_type: ColumnDataType,
        nullable: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            comment,
            _marker: PhantomData,
        }
    }
}
