use crate::domain::table_schema::{
    InternalColumnDefinition, PublicColumnDefinition, TableSchemaError,
};
use crate::util::arrow::transform::{self, TransformError};
use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::Schema;

#[derive(Debug, Clone)]
pub struct TableMapping {
    src_column: PublicColumnDefinition,
    dst_column: InternalColumnDefinition,
    pub strategy: MappingStrategy,
}

#[derive(Debug, Clone)]
pub enum MappingStrategy {
    Copy,
    // TODO: support DataFusion functions
    ToHour,
}

impl TableMapping {
    pub fn new(
        src_column: PublicColumnDefinition,
        dst_column: InternalColumnDefinition,
        strategy: MappingStrategy,
    ) -> Self {
        Self {
            src_column,
            dst_column,
            strategy,
        }
    }

    pub fn validate_schema(&self, arrow_schema: &Schema) -> Result<(), TableSchemaError> {
        required_field(arrow_schema, &self.src_column.name)
    }

    pub fn src_column_ref(&self) -> &PublicColumnDefinition {
        &self.src_column
    }

    pub fn dst_column_ref(&self) -> &InternalColumnDefinition {
        &self.dst_column
    }

    pub fn strategy(&self) -> &MappingStrategy {
        &self.strategy
    }
}

fn required_field(schema: &Schema, name: &str) -> Result<(), TableSchemaError> {
    schema
        .field_with_name(name)
        .map_err(|_| TableSchemaError::MissingColumn {
            column_name: name.to_string(),
        })?;

    Ok(())
}

impl MappingStrategy {
    pub fn create_to_hour_array<T: Array + ?Sized>(
        &self,
        array: &T,
    ) -> Result<TimestampMicrosecondArray, TransformError> {
        transform::create_hour_array(array)
    }
}
