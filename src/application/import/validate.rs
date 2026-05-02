use crate::application::import::error::ImportUserError;
use crate::domain::table_schema::TableSchema;
use arrow::datatypes::Schema;

pub fn validate_schema(
    table_schema: &TableSchema,
    arrow_schema: &Schema,
) -> Result<(), ImportUserError> {
    for field in arrow_schema.fields() {
        if !TableSchema::is_acceptable_column_name_for_public(field.name()) {
            return Err(ImportUserError::ReservedColumnName {
                column_name: field.name().clone(),
            });
        }
    }

    table_schema.validate_columns(arrow_schema).map_err(|err| {
        ImportUserError::ValidationError {
            message: err.to_string(),
        }
    })?;

    Ok(())
}
