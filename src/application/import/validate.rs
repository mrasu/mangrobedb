use crate::application::error::ApplicationUserError;
use crate::domain::table_schema::TableSchema;
use arrow::datatypes::Schema;

pub fn validate_schema(
    table_schema: &TableSchema,
    arrow_schema: &Schema,
) -> Result<(), ApplicationUserError> {
    for field in arrow_schema.fields() {
        if !TableSchema::is_acceptable_column_name_for_public(field.name()) {
            return Err(ApplicationUserError::ReservedColumnName {
                column_name: field.name().clone(),
            });
        }
    }

    table_schema.validate_columns(arrow_schema).map_err(|err| {
        ApplicationUserError::ValidationError {
            message: err.to_string(),
        }
    })?;

    Ok(())
}
