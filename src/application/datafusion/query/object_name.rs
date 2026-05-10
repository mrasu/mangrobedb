use crate::application::datafusion::query::util::validation_error;
use crate::application::error::ApplicationError;
use datafusion::logical_expr::sqlparser::ast::ObjectName;

pub fn parse_to_single_table_name(name: &ObjectName) -> Result<String, ApplicationError> {
    let [part] = name.0.as_slice() else {
        return Err(validation_error(format!(
            "qualified table names are not supported: {name}"
        )));
    };
    let Some(ident) = part.as_ident() else {
        return Err(validation_error(format!("invalid table name: {name}")));
    };

    Ok(ident.value.clone())
}
