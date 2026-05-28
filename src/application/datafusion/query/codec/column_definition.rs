use crate::application::datafusion::query::codec::column_data_type::{
    from_column_data_type, to_column_data_type,
};
use crate::application::datafusion::query::codec::error::{CodecError, validation_error};
use crate::domain::table_schema::PublicColumnDefinition;
use datafusion::logical_expr::sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident};

pub(super) fn to_domain_table_column_definition(
    column: &ColumnDef,
) -> Result<PublicColumnDefinition, CodecError> {
    let mut nullable = true;
    let mut comment = None;

    for option in &column.options {
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Comment(value) => comment = Some(value.clone()),
            ColumnOption::Default(_) => {
                return Err(validation_error(format!(
                    "column defaults are not supported: {}",
                    column.name
                )));
            }
            other => {
                return Err(validation_error(format!(
                    "unsupported column option for {}: {other}",
                    column.name
                )));
            }
        }
    }

    Ok(PublicColumnDefinition::new(
        column.name.value.clone(),
        to_column_data_type(&column.data_type)?,
        nullable,
        comment,
    ))
}

pub(super) fn from_domain_column_definition(column: &PublicColumnDefinition) -> ColumnDef {
    let mut options = Vec::new();

    if !column.nullable {
        options.push(ColumnOptionDef {
            name: None,
            option: ColumnOption::NotNull,
        });
    }

    if let Some(comment) = &column.comment {
        options.push(ColumnOptionDef {
            name: None,
            option: ColumnOption::Comment(comment.clone()),
        });
    }

    ColumnDef {
        name: Ident::new(&column.name),
        data_type: from_column_data_type(&column.data_type),
        options,
    }
}
