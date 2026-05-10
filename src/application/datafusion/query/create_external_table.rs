use crate::application::datafusion::query::object_name::parse_to_single_table_name;
use crate::application::datafusion::query::util::validation_error;
use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::domain::port::catalog::{
    ColumnDataType, CreateExternalTableRequest, ExternalLocation, ExternalTableDefinition,
    FileFormat, PartitionField, PartitionTransform, TableColumn, TimeUnit,
};
use datafusion::sql::parser::CreateExternalTable;
use datafusion::sql::sqlparser::ast::{ColumnDef, ColumnOption, DataType as SqlDataType, Value};
use url::Url;

pub fn parse_external_table_statement(
    statement: CreateExternalTable,
) -> Result<CreateExternalTableRequest, ApplicationError> {
    to_create_external_table_request(statement)
}

fn to_create_external_table_request(
    statement: CreateExternalTable,
) -> Result<CreateExternalTableRequest, ApplicationError> {
    reject_unsupported_create_external_table_features(&statement)?;

    let table_name = parse_to_single_table_name(&statement.name)?;
    let options = CreateExternalTableOptions::try_from(statement.options.as_slice())?;
    let columns = statement
        .columns
        .iter()
        .map(to_table_column)
        .collect::<Result<Vec<_>, _>>()?;
    let partition_fields = statement
        .table_partition_cols
        .iter()
        .map(|column_name| to_partition_field(column_name, &columns))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CreateExternalTableRequest {
        table: ExternalTableDefinition::new(
            table_name,
            to_external_location(&statement.location, options)?,
            FileFormat::Vortex,
            columns,
            partition_fields,
            None,
        ),
        skip_if_exists: statement.if_not_exists,
    })
}

fn reject_unsupported_create_external_table_features(
    statement: &CreateExternalTable,
) -> Result<(), ApplicationError> {
    if !statement.file_type.eq_ignore_ascii_case("vortex") {
        return Err(ApplicationUserError::NotImplemented {
            message: format!(
                "unsupported CREATE EXTERNAL TABLE format: {}",
                statement.file_type
            ),
        }
        .into());
    }
    if statement.or_replace {
        return Err(validation_error("OR REPLACE is not supported"));
    }
    if statement.temporary {
        return Err(validation_error(
            "TEMPORARY external tables are not supported",
        ));
    }
    if statement.unbounded {
        return Err(validation_error(
            "UNBOUNDED external tables are not supported",
        ));
    }
    if !statement.order_exprs.is_empty() {
        return Err(validation_error("WITH ORDER is not supported"));
    }
    if !statement.constraints.is_empty() {
        return Err(validation_error("table constraints are not supported"));
    }

    Ok(())
}

#[derive(Debug, Default)]
struct CreateExternalTableOptions {
    endpoint: Option<String>,
    region: Option<String>,
}

impl TryFrom<&[(String, Value)]> for CreateExternalTableOptions {
    type Error = ApplicationError;

    fn try_from(options: &[(String, Value)]) -> Result<Self, Self::Error> {
        let mut result = Self::default();
        for (key, value) in options {
            match key.as_str() {
                "s3.endpoint" => result.endpoint = Some(option_value_to_string(key, value)?),
                "s3.region" => result.region = Some(option_value_to_string(key, value)?),
                _ => {
                    return Err(validation_error(format!(
                        "unsupported CREATE EXTERNAL TABLE option: {key}"
                    )));
                }
            }
        }

        Ok(result)
    }
}

fn option_value_to_string(key: &str, value: &Value) -> Result<String, ApplicationError> {
    match value {
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value) => Ok(value.clone()),
        _ => Err(validation_error(format!(
            "CREATE EXTERNAL TABLE option must be a string: {key}"
        ))),
    }
}

fn to_external_location(
    location: &str,
    options: CreateExternalTableOptions,
) -> Result<ExternalLocation, ApplicationError> {
    let (bucket, prefix) = parse_location_string(location)?;

    Ok(ExternalLocation::new(
        bucket,
        prefix,
        options.endpoint,
        options.region,
    ))
}

fn parse_location_string(location: &str) -> Result<(String, String), ApplicationError> {
    let url = Url::parse(location)?;
    if url.scheme() != "s3" {
        return Err(validation_error(format!(
            "only s3 locations are supported: {location}"
        )));
    }

    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| validation_error(format!("s3 bucket is required: {location}")))?;

    let prefix = url.path().trim_start_matches('/').to_string();

    Ok((bucket.into(), prefix))
}

fn to_table_column(column: &ColumnDef) -> Result<TableColumn, ApplicationError> {
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

    Ok(TableColumn::new(
        column.name.value.clone(),
        to_column_data_type(&column.data_type)?,
        nullable,
        comment,
    ))
}

fn to_column_data_type(data_type: &SqlDataType) -> Result<ColumnDataType, ApplicationError> {
    Ok(match data_type {
        SqlDataType::Bool | SqlDataType::Boolean => ColumnDataType::Bool,
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) | SqlDataType::Int64 => ColumnDataType::Int64,
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float64 => ColumnDataType::Float64,
        SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Clob(_)
        | SqlDataType::String(_)
        | SqlDataType::Text
        | SqlDataType::Varchar(_) => ColumnDataType::String,
        SqlDataType::Date | SqlDataType::Date32 => ColumnDataType::Date,
        SqlDataType::Time(precision, _) | SqlDataType::Timestamp(precision, _) => {
            ColumnDataType::Time(to_time_unit(*precision)?)
        }
        _ => {
            return Err(validation_error(format!(
                "unsupported column type: {data_type}"
            )));
        }
    })
}

fn to_time_unit(precision: Option<u64>) -> Result<TimeUnit, ApplicationError> {
    match precision.unwrap_or(6) {
        0 => Ok(TimeUnit::Second),
        3 => Ok(TimeUnit::Millisecond),
        6 => Ok(TimeUnit::Microsecond),
        9 => Ok(TimeUnit::Nanosecond),
        other => Err(validation_error(format!(
            "unsupported time precision: {other}"
        ))),
    }
}

fn to_partition_field(
    column_name: &str,
    columns: &[TableColumn],
) -> Result<PartitionField, ApplicationError> {
    let column = columns
        .iter()
        .find(|column| column.name == column_name)
        .ok_or_else(|| {
            validation_error(format!("partition column is not declared: {column_name}"))
        })?;

    Ok(PartitionField::new(
        column_name.to_string(),
        None,
        PartitionTransform::Identity,
        column.data_type.clone(),
    ))
}
