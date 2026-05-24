use crate::application::datafusion::query::object_name::parse_to_single_table_name;
use crate::application::datafusion::query::util::validation_error;
use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::domain::column_data_type::{ColumnDataType, TimeUnit};
use crate::domain::file::FileFormat;
use crate::domain::port::catalog::CreateTableRequest;
use crate::domain::table_schema::{ExternalLocation, PublicColumnDefinition, TableSchema};
use datafusion::logical_expr::sqlparser::ast::{SqlOption, Value};
use datafusion::sql::sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, CreateTableOptions as DatafusionCreateTableOptions,
    DataType as SqlDataType, Expr, Ident,
};
use url::Url;

pub fn build_create_table_request(
    statement: &CreateTable,
) -> Result<CreateTableRequest, ApplicationError> {
    let table_name = parse_to_single_table_name(&statement.name)?;
    let options = CreateTableOptions::try_from(&statement.table_options)?;
    let hive_formats = &statement.hive_formats.clone().unwrap_or_default();
    let columns = statement
        .columns
        .iter()
        .map(to_table_column)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CreateTableRequest {
        table: TableSchema::try_new(
            table_name,
            to_external_location(&hive_formats.location, &options)?,
            options.file_format,
            columns,
            options.stream_column,
            options.partition_column,
            None,
        )?,
        skip_if_exists: statement.if_not_exists,
    })
}

#[derive(Debug)]
struct CreateTableOptions {
    file_format: FileFormat,
    stream_column: String,
    partition_column: String,
    endpoint: Option<String>,
    region: Option<String>,
}

#[derive(Debug, Default)]
struct CreateTableOptionsBuilder {
    file_format: Option<FileFormat>,
    stream_column: Option<String>,
    partition_column: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
}

impl CreateTableOptionsBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn file_format(&mut self, file_format: FileFormat) -> &mut Self {
        self.file_format = Some(file_format);
        self
    }

    fn stream_column(&mut self, v: impl Into<String>) -> &mut Self {
        self.stream_column = Some(v.into());
        self
    }

    fn partition_column(&mut self, v: impl Into<String>) -> &mut Self {
        self.partition_column = Some(v.into());
        self
    }

    fn endpoint(&mut self, v: impl Into<String>) -> &mut Self {
        self.endpoint = Some(v.into());
        self
    }

    fn region(&mut self, v: impl Into<String>) -> &mut Self {
        self.region = Some(v.into());
        self
    }

    fn build(self) -> Result<CreateTableOptions, ApplicationError> {
        let file_format = self
            .file_format
            .ok_or(validation_error("No format option provided"))?;

        let stream_column = self.stream_column.unwrap_or_default();
        if stream_column.is_empty() {
            return Err(validation_error("No stream_column option provided"));
        }

        let partition_column = self.partition_column.unwrap_or_default();
        if partition_column.is_empty() {
            return Err(validation_error("No partition_column option provided"));
        }

        Ok(CreateTableOptions {
            file_format,
            stream_column,
            partition_column,

            endpoint: self.endpoint,
            region: self.region,
        })
    }
}

impl TryFrom<&DatafusionCreateTableOptions> for CreateTableOptions {
    type Error = ApplicationError;

    fn try_from(options: &DatafusionCreateTableOptions) -> Result<Self, Self::Error> {
        let mut builder = CreateTableOptionsBuilder::new();
        match options {
            DatafusionCreateTableOptions::None => {}
            DatafusionCreateTableOptions::With(sql_options) => {
                for sql_option in sql_options {
                    match sql_option {
                        SqlOption::KeyValue { key, value } => match key.value.as_str() {
                            "format" => {
                                if option_value_to_string(key, value)?
                                    .eq_ignore_ascii_case("vortex")
                                {
                                    builder.file_format(FileFormat::Vortex);
                                } else {
                                    return Err(ApplicationUserError::NotImplemented {
                                        message: "Only VORTEX format is supported".into(),
                                    }
                                    .into());
                                }
                            }
                            "stream_column" => {
                                builder.stream_column(option_value_to_string(key, value)?);
                            }
                            "partition_column" => {
                                builder.partition_column(option_value_to_string(key, value)?);
                            }
                            "s3.endpoint" => {
                                builder.endpoint(option_value_to_string(key, value)?);
                            }
                            "s3.region" => {
                                builder.region(option_value_to_string(key, value)?);
                            }
                            _ => {
                                return Err(validation_error(format!(
                                    "unsupported CREATE TABLE option: {key}"
                                )));
                            }
                        },
                        _ => {
                            return Err(ApplicationUserError::NotImplemented {
                                message: format!("unsupported CREATE TABLE query: {}", sql_option),
                            }
                            .into());
                        }
                    }
                }
            }
            _ => {
                return Err(ApplicationUserError::NotImplemented {
                    message: format!("unsupported CREATE TABLE query: {}", options),
                }
                .into());
            }
        };

        builder.build()
    }
}

fn option_value_to_string(key: &Ident, value: &Expr) -> Result<String, ApplicationError> {
    match value {
        Expr::Identifier(value) => Ok(value.to_string()),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value)
            | Value::DoubleQuotedString(value)
            | Value::EscapedStringLiteral(value)
            | Value::UnicodeStringLiteral(value) => Ok(value.clone()),
            _ => Err(validation_error(format!(
                "CREATE TABLE option must be a string: {key}"
            ))),
        },
        _ => Err(validation_error(format!(
            "CREATE TABLE option must be a string: {key}"
        ))),
    }
}

fn to_external_location(
    location: &Option<String>,
    options: &CreateTableOptions,
) -> Result<ExternalLocation, ApplicationError> {
    let Some(location) = location else {
        return Err(validation_error("location not specified"));
    };

    let (bucket, prefix) = parse_location_string(location)?;

    Ok(ExternalLocation::new(
        bucket,
        prefix,
        options.endpoint.clone(),
        options.region.clone(),
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

fn to_table_column(column: &ColumnDef) -> Result<PublicColumnDefinition, ApplicationError> {
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

fn to_column_data_type(data_type: &SqlDataType) -> Result<ColumnDataType, ApplicationError> {
    Ok(match data_type {
        SqlDataType::Bool | SqlDataType::Boolean => ColumnDataType::Bool,
        SqlDataType::Int64 => ColumnDataType::Int64,
        SqlDataType::Float64 => ColumnDataType::Float64,
        SqlDataType::Text => ColumnDataType::String,
        SqlDataType::Date => ColumnDataType::Date,
        SqlDataType::Timestamp(precision, _) => {
            ColumnDataType::Timestamp(to_time_unit(*precision)?)
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
