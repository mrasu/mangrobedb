use crate::application::datafusion::query::codec::column_definition::{
    from_domain_column_definition, to_domain_table_column_definition,
};
use crate::application::datafusion::query::codec::error::{CodecError, validation_error};
use crate::application::datafusion::query::codec::external_location::{
    from_domain_external_location, to_domain_external_location,
};
use crate::application::datafusion::query::codec::file_format::{
    from_domain_file_format, to_domain_file_format,
};
use crate::application::datafusion::query::codec::object_name::to_single_table_name;
use crate::application::error::ApplicationError;
use crate::domain::file::FileFormat;
use crate::domain::port::catalog::CreateTableRequest;
use crate::domain::table_schema::TableSchema;
use datafusion::logical_expr::sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
use datafusion::logical_expr::sqlparser::ast::{
    CreateTable, Expr, HiveFormat, Ident, ObjectName, SqlOption, Value,
};
use datafusion::sql::sqlparser::ast::CreateTableOptions as DatafusionCreateTableOptions;

const FORMAT_OPTION_KEY: &str = "format";
const STREAM_COLUMN_OPTION_KEY: &str = "stream_column";
const PARTITION_COLUMN_OPTION_KEY: &str = "partition_column";
const S3_ENDPOINT_OPTION_KEY: &str = "s3.endpoint";
const S3_REGION_OPTION_KEY: &str = "s3.region";

pub fn to_domain_create_table_request(
    statement: &CreateTable,
) -> Result<CreateTableRequest, ApplicationError> {
    let table_name = to_single_table_name(&statement.name)?;
    let options = CreateTableOptions::try_from(&statement.table_options)?;
    let hive_formats = &statement.hive_formats.clone().unwrap_or_default();
    let columns = statement
        .columns
        .iter()
        .map(to_domain_table_column_definition)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CreateTableRequest {
        table: TableSchema::try_new(
            table_name,
            to_domain_external_location(&hive_formats.location, &options)?,
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
pub(super) struct CreateTableOptions {
    pub(super) file_format: FileFormat,
    pub(super) stream_column: String,
    pub(super) partition_column: String,
    pub(super) endpoint: Option<String>,
    pub(super) region: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct CreateTableOptionsBuilder {
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

    fn build(self) -> Result<CreateTableOptions, CodecError> {
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
    type Error = CodecError;

    fn try_from(options: &DatafusionCreateTableOptions) -> Result<Self, Self::Error> {
        let mut builder = CreateTableOptionsBuilder::new();
        match options {
            DatafusionCreateTableOptions::None => {}
            DatafusionCreateTableOptions::With(sql_options) => {
                for sql_option in sql_options {
                    match sql_option {
                        SqlOption::KeyValue { key, value } => match key.value.as_str() {
                            FORMAT_OPTION_KEY => {
                                let format =
                                    to_domain_file_format(&option_value_to_string(key, value)?)?;
                                builder.file_format(format);
                            }
                            STREAM_COLUMN_OPTION_KEY => {
                                builder.stream_column(option_value_to_string(key, value)?);
                            }
                            PARTITION_COLUMN_OPTION_KEY => {
                                builder.partition_column(option_value_to_string(key, value)?);
                            }
                            S3_ENDPOINT_OPTION_KEY => {
                                builder.endpoint(option_value_to_string(key, value)?);
                            }
                            S3_REGION_OPTION_KEY => {
                                builder.region(option_value_to_string(key, value)?);
                            }
                            _ => {
                                return Err(validation_error(format!(
                                    "unsupported CREATE TABLE option: {key}"
                                )));
                            }
                        },
                        _ => {
                            return Err(CodecError::UnsupportedCreateTableQuery {
                                message: format!("{sql_option}"),
                            });
                        }
                    }
                }
            }
            _ => {
                return Err(CodecError::UnsupportedCreateTableQuery {
                    message: format!("{options}"),
                });
            }
        };

        builder.build()
    }
}

fn option_value_to_string(key: &Ident, value: &Expr) -> Result<String, CodecError> {
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

pub fn from_domain_create_table(table: &TableSchema) -> String {
    let statement = CreateTableBuilder::new(ObjectName::from(vec![Ident::new(&table.table_name)]))
        .columns(
            table
                .public_columns
                .iter()
                .map(from_domain_column_definition)
                .collect(),
        )
        .hive_formats(Some(HiveFormat {
            row_format: None,
            serde_properties: None,
            storage: None,
            location: Some(from_domain_external_location(&table.location)),
        }))
        .table_options(to_datafusion_create_table_options(table))
        .build();

    statement.to_string()
}

fn to_datafusion_create_table_options(table: &TableSchema) -> DatafusionCreateTableOptions {
    let mut options = vec![
        string_option(FORMAT_OPTION_KEY, from_domain_file_format(&table.format)),
        string_option(STREAM_COLUMN_OPTION_KEY, table.stream_column()),
        string_option(PARTITION_COLUMN_OPTION_KEY, table.partition_column()),
    ];

    if let Some(endpoint) = &table.location.endpoint {
        options.push(string_option(S3_ENDPOINT_OPTION_KEY, endpoint));
    }
    if let Some(region) = &table.location.region {
        options.push(string_option(S3_REGION_OPTION_KEY, region));
    }

    DatafusionCreateTableOptions::With(options)
}

fn string_option(key: &str, value: impl Into<String>) -> SqlOption {
    SqlOption::KeyValue {
        key: if key.contains('.') {
            Ident::with_quote('"', key)
        } else {
            Ident::new(key)
        },
        value: Expr::Value(Value::SingleQuotedString(value.into()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DatafusionStatement;
    use datafusion::sql::sqlparser::ast::Statement;

    #[test]
    fn restores_create_table_sql_that_round_trips_to_same_request() {
        let sql = "
            CREATE TABLE events (
                id INT64 NOT NULL,
                event_time TIMESTAMP(6) NOT NULL COMMENT 'event timestamp',
                payload TEXT
            )
            LOCATION 's3://example-bucket/path/to/table'
            WITH (
                format = 'vortex',
                stream_column = 'id',
                partition_column = 'event_time',
                \"s3.endpoint\" = 'http://localhost:9000',
                \"s3.region\" = 'ap-northeast-1'
            )
        ";
        let request = to_domain_create_table_request(&parse_create_table(sql)).unwrap();

        let restored_sql = from_domain_create_table(&request.table);
        let restored_request =
            to_domain_create_table_request(&parse_create_table(&restored_sql)).unwrap();

        assert_eq!(restored_request.table.table_name, request.table.table_name);
        assert_eq!(restored_request.table.location, request.table.location);
        assert_eq!(restored_request.table.format, request.table.format);
        assert_eq!(
            restored_request.table.stream_column(),
            request.table.stream_column()
        );
        assert_eq!(
            restored_request.table.partition_column(),
            request.table.partition_column()
        );
        assert_eq!(
            restored_request.table.public_columns.len(),
            request.table.public_columns.len()
        );
        for (actual, expected) in restored_request
            .table
            .public_columns
            .iter()
            .zip(request.table.public_columns.iter())
        {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.data_type, expected.data_type);
            assert_eq!(actual.nullable, expected.nullable);
            assert_eq!(actual.comment, expected.comment);
        }
    }

    fn parse_create_table(sql: &str) -> CreateTable {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(sql, &dialect).unwrap();

        match statement {
            DatafusionStatement::Statement(statement) => match *statement {
                Statement::CreateTable(create_table) => create_table,
                other => panic!("expected CREATE TABLE, got {other}"),
            },
            other => panic!("expected sqlparser statement, got {other:?}"),
        }
    }
}
