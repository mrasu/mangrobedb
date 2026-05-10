use std::sync::Arc;

use crate::application::datafusion::sql::execute_statement;
use crate::application::datafusion::table_provider::DummyTableProvider;
use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::query::QueryOutput;
use crate::domain::port::catalog::{
    CatalogPort, ColumnDataType, CreateExternalTableRequest, ExternalLocation,
    ExternalTableDefinition, FileFormat, PartitionField, PartitionTransform, TableColumn, TimeUnit,
};
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use crate::domain::table_schema::DUMMY_TABLE;
use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{CreateExternalTable, Statement};
use datafusion::sql::sqlparser::ast::{
    ColumnDef, ColumnOption, DataType as SqlDataType, ObjectName, Value,
};
use url::Url;

#[derive(Debug)]
pub struct QueryService<C: CatalogPort, O: ObjectStorePort> {
    catalog_port: Arc<C>,
    object_store_port: Arc<O>,
}

impl<C: CatalogPort + 'static, O: ObjectStorePort> QueryService<C, O> {
    pub fn new(catalog_port: Arc<C>, object_store_port: Arc<O>) -> Self {
        Self {
            catalog_port,
            object_store_port,
        }
    }

    pub async fn query(&self, sql: &str) -> Result<QueryOutput, ApplicationError> {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(sql, &dialect)?;

        match statement {
            Statement::Statement(statement) => match statement.as_ref() {
                datafusion::sql::sqlparser::ast::Statement::Query(_) => {
                    self.query_statement(ctx, Statement::Statement(statement))
                        .await
                }
                _ => Err(ApplicationUserError::NotImplemented {
                    message: "only SELECT queries and CREATE EXTERNAL TABLE are supported"
                        .to_string(),
                }
                .into()),
            },
            Statement::CreateExternalTable(statement) => {
                self.create_external_table(statement).await
            }
            _ => Err(ApplicationUserError::NotImplemented {
                message: "only SELECT queries and CREATE EXTERNAL TABLE are supported".to_string(),
            }
            .into()),
        }
    }

    async fn create_external_table(
        &self,
        statement: CreateExternalTable,
    ) -> Result<QueryOutput, ApplicationError> {
        let request = to_create_external_table_request(statement)?;
        self.catalog_port.create_external_table(request).await?;

        Ok(QueryOutput {
            schema: Arc::new(Schema::empty()),
            batches: Vec::new(),
        })
    }

    async fn query_statement(
        &self,
        ctx: SessionContext,
        statement: Statement,
    ) -> Result<QueryOutput, ApplicationError> {
        let table = Table::load(self.catalog_port.as_ref(), DUMMY_TABLE).await?;

        let table_bucket = &table.schema.bucket;
        if !self.object_store_port.is_accessible(table_bucket) {
            return Err(ApplicationUserError::S3InaccessibleTable {
                table_name: table.schema.table_name,
            }
            .into());
        }

        let store_url = url::Url::parse(&format!("s3://{}", table_bucket))?;
        ctx.register_object_store(&store_url, self.object_store_port.object_store());

        let table_provider = Arc::new(DummyTableProvider::try_new(
            table,
            Arc::clone(&self.catalog_port),
        )?);
        ctx.register_table(DUMMY_TABLE, table_provider)?;

        let batches = execute_statement(&ctx, statement, &[DUMMY_TABLE])
            .await?
            .collect()
            .await?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        Ok(QueryOutput { schema, batches })
    }
}

fn to_create_external_table_request(
    statement: CreateExternalTable,
) -> Result<CreateExternalTableRequest, ApplicationError> {
    reject_unsupported_create_external_table_features(&statement)?;

    let table_name = single_table_name(&statement.name)?;
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
        table: ExternalTableDefinition {
            table_name,
            location: to_external_location(&statement.location, options)?,
            format: FileFormat::Vortex,
            columns,
            partition_fields,
            comment: None,
        },
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
                "s3.endpoint" => result.endpoint = Some(value_to_string(key, value)?),
                "s3.region" => result.region = Some(value_to_string(key, value)?),
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

fn value_to_string(key: &str, value: &Value) -> Result<String, ApplicationError> {
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

fn single_table_name(name: &ObjectName) -> Result<String, ApplicationError> {
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

fn to_external_location(
    location: &str,
    options: CreateExternalTableOptions,
) -> Result<ExternalLocation, ApplicationError> {
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

    Ok(ExternalLocation {
        bucket: bucket.to_string(),
        prefix: url.path().trim_start_matches('/').to_string(),
        endpoint: options.endpoint,
        region: options.region,
    })
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

    Ok(TableColumn {
        name: column.name.value.clone(),
        data_type: to_column_data_type(&column.data_type)?,
        nullable,
        comment,
    })
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

    Ok(PartitionField {
        source_column: column_name.to_string(),
        destination_column: None,
        transform: PartitionTransform::Identity,
        result_type: column.data_type.clone(),
    })
}

fn validation_error(message: impl Into<String>) -> ApplicationError {
    ApplicationUserError::ValidationError {
        message: message.into(),
    }
    .into()
}
