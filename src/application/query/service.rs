use std::collections::BTreeSet;
use std::sync::Arc;

use crate::application::datafusion::query::create_external_table::parse_external_table_statement;
use crate::application::datafusion::query::object_name::parse_to_single_table_name;
use crate::application::datafusion::sql::execute_statement;
use crate::application::datafusion::table_provider::DummyTableProvider;
use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::query::QueryOutput;
use crate::application::query::external_table_definition::convert_external_table_definition_to_response_batch;
use crate::domain::port::catalog::{CatalogPort, TableSummary};
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{CreateExternalTable, Statement};
use datafusion::sql::sqlparser::ast::{ObjectName, ShowCreateObject};

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
                datafusion::sql::sqlparser::ast::Statement::ShowCreate {
                    obj_type,
                    obj_name,
                } => self.show_create_table(obj_type, obj_name).await,
                _ => Err(ApplicationUserError::NotImplemented {
                    message:
                        "only SELECT queries, CREATE EXTERNAL TABLE, and SHOW CREATE TABLE are supported"
                            .to_string(),
                }
                .into()),
            },
            Statement::CreateExternalTable(statement) => {
                self.create_external_table(statement).await
            }
            _ => Err(ApplicationUserError::NotImplemented {
                message:
                    "only SELECT queries, CREATE EXTERNAL TABLE, and SHOW CREATE TABLE are supported"
                        .to_string(),
            }
            .into()),
        }
    }

    pub async fn list_tables(&self) -> Result<Vec<TableSummary>, ApplicationError> {
        Ok(self.catalog_port.list_tables().await?)
    }

    async fn create_external_table(
        &self,
        statement: CreateExternalTable,
    ) -> Result<QueryOutput, ApplicationError> {
        let request = parse_external_table_statement(statement)?;
        self.catalog_port.create_external_table(request).await?;

        Ok(QueryOutput {
            schema: Arc::new(Schema::empty()),
            batches: Vec::new(),
        })
    }

    async fn show_create_table(
        &self,
        obj_type: &ShowCreateObject,
        obj_name: &ObjectName,
    ) -> Result<QueryOutput, ApplicationError> {
        if obj_type != &ShowCreateObject::Table {
            return Err(ApplicationUserError::NotImplemented {
                message: format!("SHOW CREATE {obj_type} is not supported"),
            }
            .into());
        }

        let table_name = parse_to_single_table_name(obj_name)?;
        let table = self.catalog_port.get_table(&table_name).await?;

        let batch = convert_external_table_definition_to_response_batch(&table)?;
        Ok(QueryOutput {
            schema: batch.schema(),
            batches: vec![batch],
        })
    }

    async fn query_statement(
        &self,
        ctx: SessionContext,
        statement: Statement,
    ) -> Result<QueryOutput, ApplicationError> {
        let table_names = referenced_table_names(&ctx, &statement)?;
        self.register_table_providers(&ctx, &table_names).await?;

        let allowed_tables = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        let df = execute_statement(&ctx, statement, &allowed_tables).await?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;

        Ok(QueryOutput {
            schema: Arc::new(schema),
            batches,
        })
    }

    async fn register_table_providers(
        &self,
        ctx: &SessionContext,
        table_names: &[String],
    ) -> Result<(), ApplicationError> {
        for table_name in table_names {
            let table = Table::load(self.catalog_port.as_ref(), table_name).await?;

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
            ctx.register_table(table_name, table_provider)?;
        }

        Ok(())
    }
}

fn referenced_table_names(
    ctx: &SessionContext,
    statement: &Statement,
) -> Result<Vec<String>, ApplicationError> {
    let references = ctx.state().resolve_table_references(statement)?;
    let table_names = references
        .into_iter()
        .map(|reference| reference.table().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(table_names)
}
