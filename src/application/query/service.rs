use std::sync::Arc;

use crate::application::datafusion::table_provider::DummyTableProvider;
use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::query::QueryOutput;
use crate::domain::port::catalog::CatalogPort;
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use crate::domain::table_schema::DUMMY_TABLE;
use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;

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
        println!("DoGet query sql: {sql}");

        let table = Table::load(self.catalog_port.as_ref(), DUMMY_TABLE)?;

        let table_bucket = &table.schema.bucket;
        if !self.object_store_port.is_accessible(table_bucket) {
            return Err(ApplicationUserError::S3InaccessibleTable {
                table_name: table.schema.table_name,
            }
            .into());
        }

        let ctx = SessionContext::new();
        let store_url = url::Url::parse(&format!("s3://{}", table_bucket))?;
        ctx.register_object_store(&store_url, self.object_store_port.object_store());

        let table_provider = Arc::new(DummyTableProvider::new(
            table,
            Arc::clone(&self.catalog_port),
        ));
        ctx.register_table(DUMMY_TABLE, table_provider)?;

        let batches = ctx.sql(sql).await?.collect().await?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        Ok(QueryOutput { schema, batches })
    }
}
