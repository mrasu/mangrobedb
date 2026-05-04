use std::sync::Arc;

use crate::application::error::{ApplicationError, ApplicationUserError};
use crate::application::query::QueryOutput;
use crate::domain::port::catalog::CatalogPort;
use crate::domain::port::object_store::ObjectStorePort;
use crate::domain::table::Table;
use crate::domain::table_schema::DUMMY_TABLE;
use arrow::datatypes::Schema;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

const DEFAULT_STREAM_ID: i32 = 0;

#[derive(Debug)]
pub struct QueryService<C: CatalogPort, O: ObjectStorePort> {
    catalog_port: Arc<C>,
    object_store_port: Arc<O>,
}

impl<C: CatalogPort, O: ObjectStorePort> QueryService<C, O> {
    pub fn new(catalog_port: Arc<C>, object_store_port: Arc<O>) -> Self {
        Self {
            catalog_port,
            object_store_port,
        }
    }

    pub async fn query(&self, sql: &str) -> Result<QueryOutput, ApplicationError> {
        println!("DoGet query sql: {sql}");

        let files = self
            .catalog_port
            .get_current_state(DUMMY_TABLE, DEFAULT_STREAM_ID, &[])?;
        let first = files
            .first()
            .ok_or_else(|| anyhow::anyhow!("no registered files for {DUMMY_TABLE}"))?;
        let table = Table::load(self.catalog_port.as_ref(), DUMMY_TABLE)?;

        let table_bucket = &table.schema.bucket;
        if !self.object_store_port.is_accessible(table_bucket) {
            return Err(ApplicationUserError::S3InaccessibleTable {
                table_name: table.schema.table_name,
            }
            .into());
        }

        let filepath = table.build_path(first);
        let ctx = SessionContext::new();
        let store_url = url::Url::parse(&format!("s3://{}", table_bucket))?;
        ctx.register_object_store(&store_url, self.object_store_port.object_store());

        let format = Arc::new(VortexFormat::new(VortexSession::default()));
        let table_url = ListingTableUrl::parse(&filepath)?;
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                ListingOptions::new(format).with_session_config_options(ctx.state().config()),
            )
            .infer_schema(&ctx.state())
            .await?;

        let listing_table = Arc::new(ListingTable::try_new(config)?);
        ctx.register_table(DUMMY_TABLE, listing_table as _)?;

        let batches = ctx.sql(sql).await?.collect().await?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        Ok(QueryOutput { schema, batches })
    }
}
